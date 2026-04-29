"""
IR BANK (irbank.net) から日本株の財務データを取得するパーサー。

個別銘柄URL: https://f.irbank.net/files/{code}/fy-data-all.csv
フォーマット: 複数セクション（業績/財務/CF/配当）が1ファイルに結合されたCSV
提供年数: 最大約5年（IR BANK CSVの仕様上限）

キャッシュ先: data/irbank/{code}/fy-data-all.csv (24時間)
"""
import io
import logging
import os
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    import requests as _requests
except ImportError:
    _requests = None

# ── 設定 ─────────────────────────────────────────────────────────────────────
_IRBANK_DIR = Path(__file__).parent / "data" / "irbank"
_CACHE_TTL: float = 86400.0  # 24時間

# 個別銘柄の全データ入りCSV
_FILE_ALL = "fy-data-all.csv"
_BASE_URL = "https://f.irbank.net/files/{code}/" + _FILE_ALL

_dl_lock = threading.Lock()

# ── ダウンロード & キャッシュ ─────────────────────────────────────────────────

def _download_company_csv(code: str) -> bytes | None:
    """IR BANK から銘柄個別の全データCSVを取得（ローカルキャッシュ有効活用）"""
    if _requests is None:
        logger.warning("IR BANK: requests がインストールされていないためスキップ")
        return None

    cache_path = _IRBANK_DIR / code / _FILE_ALL
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    # キャッシュが有効なら使う
    if cache_path.exists() and (time.time() - cache_path.stat().st_mtime) < _CACHE_TTL:
        return cache_path.read_bytes()

    with _dl_lock:
        # ロック後に再確認
        if cache_path.exists() and (time.time() - cache_path.stat().st_mtime) < _CACHE_TTL:
            return cache_path.read_bytes()

        url = _BASE_URL.format(code=code)
        logger.info("IR BANK ダウンロード中: %s", url)
        try:
            resp = _requests.get(
                url,
                timeout=30,
                allow_redirects=True,
                headers={"User-Agent": "FinancialAnalysisApp admin@example.com"},
            )
            resp.raise_for_status()
            # レート制限ページのチェック（302 or HTML errorページ）
            if b'\xe8\xa1\xa8\xe7\xa4\xba\xe5\x88\xb6\xe9\x99\x90' in resp.content:
                logger.warning("IR BANK: レート制限中（高負荷）- %s", code)
                return None
            cache_path.write_bytes(resp.content)
            logger.info("IR BANK キャッシュ保存: %s (%d bytes)", cache_path, len(resp.content))
            return resp.content
        except Exception as e:
            logger.warning("IR BANK ダウンロード失敗 (%s): %s", code, e)
            return None


# ── CSVパーサー ───────────────────────────────────────────────────────────────

def _parse_multisection_csv(raw: bytes) -> dict[str, list[dict]]:
    """
    IR BANK の複数セクションCSVを解析して {section_name: [row_dict, ...]} を返す。

    フォーマット例:
        "7203 トヨタ自動車"
        （空行）
        業績
        年度,売上高,営業利益,...
        2025/03,48036704000000,...
        ...
        （空行）
        財務
        年度,総資産,...
        ...
    """
    text = raw.decode("utf-8-sig", errors="replace")
    sections: dict[str, list[dict]] = {}
    current_section: str | None = None
    current_headers: list[str] | None = None
    rows: list[dict] = []

    _SECTION_NAMES = {"業績", "財務", "CF", "配当"}

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # セクションヘッダー行
        if line in _SECTION_NAMES:
            if current_section and rows:
                sections[current_section] = rows
            current_section = line
            current_headers = None
            rows = []
            continue

        # 会社名行をスキップ（ダブルクォートで始まる行）
        if line.startswith('"'):
            continue

        parts = line.split(",")

        # ヘッダー行（最初の列が "年度" のとき）
        if parts[0] == "年度":
            current_headers = parts
            continue

        # データ行
        if current_headers and current_section:
            # 予想行をスキップ（末尾に（予想）が付く）
            if "（予想）" in line:
                continue
            row = {}
            for i, col in enumerate(current_headers):
                val = parts[i] if i < len(parts) else "-"
                row[col] = val.strip()
            rows.append(row)

    # 最後のセクションを保存
    if current_section and rows:
        sections[current_section] = rows

    return sections


def _safe_float(s: str | None) -> float | None:
    """文字列を float に変換（'-' や空文字は None）"""
    if not s or s.strip() in ("-", "", "None", "null", "N/A", "－", "ー"):
        return None
    try:
        return float(s.replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def _fy_to_year(fy_str: str) -> str:
    """IR BANK の年度文字列 '2025/03' → '2025'"""
    return fy_str[:4]


# ── メインエントリポイント ─────────────────────────────────────────────────────

def parse_irbank(code: str, max_years: int = 10) -> tuple[dict, dict, dict, list[str]] | None:
    """
    IR BANK CSVから証券コードに対応する財務データを取得。

    Args:
        code: 証券コード（4桁、例: "7203"）。.Tサフィックスは除いて渡すこと。
        max_years: 最大取得年数（IR BANK は通常5年分程度を提供）

    Returns:
        (inc_data, bs_data, cf_data, dates) または None（データなし）
        dates は新しい順 ["2025", "2024", ...] の西暦年リスト
    """
    raw = _download_company_csv(code)
    if raw is None:
        return None

    sections = _parse_multisection_csv(raw)
    if not sections:
        logger.info("IR BANK: コード %s のセクションデータなし", code)
        return None

    pl_rows = sorted(sections.get("業績", []), key=lambda r: r.get("年度", ""), reverse=True)
    bs_rows = sorted(sections.get("財務", []), key=lambda r: r.get("年度", ""), reverse=True)
    cf_rows = sorted(sections.get("CF", []), key=lambda r: r.get("年度", ""), reverse=True)

    if not pl_rows:
        logger.info("IR BANK: コード %s の業績データなし", code)
        return None

    # max_years で絞る
    pl_rows = pl_rows[:max_years]
    dates = [_fy_to_year(r["年度"]) for r in pl_rows]
    n = len(dates)

    # BS / CF を年度キー辞書に変換
    bs_by_year = {_fy_to_year(r["年度"]): r for r in bs_rows}
    cf_by_year = {_fy_to_year(r["年度"]): r for r in cf_rows}

    def bs(col: str, yr: str) -> float | None:
        return _safe_float(bs_by_year.get(yr, {}).get(col))

    def cf(col: str, yr: str) -> float | None:
        return _safe_float(cf_by_year.get(yr, {}).get(col))

    # ── 損益計算書 ────────────────────────────────────────────────────────────
    inc_data: dict[str, list] = {
        "revenue":    [_safe_float(r.get("売上高"))   for r in pl_rows],
        "op_income":  [_safe_float(r.get("営業利益")) for r in pl_rows],
        "net_income": [_safe_float(r.get("純利益"))   for r in pl_rows],
        "eps":        [_safe_float(r.get("EPS"))       for r in pl_rows],
    }

    # ── 貸借対照表 ────────────────────────────────────────────────────────────
    bs_short = [bs("短期借入金", d) for d in dates]
    bs_long  = [bs("長期借入金",  d) for d in dates]
    total_debt = [
        (s or 0) + (l or 0) if (s is not None or l is not None) else None
        for s, l in zip(bs_short, bs_long)
    ]
    cash_list = [cf("現金同等物", d) for d in dates]

    bs_data: dict[str, list] = {
        "total_assets":      [bs("総資産",     d) for d in dates],
        "total_equity":      [bs("株主資本",   d) for d in dates],
        "retained_earnings": [bs("利益剰余金", d) for d in dates],
        "long_term_debt":    bs_long,
        "total_debt":        total_debt,
        "cash":              cash_list,
        "net_debt": [
            (d - c) if (d is not None and c is not None) else None
            for d, c in zip(total_debt, cash_list)
        ],
    }

    # ── キャッシュフロー計算書 ───────────────────────────────────────────────
    ocf_list   = [cf("営業CF",   d) for d in dates]
    capex_list = [cf("設備投資", d) for d in dates]  # IR BANK の設備投資は通常負値

    # CapEx を負値に統一（IR BANK は既に負値のはずだが念のため）
    capex_list = [-abs(v) if v is not None else None for v in capex_list]

    # FCF = OCF + capex (capex は負値)
    fcf_list = [
        (o or 0) + (c or 0) if o is not None else None
        for o, c in zip(ocf_list, capex_list)
    ]

    cf_data: dict[str, list] = {
        "ocf":          ocf_list,
        "investing_cf": [cf("投資CF", d) for d in dates],
        "financing_cf": [cf("財務CF", d) for d in dates],
        "capex":        capex_list,
        "fcf":          fcf_list,
    }

    logger.info(
        "IR BANK 取得成功: %s (%d年分 / %s〜%s)",
        code, n,
        dates[-1] if dates else "-",
        dates[0]  if dates else "-",
    )
    return inc_data, bs_data, cf_data, dates
