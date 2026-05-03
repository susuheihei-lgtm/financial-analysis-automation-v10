"""
IR BANK (irbank.net) から日本株の財務データを取得するパーサー。

年次データ URL: https://f.irbank.net/files/{code}/fy-data-all.csv
四半期データ URL: https://irbank.net/{code}/quarter (HTMLスクレイピング)

キャッシュ先: data/irbank/{code}/fy-data-all.csv, q-data.json (24時間)
"""
import calendar
import json
import logging
import re
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    import requests as _requests
except ImportError:
    _requests = None

try:
    from bs4 import BeautifulSoup as _BeautifulSoup
except ImportError:
    _BeautifulSoup = None

# ── 設定 ─────────────────────────────────────────────────────────────────────
_IRBANK_DIR = Path(__file__).parent / "data" / "irbank"
_CACHE_TTL: float = 86400.0  # 24時間

# 個別銘柄の全データ入りCSV（年次）
_FILE_ALL = "fy-data-all.csv"
_BASE_URL = "https://f.irbank.net/files/{code}/" + _FILE_ALL

_dl_lock = threading.Lock()

# ── ダウンロード & キャッシュ ─────────────────────────────────────────────────

def _download_csv(code: str, filename: str, base_url: str) -> bytes | None:
    """IR BANK から指定ファイルを取得（ローカルキャッシュ有効活用）"""
    if _requests is None:
        return None
    cache_path = _IRBANK_DIR / code / filename
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.exists() and (time.time() - cache_path.stat().st_mtime) < _CACHE_TTL:
        return cache_path.read_bytes()
    with _dl_lock:
        if cache_path.exists() and (time.time() - cache_path.stat().st_mtime) < _CACHE_TTL:
            return cache_path.read_bytes()
        url = base_url.format(code=code)
        logger.info("IR BANK ダウンロード中: %s", url)
        try:
            resp = _requests.get(
                url, timeout=30, allow_redirects=True,
                headers={"User-Agent": "FinancialAnalysisApp admin@example.com"},
            )
            resp.raise_for_status()
            if b'\xe8\xa1\xa8\xe7\xa4\xba\xe5\x88\xb6\xe9\x99\x90' in resp.content:
                logger.warning("IR BANK: レート制限中 - %s", code)
                return None
            cache_path.write_bytes(resp.content)
            logger.info("IR BANK キャッシュ保存: %s (%d bytes)", cache_path, len(resp.content))
            return resp.content
        except Exception as e:
            logger.warning("IR BANK ダウンロード失敗 (%s / %s): %s", code, filename, e)
            return None


def _fetch_html(url: str, timeout: int = 15) -> str | None:
    """URL から HTML を取得（requests or urllib フォールバック）"""
    headers = {"User-Agent": "FinancialAnalysisApp admin@example.com"}
    try:
        if _requests is not None:
            resp = _requests.get(url, timeout=timeout, allow_redirects=True, headers=headers)
            resp.raise_for_status()
            return resp.text
        else:
            import urllib.request
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("IR BANK HTML 取得失敗 (%s): %s", url, e)
        return None


def _download_company_csv(code: str) -> bytes | None:
    """IR BANK から銘柄個別の年次全データCSVを取得"""
    if _requests is None:
        logger.warning("IR BANK: requests がインストールされていないためスキップ")
        return None
    return _download_csv(code, _FILE_ALL, _BASE_URL)


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


# ── 四半期データ（HTMLスクレイピング） ────────────────────────────────────────

def _quarter_end_date(fy_year: int, fy_month: int, q: int) -> str:
    """FY終了年月とQ番号 → 四半期末日付 (YYYY-MM-DD)"""
    end_m = fy_month - (4 - q) * 3
    end_y = fy_year
    while end_m <= 0:
        end_m += 12
        end_y -= 1
    last_day = calendar.monthrange(end_y, end_m)[1]
    return f"{end_y:04d}-{end_m:02d}-{last_day:02d}"


def _parse_fy_label(text: str) -> tuple[int | None, int | None]:
    """'2026年3月期連結' → (2026, 3)"""
    m = re.search(r"(\d{4})年(\d{1,2})月期", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _parse_shihanki_value(td, unit_mult: float) -> float | None:
    """TD要素の shihanki span から単季値を取得"""
    if _BeautifulSoup is None:
        return None
    shihanki = td.find("span", class_="shihanki")
    if not shihanki:
        return None
    text = shihanki.get_text(strip=True).replace("+", "").replace(",", "").strip()
    if not text or text == "-":
        return None
    try:
        return float(text) * unit_mult
    except ValueError:
        return None


def _scrape_quarterly_html(code: str, max_q: int = 8) -> dict | None:
    """
    irbank.net/{code}/quarter の「四半期毎履歴」テーブルをスクレイピングして
    実績四半期データを返す。BeautifulSoup が必要。
    """
    if _BeautifulSoup is None:
        logger.warning("IR BANK 四半期HTML: BeautifulSoup4 が未インストール（pip install beautifulsoup4）")
        return None

    html = _fetch_html(f"https://irbank.net/{code}/quarter")
    if not html:
        return None

    soup = _BeautifulSoup(html, "html.parser")

    # 「四半期毎履歴」キャプション付きテーブルを検索
    hist_table = None
    unit_mult: float = 1_000_000  # デフォルト: 百万円
    for t in soup.find_all("table"):
        cap = t.find("caption")
        if cap and "四半期毎履歴" in cap.get_text():
            hist_table = t
            cap_text = cap.get_text()
            if "億円" in cap_text:
                unit_mult = 100_000_000.0
            elif "百万円" in cap_text:
                unit_mult = 1_000_000.0
            elif "千円" in cap_text:
                unit_mult = 1_000.0
            break

    if hist_table is None:
        logger.info("IR BANK 四半期HTML: %s — 四半期毎履歴テーブル未検出", code)
        return None

    tbody = hist_table.find("tbody")
    if not tbody:
        return None

    records: list[dict] = []
    current_fy_year: int | None = None
    current_fy_month: int | None = None
    td_buffer: list = []

    def _process_row(q_td, data_tds: list, fy_year: int, fy_month: int) -> None:
        # co_red = 実績のみ取得（co_gr=予想 / co_br=修正 はスキップ）
        if not q_td.find("span", class_="co_red"):
            return
        q_text = q_td.get_text(strip=True)
        q_num = re.search(r"(\d+)Q", q_text)
        if not q_num:
            return
        q = int(q_num.group(1))
        date_str = _quarter_end_date(fy_year, fy_month, q)
        vals = [_parse_shihanki_value(td, unit_mult) for td in data_tds[:4]]
        records.append({
            "date":             date_str,
            "revenue":          vals[0] if len(vals) > 0 else None,
            "op_income":        vals[1] if len(vals) > 1 else None,
            "ordinary_income":  vals[2] if len(vals) > 2 else None,
            "net_income":       vals[3] if len(vals) > 3 else None,
        })

    for child in tbody.children:
        if not hasattr(child, "name") or not child.name:
            continue

        if child.name == "tr":
            td_buffer = []  # 新FY開始でバッファリセット
            tds = child.find_all("td")
            if not tds:
                continue
            if tds[0].get("rowspan"):
                fy_text = tds[0].get_text(strip=True)
                current_fy_year, current_fy_month = _parse_fy_label(fy_text)
                if len(tds) >= 7 and current_fy_year and current_fy_month:
                    _process_row(tds[1], tds[2:7], current_fy_year, current_fy_month)

        elif child.name == "td" and current_fy_year and current_fy_month:
            td_buffer.append(child)
            if len(td_buffer) == 6:
                _process_row(td_buffer[0], td_buffer[1:], current_fy_year, current_fy_month)
                td_buffer = []

    if not records:
        logger.info("IR BANK 四半期HTML: %s — 実績データなし", code)
        return None

    # 新しい順にソートしてmax_q件に絞る
    records.sort(key=lambda r: r["date"], reverse=True)
    records = records[:max_q]

    dates = [r["date"] for r in records]

    def _build_list(key: str) -> list | None:
        vals = [r[key] for r in records]
        return vals if any(v is not None for v in vals) else None

    income: dict[str, list] = {}
    for src_key, dst_key in [
        ("revenue",         "Total Revenue"),
        ("op_income",       "Operating Income"),
        ("net_income",      "Net Income"),
        ("ordinary_income", "Pretax Income"),
    ]:
        lst = _build_list(src_key)
        if lst is not None:
            income[dst_key] = lst

    logger.info(
        "IR BANK 四半期HTML取得成功: %s (%dQ / %s〜%s)",
        code, len(dates),
        dates[-1] if dates else "-",
        dates[0]  if dates else "-",
    )
    return {"dates": dates, "income": income, "balance": {}, "cashflow": {}}


def parse_irbank_quarterly(code: str, max_q: int = 8) -> dict | None:
    """
    IR BANK から四半期財務データを取得（HTMLスクレイピング）。
    キャッシュ: data/irbank/{code}/q-data.json (24時間)

    Returns:
        {
          'dates':    ['2025-12-31', '2025-09-30', ...],  # 新しい順
          'income':   {'Total Revenue': [...], 'Operating Income': [...], ...},
          'balance':  {},
          'cashflow': {},
        }
        または None（データなし）
    """
    cache_path = _IRBANK_DIR / code / "q-data.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    # キャッシュヒット
    if cache_path.exists() and (time.time() - cache_path.stat().st_mtime) < _CACHE_TTL:
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if cached.get("dates"):
                logger.info("IR BANK 四半期キャッシュ使用: %s", code)
                return cached
        except Exception:
            pass

    result = _scrape_quarterly_html(code, max_q=max_q)
    if result:
        try:
            cache_path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            logger.warning("IR BANK 四半期キャッシュ保存失敗: %s", e)
    return result
