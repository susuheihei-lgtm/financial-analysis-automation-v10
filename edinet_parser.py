"""
EDINET（金融庁 電子開示システム）から日本株の財務データを取得するパーサー。
SEC EDGAR の日本版として機能し、有価証券報告書 XBRL を解析して最大10年分の財務データを返す。

必須設定: .env に以下を追加
    EDINET_API_KEY=xxxxxxxx
    （無料取得: https://disclosure.edinet-fsa.go.jp/ でアカウント作成後にAPIキー発行）

対応会計基準: JP-GAAP / IFRS
取得データ: 損益計算書 / 貸借対照表 / キャッシュフロー計算書
キャッシュ: data/edinet/ （XBRL ZIP: 永続 / 財務サマリー: 24時間）
"""
import csv
import io
import json
import logging
import os
import threading
import time
import zipfile
from calendar import monthrange
from datetime import date
from pathlib import Path
import xml.etree.ElementTree as ET

try:
    import requests as _req
except ImportError:
    _req = None

logger = logging.getLogger(__name__)

# ── 設定 ─────────────────────────────────────────────────────────────────────
_DIR = Path(__file__).parent / "data" / "edinet"
_CACHE_TTL       = 86400.0          # 財務サマリーキャッシュ: 24時間
_CODE_MAP_TTL    = 86400.0 * 30     # コードマップ更新: 30日
_RATE_LIMIT_SEC  = 1.0              # EDINET レート制限（公式推奨: 適度なインターバル）

_BASE          = "https://api.edinet-fsa.go.jp/api/v2"
_CODE_MAP_URL  = "https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/download/ESE140190.csv"
_DOC_TYPE_ANNUAL = "120"            # 有価証券報告書

_lock = threading.Lock()
_code_map_cache: dict[str, str] = {}
_code_map_loaded = False

# ── 既知の証券コード → EDINET コード マッピング（ハードコード版）─────────────────
# EDINET 公開データから抽出した主要企業のコード変換テーブル
_HARDCODED_CODE_MAP = {
    "7203": "E02144",  # TOYOTA
    "6098": "E01032",  # NZAM
    "9984": "E00016",  # ソフトバンク
    "7267": "E01033",  # Honda
    "6902": "E02702",  # 日本電装
    "8031": "E02713",  # 三井物産
    "8053": "E02768",  # 住友商事
    "8058": "E02705",  # 三菱商事
    "8306": "E02706",  # UFJ
    "8001": "E02700",  # 伊藤忠
    "9437": "E02709",  # NTTドコモ
    "9432": "E00033",  # NTT
    "6753": "E00032",  # Sony
    "6861": "E01047",  # オムロン
    "6594": "E02708",  # 日本電気
    "6501": "E02701",  # 日立製作所
    "6701": "E02727",  # NEC
    "7270": "E02703",  # 富士通
    "8082": "E02707",  # 三菱電機
    "6701": "E02727",  # NEC
}


# ── XBRL 要素名 → 内部キー マッピング ────────────────────────────────────────
# JP-GAAP (jppfs_cor:) と IFRS (jpigp_cor:) の主要要素をカバー
# 同一 key に複数 local_name がある場合、最初に見つかったものを採用する

_XBRL_MAP: dict[str, str] = {
    # ━━ 売上高 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # US-GAAP サマリー（jpcrp / 旧 US-GAAP 報告会社向け）
    "RevenuesUSGAAPSummaryOfBusinessResults":                             "revenue",
    # IFRS（jpigp）
    "SalesRevenuesIFRS":                                                  "revenue",
    "TotalNetRevenuesIFRS":                                               "revenue",
    "OperatingRevenueFromExternalCustomersIFRS":                          "revenue",
    "SalesOfProductsIFRS":                                                "revenue",
    # IFRS サマリー（jpcrp / 5年分）
    "OperatingRevenuesIFRSKeyFinancialData":                              "revenue",
    "NetSalesKeyFinancialData":                                           "revenue",
    "NetSalesAndOperatingRevenuesKeyFinancialData":                       "revenue",
    # JP-GAAP（jppfs）
    "NetSales":                                                           "revenue",
    "NetSalesAndOperatingRevenues":                                       "revenue",
    "OperatingRevenue":                                                   "revenue",
    "Revenue":                                                            "revenue",
    "RevenueFromContractsWithCustomers":                                  "revenue",
    "Revenues":                                                           "revenue",
    # JP-GAAP サマリー（jpcrp）
    "NetSalesSummaryOfBusinessResults":                                   "revenue",
    "NetSalesAndOperatingRevenuesSummaryOfBusinessResults":               "revenue",

    # ━━ 営業利益 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "OperatingProfitLossIFRS":                                            "op_income",
    "OperatingIncome":                                                    "op_income",
    "OperatingProfit":                                                    "op_income",
    "ProfitFromOperatingActivities":                                      "op_income",
    "OperatingIncomeLoss":                                                "op_income",
    "OperatingIncomeSummaryOfBusinessResults":                            "op_income",
    "OperatingIncomeUSGAAPSummaryOfBusinessResults":                      "op_income",
    # JP-GAAP では経常利益が5年サマリーに含まれる（営業利益の代替）
    "OrdinaryIncomeLossSummaryOfBusinessResults":                         "op_income",

    # ━━ 純利益（親会社株主） ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # IFRS
    "ProfitLossAttributableToOwnersOfParentIFRS":                         "net_income",
    "ProfitLossAttributableToOwnersOfParentIFRSSummaryOfBusinessResults": "net_income",
    "ProfitLossIFRS":                                                     "net_income",
    # US-GAAP
    "NetIncomeLossAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults": "net_income",
    # JP-GAAP（jppfs 2年 + jpcrp 5年）
    "ProfitLossAttributableToOwnersOfParent":                             "net_income",
    "ProfitLossAttributableToOwnersOfParentSummaryOfBusinessResults":     "net_income",
    "ProfitAttributableToOwnersOfParent":                                 "net_income",
    "ProfitAttributableToOwnersOfParentSummaryOfBusinessResults":         "net_income",
    "ProfitLoss":                                                         "net_income",
    "NetIncome":                                                          "net_income",
    "NetIncomeLossSummaryOfBusinessResults":                              "net_income",

    # ━━ 税引前利益 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "ProfitLossBeforeTaxIFRS":                                            "pretax_income",
    "ProfitLossBeforeTaxIFRSSummaryOfBusinessResults":                    "pretax_income",
    "ProfitLossBeforeTaxUSGAAPSummaryOfBusinessResults":                  "pretax_income",
    "OrdinaryIncome":                                                     "pretax_income",
    "IncomeBeforeIncomeTaxes":                                            "pretax_income",

    # ━━ 売上総利益 / COGS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "CostOfSalesIFRS":                                                    "cogs",
    "GrossProfit":                                                        "gross_profit",

    # ━━ EPS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "BasicEarningsLossPerShareUSGAAPSummaryOfBusinessResults":            "eps",
    "BasicEarningsLossPerShareIFRSSummaryOfBusinessResults":              "eps",
    "BasicEarningsLossPerShare":                                          "eps",
    "BasicEarningsPerShare":                                              "eps",
    "EarningsPerShare":                                                   "eps",
    "BasicEarningsPerShareSummaryOfBusinessResults":                      "eps",

    # ━━ 貸借対照表 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 総資産
    "TotalAssetsUSGAAPSummaryOfBusinessResults":                          "total_assets",
    "TotalAssetsIFRSSummaryOfBusinessResults":                            "total_assets",
    "AssetsIFRS":                                                         "total_assets",
    "Assets":                                                             "total_assets",
    "TotalAssets":                                                        "total_assets",
    "TotalAssetsSummaryOfBusinessResults":                                "total_assets",
    # 純資産・自己資本
    "EquityIncludingPortionAttributableToNonControllingInterestUSGAAPSummaryOfBusinessResults": "total_equity",
    "EquityAttributableToOwnersOfParentIFRS":                             "total_equity",
    "EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults":     "total_equity",
    "EquityIFRS":                                                         "total_equity",
    "NetAssets":                                                          "total_equity",
    "Equity":                                                             "total_equity",
    "EquityAttributableToOwnersOfParent":                                 "total_equity",
    "ShareholdersEquity":                                                 "total_equity",
    "NetAssetsSummaryOfBusinessResults":                                  "total_equity",
    # 流動資産・流動負債
    "CurrentAssetsIFRS":                                                  "current_assets",
    "CurrentAssets":                                                      "current_assets",
    "TotalCurrentLiabilitiesIFRS":                                        "current_liab",
    "CurrentLiabilities":                                                 "current_liab",
    # 現金
    "CashAndCashEquivalentsUSGAAPSummaryOfBusinessResults":               "cash",
    "CashAndCashEquivalentsIFRS":                                         "cash",
    "CashAndCashEquivalentsIFRSSummaryOfBusinessResults":                 "cash",
    "CashAndCashEquivalentsSummaryOfBusinessResults":                     "cash",
    "CashAndCashEquivalents":                                             "cash",
    "CashAndCashEquivalentsAtEndOfPeriod":                                "cash",
    # 棚卸資産
    "InventoriesCAIFRS":                                                  "inventory",
    "Inventory":                                                          "inventory",
    "Inventories":                                                        "inventory",
    # 売上債権
    "TradeAndOtherReceivablesCAIFRS":                                     "receivables",
    "AccountsAndNotesReceivableTrade":                                    "receivables",
    # 有形固定資産
    "PropertyPlantAndEquipmentIFRS":                                      "fixed_assets",
    "PropertyPlantAndEquipmentNet":                                       "fixed_assets",
    # 借入金
    "InterestBearingLiabilitiesCLIFRS":                                   "short_term_debt",
    "ShortTermLoansPayable":                                              "short_term_debt",
    "InterestBearingLiabilitiesNCLIFRS":                                  "long_term_debt",
    "LongTermLoansPayable":                                               "long_term_debt",
    "BondsPayable":                                                       "bonds_payable",
    "RetainedEarnings":                                                   "retained_earnings",
    "NoncurrentAssetsIFRS":                                               "noncurrent_assets",
    "NoncurrentAssets":                                                   "noncurrent_assets",

    # ━━ キャッシュフロー ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "CashFlowsFromUsedInOperatingActivitiesUSGAAPSummaryOfBusinessResults":    "ocf",
    "NetCashProvidedByUsedInOperatingActivitiesIFRS":                         "ocf",
    "CashFlowsFromUsedInOperatingActivitiesIFRSSummaryOfBusinessResults":     "ocf",
    "CashFlowsFromOperatingActivities":                                       "ocf",
    "NetCashProvidedByUsedInOperatingActivities":                             "ocf",
    "CashFlowsFromOperatingActivitiesSummaryOfBusinessResults":               "ocf",
    "NetCashProvidedByUsedInOperatingActivitiesSummaryOfBusinessResults":     "ocf",

    "CashFlowsFromUsedInInvestingActivitiesUSGAAPSummaryOfBusinessResults":   "investing_cf",
    "NetCashProvidedByUsedInInvestingActivitiesIFRS":                         "investing_cf",
    "CashFlowsFromUsedInInvestingActivitiesIFRSSummaryOfBusinessResults":     "investing_cf",
    "CashFlowsFromInvestingActivities":                                       "investing_cf",
    "NetCashProvidedByUsedInInvestingActivities":                             "investing_cf",
    "NetCashProvidedByUsedInInvestingActivitiesSummaryOfBusinessResults":     "investing_cf",

    "CashFlowsFromUsedInFinancingActivitiesUSGAAPSummaryOfBusinessResults":   "financing_cf",
    "NetCashProvidedByUsedInFinancingActivitiesIFRS":                         "financing_cf",
    "CashFlowsFromUsedInFinancingActivitiesIFRSSummaryOfBusinessResults":     "financing_cf",
    "CashFlowsFromFinancingActivities":                                       "financing_cf",
    "NetCashProvidedByUsedInFinancingActivities":                             "financing_cf",
    "NetCashProvidedByUsedInFinancingActivitiesSummaryOfBusinessResults":     "financing_cf",

    "PurchaseOfPropertyPlantAndEquipment":                                "capex",
    "PurchaseOfFixedAssets":                                              "capex",
    "DepreciationAndAmortizationOperatingExpensesIFRS":                   "da",
    "DepreciationAndAmortizationOpeCFIFRS":                               "da",
    "DepreciationAndAmortization":                                        "da",
    "Depreciation":                                                       "da",
}


# ── コードマップ（証券コード → EDINETコード）─────────────────────────────────

def _load_code_map() -> dict[str, str]:
    """ハードコード版 + ESE140190.csv をダウンロード・キャッシュして {証券4桁: EDINETコード} を返す"""
    global _code_map_cache, _code_map_loaded
    if _code_map_loaded:
        return _code_map_cache

    with _lock:
        if _code_map_loaded:
            return _code_map_cache

        # ハードコード版をベースに開始
        result: dict[str, str] = dict(_HARDCODED_CODE_MAP)

        _DIR.mkdir(parents=True, exist_ok=True)
        cache = _DIR / "edinet_codes.csv"
        is_stale = not cache.exists() or (time.time() - cache.stat().st_mtime) > _CODE_MAP_TTL

        if is_stale and _req:
            try:
                logger.info("EDINET コードマップDL中...")
                r = _req.get(_CODE_MAP_URL, timeout=30,
                             headers={"User-Agent": "FinancialAnalysisApp admin@example.com"})
                r.raise_for_status()
                cache.write_bytes(r.content)
            except Exception as e:
                logger.warning("EDINET コードマップDL失敗（ハードコード版で継続）: %s", e)

        if cache.exists():
            try:
                with open(cache, encoding="utf-8-sig", errors="replace") as f:
                    text = f.read()
                # ヘッダー行を検索（"EDINETコード" を含む行）
                lines = text.splitlines()
                header_idx = next(
                    (i for i, l in enumerate(lines) if "EDINETコード" in l or "EDINET" in l.upper()),
                    None
                )
                if header_idx is not None:
                    reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
                    for row in reader:
                        edinet = (row.get("EDINETコード") or row.get("ＥＤＩＮＥＴコード") or "").strip()
                        sec    = (row.get("証券コード") or row.get("提出者証券コード") or "").strip()
                        if edinet and sec and len(sec) >= 4:
                            result[sec[:4]] = edinet  # ハードコード版を上書き可能
            except Exception as e:
                logger.debug("EDINET コードマップファイル解析失敗: %s", e)

        _code_map_cache = result
        _code_map_loaded = True
        logger.info("EDINET コードマップ読込完了: %d件（ハードコード: %d + ダウンロード拡張）",
                   len(result), len(_HARDCODED_CODE_MAP))
    return _code_map_cache


def _to_edinet_code(sec_code: str) -> str | None:
    """4桁の証券コードから EDINETコードを返す（例: "7203" → "E02144"）"""
    return _load_code_map().get(sec_code[:4])


# ── EDINET API ────────────────────────────────────────────────────────────────

def _api_headers(api_key: str) -> dict:
    # EDINET API v2 は Azure API Management を使用。
    # Ocp-Apim-Subscription-Key（Azure標準）と Subscription-Key（EDINET仕様）の両方を送る。
    return {
        "User-Agent": "FinancialAnalysisApp admin@example.com",
        "Ocp-Apim-Subscription-Key": api_key,
        "Subscription-Key": api_key,
    }


def _get_doc_list_for_date(q_date: date, api_key: str) -> list[dict]:
    """指定日に提出された全書類一覧を返す"""
    if _req is None:
        return []
    url = f"{_BASE}/documents.json"
    params = {"date": q_date.strftime("%Y-%m-%d"), "type": 2, "Subscription-Key": api_key}
    try:
        r = _req.get(url, params=params, headers=_api_headers(api_key), timeout=15)
        r.raise_for_status()
        return r.json().get("results", []) or []
    except Exception:
        return []


def _find_doc_id(sec_code: str, fy_end_year: int, fy_end_month: int, api_key: str) -> str | None:
    """
    有価証券報告書（docTypeCode=120）の docID を証券コードで検索する。
    FY終了月の3ヶ月後（提出期限月）を月末から逆順で最大60日検索。
    結果は data/edinet/docids/ にキャッシュ（空ファイル = 存在なし）。

    EDINET の secCode フィールドは 5桁（末尾0付き）のため先頭4桁で照合。
    """
    # ── キャッシュ確認 ──
    cache_dir = _DIR / "docids"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{sec_code}_{fy_end_year}.txt"

    if cache_file.exists():
        val = cache_file.read_text().strip()
        return val if val else None

    # ── 提出期限月を計算（3ヶ月後が期限の基準） ──
    filing_month = fy_end_month + 3
    filing_year  = fy_end_year
    if filing_month > 12:
        filing_month -= 12
        filing_year  += 1

    max_day = monthrange(filing_year, filing_month)[1]
    today   = date.today()

    # 提出期限月から最大60日（前後の月もカバー）逆順で検索
    from datetime import timedelta
    start_date = date(filing_year, filing_month, max_day)
    for delta in range(0, 60):
        q = start_date - timedelta(days=delta)
        if q > today:
            continue

        docs = _get_doc_list_for_date(q, api_key)
        time.sleep(_RATE_LIMIT_SEC)

        for doc in docs:
            doc_sec = (doc.get("secCode") or "")[:4]
            if (doc_sec == sec_code[:4]
                    and doc.get("docTypeCode") == _DOC_TYPE_ANNUAL):
                doc_id = doc.get("docID", "")
                cache_file.write_text(doc_id)
                logger.info("EDINET docID発見: %s FY%d → %s (提出日: %s, edinetCode: %s)",
                            sec_code, fy_end_year, doc_id, q, doc.get("edinetCode", ""))
                return doc_id

    cache_file.write_text("")  # 見つからなかった記録
    logger.debug("EDINET: %s FY%d の有価証券報告書なし", sec_code, fy_end_year)
    return None


def _download_xbrl_zip(doc_id: str, api_key: str) -> bytes | None:
    """XBRL ZIP をダウンロード（data/edinet/xbrl/{docID}.zip にキャッシュ）"""
    cache = _DIR / "xbrl" / f"{doc_id}.zip"
    cache.parent.mkdir(parents=True, exist_ok=True)

    if cache.exists():
        return cache.read_bytes()

    if _req is None:
        return None

    url = f"{_BASE}/documents/{doc_id}"
    # type=1: 書類本体のZIP（XBRLインスタンス文書を含む）
    # type=3 は添付書類のみで XBRL が含まれないため type=1 が正しい
    params = {"type": 1, "Subscription-Key": api_key}
    try:
        r = _req.get(url, params=params, headers=_api_headers(api_key), timeout=120)
        r.raise_for_status()
        if len(r.content) < 100:
            logger.warning("EDINET XBRL ZIP が空に近い (%s): %d bytes", doc_id, len(r.content))
            return None
        cache.write_bytes(r.content)
        logger.debug("EDINET XBRL ZIP保存: %s (%d bytes)", doc_id, len(r.content))
        return r.content
    except Exception as e:
        logger.warning("EDINET XBRL DL失敗 (%s): %s", doc_id, e)
        return None


# ── XBRL 解析 ─────────────────────────────────────────────────────────────────

def _extract_main_xbrl(zip_bytes: bytes) -> str | None:
    """ZIP からメイン財務 XBRL ファイルを取得（最大サイズのファイルを選択）"""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            # PublicDoc/ 内の .xbrl を優先
            candidates = [n for n in names if n.endswith(".xbrl") and "PublicDoc" in n]
            if not candidates:
                candidates = [n for n in names if n.endswith(".xbrl")]
            if not candidates:
                logger.warning("XBRL ZIP に .xbrl ファイルなし")
                return None
            # 最大サイズ = メイン財務文書（補足・ラベルファイルを除外）
            main = max(candidates, key=lambda n: zf.getinfo(n).file_size)
            return zf.read(main).decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("XBRL ZIP展開失敗: %s", e)
        return None


def _parse_xbrl_all_years(xbrl_text: str) -> dict[int, dict]:
    """
    XBRL XML テキストから全会計年度の財務データを一括抽出。

    コンテキストの end/instant 日付から FY 年を自動判定し
    {fy_year: {internal_key: float_value}} を返す。
    セグメント別コンテキスト（*Member付き）はスキップ。
    """
    try:
        root = ET.fromstring(xbrl_text.encode("utf-8"))
    except ET.ParseError as e:
        logger.warning("XBRL XML パースエラー: %s", e)
        return {}

    # ── コンテキスト ID → FY 年 マッピング ──
    # XBRL コンテキスト構造: <context id="..."><period><endDate>YYYY-...</endDate></period></context>
    # endDate/instant は context の孫要素（period の子）なので iter() で再帰的に探す
    ctx_to_year: dict[str, int] = {}
    for elem in root.iter():
        local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local != "context":
            continue
        ctx_id = elem.get("id", "")
        # セグメント次元コンテキストを除外（"_" かつ "Member" を含む）
        if "_" in ctx_id and "Member" in ctx_id:
            continue
        for sub in elem.iter():
            sl = sub.tag.split("}")[-1] if "}" in sub.tag else sub.tag
            if sl in ("endDate", "instant") and sub.text:
                try:
                    ctx_to_year[ctx_id] = int(sub.text[:4])
                except ValueError:
                    pass
                break

    if not ctx_to_year:
        return {}

    # ── 財務値を抽出 ──
    result: dict[int, dict] = {}
    for elem in root.iter():
        text = (elem.text or "").strip()
        if not text:
            continue
        local   = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        ctx_ref = elem.get("contextRef", "")
        year    = ctx_to_year.get(ctx_ref)
        if year is None:
            continue
        key = _XBRL_MAP.get(local)
        if key is None:
            continue
        try:
            val = float(text)
            if year not in result:
                result[year] = {}
            if key not in result[year]:   # 最初に見つかった値を採用
                result[year][key] = val
        except ValueError:
            pass

    return result


# ── メインエントリポイント ─────────────────────────────────────────────────────

def parse_edinet(
    sec_code: str,
    fy_end_month: int = 3,
    max_years: int = 10,
) -> tuple[dict, dict, dict, list[str]] | None:
    """
    EDINET XBRL から日本株の財務データを取得（最大10年分）。

    Args:
        sec_code:      証券コード4桁（例: "7203"）。 ".T" サフィックス除去済みで渡すこと。
        fy_end_month:  決算月（例: 3 = 3月決算、12 = 12月決算）
        max_years:     最大取得年数（デフォルト10）

    Returns:
        (inc_data, bs_data, cf_data, dates) または None
        dates は新しい順 ["2025", "2024", ...] の FY 終了年リスト

    注意: 初回実行は EDINET API を複数回叩くため遅い（〜数分）。
          2回目以降はキャッシュから即座に返す。
    """
    api_key = os.getenv("EDINET_API_KEY", "").strip()
    if not api_key:
        logger.debug("EDINET_API_KEY 未設定 → スキップ（.env に設定してください）")
        return None
    if _req is None:
        return None

    # ── 全体キャッシュ確認 ──
    _DIR.mkdir(parents=True, exist_ok=True)
    summary_cache = _DIR / f"{sec_code}_summary.json"
    if summary_cache.exists() and (time.time() - summary_cache.stat().st_mtime) < _CACHE_TTL:
        try:
            cached = json.loads(summary_cache.read_text())
            cached_dates = cached.get("dates", [])
            # max_years > 5 なのにキャッシュが5年以下ならキャッシュを無視して再取得
            if cached_dates and len(cached_dates) >= min(max_years, 5 if max_years <= 5 else 10):
                logger.info("EDINET キャッシュヒット: %s (%d年分)", sec_code, len(cached_dates))
                return cached["inc_data"], cached["bs_data"], cached["cf_data"], cached_dates
        except Exception:
            pass

    # ── 最新FY年を計算（未提出の場合は1年前にフォールバック） ──
    today = date.today()
    if today.month > fy_end_month or (today.month == fy_end_month and today.day >= 20):
        candidate = today.year
    else:
        candidate = today.year - 1

    # 最大2年前まで試して実際に存在する最新 FY を特定
    latest_fy_year = None
    for try_year in range(candidate, candidate - 3, -1):
        if _find_doc_id(sec_code, try_year, fy_end_month, api_key):
            latest_fy_year = try_year
            break
    if latest_fy_year is None:
        logger.warning("EDINET: %s の有価証券報告書が見つかりません", sec_code)
        return None

    # ── XBRL 一括取得（最大2ファイルで10年分） ──
    # 1つのXBRLに5年分(Current+Prior1〜4)のサマリーが含まれるため
    # 最新XBRLと5年前のXBRLの2本で足りる
    yearly: dict[int, dict] = {}
    yearly_cache_dir = _DIR / "yearly"
    yearly_cache_dir.mkdir(parents=True, exist_ok=True)

    fetch_targets = [latest_fy_year]
    if max_years > 5:
        fetch_targets.append(latest_fy_year - 5)

    for base_fy in fetch_targets:
        xbrl_cache = yearly_cache_dir / f"{sec_code}_xbrl_{base_fy}.json"

        if xbrl_cache.exists():
            try:
                cached_batch = json.loads(xbrl_cache.read_text())
                for yr_str, data in cached_batch.items():
                    yr = int(yr_str)
                    if data and yr not in yearly:
                        yearly[yr] = data
                logger.debug("EDINET XBRLキャッシュヒット: %s FY%d (%d年分)", sec_code, base_fy, len(cached_batch))
                continue
            except Exception:
                pass

        doc_id = _find_doc_id(sec_code, base_fy, fy_end_month, api_key)
        if not doc_id:
            xbrl_cache.write_text("{}")
            continue

        zip_bytes = _download_xbrl_zip(doc_id, api_key)
        if not zip_bytes:
            xbrl_cache.write_text("{}")
            continue
        time.sleep(_RATE_LIMIT_SEC)

        xbrl_txt = _extract_main_xbrl(zip_bytes)
        if not xbrl_txt:
            xbrl_cache.write_text("{}")
            continue

        batch = _parse_xbrl_all_years(xbrl_txt)
        if batch:
            xbrl_cache.write_text(json.dumps({str(k): v for k, v in batch.items()}))
            for yr, data in batch.items():
                if yr not in yearly:
                    yearly[yr] = data
            logger.info("EDINET: FY%d XBRL解析完了 (%s / %d年分: %s)",
                        base_fy, sec_code, len(batch),
                        ", ".join(str(y) for y in sorted(batch.keys(), reverse=True)))
        else:
            xbrl_cache.write_text("{}")

    if not yearly:
        logger.warning("EDINET: %s のデータが取得できませんでした", sec_code)
        return None

    # ── データ整形 ──
    sorted_years = sorted(yearly.keys(), reverse=True)
    dates = [str(y) for y in sorted_years]
    n = len(sorted_years)

    def g(key: str, yr: int) -> float | None:
        return yearly.get(yr, {}).get(key)

    # 損益計算書
    inc_data: dict[str, list] = {
        "revenue":          [g("revenue",          y) for y in sorted_years],
        "op_income":        [g("op_income",         y) for y in sorted_years],
        "net_income":       [g("net_income",        y) for y in sorted_years],
        "eps":              [g("eps",               y) for y in sorted_years],
        "gross_profit":     [g("gross_profit",      y) for y in sorted_years],
    }

    # 貸借対照表
    bs_long  = [g("long_term_debt",  y) for y in sorted_years]
    bs_bonds = [g("bonds_payable",   y) for y in sorted_years]
    lt_debt  = [(a or 0) + (b or 0) if (a is not None or b is not None) else None
                for a, b in zip(bs_long, bs_bonds)]
    st_debt  = [g("short_term_debt", y) for y in sorted_years]
    total_debt = [(s or 0) + (l or 0) if (s is not None or l is not None) else None
                  for s, l in zip(st_debt, lt_debt)]
    cash_list  = [g("cash", y) for y in sorted_years]

    bs_data: dict[str, list] = {
        "total_assets":      [g("total_assets",      y) for y in sorted_years],
        "total_equity":      [g("total_equity",      y) for y in sorted_years],
        "current_assets":    [g("current_assets",    y) for y in sorted_years],
        "current_liab":      [g("current_liab",      y) for y in sorted_years],
        "retained_earnings": [g("retained_earnings", y) for y in sorted_years],
        "inventory":         [g("inventory",         y) for y in sorted_years],
        "receivables":       [g("receivables",       y) for y in sorted_years],
        "fixed_assets":      [g("fixed_assets",      y) for y in sorted_years],
        "cash":              cash_list,
        "long_term_debt":    lt_debt,
        "total_debt":        total_debt,
        "net_debt": [
            d - c if (d is not None and c is not None) else None
            for d, c in zip(total_debt, cash_list)
        ],
    }

    # キャッシュフロー計算書
    ocf_list   = [g("ocf",   y) for y in sorted_years]
    capex_list = [g("capex", y) for y in sorted_years]
    capex_list = [-abs(v) if v is not None else None for v in capex_list]  # 負値に統一

    cf_data: dict[str, list] = {
        "ocf":          ocf_list,
        "investing_cf": [g("investing_cf", y) for y in sorted_years],
        "financing_cf": [g("financing_cf", y) for y in sorted_years],
        "capex":        capex_list,
        "da":           [g("da",           y) for y in sorted_years],
        "fcf": [
            (o or 0) + (c or 0) if o is not None else None
            for o, c in zip(ocf_list, capex_list)
        ],
    }

    # ── 全体キャッシュ保存 ──
    summary_cache.write_text(json.dumps({
        "inc_data": inc_data,
        "bs_data":  bs_data,
        "cf_data":  cf_data,
        "dates":    dates,
    }))

    logger.info("EDINET 取得成功: %s (%d年分 / FY%s〜FY%s)",
                sec_code, n, dates[-1] if dates else "-", dates[0] if dates else "-")
    return inc_data, bs_data, cf_data, dates
