"""
yfinanceからティッカーシンボルで財務データを取得し、
parse_excel()と同一形式の (data, ts_data) を返すパーサー

米国株: SEC EDGAR XBRL API（公式）→ yfinance（市場データ補完）→ 10年分
日本株: IR BANK CSV（自動DL、10年分）→ yfinance（市場データ補完）
"""
import json
import logging
import math
import threading
import time
import yfinance as yf

try:
    import requests
except ImportError:
    requests = None

try:
    from irbank_parser import parse_irbank as _parse_irbank
    from irbank_parser import parse_irbank_quarterly as _parse_irbank_quarterly
except ImportError:
    _parse_irbank = None
    _parse_irbank_quarterly = None

logger = logging.getLogger(__name__)

# ── モジュールレベルキャッシュ（^TNX リスクフリーレート）─────────────────────
# CPython の GIL で単純な代入はスレッドセーフだが、check-then-act を保護するため Lock を使用
_tnx_cache: dict = {"rate": None, "ts": 0.0}
_tnx_lock = threading.Lock()
_TNX_CACHE_TTL: float = 86400.0  # 24時間

# ── SEC EDGAR キャッシュ（ticker→CIK、companyfacts）──────────────────────────
_SEC_HEADERS = {
    'User-Agent': 'FinancialAnalysisApp admin@example.com',
    'Accept': 'application/json',
}
_sec_ticker_cik: dict[str, int] = {}
_sec_ticker_lock = threading.Lock()
_sec_ticker_loaded = False
_sec_facts_cache: dict[str, tuple[float, dict]] = {}
_sec_facts_lock = threading.Lock()
_SEC_FACTS_TTL: float = 3600.0  # 1時間

# SEC XBRL コンセプト → 内部キー マッピング
_SEC_INCOME_TAGS = {
    'revenue': ['RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues', 'SalesRevenueNet',
                'SalesRevenueGoodsNet', 'SalesRevenueServicesNet', 'RevenueNet',
                'SalesRevenue', 'RevenueFromContractWithCustomerIncludingAssessedTax'],
    'cogs': ['CostOfGoodsAndServicesSold', 'CostOfRevenue', 'CostOfGoodsSold'],
    'gross_profit': ['GrossProfit'],
    'op_income': ['OperatingIncomeLoss'],
    'net_income': ['NetIncomeLoss'],
    'sga': ['SellingGeneralAndAdministrativeExpense'],
    'interest_exp': ['InterestExpense', 'InterestExpenseDebt'],
    'pretax_income': ['IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest'],
    'income_tax': ['IncomeTaxExpenseBenefit'],
    'other_exp': ['OtherNonoperatingIncomeExpense'],
}
_SEC_BALANCE_TAGS = {
    'total_assets': ['Assets'],
    'total_equity': ['StockholdersEquity'],
    'current_assets': ['AssetsCurrent'],
    'current_liab': ['LiabilitiesCurrent'],
    'cash': ['CashAndCashEquivalentsAtCarryingValue'],
    'cash_and_st': ['CashCashEquivalentsAndShortTermInvestments'],
    'receivables': ['AccountsReceivableNetCurrent', 'AccountsReceivableNet'],
    'inventory': ['InventoryNet'],
    'payables': ['AccountsPayableCurrent'],
    'fixed_assets': ['PropertyPlantAndEquipmentNet'],
    'intangibles': ['Goodwill'],
    'intangibles_other': ['IntangibleAssetsNetExcludingGoodwill'],
    'long_term_debt': ['LongTermDebtNoncurrent', 'LongTermDebt'],
    'retained_earnings': ['RetainedEarningsAccumulatedDeficit'],
}
_SEC_CASHFLOW_TAGS = {
    'ocf': [
        'NetCashProvidedByUsedInOperatingActivities',
        'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
    ],
    'capex': [
        'PaymentsToAcquirePropertyPlantAndEquipment',
        'PaymentsToAcquirePropertyPlantAndEquipmentContinuingOperations',
        'PaymentsToAcquireProductiveAssets',
    ],
    'investing_cf': [
        'NetCashProvidedByUsedInInvestingActivities',
        'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
    ],
    'financing_cf': [
        'NetCashProvidedByUsedInFinancingActivities',
        'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
    ],
    'da': ['DepreciationDepletionAndAmortization', 'DepreciationAndAmortization'],
}
_SEC_EPS_TAGS = {
    'eps': ['EarningsPerShareBasic'],
    'eps_diluted': ['EarningsPerShareDiluted'],
}

# SEC quarterly: 内部キー → yfinance field名（q_income/q_balance/q_cashflow へのマージ用）
_SEC_Q_INCOME_MAP = {
    'revenue':       'Total Revenue',
    'gross_profit':  'Gross Profit',
    'op_income':     'Operating Income',
    'net_income':    'Net Income',
    'cogs':          'Cost Of Revenue',
    'sga':           'Selling General Administrative',
    'pretax_income': 'Pretax Income',
    'income_tax':    'Tax Provision',
    'eps_diluted':   'Diluted EPS',
}
_SEC_Q_BALANCE_MAP = {
    'total_assets':   'Total Assets',
    'total_equity':   'Stockholders Equity',
    'receivables':    'Accounts Receivable',
    'inventory':      'Inventory',
    'fixed_assets':   'Net PPE',
    'payables':       'Accounts Payable',
    'current_assets': 'Current Assets',
    'current_liab':   'Current Liabilities',
    'long_term_debt': 'Long Term Debt',
}
_SEC_Q_CF_MAP = {
    'ocf':          'Operating Cash Flow',
    'capex':        'Capital Expenditure',
    'investing_cf': 'Investing Cash Flow',
    'financing_cf': 'Financing Cash Flow',
}


# yfinance行名 → 内部キーのマッピング
_INCOME_MAP = {
    'Total Revenue': 'revenue',
    'Operating Revenue': 'revenue',
    'Cost Of Revenue': 'cogs',
    'Reconciled Cost Of Revenue': 'cogs',
    'Operating Income': 'op_income',
    'Net Income': 'net_income',
    'Net Income Common Stockholders': 'net_income',
    'Basic EPS': 'eps',
    'Diluted EPS': 'eps_diluted',
    'Selling General And Administration': 'sga',
    'Interest Expense Non Operating': 'interest_exp',
    'Interest Expense': 'interest_exp',
    'Net Interest Income': 'net_interest',
    'Other Income Expense': 'other_exp',
    'Other Non Operating Income Expenses': 'other_exp',
    'Pretax Income': 'pretax_income',
    'Tax Provision': 'income_tax',
    'EBITDA': 'ebitda',
    'Gross Profit': 'gross_profit',
    'Reconciled Depreciation': 'da',
}

_CASHFLOW_MAP = {
    'Free Cash Flow': 'fcf',
    'Operating Cash Flow': 'ocf',
    'Cash Flow From Continuing Operating Activities': 'ocf',
    'Capital Expenditure': 'capex',
    'Investing Cash Flow': 'investing_cf',
    'Cash Flow From Continuing Investing Activities': 'investing_cf',
    'Financing Cash Flow': 'financing_cf',
    'Cash Flow From Continuing Financing Activities': 'financing_cf',
    'Depreciation And Amortization': 'da',
    'Depreciation Amortization Depletion': 'da',
}

_BALANCE_MAP = {
    'Total Assets': 'total_assets',
    'Stockholders Equity': 'total_equity',
    'Common Stock Equity': 'total_equity',
    'Total Debt': 'total_debt',
    'Accounts Receivable': 'receivables',
    'Receivables': 'receivables',
    'Inventory': 'inventory',
    'Accounts Payable': 'payables',
    'Payables': 'payables',
    'Current Assets': 'current_assets',
    'Current Liabilities': 'current_liab',
    'Cash And Cash Equivalents': 'cash',
    'Cash Cash Equivalents And Short Term Investments': 'cash_and_st',
    'Net PPE': 'fixed_assets',
    'Goodwill And Other Intangible Assets': 'intangibles',
    'Other Intangible Assets': 'intangibles_other',
    'Net Debt': 'net_debt',
    'Total Non Current Assets': 'long_term_assets',
    'Long Term Debt': 'long_term_debt',
    'Retained Earnings': 'retained_earnings',
    'Invested Capital': 'invested_capital',
    'Working Capital': 'working_capital',
    'Tangible Book Value': 'tangible_book',
}


def _safe(v):
    """NaN/Inf → None 変換"""
    if v is None:
        return None
    try:
        if math.isnan(v) or math.isinf(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# SEC EDGAR XBRL API ヘルパー関数
# ═══════════════════════════════════════════════════════════════════════════════

def _load_sec_ticker_map():
    """SEC公式のticker→CIKマッピングをロード（スレッドセーフ）"""
    global _sec_ticker_cik, _sec_ticker_loaded
    with _sec_ticker_lock:
        if _sec_ticker_loaded or not requests:
            return
        try:
            resp = requests.get(
                'https://www.sec.gov/files/company_tickers.json',
                headers=_SEC_HEADERS, timeout=15
            )
            resp.raise_for_status()
            for entry in resp.json().values():
                t = entry.get('ticker', '').upper()
                cik = entry.get('cik_str')
                if t and cik:
                    _sec_ticker_cik[t] = int(cik)
            _sec_ticker_loaded = True
            logger.info("SEC ticker→CIK マッピング読込: %d件", len(_sec_ticker_cik))
        except Exception as e:
            logger.warning("SEC ticker→CIK 取得失敗: %s", e)
            _sec_ticker_loaded = True

def _ticker_to_cik(symbol: str) -> str | None:
    """ティッカー → ゼロパディングCIK (e.g. 'CIK0000320193')"""
    _load_sec_ticker_map()
    key = symbol.upper().split('.')[0]
    cik = _sec_ticker_cik.get(key)
    if cik is None:
        key_alt = symbol.upper().replace('.', '-')
        cik = _sec_ticker_cik.get(key_alt)
    return f"CIK{cik:010d}" if cik else None

def _fetch_sec_facts(cik_padded: str) -> dict | None:
    """companyfacts JSONを取得（キャッシュ付き）"""
    if not requests:
        return None
    now = time.time()
    with _sec_facts_lock:
        cached = _sec_facts_cache.get(cik_padded)
        if cached and (now - cached[0]) < _SEC_FACTS_TTL:
            return cached[1]

    url = f"https://data.sec.gov/api/xbrl/companyfacts/{cik_padded}.json"
    try:
        resp = requests.get(url, headers=_SEC_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        with _sec_facts_lock:
            _sec_facts_cache[cik_padded] = (now, data)
        return data
    except Exception as e:
        logger.warning("SEC companyfacts 取得失敗 (%s): %s", cik_padded, e)
        return None

def _get_sec_annual_series(
    us_gaap: dict, concept_names: list[str], unit_key: str = 'USD', max_years: int = 11
) -> list[tuple[int, float]]:
    """10-K の FY エントリを [(fiscal_year, value), ...] で新しい順に返す。
    複数タグをマージして年別に補完する（優先度: concept_names の先頭タグが高）。
    """
    merged: dict[int, dict] = {}
    for concept in concept_names:
        if concept not in us_gaap:
            continue
        entries = us_gaap[concept].get('units', {}).get(unit_key, [])
        if not entries:
            continue

        # 10-K のみ → fy でグループ化
        # 優先順位: ①filed日が新しい ②同一filed内はend年がfyと一致 ③それでも同点は値が大きい
        # 背景: 10-Kには当期＋比較期間データが全て同一fyタグで入ることがあり、
        #       また年間合計と四半期値が同一end日に重複するケースもある（例: LLY）。
        by_fy: dict[int, dict] = {}
        for e in entries:
            if e.get('form') != '10-K':
                continue
            fy = e.get('fy')
            if fy is None:
                continue
            prev = by_fy.get(fy)
            if prev is None:
                by_fy[fy] = e
                continue
            e_filed = e.get('filed', '')
            p_filed = prev.get('filed', '')
            if e_filed > p_filed:
                by_fy[fy] = e
            elif e_filed == p_filed:
                e_end_yr = int(e.get('end', '0000')[:4]) if e.get('end') else 0
                p_end_yr = int(prev.get('end', '0000')[:4]) if prev.get('end') else 0
                e_match = (e_end_yr == fy)
                p_match = (p_end_yr == fy)
                if e_match and not p_match:
                    by_fy[fy] = e  # 当期エントリを優先
                elif e_match == p_match:
                    # 同点 → 値が大きい方（年間合計 > 四半期値）
                    if (e.get('val') or 0) > (prev.get('val') or 0):
                        by_fy[fy] = e

        # 優先タグにないFYのみ補完（先頭タグ優先）
        for fy, e in by_fy.items():
            if fy not in merged:
                merged[fy] = e

    if not merged:
        return []
    result = sorted(merged.items(), key=lambda x: x[0], reverse=True)[:max_years]
    return [(fy, e['val']) for fy, e in result]

def _sec_quarterly_series(
    us_gaap: dict, concept_names: list[str],
    unit_key: str = 'USD', is_instant: bool = False, max_q: int = 5
) -> list[tuple[str, float]]:
    """
    SEC 10-Q ファイリングから単独四半期値を抽出。
    - is_instant=False (P/L・CF): YTD累積を単独四半期に変換
    - is_instant=True  (B/S)    : 期末残高をそのまま使用
    Returns: [(end_date_str, value), ...] 新しい順
    """
    from datetime import date as _date
    by_key: dict = {}
    for concept in concept_names:
        if concept not in us_gaap:
            continue
        for e in us_gaap[concept].get('units', {}).get(unit_key, []):
            if e.get('form') != '10-Q' or e.get('val') is None or not e.get('end'):
                continue
            fy, fp, end = e.get('fy'), e.get('fp', ''), e['end']
            if is_instant:
                key = end
            else:
                if not fy or fp not in ('Q1', 'Q2', 'Q3'):
                    continue
                key = (fy, fp)
            prev = by_key.get(key)
            if prev is None or (e.get('filed') or '') > (prev.get('filed') or ''):
                by_key[key] = e
    if not by_key:
        return []

    if is_instant:
        result = [(e['end'], float(e['val'])) for e in by_key.values()]
        result.sort(key=lambda x: x[0], reverse=True)
        return result[:max_q]

    # Duration: YTD累積 → 単独四半期
    by_fy: dict = {}
    for (fy, fp), e in by_key.items():
        by_fy.setdefault(fy, {})[fp] = e

    standalone: dict[str, float] = {}
    for fy, fps in by_fy.items():
        q1 = fps.get('Q1')
        q2 = fps.get('Q2')
        q3 = fps.get('Q3')
        if q1:
            standalone[q1['end']] = float(q1['val'])
        if q2:
            days = None
            if q2.get('start'):
                try:
                    days = (_date.fromisoformat(q2['end']) - _date.fromisoformat(q2['start'])).days
                except Exception:
                    pass
            if days is not None and days < 110:
                standalone[q2['end']] = float(q2['val'])
            elif q1:
                standalone[q2['end']] = float(q2['val']) - float(q1['val'])
        if q3:
            days = None
            if q3.get('start'):
                try:
                    days = (_date.fromisoformat(q3['end']) - _date.fromisoformat(q3['start'])).days
                except Exception:
                    pass
            if days is not None and days < 110:
                standalone[q3['end']] = float(q3['val'])
            elif q2:
                standalone[q3['end']] = float(q3['val']) - float(q2['val'])

    result = sorted(standalone.items(), key=lambda x: x[0], reverse=True)
    return result[:max_q]


def parse_edgar_us(ticker_symbol: str) -> tuple[dict, dict, dict, list[str]] | None:
    """
    SEC EDGAR XBRL API から米国株の財務データを取得。
    _extract_series() 互換の形式で返す。

    Returns:
        (inc_data, bs_data, cf_data, dates) or None
    """
    if not requests:
        return None

    cik = _ticker_to_cik(ticker_symbol)
    if cik is None:
        return None

    facts = _fetch_sec_facts(cik)
    if facts is None:
        return None

    us_gaap = facts.get('facts', {}).get('us-gaap', {})
    if not us_gaap:
        logger.warning("SEC EDGAR: us-gaap data not found for %s", ticker_symbol)
        return None

    # 対象 fiscal years を確定
    fy_set: set[int] = set()
    for tag in ('RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues', 'Assets', 'NetIncomeLoss'):
        if tag in us_gaap:
            for e in us_gaap[tag].get('units', {}).get('USD', []):
                if e.get('form') == '10-K' and e.get('fp') == 'FY' and e.get('fy'):
                    fy_set.add(e['fy'])

    if not fy_set:
        return None

    sorted_fy = sorted(fy_set, reverse=True)[:10]
    dates = [str(y) for y in sorted_fy]
    n = len(dates)

    def _align(tags_map: dict[str, list[str]], unit: str = 'USD') -> dict[str, list]:
        """コンセプトマップ → aligned dict"""
        out: dict[str, list] = {}
        for key, concepts in tags_map.items():
            if not concepts:
                continue
            series = _get_sec_annual_series(us_gaap, concepts, unit)
            if not series:
                continue
            lookup = {fy: val for fy, val in series}
            aligned = [lookup.get(y) for y in sorted_fy]
            if any(v is not None for v in aligned):
                out[key] = aligned
        return out

    # 各財務諸表を抽出
    inc_data = _align(_SEC_INCOME_TAGS)
    inc_data.update(_align(_SEC_EPS_TAGS, unit='USD/shares'))
    bs_data = _align(_SEC_BALANCE_TAGS)
    # 株数は 'shares' 単位で別途取得（USD unitと混在させない）
    bs_data.update(_align({'shares_outstanding': ['CommonStockSharesOutstanding', 'CommonStockSharesIssued']}, unit='shares'))
    cf_data = _align(_SEC_CASHFLOW_TAGS)

    # CapEx の符号を yfinance 互換に（負値）
    if 'capex' in cf_data:
        cf_data['capex'] = [-abs(v) if v is not None else None for v in cf_data['capex']]

    # EBITDA = Operating Income + |D&A|
    da_src = cf_data.get('da') or inc_data.get('da')
    if 'op_income' in inc_data and da_src:
        ebitda = []
        for i in range(n):
            oi = inc_data['op_income'][i] if i < len(inc_data['op_income']) else None
            da = da_src[i] if i < len(da_src) else None
            if oi is not None and da is not None:
                ebitda.append(oi + abs(da))
            elif oi is not None:
                ebitda.append(oi)
            else:
                ebitda.append(None)
        inc_data['ebitda'] = ebitda

    # FCF = OCF - |CapEx|
    if 'ocf' in cf_data and 'capex' in cf_data:
        fcf = []
        for i in range(n):
            o = cf_data['ocf'][i] if i < len(cf_data['ocf']) else None
            c = cf_data['capex'][i] if i < len(cf_data['capex']) else None
            if o is not None and c is not None:
                fcf.append(o + c)  # capex は負値
            elif o is not None:
                fcf.append(o)
            else:
                fcf.append(None)
        cf_data['fcf'] = fcf

    # Total Debt （long-term debt が主体）
    if 'long_term_debt' in bs_data:
        bs_data['total_debt'] = bs_data['long_term_debt']

    # Net Debt = Total Debt - Cash
    if 'total_debt' in bs_data and 'cash' in bs_data:
        net_debt = []
        for i in range(n):
            d = bs_data['total_debt'][i] if i < len(bs_data['total_debt']) else None
            c = bs_data['cash'][i] if i < len(bs_data['cash']) else None
            if d is not None and c is not None:
                net_debt.append(d - c)
            else:
                net_debt.append(None)
        bs_data['net_debt'] = net_debt

    logger.info("SEC EDGAR 取得成功: %s (%d年分)", ticker_symbol, n)
    return inc_data, bs_data, cf_data, dates


def _extract_series(df, mapping):
    """DataFrameから指定マッピングに従い {key: [新しい→古い順のリスト]} を返す。
    yfinanceのDataFrameは列が日付（新しい順）、行が項目名。"""
    result = {}
    if df is None or df.empty:
        return result, []

    # 日付（列名）を文字列に変換
    dates = [str(c)[:4] for c in df.columns]

    for row_name, key in mapping.items():
        if row_name in df.index:
            vals = [_safe(df.loc[row_name].iloc[i]) for i in range(len(df.columns))]
            # 重複キーは最初に見つかった（より優先度の高い）ものを保持
            if key not in result:
                result[key] = vals
            else:
                # 既存データが全Noneなら上書き
                if all(v is None for v in result[key]):
                    result[key] = vals

    return result, dates


def _get_year_end_price(hist_df, year_str, is_japan=False):
    """株価履歴から当該年度末の終値を返す。"""
    if hist_df is None or hist_df.empty:
        return None
    try:
        year = int(year_str)
        # 日本株: 3月決算が主流 → 3月末
        # 米国株: 12月決算が主流 → 12月末
        target_month = 3 if is_japan else 12
        target_year = year

        mask = (hist_df.index.year == target_year) & (hist_df.index.month == target_month)
        prices = hist_df.loc[mask, 'Close']
        if len(prices) > 0:
            return float(prices.iloc[-1])

        # 対象月のデータがなければ前後1ヶ月を探す
        for delta in [1, -1, 2, -2]:
            m = target_month + delta
            y = target_year
            if m > 12:
                m -= 12
                y += 1
            elif m < 1:
                m += 12
                y -= 1
            mask2 = (hist_df.index.year == y) & (hist_df.index.month == m)
            prices2 = hist_df.loc[mask2, 'Close']
            if len(prices2) > 0:
                return float(prices2.iloc[-1])
    except Exception as e:
        logger.warning("_get_year_end_price: 株価取得失敗 (year=%s, month=%s): %s", target_year, target_month, e)
    return None


def _assess_esg(sustainability_df):
    """ESGデータから定性スコアを返す ("○"/"▲"/"×")。"""
    if sustainability_df is None or sustainability_df.empty:
        return "○"
    try:
        val = sustainability_df.loc['totalEsg', 'Value'] if 'totalEsg' in sustainability_df.index else None
        if val is None:
            # カラムが違う形式の場合
            if 'Value' not in sustainability_df.columns and len(sustainability_df.columns) > 0:
                col = sustainability_df.columns[0]
                val = sustainability_df['totalEsg'][col] if 'totalEsg' in sustainability_df.index else None
        if val is not None:
            score = float(val)
            # ESGリスクスコア: 低いほど良い (0-10: negligible, 10-20: low, 20-30: medium, 30+: high)
            if score < 20:
                return "○"
            elif score < 30:
                return "▲"
            else:
                return "×"
    except Exception as e:
        logger.warning("_assess_esg: ESGスコア評価失敗: %s", e)
    return "○"


def _assess_ownership(major_holders_df):
    """機関投資家保有比率から株主構造スコアを返す ("○"/"▲"/"×")。
    yfinance の major_holders は新旧2形式に対応:
      新形式: index=文字列('institutionsPercentHeld'等), columns=['Value']
      旧形式: index=整数(0,1,2,3), columns=[0,1] (値, ラベル)
    """
    if major_holders_df is None or major_holders_df.empty:
        return "○"
    try:
        inst_pct = None
        insider_pct = None

        # ── 新形式: 文字列インデックス ────────────────────────────────────
        if major_holders_df.index.dtype == object:
            idx_lower = {str(i).lower(): i for i in major_holders_df.index}
            # 機関投資家比率
            for key in ('institutionspercentheld', 'institutionsfloatpercentheld'):
                if key in idx_lower:
                    try:
                        inst_pct = float(major_holders_df.loc[idx_lower[key], 'Value'])
                        break
                    except (KeyError, ValueError, TypeError):
                        pass
            # インサイダー比率
            for key in ('insiderspercentheld',):
                if key in idx_lower:
                    try:
                        insider_pct = float(major_holders_df.loc[idx_lower[key], 'Value'])
                    except (KeyError, ValueError, TypeError):
                        pass

        # ── 旧形式: 整数インデックス + 2列 ──────────────────────────────
        else:
            for pos in range(len(major_holders_df)):
                try:
                    row_label = str(major_holders_df.iloc[pos, 1]).lower() if major_holders_df.shape[1] > 1 else ""
                    val_str = str(major_holders_df.iloc[pos, 0])
                    val = float(val_str.replace('%', '')) / 100 if '%' in val_str else float(val_str)
                    if 'institution' in row_label:
                        inst_pct = val
                    elif 'insider' in row_label:
                        insider_pct = val
                except (ValueError, TypeError):
                    continue

        # フォールバック: 最初の数値行
        if inst_pct is None:
            try:
                val = float(major_holders_df.iloc[1, 0] if major_holders_df.shape[1] > 1
                            else major_holders_df.iloc[1]['Value'])
                inst_pct = val
            except (IndexError, ValueError, TypeError, KeyError) as e:
                logger.warning("_assess_ownership: フォールバック行取得失敗: %s", e)

        if inst_pct is not None:
            if inst_pct >= 0.50:
                return "○"
            elif inst_pct >= 0.20:
                return "▲"
            else:
                return "×"
    except Exception as e:
        logger.warning("_assess_ownership: 株主構造評価失敗: %s", e)
    return "○"


def _get_risk_free_rate(is_jpy: bool = False) -> float:
    """リスクフリーレートを返す。米国株は^TNX（24hキャッシュ）、日本株はJGB~1%を使用。"""
    if is_jpy:
        # 日本株: 日本国債10年利回り (JGB ~1%)
        return 0.010

    global _tnx_cache
    now = time.time()
    if _tnx_cache["rate"] is not None and (now - _tnx_cache["ts"]) < _TNX_CACHE_TTL:
        return _tnx_cache["rate"]

    rate = 0.045  # フォールバック: 4.5%
    try:
        tnx = yf.Ticker('^TNX')
        tnx_price = _safe(tnx.info.get('regularMarketPrice'))
        if tnx_price and 0 < tnx_price < 20:
            rate = tnx_price / 100
    except Exception as e:
        logger.warning("_get_risk_free_rate: ^TNX 取得失敗、フォールバック 4.5%% を使用: %s", e)

    _tnx_cache["rate"] = rate
    _tnx_cache["ts"] = now
    return rate


def _calc_dividend_growth_rate(dividends_series) -> float | None:
    """配当履歴から5年間の配当成長率(CAGR%)を計算する。"""
    if dividends_series is None or len(dividends_series) < 2:
        return None
    try:
        # 年次配当合計で計算（直近5年分）
        by_year = dividends_series.groupby(dividends_series.index.year).sum()
        by_year = by_year[by_year > 0]
        if len(by_year) < 2:
            return None

        years = sorted(by_year.index)
        # 利用可能な最大5年スパン
        span = min(5, len(years) - 1)
        if span < 1:
            return None

        d_start = float(by_year.loc[years[-(span + 1)]])
        d_end = float(by_year.loc[years[-1]])
        if d_start <= 0 or d_end <= 0:
            return None

        cagr = ((d_end / d_start) ** (1.0 / span) - 1.0) * 100
        # 極端な値はNone (例: -100% ~ +100%)
        return round(cagr, 2) if -50 < cagr < 100 else None
    except Exception:
        return None


def parse_yfinance(ticker_symbol):
    """yfinanceからデータを取得し、parse_excel()と同一形式の(data, ts_data)を返す。

    Args:
        ticker_symbol: ティッカーシンボル (例: '7203.T', 'AAPL')

    Returns:
        (data, ts_data) タプル。parse_excel()と同一のキー構造。

    Raises:
        ValueError: ティッカーが無効またはデータ取得失敗時
    """
    ticker = yf.Ticker(ticker_symbol)

    # ── 基本財務データ取得 ────────────────────────────────────────────────────
    try:
        inc_df = ticker.financials
    except Exception:
        inc_df = None
    try:
        bs_df = ticker.balance_sheet
    except Exception:
        bs_df = None
    try:
        cf_df = ticker.cashflow
    except Exception:
        cf_df = None
    try:
        info = ticker.info or {}
    except Exception:
        info = {}

    # ── 米国株判定（ティッカーから早期判定）──────────────────────────────────
    _is_likely_us = not (ticker_symbol.endswith('.T') or ticker_symbol.endswith('.J'))
    _is_japan = ticker_symbol.endswith('.T') or ticker_symbol.endswith('.J')

    # ── 米国株: SEC EDGAR API で公式財務データ取得を試行（10年分）────────────
    _sec_data = None
    if _is_likely_us:
        try:
            _sec_data = parse_edgar_us(ticker_symbol)
            if _sec_data is not None:
                logger.info("SEC EDGAR データを使用: %s", ticker_symbol)
        except Exception as e:
            logger.warning("SEC EDGAR フォールバック (yfinance): %s", e)

    # ── 日本株: IR BANK で財務データ取得を試行 ─────────────────
    _jp_code = ticker_symbol.replace('.T', '').replace('.J', '') if _is_japan else None
    _irbank_data = None

    if _is_japan and _jp_code and _parse_irbank is not None:
        try:
            _irbank_data = _parse_irbank(_jp_code, max_years=5)
            if _irbank_data is not None:
                logger.info("IR BANK データを使用: %s (code=%s)", ticker_symbol, _jp_code)
        except Exception as e:
            logger.warning("IR BANK フォールバック (yfinance): %s", e)

    # ── ETF / ファンド / 暗号通貨など非株式の弾き出し ─────────────────────────
    _quote_type = info.get('quoteType', '')
    _NON_EQUITY_TYPES = {'ETF', 'MUTUALFUND', 'INDEX', 'CRYPTOCURRENCY', 'FUTURE', 'OPTION'}
    if _quote_type.upper() in _NON_EQUITY_TYPES:
        type_label = {
            'ETF': 'ETF（上場投資信託）',
            'MUTUALFUND': '投資信託',
            'INDEX': '株価指数',
            'CRYPTOCURRENCY': '暗号通貨',
            'FUTURE': '先物',
            'OPTION': 'オプション',
        }.get(_quote_type.upper(), _quote_type)
        raise ValueError(
            f"'{ticker_symbol}' は {type_label} です。"
            "このアプリは個別株式（EQUITY）の分析専用です。"
        )

    # ── データ存在チェック ───────────────────────────────────────────────────
    _has_yf = not ((inc_df is None or inc_df.empty) and (bs_df is None or bs_df.empty))
    if not _has_yf and _sec_data is None and _irbank_data is None:
        # 上場廃止・シンボル誤り・データなし
        _exchange = info.get('exchange', '')
        if not _exchange and not info.get('regularMarketPrice'):
            raise ValueError(
                f"ティッカー '{ticker_symbol}' が見つかりません。"
                "上場廃止またはシンボルの誤りの可能性があります。"
            )
        raise ValueError(
            f"ティッカー '{ticker_symbol}' の財務データを取得できませんでした。"
            "データが存在しないか、取得に失敗しました。"
        )

    # ── 追加データ取得（yfinance: 株価・ESG・アナリスト等）──────────────────
    try:
        hist_df = ticker.history(period='11y', interval='1mo')
    except Exception:
        hist_df = None

    try:
        hist_daily_df = ticker.history(period='5y', interval='1d')
    except Exception:
        hist_daily_df = None

    try:
        sustainability_df = ticker.sustainability
    except Exception:
        sustainability_df = None

    try:
        major_holders_df = ticker.major_holders
    except Exception:
        major_holders_df = None

    try:
        dividends_series = ticker.dividends
    except Exception:
        dividends_series = None

    # ── 財務データ抽出 ────────────────────────────────────────────────────────
    # yfinance データを先に抽出（SEC / IR BANK が None のフィールドへのフォールバック用）
    inc_data_yf, inc_dates_yf = _extract_series(inc_df, _INCOME_MAP)
    cf_data_yf,  cf_dates_yf  = _extract_series(cf_df,  _CASHFLOW_MAP)
    bs_data_yf,  bs_dates_yf  = _extract_series(bs_df,  _BALANCE_MAP)

    def _merge_with_yf_fallback(primary_inc, primary_bs, primary_cf, primary_dates):
        """プライマリデータ（SEC/IR BANK）と yfinance をマージする共通ロジック"""
        _merged = {}
        _merged.update(primary_bs)
        _merged.update(primary_cf)
        _merged.update(primary_inc)
        yf_fallback = {**bs_data_yf, **cf_data_yf, **inc_data_yf}
        for k, v in yf_fallback.items():
            if not v or all(x is None for x in v):
                continue
            existing = _merged.get(k)
            if not existing:
                _merged[k] = v
            elif isinstance(existing, list) and isinstance(v, list):
                merged_list = []
                for i in range(max(len(existing), len(v))):
                    ex_val = existing[i] if i < len(existing) else None
                    yf_val = v[i] if i < len(v) else None
                    merged_list.append(ex_val if ex_val is not None else yf_val)
                _merged[k] = merged_list
        return _merged, primary_dates

    if _sec_data is not None:
        # 米国株: SEC EDGAR の公式データを使用（10年分）
        _s_inc, _s_bs, _s_cf, dates = _sec_data
        all_data, dates = _merge_with_yf_fallback(_s_inc, _s_bs, _s_cf, dates)

    elif _irbank_data is not None:
        # 日本株: IR BANK CSV（5年分）をベースにyfinanceで補完
        _ib_inc, _ib_bs, _ib_cf, dates = _irbank_data
        all_data, dates = _merge_with_yf_fallback(_ib_inc, _ib_bs, _ib_cf, dates)

    else:
        # yfinance フォールバック（日本株でIR BANKが取れない場合 / 海外株など）
        dates = inc_dates_yf or cf_dates_yf or bs_dates_yf
        all_data = {}
        all_data.update(bs_data_yf)
        all_data.update(cf_data_yf)
        all_data.update(inc_data_yf)  # incが最優先

    def g(key, idx=0):
        lst = all_data.get(key, [])
        return lst[idx] if idx < len(lst) else None

    def g_list(key):
        return all_data.get(key, [])

    # ── 米国株: 10年ベースに統一（日本株はIR BANK仕様の5年のままとする）──────
    _TARGET_YEARS = 10
    if _irbank_data is None and _sec_data is None and dates and len(dates) < _TARGET_YEARS:
        _oldest = int(dates[-1])
        _missing = _TARGET_YEARS - len(dates)
        dates = dates + [str(_oldest - i - 1) for i in range(_missing)]
        for _k in list(all_data.keys()):
            if isinstance(all_data[_k], list):
                all_data[_k] = all_data[_k] + [None] * _missing

    n = len(dates)

    # ── 通貨・地域メタデータ（早期決定） ─────────────────────────────────────
    currency = info.get("currency", "USD")
    is_jpy = currency == "JPY" or ticker_symbol.endswith(".T") or ticker_symbol.endswith(".J")

    # ── 実効税率（年別）──────────────────────────────────────────────────────
    # income_tax / pretax_income で実際の税率を計算。異常値はフォールバック。
    eff_tax_rates = []
    for i in range(n):
        pt = g('pretax_income', i)
        tax = g('income_tax', i)
        if pt and pt != 0 and tax is not None:
            rate = abs(tax) / abs(pt)
            eff_tax_rates.append(rate if 0.01 < rate < 0.60 else 0.25)
        else:
            eff_tax_rates.append(0.25)
    eff_tax_now = eff_tax_rates[0] if eff_tax_rates else 0.25

    # ── 収益性指標（各年）────────────────────────────────────────────────────
    revenue = g_list('revenue')
    op_income = g_list('op_income')
    net_income = g_list('net_income')
    total_assets = g_list('total_assets')
    total_equity = g_list('total_equity')
    ebitda_list = g_list('ebitda')

    roe_list = []
    for i in range(n):
        ni = g('net_income', i)
        eq = g('total_equity', i)
        roe_list.append(ni / eq if (ni is not None and eq and eq != 0) else None)

    roa_list = []
    for i in range(n):
        ni = g('net_income', i)
        ta = g('total_assets', i)
        roa_list.append(ni / ta if (ni is not None and ta and ta != 0) else None)

    op_margin_list = []
    for i in range(n):
        oi = g('op_income', i)
        rev = g('revenue', i)
        op_margin_list.append(oi / rev if (oi is not None and rev and rev != 0) else None)

    ebitda_margin_list = []
    for i in range(n):
        eb = g('ebitda', i)
        rev = g('revenue', i)
        ebitda_margin_list.append(eb / rev if (eb is not None and rev and rev != 0) else None)

    def to_pct(v):
        return v * 100 if v is not None else None

    # ── 財務健全性指標 ────────────────────────────────────────────────────────
    equity_ratio = None
    equity_ratio_5y = None
    eq0 = g('total_equity', 0)
    ta0 = g('total_assets', 0)
    if eq0 and ta0 and ta0 != 0:
        equity_ratio = (eq0 / ta0) * 100
    i5 = min(4, n - 1) if n > 0 else 0
    eq4 = g('total_equity', i5)
    ta4 = g('total_assets', i5)
    if eq4 and ta4 and ta4 != 0:
        equity_ratio_5y = (eq4 / ta4) * 100

    current_r = None
    current_r_5y = None
    ca0 = g('current_assets', 0)
    cl0 = g('current_liab', 0)
    if ca0 and cl0 and cl0 != 0:
        current_r = (ca0 / cl0) * 100
    ca4 = g('current_assets', i5)
    cl4 = g('current_liab', i5)
    if ca4 and cl4 and cl4 != 0:
        current_r_5y = (ca4 / cl4) * 100

    quick_r = None
    quick_r_5y = None
    inv0 = g('inventory', 0)
    if ca0 and cl0 and cl0 != 0:
        quick_r = ((ca0 - (inv0 or 0)) / cl0) * 100
    inv4 = g('inventory', i5)
    if ca4 and cl4 and cl4 != 0:
        quick_r_5y = ((ca4 - (inv4 or 0)) / cl4) * 100

    # ── ROE/ROA サマリー ──────────────────────────────────────────────────────
    op_margin_vals = [to_pct(v) for v in op_margin_list]
    roe_now = to_pct(roe_list[0]) if roe_list else None
    roe_3y = to_pct(roe_list[2]) if len(roe_list) > 2 else None
    roe_5y = to_pct(roe_list[i5]) if roe_list and n > 0 else None
    roa_now = to_pct(roa_list[0]) if roa_list else None
    roa_3y = to_pct(roa_list[2]) if len(roa_list) > 2 else None
    roa_5y = to_pct(roa_list[i5]) if roa_list and n > 0 else None
    roe_growth = roe_now - roe_5y if (roe_now is not None and roe_5y is not None) else None

    ebitda_margin_val = to_pct(ebitda_margin_list[0]) if ebitda_margin_list else None
    ebitda_margin_5y = to_pct(ebitda_margin_list[i5]) if ebitda_margin_list and n > 0 else None

    # ── NOPAT（実効税率ベース）────────────────────────────────────────────────
    nopat = g('op_income', 0) * (1 - eff_tax_now) if g('op_income', 0) else None
    tax_rate_5y = eff_tax_rates[i5] if i5 < len(eff_tax_rates) else 0.25
    nopat_5y = g('op_income', i5) * (1 - tax_rate_5y) if n > 0 and g('op_income', i5) else None

    # ── 投下資本 ──────────────────────────────────────────────────────────────
    ic = g('invested_capital', 0)
    ic_5y = g('invested_capital', i5) if n > 0 else None
    if ic is None:
        eq = g('total_equity', 0)
        debt = g('total_debt', 0)
        cash = g('cash', 0)
        if eq is not None and debt is not None and cash is not None:
            ic = eq + debt - cash
    if ic_5y is None and n > 0:
        eq = g('total_equity', i5)
        debt = g('total_debt', i5)
        cash_v = g('cash', i5)
        if eq is not None and debt is not None and cash_v is not None:
            ic_5y = eq + debt - cash_v

    # ── WACC（CAPMベース）────────────────────────────────────────────────────
    # Ke = Rf + β × ERP
    # Kd = 支払利息 / 総負債（またはフォールバック3%）
    # WACC = E/V × Ke + D/V × Kd × (1 - 実効税率)
    beta = _safe(info.get('beta')) or 1.0

    # リスクフリーレート: 日本株=JGB~1%、米国株=^TNX（24hキャッシュ）
    rf_rate = _get_risk_free_rate(is_jpy)

    # 株式リスクプレミアム: 日本株~5%、米国株~5.5%（Damodaran推計）
    equity_premium = 0.050 if is_jpy else 0.055
    ke = rf_rate + beta * equity_premium

    wacc_val = None
    eq_val = g('total_equity', 0)
    debt_val = g('total_debt', 0)
    if eq_val and debt_val is not None:
        total_cap = eq_val + (debt_val or 0)
        if total_cap > 0:
            e_ratio = eq_val / total_cap
            d_ratio = (debt_val or 0) / total_cap
            # 実際の借入コスト: 支払利息 / 総負債
            kd = 0.03  # フォールバック
            int_exp = g('interest_exp', 0)
            if int_exp and debt_val and debt_val > 0:
                kd_calc = abs(int_exp) / debt_val
                if 0.001 < kd_calc < 0.20:
                    kd = kd_calc
            wacc_val = e_ratio * ke * 100 + d_ratio * kd * (1 - eff_tax_now) * 100
    elif eq_val:
        wacc_val = ke * 100  # 無借金企業

    # ── 販管費率 ──────────────────────────────────────────────────────────────
    sga_ratio = None
    sga_ratio_5y = None
    if g('sga', 0) and g('revenue', 0) and g('revenue', 0) != 0:
        sga_ratio = (g('sga', 0) / g('revenue', 0)) * 100
    if n > 0 and g('sga', i5) and g('revenue', i5):
        rev4 = g('revenue', i5)
        if rev4 and rev4 != 0:
            sga_ratio_5y = (g('sga', i5) / rev4) * 100

    # ── キャッシュフロー関連 ──────────────────────────────────────────────────
    fcf_list = g_list('fcf')
    ocf_list = g_list('ocf')
    capex_list = g_list('capex')

    debt_fcf = None
    debt_fcf_5y = None
    if g('total_debt', 0) is not None and g('fcf', 0) and g('fcf', 0) != 0:
        debt_fcf = g('total_debt', 0) / g('fcf', 0)
    if n > 0 and g('total_debt', i5) is not None and g('fcf', i5) and g('fcf', i5) != 0:
        debt_fcf_5y = g('total_debt', i5) / g('fcf', i5)

    nd_ebitda = None
    if g('net_debt', 0) is not None and g('ebitda', 0) and g('ebitda', 0) != 0:
        nd_ebitda = g('net_debt', 0) / g('ebitda', 0)

    debt_ebitda_val = None
    if g('total_debt', 0) is not None and g('ebitda', 0) and g('ebitda', 0) != 0:
        debt_ebitda_val = g('total_debt', 0) / g('ebitda', 0)

    # ── バリュエーション（info系）────────────────────────────────────────────
    per = _safe(info.get('trailingPE'))
    pbr = _safe(info.get('priceToBook'))
    div_yield = info.get('dividendYield')
    ev_val = info.get('enterpriseValue')
    company_name = info.get('shortName') or info.get('longName') or ticker_symbol
    industry_raw = info.get('industry', 'Industrials')
    # yfinanceが日本語でセクター情報を返す場合、英語にマッピング
    ja_to_en_sector = {
        '製造・サービス': 'Industrials',
        '金融': 'Financials',
        'テクノロジー': 'Technology',
        '電気通信': 'Communication Services',
        'エネルギー': 'Energy',
        'ユーティリティ': 'Utilities',
        '素材': 'Materials',
        '不動産': 'Real Estate',
        'ヘルスケア': 'Healthcare',
        '生活必需品': 'Consumer Staples',
        '裁量的消費': 'Consumer Discretionary',
    }
    industry = ja_to_en_sector.get(industry_raw, industry_raw if industry_raw else 'Industrials')
    dividend_yield_pct = div_yield * 100 if div_yield else None

    # 配当性向: info.get('payoutRatio') が小数 (0.30 = 30%)
    payout_ratio_now = _safe(info.get('payoutRatio'))
    if payout_ratio_now is not None:
        payout_ratio_now = payout_ratio_now * 100  # % 変換
    # 5年前の配当性向: 配当実績 / 純利益 から推計
    payout_ratio_5y = None
    if dividends_series is not None and len(dividends_series) > 0 and g('net_income', i5):
        try:
            div_5y_total = float(dividends_series[dividends_series.index.year == int(dates[i5])].sum()) if dates and i5 < len(dates) else 0
            shares_out = _safe(info.get('sharesOutstanding') or info.get('impliedSharesOutstanding'))
            if div_5y_total > 0 and shares_out and shares_out > 0:
                dps_5y = div_5y_total
                ni_5y = g('net_income', i5)
                eps_5y = g('eps', i5) or (g('eps_diluted', i5))
                if eps_5y and eps_5y != 0:
                    payout_ratio_5y = (dps_5y / eps_5y) * 100
        except Exception:
            pass

    # 5年前配当利回り: 配当履歴 / 株価履歴 から推計
    dividend_yield_5y = None
    if dividends_series is not None and len(dividends_series) > 0 and hist_df is not None and dates:
        try:
            year_5y_str = dates[i5] if i5 < len(dates) else None
            if year_5y_str:
                price_5y = _get_year_end_price(hist_df, year_5y_str, is_jpy)
                year_5y_int = int(year_5y_str)
                div_that_year = float(dividends_series[dividends_series.index.year == year_5y_int].sum())
                if price_5y and price_5y > 0 and div_that_year > 0:
                    dividend_yield_5y = (div_that_year / price_5y) * 100
        except Exception:
            pass

    # ── PER/PBR 履歴（株価履歴×EPS/BPSで計算）────────────────────────────────
    # yfinanceのsharesOutstandingは現在値のみ。EDGAR年次データを年ごとに優先使用
    _shares_out_yf = _safe(info.get('sharesOutstanding') or info.get('impliedSharesOutstanding'))

    per_ts = []
    pbr_ts = []
    for i in range(n):
        price_y = _get_year_end_price(hist_df, dates[i], is_jpy) if dates and i < len(dates) else None
        # PER = 株価 / EPS
        eps_y = g('eps', i) or g('eps_diluted', i)
        if price_y and eps_y and eps_y > 0:
            per_ts.append(_safe(price_y / eps_y))
        else:
            per_ts.append(None)
        # PBR = 株価 / BPS (BPS = 純資産 / 発行済株式数)
        # EDGAR年次株数を優先、なければyfinance現在値にフォールバック
        shares_y = g('shares_outstanding', i) or _shares_out_yf
        eq_y = g('total_equity', i)
        if price_y and eq_y and shares_y and shares_y > 0:
            bps = eq_y / shares_y
            if bps > 0:
                pbr_ts.append(_safe(price_y / bps))
            else:
                pbr_ts.append(None)
        else:
            pbr_ts.append(None)

    per_5y_val = per_ts[i5] if i5 < len(per_ts) else None
    pbr_5y_val = pbr_ts[i5] if i5 < len(pbr_ts) else None
    # 現在のPER/PBRはinfoの値を優先、なければ計算値
    per_now = per if per is not None else (per_ts[0] if per_ts else None)
    pbr_now = pbr if pbr is not None else (pbr_ts[0] if pbr_ts else None)

    # priceToBook / pbr_ts が両方 None の場合、bookValue (per share) で補完
    _current_price = _safe(info.get('currentPrice') or info.get('regularMarketPrice'))
    if pbr_now is None and _current_price and _current_price > 0:
        bvps = _safe(info.get('bookValue'))
        if bvps and bvps > 0:
            pbr_now = _safe(_current_price / bvps)

    # ── 配当成長率（CAGR）────────────────────────────────────────────────────
    div_growth_rate = _calc_dividend_growth_rate(dividends_series)

    # ── ESGリスク評価 ─────────────────────────────────────────────────────────
    d3_esg = _assess_esg(sustainability_df)

    # ── 株主構造評価 ──────────────────────────────────────────────────────────
    d2_ownership = _assess_ownership(major_holders_df)

    # ── data dict (parse_excel互換) ───────────────────────────────────────────
    data = {
        "company": company_name,
        "ticker": ticker_symbol,
        "industry": industry,
        "current_price": _current_price,

        "revenue": [g('revenue', i) for i in range(n)],
        "fcf": [g('fcf', i) for i in range(len(fcf_list))],
        "eps": [g('eps', i) or g('eps_diluted', i) for i in range(n)],

        "roe": [roe_now, roe_3y, roe_5y],
        "roe_growth_rate": roe_growth,
        "roa": [roa_now, roa_3y, roa_5y],

        "equity_ratio": equity_ratio,
        "equity_ratio_5y": equity_ratio_5y,
        "quick_ratio": quick_r,
        "quick_ratio_5y": quick_r_5y,
        "current_ratio": current_r,
        "current_ratio_5y": current_r_5y,

        "operating_cf": [g('ocf', i) for i in range(len(ocf_list))],
        "investing_cf": [g('investing_cf', i) for i in range(len(g_list('investing_cf')))],
        "financing_cf": [g('financing_cf', i) for i in range(len(g_list('financing_cf')))],
        "op_margin": op_margin_vals,
        "ebitda_margin": ebitda_margin_val,
        "ebitda_margin_5y": ebitda_margin_5y,

        "debt_fcf": debt_fcf,
        "debt_fcf_5y": debt_fcf_5y,
        "nd_ebitda": nd_ebitda,
        "ev": _safe(ev_val),
        "per": per_now,
        "per_5y": per_5y_val,
        "pbr": pbr_now,
        "pbr_5y": pbr_5y_val,

        "nopat": nopat,
        "nopat_5y": nopat_5y,
        "invested_capital": ic,
        "invested_capital_5y": ic_5y,
        "wacc": wacc_val,

        "accounts_receivable": g('receivables', 0),
        "accounts_receivable_5y": g('receivables', i5) if n > 0 else None,
        "inventory": g('inventory', 0),
        "inventory_5y": g('inventory', i5) if n > 0 else None,
        "accounts_payable": g('payables', 0),
        "accounts_payable_5y": g('payables', i5) if n > 0 else None,
        "cogs": g('cogs', 0),
        "cogs_5y": g('cogs', i5) if n > 0 else None,
        "sga_ratio": sga_ratio,
        "sga_ratio_5y": sga_ratio_5y,

        "total_assets": g('total_assets', 0),
        "total_assets_5y": g('total_assets', i5) if n > 0 else None,
        "fixed_assets": g('fixed_assets', 0),
        "fixed_assets_5y": g('fixed_assets', i5) if n > 0 else None,
        "tangible_fixed_assets": g('fixed_assets', 0),
        "tangible_fixed_assets_5y": g('fixed_assets', i5) if n > 0 else None,
        "intangible_fixed_assets": g('intangibles', 0),
        "intangible_fixed_assets_5y": g('intangibles', i5) if n > 0 else None,

        "net_income_val": g('net_income', 0),
        "net_income_val_5y": g('net_income', i5) if n > 0 else None,
        "op_income_val": g('op_income', 0),
        "op_income_val_5y": g('op_income', i5) if n > 0 else None,
        "interest_exp": g('interest_exp', 0),
        "interest_exp_5y": g('interest_exp', i5) if n > 0 else None,
        "other_exp": g('other_exp', 0),
        "other_exp_5y": g('other_exp', i5) if n > 0 else None,
        "pretax_income": g('pretax_income', 0),
        "pretax_income_5y": g('pretax_income', i5) if n > 0 else None,
        "income_tax": g('income_tax', 0),
        "income_tax_5y": g('income_tax', i5) if n > 0 else None,
        "total_equity": g('total_equity', 0),
        "total_equity_5y": g('total_equity', i5) if n > 0 else None,

        "dividend_yield": dividend_yield_pct,
        "dividend_yield_5y": dividend_yield_5y,
        "payout_ratio": payout_ratio_now,
        "payout_ratio_5y": payout_ratio_5y,
        "dividend_growth_rate": div_growth_rate,

        "d1_mgmt_change": "○",
        "d2_ownership": d2_ownership,
        "d3_esg": d3_esg,

        "analyst_recommendation": info.get("recommendationKey"),
        "analyst_mean": info.get("recommendationMean"),
        "analyst_count": info.get("numberOfAnalystOpinions"),
        "analyst_target_mean": info.get("targetMeanPrice"),
        "analyst_target_high": info.get("targetHighPrice"),
        "analyst_target_low": info.get("targetLowPrice"),
    }

    # ── ts_data dict (時系列、parse_excel互換) ────────────────────────────────
    da_list = g_list('da')
    sga_list_ts = g_list('sga')
    investing_cf_list = g_list('investing_cf')
    financing_cf_list = g_list('financing_cf')

    current_ratio_ts = []
    for i in range(n):
        ca = g('current_assets', i)
        cl = g('current_liab', i)
        current_ratio_ts.append((ca / cl) * 100 if ca and cl and cl != 0 else None)

    quick_ratio_ts = []
    for i in range(n):
        ca = g('current_assets', i)
        cl = g('current_liab', i)
        inv = g('inventory', i) or 0
        quick_ratio_ts.append(((ca - inv) / cl) * 100 if ca and cl and cl != 0 else None)

    equity_ratio_ts = []
    for i in range(n):
        eq = g('total_equity', i)
        ta = g('total_assets', i)
        equity_ratio_ts.append((eq / ta) * 100 if eq and ta and ta != 0 else None)

    # ROIC（実効税率ベース）
    roic_ts = []
    for i in range(n):
        oi = g('op_income', i)
        ic_i = g('invested_capital', i)
        if ic_i is None:
            eq = g('total_equity', i)
            debt = g('total_debt', i)
            cash_i = g('cash', i)
            if eq is not None and debt is not None and cash_i is not None:
                ic_i = eq + debt - cash_i
        tax_i = eff_tax_rates[i] if i < len(eff_tax_rates) else 0.25
        if oi is not None and ic_i and ic_i != 0:
            roic_ts.append((oi * (1 - tax_i) / ic_i) * 100)
        else:
            roic_ts.append(None)

    debt_fcf_ts = []
    for i in range(n):
        debt = g('total_debt', i)
        fcf_i = g('fcf', i)
        debt_fcf_ts.append(debt / fcf_i if (debt is not None and fcf_i and fcf_i != 0) else None)

    debt_ebitda_ts = []
    for i in range(n):
        debt = g('total_debt', i)
        eb = g('ebitda', i)
        debt_ebitda_ts.append(debt / eb if (debt is not None and eb and eb != 0) else None)

    nd_ebitda_ts = []
    for i in range(n):
        nd = g('net_debt', i)
        eb = g('ebitda', i)
        nd_ebitda_ts.append(nd / eb if (nd is not None and eb and eb != 0) else None)

    ts_data = {
        "dates": dates,
        "revenue": list(revenue),
        "net_income": list(net_income),
        "fcf": list(fcf_list),
        "eps": [g('eps', i) or g('eps_diluted', i) for i in range(n)],
        "ocf": list(ocf_list),
        "investing_cf": list(investing_cf_list),
        "financing_cf": list(financing_cf_list),
        "ebitda": list(ebitda_list),
        "total_assets": list(total_assets),
        "total_equity": list(total_equity),
        "total_debt": list(g_list('total_debt')),
        "roe": [to_pct(v) for v in roe_list],
        "roa": [to_pct(v) for v in roa_list],
        "op_margin": [to_pct(v) for v in op_margin_list],
        "quick_ratio": quick_ratio_ts,
        "current_ratio": current_ratio_ts,
        "equity_ratio": equity_ratio_ts,
        "ebitda_margin": [to_pct(v) for v in ebitda_margin_list],
        "debt_fcf": debt_fcf_ts,
        "roic": roic_ts,
        "capex": list(capex_list),
        "sga": list(sga_list_ts),
        "da": list(da_list),
        "pe_ratio": per_ts,
        "pb_ratio": pbr_ts,
        "debt_ebitda": debt_ebitda_ts,
        "nd_ebitda": nd_ebitda_ts,
        "dividend_yield": [],
        "payout_ratio": [],
        "eff_tax_rate": eff_tax_rates,
    }

    # ── DuPont分解 ────────────────────────────────────────────────────────────
    net_margin_ts = []
    asset_turnover_ts = []
    fin_leverage_ts = []
    for i in range(n):
        ni = g('net_income', i)
        rev = g('revenue', i)
        ta = g('total_assets', i)
        eq = g('total_equity', i)
        net_margin_ts.append(round(ni / rev * 100, 2) if ni is not None and rev and rev != 0 else None)
        asset_turnover_ts.append(round(rev / ta, 3) if rev is not None and ta and ta != 0 else None)
        fin_leverage_ts.append(round(ta / eq, 3) if ta is not None and eq and eq != 0 else None)

    ts_data["net_margin"] = net_margin_ts
    ts_data["asset_turnover"] = asset_turnover_ts
    ts_data["financial_leverage"] = fin_leverage_ts

    # ── 純利益率分解 ──────────────────────────────────────────────────────────
    interest_burden_ts = []
    tax_burden_ts = []
    nonop_burden_ts = []
    for i in range(n):
        oi = g('op_income', i)
        pt = g('pretax_income', i)
        ni = g('net_income', i)
        ie = g('interest_exp', i)

        if oi is not None and oi != 0 and ie is not None:
            interest_burden_ts.append(round((oi + ie) / oi * 100, 2))
        else:
            interest_burden_ts.append(None)

        if oi is not None and ie is not None and (oi + ie) != 0 and pt is not None:
            nonop_burden_ts.append(round(pt / (oi + ie) * 100, 2))
        else:
            nonop_burden_ts.append(None)

        if pt is not None and pt != 0 and ni is not None:
            tax_burden_ts.append(round(ni / pt * 100, 2))
        else:
            tax_burden_ts.append(None)

    ts_data["interest_burden"] = interest_burden_ts
    ts_data["nonop_burden"] = nonop_burden_ts
    ts_data["tax_burden"] = tax_burden_ts

    # ── 月次株価時系列 ───────────────────────────────────────────────────────
    if hist_df is not None and not hist_df.empty and 'Close' in hist_df.columns:
        price_series = hist_df['Close'].dropna()
        ts_data["price_monthly_dates"] = [str(d.date()) if hasattr(d, 'date') else str(d) for d in price_series.index]
        def _sv(v):
            try:
                f = float(v)
                return None if math.isnan(f) or math.isinf(f) else f
            except Exception:
                return None
        ts_data["price_monthly"] = [_sv(v) for v in price_series.values]
    else:
        ts_data["price_monthly_dates"] = []
        ts_data["price_monthly"] = []

    # ── 日次株価時系列 ───────────────────────────────────────────────────────
    if hist_daily_df is not None and not hist_daily_df.empty and 'Close' in hist_daily_df.columns:
        daily_series = hist_daily_df['Close'].dropna()
        ts_data["price_daily_dates"] = [str(d.date()) if hasattr(d, 'date') else str(d) for d in daily_series.index]
        ts_data["price_daily"] = [_sv(v) for v in daily_series.values]
    else:
        ts_data["price_daily_dates"] = []
        ts_data["price_daily"] = []

    # ── アナリスト推奨サマリー（内訳件数）────────────────────────────────────
    try:
        rec_summary = ticker.get_recommendations_summary()
        if rec_summary is not None and not rec_summary.empty:
            row = rec_summary[rec_summary['period'] == '0m']
            if row.empty:
                row = rec_summary.iloc[[0]]
            r = row.iloc[0]
            ts_data["analyst_summary"] = {
                "strongBuy":  int(r.get('strongBuy',  0) or 0),
                "buy":        int(r.get('buy',        0) or 0),
                "hold":       int(r.get('hold',       0) or 0),
                "sell":       int(r.get('sell',       0) or 0),
                "strongSell": int(r.get('strongSell', 0) or 0),
            }
    except Exception:
        pass

    # ── アナリスト・メタデータ ────────────────────────────────────────────────
    ts_data["_source"] = "yfinance"
    ts_data["_currency"] = currency
    ts_data["_country"] = info.get("country", "US")
    ts_data["_is_jpy"] = is_jpy
    ts_data["_beta"] = beta
    ts_data["_risk_free_rate"] = rf_rate * 100
    ts_data["_equity_premium"] = equity_premium * 100
    ts_data["_eff_tax_rate_now"] = round(eff_tax_now * 100, 1)

    # 全時系列配列を dates 長に揃える（短い場合は末尾を None で埋める）
    # これにより、フロントエンドで欠損年が N/A として正しく描画される
    for _k, _v in ts_data.items():
        if isinstance(_v, list) and not _k.startswith('_') and _k != 'dates':
            if len(_v) < n:
                ts_data[_k] = _v + [None] * (n - len(_v))

    # ── 直近2年・四半期データ（8Q分）────────────────────────────────────────
    def _safe_val(v):
        try:
            f = float(v)
            return None if math.isnan(f) or math.isinf(f) else f
        except Exception:
            return None

    def _unified_quarterly_dates(dfs, max_q=8):
        """全DataFrameの日付を統合し最大max_q件を新しい順で返す"""
        all_cols = set()
        for df in dfs:
            if df is not None and not df.empty:
                all_cols.update(df.columns)
        if not all_cols:
            return []
        sorted_cols = sorted(all_cols, reverse=True)[:max_q]
        return [str(c.date()) if hasattr(c, 'date') else str(c) for c in sorted_cols], sorted_cols

    def _extract_quarterly_aligned(df, row_names, unified_cols):
        """統一日付列に揃えてデータ抽出（欠損はNone）"""
        result = {}
        if df is None or df.empty or not unified_cols:
            return result
        for row in row_names:
            if row in df.index:
                vals = []
                for c in unified_cols:
                    if c in df.columns:
                        vals.append(_safe_val(df.loc[row, c]))
                    else:
                        vals.append(None)
                result[row] = vals
        return result

    try:
        qi_df = ticker.quarterly_income_stmt
        qb_df = ticker.quarterly_balance_sheet
        qc_df = ticker.quarterly_cashflow
    except Exception:
        qi_df = qb_df = qc_df = None

    _unified_result = _unified_quarterly_dates([qi_df, qb_df, qc_df], max_q=8)
    if _unified_result:
        q_dates, _unified_cols = _unified_result
    else:
        q_dates, _unified_cols = [], []

    q_income   = _extract_quarterly_aligned(qi_df, [
        'Total Revenue', 'Gross Profit', 'Operating Income',
        'Net Income', 'EBITDA', 'Diluted EPS',
    ], _unified_cols)
    q_balance  = _extract_quarterly_aligned(qb_df, [
        'Total Assets', 'Stockholders Equity', 'Total Debt',
        'Current Assets', 'Current Liabilities', 'Cash And Cash Equivalents',
        'Ordinary Shares Number', 'Share Issued',
    ], _unified_cols)
    q_cashflow = _extract_quarterly_aligned(qc_df, [
        'Operating Cash Flow', 'Free Cash Flow', 'Capital Expenditure',
    ], _unified_cols)

    # ── 米国株: SEC EDGAR 10-Q から四半期データを補完 ───────────────────────
    if _is_likely_us and q_dates:
        try:
            _sec_cik_q = _ticker_to_cik(ticker_symbol)
            _sec_facts_q = _fetch_sec_facts(_sec_cik_q) if _sec_cik_q else None
            if _sec_facts_q is not None:
                _us_gaap_q = _sec_facts_q.get('facts', {}).get('us-gaap', {})
                if _us_gaap_q:
                    _all_inc_tags = {**_SEC_INCOME_TAGS, **_SEC_EPS_TAGS}

                    from datetime import datetime as _dt

                    def _fuzzy_lookup(target_str, by_date, window=7):
                        """±window日以内で最近傍のSEC日付値を返す（完全一致優先）"""
                        if target_str in by_date:
                            return by_date[target_str]
                        try:
                            t = _dt.fromisoformat(target_str)
                        except Exception:
                            return None
                        best_val, best_diff = None, window + 1
                        for d, v in by_date.items():
                            try:
                                diff = abs((_dt.fromisoformat(d) - t).days)
                                if diff < best_diff:
                                    best_diff, best_val = diff, v
                            except Exception:
                                continue
                        return best_val

                    def _merge_sec_q(q_dict, sec_key_map, tag_src, is_instant):
                        """SEC四半期データをq_dictに補完（Noneのみ上書き、±7日ファジーマッチ）"""
                        for int_key, field_name in sec_key_map.items():
                            tags = tag_src.get(int_key, [])
                            if not tags:
                                continue
                            sec_series = _sec_quarterly_series(
                                _us_gaap_q, tags, is_instant=is_instant
                            )
                            if not sec_series:
                                continue
                            sec_by_date = {d: v for d, v in sec_series}
                            current = q_dict.get(field_name, [None] * len(q_dates))
                            filled = list(current) + [None] * max(0, len(q_dates) - len(current))
                            updated = False
                            for i, qd in enumerate(q_dates):
                                if filled[i] is None:
                                    v = _fuzzy_lookup(qd, sec_by_date)
                                    if v is not None:
                                        filled[i] = v
                                        updated = True
                            if updated or (field_name not in q_dict and
                                           any(v is not None for v in filled)):
                                q_dict[field_name] = filled[:len(q_dates)]

                    _merge_sec_q(q_income,   _SEC_Q_INCOME_MAP,  _all_inc_tags,       False)
                    _merge_sec_q(q_balance,  _SEC_Q_BALANCE_MAP, _SEC_BALANCE_TAGS,   True)
                    _merge_sec_q(q_cashflow, _SEC_Q_CF_MAP,      _SEC_CASHFLOW_TAGS,  False)
                    logger.info("SEC 10-Q 四半期補完完了: %s", ticker_symbol)
        except Exception as _e:
            logger.warning("SEC四半期補完スキップ (%s): %s", ticker_symbol, _e)

    # ── 日本株: IR BANK 四半期データを補完 ─────────────────────────────────────
    if _is_japan and _jp_code and _parse_irbank_quarterly is not None:
        try:
            _ibq = _parse_irbank_quarterly(_jp_code, max_q=8)
            if _ibq is not None:
                from datetime import datetime as _dt2

                def _fuzzy_lookup_generic(target_str, by_date, window=7):
                    if target_str in by_date:
                        return by_date[target_str]
                    try:
                        t = _dt2.fromisoformat(target_str)
                    except Exception:
                        return None
                    best_val, best_diff = None, window + 1
                    for d, v in by_date.items():
                        try:
                            diff = abs((_dt2.fromisoformat(d) - t).days)
                            if diff < best_diff:
                                best_diff, best_val = diff, v
                        except Exception:
                            continue
                    return best_val

                def _merge_irbank_q(q_dict, ibq_dict):
                    """IR BANK四半期データを q_dict に補完（Noneのみ、±7日ファジーマッチ）"""
                    ibq_dates = _ibq['dates']
                    for field, values in ibq_dict.items():
                        by_date = {ibq_dates[i]: values[i]
                                   for i in range(min(len(ibq_dates), len(values)))
                                   if values[i] is not None}
                        if not by_date:
                            continue
                        current = q_dict.get(field, [None] * len(q_dates))
                        filled = list(current) + [None] * max(0, len(q_dates) - len(current))
                        updated = False
                        for i, qd in enumerate(q_dates):
                            if filled[i] is None:
                                v = _fuzzy_lookup_generic(qd, by_date)
                                if v is not None:
                                    filled[i] = v
                                    updated = True
                        if updated or (field not in q_dict and any(v is not None for v in filled)):
                            q_dict[field] = filled[:len(q_dates)]

                _merge_irbank_q(q_income,   _ibq['income'])
                _merge_irbank_q(q_balance,  _ibq['balance'])
                _merge_irbank_q(q_cashflow, _ibq['cashflow'])
                # q_dates が空の場合 IR BANK 日付で補完
                if not q_dates and _ibq['dates']:
                    q_dates = _ibq['dates']
                    for field, values in _ibq['income'].items():
                        q_income[field] = list(values[:len(q_dates)])
                    for field, values in _ibq['balance'].items():
                        q_balance[field] = list(values[:len(q_dates)])
                    for field, values in _ibq['cashflow'].items():
                        q_cashflow[field] = list(values[:len(q_dates)])
                logger.info("IR BANK 四半期補完完了: %s", ticker_symbol)
        except Exception as _e:
            logger.warning("IR BANK四半期補完スキップ (%s): %s", ticker_symbol, _e)

    if q_dates:
        ts_data['quarterly'] = {
            'dates': q_dates,
            'income': q_income,
            'balance': q_balance,
            'cashflow': q_cashflow,
            'currency': currency,
        }

    return data, ts_data
