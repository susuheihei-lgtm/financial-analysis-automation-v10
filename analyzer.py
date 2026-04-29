"""
個別株式分析エンジン — 公開API（薄いオーケストレーター）

内部実装は _analyzer_*.py モジュールに分割されている:
  _analyzer_helpers.py     — safe_div, rate_change, consecutive_increase, get_latest_value
  _analyzer_thresholds.py  — DEFAULT_THRESHOLDS, INVESTOR_PROFILES, generate_dynamic_thresholds,
                              generate_evaluation_criteria
  _analyzer_quantitative.py — analyze_quantitative()
  _analyzer_screening.py   — analyze_screening()
  _analyzer_trees.py       — analyze_roa_tree(), analyze_roe_tree(), compute_pbr_contribution()

公開シンボル:
  run_full_analysis(data, benchmark=None, investor_profile='balanced') -> dict
  generate_dynamic_thresholds(benchmark, profile=None) -> dict
  INDUSTRY_LIST
"""

# ── 標準ライブラリ ────────────────────────────────────────────────────────────
import logging
import math

# ── 公開インポート ────────────────────────────────────────────────────────────
from _analyzer_thresholds import (
    DEFAULT_THRESHOLDS,
    INVESTOR_PROFILES,
    generate_dynamic_thresholds,
    generate_evaluation_criteria,
)
from _analyzer_quantitative import analyze_quantitative
from _analyzer_screening import analyze_screening
from _analyzer_trees import analyze_roa_tree, analyze_roe_tree, compute_pbr_contribution

logger = logging.getLogger(__name__)

INDUSTRY_LIST = ["製造・サービス"]

# ── データ契約 ────────────────────────────────────────────────────────────────
# parse_excel / parse_yfinance / stock_data.json が提供するキーの全一覧。
# 全パーサーはこのコントラクトに準拠すること。
#
# 配列型キー: 要素順は [最新年, 1年前, 2年前, 3年前, 4年前] (= 新しい順)
# スカラー型キー: 最新年の単一値
#
DATA_CONTRACT = {
    # ── 識別情報 ──────────────────────────────────────────────
    "company":          str,   # 会社名
    "ticker":           str,   # ティッカー記号（任意）
    "industry":         str,   # 業種（INDUSTRY_LIST 参照）

    # ── 損益計算書 ────────────────────────────────────────────
    "revenue":          list,  # 売上高 [最新..5年前]
    "eps":              list,  # 1株当たり利益 [最新..5年前]
    "op_margin":        list,  # 営業利益率(%) [最新..5年前]
    "ebitda_margin":    float, # EBITDAマージン(%) 最新値
    "ebitda_margin_5y": float, # EBITDAマージン(%) 5年前
    "cogs":             float, # 売上原価 最新値
    "cogs_5y":          float, # 売上原価 5年前
    "sga_ratio":        float, # 販管費率(%) 最新値
    "sga_ratio_5y":     float, # 販管費率(%) 5年前
    "op_income_val":    float, # 営業利益額 最新値
    "op_income_val_5y": float, # 営業利益額 5年前
    "interest_exp":     float, # 支払利息 最新値
    "interest_exp_5y":  float, # 支払利息 5年前
    "pretax_income":    float, # 税引前利益 最新値
    "pretax_income_5y": float, # 税引前利益 5年前
    "income_tax":       float, # 法人税 最新値
    "income_tax_5y":    float, # 法人税 5年前
    "net_income_val":   float, # 純利益額 最新値
    "net_income_val_5y":float, # 純利益額 5年前
    "other_exp":        float, # 営業外費用 最新値
    "other_exp_5y":     float, # 営業外費用 5年前

    # ── バランスシート ────────────────────────────────────────
    "equity_ratio":     float, # 自己資本比率(%) 最新値
    "equity_ratio_5y":  float, # 自己資本比率(%) 5年前
    "quick_ratio":      float, # 当座比率(%) 最新値
    "quick_ratio_5y":   float, # 当座比率(%) 5年前
    "current_ratio":    float, # 流動比率(%) 最新値
    "current_ratio_5y": float, # 流動比率(%) 5年前
    "total_assets":     float, # 総資産 最新値
    "total_assets_5y":  float, # 総資産 5年前
    "total_equity":     float, # 純資産 最新値
    "total_equity_5y":  float, # 純資産 5年前
    "fixed_assets":     float, # 固定資産 最新値
    "fixed_assets_5y":  float, # 固定資産 5年前
    "tangible_fixed_assets":    float,
    "tangible_fixed_assets_5y": float,
    "intangible_fixed_assets":    float,
    "intangible_fixed_assets_5y": float,
    "accounts_receivable":    float, # 売上債権 最新値
    "accounts_receivable_5y": float,
    "inventory":        float, # 棚卸資産 最新値
    "inventory_5y":     float,
    "accounts_payable": float, # 買掛金 最新値
    "accounts_payable_5y": float,

    # ── キャッシュフロー ──────────────────────────────────────
    "operating_cf":     list,  # 営業CF [最新..5年前]
    "investing_cf":     list,  # 投資CF [最新..5年前]
    "financing_cf":     list,  # 財務CF [最新..5年前]
    "fcf":              list,  # フリーCF [最新..5年前]

    # ── 収益性・効率性 ────────────────────────────────────────
    "roe":              list,  # ROE(%) [最新..5年前]
    "roe_growth_rate":  float, # ROE 5年成長率(pt/年)
    "roa":              list,  # ROA(%) [最新..5年前]
    "nopat":            float, # 税引後営業利益 最新値
    "nopat_5y":         float,
    "invested_capital": float, # 投下資本 最新値
    "invested_capital_5y": float,
    "wacc":             float, # WACC(%)

    # ── バリュエーション ──────────────────────────────────────
    "per":              float, # PER(倍) 最新値
    "per_5y":           float,
    "pbr":              float, # PBR(倍) 最新値
    "pbr_5y":           float,
    "ev":               float, # 企業価値(EV)
    "nd_ebitda":        float, # Net Debt / EBITDA
    "debt_fcf":         float, # Net Debt / FCF
    "debt_fcf_5y":      float,

    # ── 配当 ──────────────────────────────────────────────────
    "dividend_yield":   float, # 配当利回り(%) 最新値
    "dividend_yield_5y":float,
    "payout_ratio":     float, # 配当性向(%) 最新値
    "payout_ratio_5y":  float,

    # ── 定性・ESG ─────────────────────────────────────────────
    "d1_mgmt_change":   str,   # 経営陣変更 ("○"/"▲"/"×")
    "d2_ownership":     str,   # 株主構造
    "d3_esg":           str,   # ESGリスク
}


# ── データ正規化 ──────────────────────────────────────────────────────────────
# listとして期待するフィールド
_LIST_FIELDS = {
    'revenue', 'eps', 'op_margin', 'roe', 'roa', 'fcf',
    'operating_cf', 'investing_cf', 'financing_cf',
}
# scalarとして期待するフィールド
_FLOAT_FIELDS = {
    'equity_ratio', 'equity_ratio_5y', 'quick_ratio', 'quick_ratio_5y',
    'current_ratio', 'current_ratio_5y', 'total_assets', 'total_assets_5y',
    'total_equity', 'total_equity_5y', 'ebitda_margin', 'ebitda_margin_5y',
    'per', 'per_5y', 'pbr', 'pbr_5y', 'dividend_yield', 'dividend_yield_5y',
    'payout_ratio', 'payout_ratio_5y', 'nopat', 'nopat_5y',
    'invested_capital', 'invested_capital_5y', 'wacc', 'ev', 'nd_ebitda',
    'debt_fcf', 'debt_fcf_5y', 'cogs', 'cogs_5y', 'sga_ratio', 'sga_ratio_5y',
    'op_income_val', 'op_income_val_5y', 'interest_exp', 'interest_exp_5y',
    'pretax_income', 'pretax_income_5y', 'income_tax', 'income_tax_5y',
    'net_income_val', 'net_income_val_5y', 'other_exp', 'other_exp_5y',
    'fixed_assets', 'fixed_assets_5y', 'roe_growth_rate',
    'accounts_receivable', 'accounts_receivable_5y',
    'inventory', 'inventory_5y', 'accounts_payable', 'accounts_payable_5y',
}


def _safe_num(v):
    """数値をfloatに変換。NaN / inf / None / 変換不可は None を返す。"""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def normalize_financial_data(data: dict) -> dict:
    """財務データを正規化する。

    - NaN / inf → None
    - listフィールドの各要素を安全なfloatに変換
    - listが来るべきフィールドにscalarが来た場合は [v] に変換
    - scalarフィールドに不正な値が来た場合は None に変換

    parse_excel / parse_yfinance の出力を run_full_analysis に渡す前に
    必ず呼ぶこと。分析ロジックが NaN/inf によるクラッシュを起こさなくなる。
    """
    result = dict(data)

    for field in _LIST_FIELDS:
        val = result.get(field)
        if val is None:
            result[field] = []
        elif not isinstance(val, list):
            result[field] = [_safe_num(val)]
        else:
            result[field] = [_safe_num(x) for x in val]

    for field in _FLOAT_FIELDS:
        result[field] = _safe_num(result.get(field))

    return result


# ── バリデーション ────────────────────────────────────────────────────────────
def validate_financial_data(data: dict) -> None:
    """分析実行前にデータ品質を検証する。

    問題は ValueError を raise せず _validation_warnings に追記するだけ。
    ランダムなティッカーでデータが部分的に欠損していても分析を継続させる。
    （致命的なクラッシュは run_full_analysis 内の try/except が受け持つ）
    """
    if not isinstance(data, dict):
        raise ValueError("分析データは dict 型である必要があります")

    warnings = []

    # 主要フィールドのデータ量チェック（警告のみ）
    check_fields = {"revenue": 2, "eps": 2, "roe": 2, "roa": 2}
    for field, min_len in check_fields.items():
        val = data.get(field)
        if not val or not isinstance(val, list):
            warnings.append(f"{field}: データなし")
        else:
            non_none = [v for v in val if v is not None]
            if len(non_none) < min_len:
                warnings.append(f"{field}: データが{len(non_none)}件（推奨{min_len}件以上）")

    # 数値範囲の基本チェック（明らかな異常値は警告）
    equity_ratio = data.get("equity_ratio")
    if equity_ratio is not None and not (-200 <= equity_ratio <= 200):
        warnings.append(f"equity_ratio が範囲外: {equity_ratio:.1f}%")

    if warnings:
        data.setdefault("_validation_warnings", []).extend(warnings)


# ── 公開エントリーポイント ────────────────────────────────────────────────────
def run_full_analysis(data: dict, benchmark=None, investor_profile: str = 'balanced') -> dict:
    """株式データの全分析を実行し、結果 dict を返す。

    Args:
        data: DATA_CONTRACT に準拠した財務データ dict（parse_excel / parse_yfinance の出力）
        benchmark: Damodaran 業界ベンチマーク dict（任意）
        investor_profile: 投資家プロファイル ID（'balanced'/'value'/'growth'/'quality'/'income'）

    Returns:
        分析結果 dict（quantitative / screening / roa_tree / roe_tree / pbr_contribution 等を含む）
        サブ分析が部分的に失敗しても、残りの結果を返す（部分的成功）。
    """
    # ── Layer A: 正規化（NaN/inf/型不整合を除去）────────────────────────────
    data = normalize_financial_data(data)
    validate_financial_data(data)

    # ── サブ分析（各モジュールを独立して実行 — 1つ失敗しても他は継続）────────
    q_results = {}
    try:
        q_results = analyze_quantitative(data, benchmark=benchmark)
    except Exception as e:
        logger.warning("analyze_quantitative failed for %s: %s", data.get("ticker", "?"), e)

    s_results = {}
    try:
        s_results = analyze_screening(data, q_results, benchmark=benchmark, investor_profile=investor_profile)
    except Exception as e:
        logger.warning("analyze_screening failed for %s: %s", data.get("ticker", "?"), e)

    r_results = {}
    try:
        r_results = analyze_roa_tree(data)
    except Exception as e:
        logger.warning("analyze_roa_tree failed for %s: %s", data.get("ticker", "?"), e)

    roe_results = {}
    try:
        roe_results = analyze_roe_tree(data)
    except Exception as e:
        logger.warning("analyze_roe_tree failed for %s: %s", data.get("ticker", "?"), e)

    pbr_contrib = {}
    try:
        pbr_contrib = compute_pbr_contribution(roe_results, s_results, data, benchmark=benchmark)
    except Exception as e:
        logger.warning("compute_pbr_contribution failed for %s: %s", data.get("ticker", "?"), e)

    evaluation_criteria = {}
    try:
        evaluation_criteria = generate_evaluation_criteria(benchmark)
    except Exception as e:
        logger.warning("generate_evaluation_criteria failed: %s", e)

    prof = INVESTOR_PROFILES.get(investor_profile, INVESTOR_PROFILES['balanced'])

    return {
        "company": data.get("company", "Unknown"),
        "ticker": data.get("ticker", ""),
        "industry": data.get("industry", "製造・サービス"),
        "investor_profile": {
            "id": investor_profile,
            "name_ja": prof["name_ja"],
            "name_en": prof["name_en"],
            "description_ja": prof["description_ja"],
            "priorities_ja": prof["priorities_ja"],
            "ref": prof["ref"],
            "weights": prof["weights"],
            "verdict": prof["verdict"],
        },
        "quantitative": q_results,
        "screening": s_results,
        "roa_tree": r_results,
        "roe_tree": roe_results,
        "pbr_contribution": pbr_contrib,
        "evaluation_criteria": evaluation_criteria,
        "validation_warnings": data.get("_validation_warnings", []),
        "raw_data": {
            "revenue": data.get("revenue"),
            "fcf": data.get("fcf"),
            "eps": data.get("eps"),
            "roe": data.get("roe"),
            "roa": data.get("roa"),
            "op_margin": data.get("op_margin"),
            "operating_cf": data.get("operating_cf"),
            "investing_cf": data.get("investing_cf"),
            "financing_cf": data.get("financing_cf"),
            "equity_ratio": data.get("equity_ratio"),
            "equity_ratio_5y": data.get("equity_ratio_5y"),
            "quick_ratio": data.get("quick_ratio"),
            "quick_ratio_5y": data.get("quick_ratio_5y"),
            "current_ratio": data.get("current_ratio"),
            "current_ratio_5y": data.get("current_ratio_5y"),
            "debt_fcf": data.get("debt_fcf"),
            "debt_fcf_5y": data.get("debt_fcf_5y"),
            "ebitda_margin": data.get("ebitda_margin"),
            "ebitda_margin_5y": data.get("ebitda_margin_5y"),
        },
    }
