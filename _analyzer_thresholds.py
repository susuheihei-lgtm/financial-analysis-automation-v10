"""
閾値定義・投資家プロファイル・動的閾値生成 / 評価基準生成
"""

DEFAULT_THRESHOLDS = {
    "equity_ratio_x": 20, "equity_ratio_tri": 35,
    "ebitda_margin_x": 5, "ebitda_margin_tri": 10,
    "op_margin_x": 3, "op_margin_tri": 5,
    "nd_ebitda_x": 5, "nd_ebitda_tri": 3,
    "per_hi": 40, "per_lo": 5,
    "pbr_hi": 5, "pbr_lo": 0.5,
    "current_ratio_x": 100, "current_ratio_tri": 120,
    "revenue_cagr_x": 0, "revenue_cagr_tri": 5,
    "eps_growth_x": 0, "eps_growth_tri": 10,
}

# ── 投資家プロファイル定義 ──────────────────────────────────────────────────────
# 各プロファイルは以下を持つ:
#   thresholds   : ベンチマークなし時の絶対閾値
#   dyn_mults    : Damodaran業界平均に掛けるプロファイル固有の倍率
#                  (eq_ratio, ebitda, op_margin) → (×倍率, ▲倍率)
#                  (nd_ebitda)                  → (▲倍率, ×倍率) ← 低い方が良い
#                  (per, pbr)                   → (下限倍率, 上限倍率)
#                  (growth)                     → (×倍率, ▲倍率)
#   weights      : セクション加重 {A, B, C, D}
#   verdict      : BUY/SELL判定スコア境界 {buy, sell}
#   immediate_sell_xs : SectionA の × が何個でSELLになるか
INVESTOR_PROFILES = {
    "balanced": {
        "name_ja": "バランス型（GARP）",
        "name_en": "Balanced / GARP",
        "description_ja": "成長・割安・品質を均等に評価する標準的アプローチ。最も一般的な機関投資家の基本形。",
        "priorities_ja": ["財務健全性", "安定成長", "適正バリュエーション"],
        "ref": "典型例: ウェリントン・マネジメント、Capital Group",
        "thresholds": {
            "equity_ratio_x": 20, "equity_ratio_tri": 35,
            "ebitda_margin_x": 5, "ebitda_margin_tri": 10,
            "op_margin_x": 3, "op_margin_tri": 5,
            "nd_ebitda_x": 5, "nd_ebitda_tri": 3,
            "per_hi": 40, "per_lo": 5,
            "pbr_hi": 5, "pbr_lo": 0.5,
            "current_ratio_x": 100, "current_ratio_tri": 120,
            "revenue_cagr_x": 0, "revenue_cagr_tri": 5,
            "eps_growth_x": 0, "eps_growth_tri": 10,
        },
        "dyn_mults": {
            "eq_ratio": (0.30, 0.60), "ebitda": (0.30, 0.60), "op_margin": (0.30, 0.60),
            "nd_ebitda": (1.5, 2.5), "per": (0.20, 2.5), "pbr": (0.15, 2.5),
            "growth": (-0.5, 0.5),
        },
        "weights": {"A": 5, "B": 4, "C": 3, "D": 2},
        "verdict": {"buy": 5, "sell": 15},
        "immediate_sell_xs": 1,
    },
    "value": {
        "name_ja": "バリュー投資",
        "name_en": "Value Investing",
        "description_ja": "割安性と安全余白を最重視。低PER・低PBR・強固なバランスシートを優先する。",
        "priorities_ja": ["低PER/PBR", "強固な財務", "キャッシュフロー"],
        "ref": "典型例: バフェット（バークシャー）、グレアム流、Dodge & Cox",
        "thresholds": {
            "equity_ratio_x": 25, "equity_ratio_tri": 40,
            "ebitda_margin_x": 8, "ebitda_margin_tri": 14,
            "op_margin_x": 5, "op_margin_tri": 8,
            "nd_ebitda_x": 3, "nd_ebitda_tri": 2,
            "per_hi": 18, "per_lo": 5,
            "pbr_hi": 2, "pbr_lo": 0.3,
            "current_ratio_x": 120, "current_ratio_tri": 150,
            "revenue_cagr_x": -3, "revenue_cagr_tri": 3,
            "eps_growth_x": -3, "eps_growth_tri": 5,
        },
        "dyn_mults": {
            "eq_ratio": (0.40, 0.70), "ebitda": (0.40, 0.70), "op_margin": (0.40, 0.70),
            "nd_ebitda": (1.0, 1.8), "per": (0.15, 1.5), "pbr": (0.10, 1.5),
            "growth": (-1.0, 0.3),
        },
        "weights": {"A": 6, "B": 3, "C": 6, "D": 2},
        "verdict": {"buy": 4, "sell": 14},
        "immediate_sell_xs": 1,
    },
    "growth": {
        "name_ja": "グロース投資",
        "name_en": "Growth Investing",
        "description_ja": "高い売上・EPS成長率を最重視。現在の割高は将来成長で正当化できると判断する。",
        "priorities_ja": ["売上成長率", "EPS成長率", "市場拡大余地"],
        "ref": "典型例: リンチ（マゼラン）、フィッシャー流、ARK Invest",
        "thresholds": {
            "equity_ratio_x": 15, "equity_ratio_tri": 25,
            "ebitda_margin_x": 3, "ebitda_margin_tri": 7,
            "op_margin_x": 1, "op_margin_tri": 3,
            "nd_ebitda_x": 8, "nd_ebitda_tri": 5,
            "per_hi": 80, "per_lo": 5,
            "pbr_hi": 15, "pbr_lo": 0.5,
            "current_ratio_x": 80, "current_ratio_tri": 100,
            "revenue_cagr_x": 3, "revenue_cagr_tri": 12,
            "eps_growth_x": 0, "eps_growth_tri": 15,
        },
        "dyn_mults": {
            "eq_ratio": (0.15, 0.40), "ebitda": (0.15, 0.40), "op_margin": (0.15, 0.40),
            "nd_ebitda": (2.5, 4.0), "per": (0.20, 4.0), "pbr": (0.10, 5.0),
            "growth": (0.3, 1.5),
        },
        "weights": {"A": 3, "B": 7, "C": 2, "D": 2},
        "verdict": {"buy": 6, "sell": 17},
        "immediate_sell_xs": 2,
    },
    "quality": {
        "name_ja": "クオリティ投資",
        "name_en": "Quality Investing",
        "description_ja": "高ROE・高マージン・経済的な堀（競争優位）を重視。適正価格での優良企業保有。",
        "priorities_ja": ["高ROE/ROIC", "安定マージン", "競争優位性"],
        "ref": "典型例: テリー・スミス（Fundsmith）、マンガー流、Artisan Partners",
        "thresholds": {
            "equity_ratio_x": 30, "equity_ratio_tri": 45,
            "ebitda_margin_x": 12, "ebitda_margin_tri": 18,
            "op_margin_x": 8, "op_margin_tri": 13,
            "nd_ebitda_x": 3, "nd_ebitda_tri": 1.5,
            "per_hi": 40, "per_lo": 10,
            "pbr_hi": 8, "pbr_lo": 1.0,
            "current_ratio_x": 120, "current_ratio_tri": 150,
            "revenue_cagr_x": 2, "revenue_cagr_tri": 6,
            "eps_growth_x": 5, "eps_growth_tri": 12,
        },
        "dyn_mults": {
            "eq_ratio": (0.50, 0.80), "ebitda": (0.50, 0.80), "op_margin": (0.50, 0.80),
            "nd_ebitda": (1.0, 1.8), "per": (0.30, 2.0), "pbr": (0.20, 3.0),
            "growth": (-0.2, 0.7),
        },
        "weights": {"A": 6, "B": 5, "C": 2, "D": 2},
        "verdict": {"buy": 4, "sell": 13},
        "immediate_sell_xs": 1,
    },
    "income": {
        "name_ja": "インカム投資",
        "name_en": "Income / Dividend",
        "description_ja": "安定配当・低リスク・成熟企業を重視。低ボラティリティの長期保有を前提とする。",
        "priorities_ja": ["安定配当", "低債務水準", "FCF安定性"],
        "ref": "典型例: 年金基金（GPIF等）、バンガード・インカムファンド",
        "thresholds": {
            "equity_ratio_x": 30, "equity_ratio_tri": 45,
            "ebitda_margin_x": 10, "ebitda_margin_tri": 15,
            "op_margin_x": 6, "op_margin_tri": 10,
            "nd_ebitda_x": 3, "nd_ebitda_tri": 2,
            "per_hi": 22, "per_lo": 8,
            "pbr_hi": 3, "pbr_lo": 0.5,
            "current_ratio_x": 130, "current_ratio_tri": 160,
            "revenue_cagr_x": -3, "revenue_cagr_tri": 3,
            "eps_growth_x": -3, "eps_growth_tri": 5,
        },
        "dyn_mults": {
            "eq_ratio": (0.45, 0.75), "ebitda": (0.45, 0.75), "op_margin": (0.45, 0.75),
            "nd_ebitda": (1.0, 1.8), "per": (0.20, 1.8), "pbr": (0.15, 2.0),
            "growth": (-1.0, 0.3),
        },
        "weights": {"A": 6, "B": 3, "C": 5, "D": 2},
        "verdict": {"buy": 4, "sell": 12},
        "immediate_sell_xs": 1,
    },
}


def generate_dynamic_thresholds(benchmark, profile=None):
    """Damodaranの業界平均データから動的に閾値を生成する。"""
    prof = INVESTOR_PROFILES.get(profile or 'balanced', INVESTOR_PROFILES['balanced'])

    if not benchmark:
        return prof['thresholds'].copy()

    th = prof['thresholds'].copy()
    dm = prof['dyn_mults']

    eq_x_mult, eq_tri_mult = dm.get('eq_ratio', (0.30, 0.60))
    em_x_mult, em_tri_mult = dm.get('ebitda', (0.30, 0.60))
    op_x_mult, op_tri_mult = dm.get('op_margin', (0.30, 0.60))
    nd_tri_mult, nd_x_mult = dm.get('nd_ebitda', (1.5, 2.5))
    per_lo_mult, per_hi_mult = dm.get('per', (0.20, 2.5))
    pbr_lo_mult, pbr_hi_mult = dm.get('pbr', (0.15, 2.5))
    gr_x_mult, gr_tri_mult = dm.get('growth', (-0.5, 0.5))

    dtc = benchmark.get('debt_to_capital_book')
    if dtc is not None and dtc < 1:
        avg_eq = (1 - dtc) * 100
        th["equity_ratio_x"] = round(avg_eq * eq_x_mult, 1)
        th["equity_ratio_tri"] = round(avg_eq * eq_tri_mult, 1)

    em = benchmark.get('ebitda_margin')
    if em is not None and em > 0.01:
        avg_em = em * 100
        th["ebitda_margin_x"] = round(avg_em * em_x_mult, 1)
        th["ebitda_margin_tri"] = round(avg_em * em_tri_mult, 1)

    nd = benchmark.get('debt_to_ebitda')
    if nd is not None and 0 < nd < 100:
        th["nd_ebitda_tri"] = round(nd * nd_tri_mult, 1)
        th["nd_ebitda_x"] = round(nd * nd_x_mult, 1)

    pe = benchmark.get('pe_aggregate_all')
    if pe is not None and pe > 0:
        th["per_lo"] = round(pe * per_lo_mult, 1)
        th["per_hi"] = round(pe * per_hi_mult, 1)

    pbr = benchmark.get('pbr')
    if pbr is not None and pbr > 0:
        th["pbr_lo"] = round(pbr * pbr_lo_mult, 2)
        th["pbr_hi"] = round(pbr * pbr_hi_mult, 2)

    op = benchmark.get('operating_margin')
    if op is not None and op > 0.01:
        avg_op = op * 100
        th["op_margin_x"] = round(avg_op * op_x_mult, 1)
        th["op_margin_tri"] = round(avg_op * op_tri_mult, 1)

    eg = benchmark.get('expected_growth_5y')
    if eg is not None:
        avg_growth = eg * 100
        th["revenue_cagr_x"] = round(min(avg_growth * gr_x_mult, 0), 1) if gr_x_mult < 0 else round(avg_growth * gr_x_mult, 1)
        th["revenue_cagr_tri"] = round(max(avg_growth * gr_tri_mult, 0), 1)
        th["eps_growth_x"] = round(min(avg_growth * gr_x_mult, 0), 1) if gr_x_mult < 0 else round(avg_growth * gr_x_mult, 1)
        th["eps_growth_tri"] = round(max(avg_growth * gr_tri_mult, 0), 1)

    if dtc is not None and dtc > 0.6:
        th["current_ratio_x"] = 60
        th["current_ratio_tri"] = 80

    return th


def generate_evaluation_criteria(benchmark=None):
    """業種ベンチマークから動的な評価基準を生成。"""
    criteria = {}

    if not benchmark:
        criteria["ROE"] = {
            "◎": {"roe_min": 15, "growth_min": 5, "improve_3y": True},
            "○": {"roe_min": 10, "growth_min": 3},
            "▲": {"roe_min": 0, "growth_min": 0},
            "評価軸": "ROE水準と5年成長率 + 3年比較"
        }
        criteria["ROA"] = {"評価軸": "ROA水準のみ表示（ベンチマーク無し時は目安なし）"}
        criteria["配当利回り"] = {
            "◎": 4.0, "○": 2.0, "▲": 1.0,
            "評価軸": "配当利回り水準"
        }
        return criteria

    roe_median = benchmark.get("roe_median") or benchmark.get("roe", 0)
    roa_median = benchmark.get("roa_median") or benchmark.get("roa", 0)

    if not roe_median:
        criteria["ROE"] = {
            "◎": {"roe_min": 15, "growth_min": 5, "improve_3y": True},
            "○": {"roe_min": 10, "growth_min": 3},
            "▲": {"roe_min": 0, "growth_min": 0},
            "評価軸": "ROE水準と5年成長率 + 3年比較（業種中央値なし）"
        }
    else:
        criteria["ROE"] = {
            "◎": {"rel_min": 1.5, "growth_min": 5},
            "○": {"rel_min": 1.0, "growth_min": 3},
            "▲": {"rel_min": 0.5, "growth_min": 0},
            "業種中央値": roe_median,
            "評価軸": f"業種中央値（{roe_median:.1f}%）との相対比較 + 成長トレンド"
        }

    if not roa_median:
        criteria["ROA"] = {"評価軸": "ROA水準のみ表示（業種中央値なし）"}
    else:
        criteria["ROA"] = {
            "◎": {"rel_min": 1.5},
            "○": {"rel_min": 1.0},
            "▲": {"rel_min": 0.5},
            "業種中央値": roa_median,
            "評価軸": f"業種中央値（{roa_median:.1f}%）との相対比較"
        }

    return criteria
