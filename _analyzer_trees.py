"""
DuPont分解・PBR寄与度モジュール — analyze_roa_tree, analyze_roe_tree, compute_pbr_contribution を公開。
依存: _analyzer_helpers, _analyzer_thresholds
"""
from _analyzer_helpers import safe_div
from _analyzer_thresholds import generate_dynamic_thresholds


def analyze_roa_tree(d):
    rev = d.get("revenue", [])
    roa = d.get("roa", [])
    op_margin = d.get("op_margin", [])
    rev_now = rev[0] if rev else None
    rev_5y = rev[min(4, len(rev) - 1)] if rev else None
    roa_now = roa[0] if roa else None
    roa_5y = roa[min(4, len(roa) - 1)] if roa else None
    op_now = op_margin[0] if op_margin else None
    op_5y = op_margin[min(4, len(op_margin) - 1)] if op_margin else None
    total_assets = d.get("total_assets")
    total_assets_5y = d.get("total_assets_5y")
    fixed_assets = d.get("fixed_assets")
    fixed_assets_5y = d.get("fixed_assets_5y")
    tangible_assets = d.get("tangible_fixed_assets")
    tangible_5y = d.get("tangible_fixed_assets_5y")
    intangible_assets = d.get("intangible_fixed_assets")
    intangible_5y = d.get("intangible_fixed_assets_5y")
    ar = d.get("accounts_receivable")
    ar_5y = d.get("accounts_receivable_5y")
    ap = d.get("accounts_payable")
    ap_5y = d.get("accounts_payable_5y")
    inventory = d.get("inventory")
    inventory_5y = d.get("inventory_5y")
    cogs = d.get("cogs")
    cogs_5y = d.get("cogs_5y")
    sga = d.get("sga_ratio")
    sga_5y = d.get("sga_ratio_5y")

    tree = {}

    tree["ROA"] = {
        "現在値": roa_now,
        "5年変化pt": roa_now - roa_5y if (roa_now and roa_5y) else None,
    }

    asset_turn = safe_div(rev_now, total_assets)
    asset_turn_5y = safe_div(rev_5y, total_assets_5y)
    tree["資産回転率"] = {
        "現在値": asset_turn,
        "5年変化": asset_turn - asset_turn_5y if (asset_turn and asset_turn_5y) else None,
    }

    fixed_turn = safe_div(rev_now, fixed_assets)
    fixed_turn_5y = safe_div(rev_5y, fixed_assets_5y)
    tree["固定資産回転率"] = {
        "現在値": fixed_turn,
        "5年変化": fixed_turn - fixed_turn_5y if (fixed_turn and fixed_turn_5y) else None,
    }

    tang_turn = safe_div(rev_now, tangible_assets)
    tang_turn_5y = safe_div(rev_5y, tangible_5y)
    tang_chg = tang_turn - tang_turn_5y if (tang_turn and tang_turn_5y) else None
    tree["有形固定資産回転率"] = {"現在値": tang_turn, "5年変化": tang_chg, "評価": "改善" if tang_chg and tang_chg > 0 else ("悪化" if tang_chg and tang_chg < 0 else "横ばい")}

    intan_turn = safe_div(rev_now, intangible_assets)
    intan_turn_5y = safe_div(rev_5y, intangible_5y)
    intan_chg = intan_turn - intan_turn_5y if (intan_turn and intan_turn_5y) else None
    tree["無形固定資産回転率"] = {"現在値": intan_turn, "5年変化": intan_chg, "評価": "改善" if intan_chg and intan_chg > 0 else ("悪化" if intan_chg and intan_chg < 0 else "横ばい")}

    dso = safe_div(ar, rev_now) * 365 if (ar and rev_now) else None
    dso_5y_v = safe_div(ar_5y, rev_5y) * 365 if (ar_5y and rev_5y) else None
    dso_chg = dso - dso_5y_v if (dso and dso_5y_v) else None

    dpo = safe_div(ap, rev_now) * 365 if (ap and rev_now) else None
    dpo_5y_v = safe_div(ap_5y, rev_5y) * 365 if (ap_5y and rev_5y) else None
    dpo_chg = dpo - dpo_5y_v if (dpo and dpo_5y_v) else None

    dio = safe_div(inventory, cogs) * 365 if (inventory and cogs) else None
    dio_5y_v = safe_div(inventory_5y, cogs_5y) * 365 if (inventory_5y and cogs_5y) else None
    dio_chg = dio - dio_5y_v if (dio and dio_5y_v) else None

    # DSO・DIO は低下が改善、DPO は上昇が改善
    tree["DSO"] = {"現在値": dso, "5年変化": dso_chg, "評価": "改善" if dso_chg and dso_chg < 0 else ("悪化" if dso_chg and dso_chg > 0 else "横ばい")}
    tree["DPO"] = {"現在値": dpo, "5年変化": dpo_chg, "評価": "改善" if dpo_chg and dpo_chg > 0 else ("悪化" if dpo_chg and dpo_chg < 0 else "横ばい")}
    tree["DIO"] = {"現在値": dio, "5年変化": dio_chg, "評価": "改善" if dio_chg and dio_chg < 0 else ("悪化" if dio_chg and dio_chg > 0 else "横ばい")}

    _op_chg = op_now - op_5y if (op_now and op_5y) else None
    tree["営業利益率"] = {
        "現在値": op_now,
        "5年変化pt": _op_chg,
        "評価": "改善" if _op_chg and _op_chg > 0 else ("悪化" if _op_chg and _op_chg < 0 else "横ばい"),
    }

    cogs_rate = safe_div(cogs, rev_now) * 100 if (cogs and rev_now) else None
    cogs_rate_5y = safe_div(cogs_5y, rev_5y) * 100 if (cogs_5y and rev_5y) else None
    cogs_chg = cogs_rate - cogs_rate_5y if (cogs_rate and cogs_rate_5y) else None
    tree["原価率"] = {"現在値": cogs_rate, "5年変化": cogs_chg, "評価": "改善" if cogs_chg and cogs_chg < 0 else ("悪化" if cogs_chg and cogs_chg > 0 else "横ばい")}

    sga_chg = sga - sga_5y if (sga and sga_5y) else None
    tree["販管費率"] = {"現在値": sga, "5年変化": sga_chg, "評価": "改善" if sga_chg and sga_chg < 0 else ("悪化" if sga_chg and sga_chg > 0 else "横ばい")}

    contributions = {
        "有形固定資産回転率": tang_chg,
        "無形固定資産回転率": intan_chg,
        "DSO": dso_chg, "DPO": dpo_chg, "DIO": dio_chg,
        "原価率": cogs_chg, "販管費率": sga_chg,
    }
    ranked = sorted(
        [(k, v) for k, v in contributions.items() if v is not None],
        key=lambda x: abs(x[1]), reverse=True
    )
    tree["貢献度ランキング"] = [{"順位": i + 1, "指標": k, "改善寄与度": v, "評価": "改善" if v > 0 else "悪化"} for i, (k, v) in enumerate(ranked)]

    return tree


def analyze_roe_tree(d):
    """DuPont ROE decomposition (3-factor)"""
    rev = d.get("revenue", [])
    roe_vals = d.get("roe", [])
    rev_now = rev[0] if rev else None
    rev_5y = rev[min(4, len(rev) - 1)] if rev else None
    net_income = d.get("net_income_val")
    net_income_5y = d.get("net_income_val_5y")
    total_assets = d.get("total_assets")
    total_assets_5y = d.get("total_assets_5y")
    total_equity = d.get("total_equity")
    total_equity_5y = d.get("total_equity_5y")
    roe_now = roe_vals[0] if roe_vals else None
    roe_5y = roe_vals[min(4, len(roe_vals) - 1)] if roe_vals else None

    tree = {}
    tree["ROE"] = {
        "現在値": roe_now,
        "5年変化pt": roe_now - roe_5y if (roe_now is not None and roe_5y is not None) else None,
    }

    # Factor 1: Net Profit Margin = Net Income / Revenue
    npm = safe_div(net_income, rev_now)
    npm_pct = npm * 100 if npm is not None else None
    npm_5y = safe_div(net_income_5y, rev_5y)
    npm_5y_pct = npm_5y * 100 if npm_5y is not None else None
    npm_chg = npm_pct - npm_5y_pct if (npm_pct is not None and npm_5y_pct is not None) else None
    tree["純利益率"] = {
        "現在値": npm_pct,
        "5年前": npm_5y_pct,
        "5年変化pt": npm_chg,
        "評価": "改善" if npm_chg and npm_chg > 0 else ("悪化" if npm_chg and npm_chg < 0 else "横ばい"),
    }

    # Factor 2: Asset Turnover = Revenue / Total Assets
    at = safe_div(rev_now, total_assets)
    at_5y = safe_div(rev_5y, total_assets_5y)
    at_chg = at - at_5y if (at is not None and at_5y is not None) else None
    tree["総資産回転率"] = {
        "現在値": at,
        "5年前": at_5y,
        "5年変化": at_chg,
        "評価": "改善" if at_chg and at_chg > 0 else ("悪化" if at_chg and at_chg < 0 else "横ばい"),
    }

    # Factor 3: Equity Multiplier = Total Assets / Equity
    em = safe_div(total_assets, total_equity)
    em_5y = safe_div(total_assets_5y, total_equity_5y)
    em_chg = em - em_5y if (em is not None and em_5y is not None) else None
    tree["財務レバレッジ"] = {
        "現在値": em,
        "5年前": em_5y,
        "5年変化": em_chg,
        "評価": "悪化" if em_chg and em_chg < 0 else ("改善" if em_chg and em_chg > 0 else "横ばい"),
    }

    # Verify: ROE ≈ NPM × AT × EM
    if npm is not None and at is not None and em is not None:
        computed_roe = npm * at * em * 100
        tree["ROE検算"] = {"計算値": computed_roe}

    # Sub-decomposition of Net Profit Margin
    op_margin = d.get("op_margin", [])
    op_now = op_margin[0] if op_margin else None
    op_5y = op_margin[min(4, len(op_margin) - 1)] if op_margin else None
    cogs = d.get("cogs")
    cogs_5y = d.get("cogs_5y")
    sga = d.get("sga_ratio")
    sga_5y = d.get("sga_ratio_5y")

    cogs_rate = safe_div(cogs, rev_now) * 100 if (cogs and rev_now) else None
    cogs_rate_5y = safe_div(cogs_5y, rev_5y) * 100 if (cogs_5y and rev_5y) else None
    cogs_chg = cogs_rate - cogs_rate_5y if (cogs_rate is not None and cogs_rate_5y is not None) else None
    tree["原価率"] = {"現在値": cogs_rate, "5年変化": cogs_chg, "評価": "改善" if cogs_chg and cogs_chg < 0 else ("悪化" if cogs_chg and cogs_chg > 0 else "横ばい")}

    sga_chg = sga - sga_5y if (sga is not None and sga_5y is not None) else None
    tree["販管費率"] = {"現在値": sga, "5年変化": sga_chg, "評価": "改善" if sga_chg and sga_chg < 0 else ("悪化" if sga_chg and sga_chg > 0 else "横ばい")}

    _op_chg2 = op_now - op_5y if (op_now is not None and op_5y is not None) else None
    tree["営業利益率"] = {
        "現在値": op_now,
        "5年変化pt": _op_chg2,
        "評価": "改善" if _op_chg2 and _op_chg2 > 0 else ("悪化" if _op_chg2 and _op_chg2 < 0 else "横ばい"),
    }

    # 営業利益→純利益ギャップ分解
    op_income_val = d.get("op_income_val")
    op_income_val_5y = d.get("op_income_val_5y")
    interest_exp = d.get("interest_exp")
    interest_exp_5y = d.get("interest_exp_5y")
    other_exp = d.get("other_exp")
    other_exp_5y = d.get("other_exp_5y")
    pretax_income = d.get("pretax_income")
    pretax_income_5y = d.get("pretax_income_5y")
    income_tax = d.get("income_tax")
    income_tax_5y = d.get("income_tax_5y")

    int_burden = None
    int_burden_5y = None
    if op_income_val and op_income_val != 0 and interest_exp is not None:
        int_burden = (op_income_val + interest_exp) / op_income_val * 100
    if op_income_val_5y and op_income_val_5y != 0 and interest_exp_5y is not None:
        int_burden_5y = (op_income_val_5y + interest_exp_5y) / op_income_val_5y * 100
    int_burden_chg = int_burden - int_burden_5y if (int_burden is not None and int_burden_5y is not None) else None
    tree["金利負担率"] = {
        "現在値": round(int_burden, 2) if int_burden is not None else None,
        "5年前": round(int_burden_5y, 2) if int_burden_5y is not None else None,
        "5年変化pt": round(int_burden_chg, 2) if int_burden_chg is not None else None,
        "評価": "改善" if int_burden_chg and int_burden_chg > 0 else ("悪化" if int_burden_chg and int_burden_chg < 0 else "横ばい"),
        "説明": "100%=金利負担なし。低いほど金利コストが重い",
    }

    nonop_burden = None
    nonop_burden_5y = None
    oi_after_int = (op_income_val + interest_exp) if (op_income_val and interest_exp is not None) else None
    oi_after_int_5y = (op_income_val_5y + interest_exp_5y) if (op_income_val_5y and interest_exp_5y is not None) else None
    if oi_after_int and oi_after_int != 0 and pretax_income is not None:
        nonop_burden = pretax_income / oi_after_int * 100
    if oi_after_int_5y and oi_after_int_5y != 0 and pretax_income_5y is not None:
        nonop_burden_5y = pretax_income_5y / oi_after_int_5y * 100
    nonop_chg = nonop_burden - nonop_burden_5y if (nonop_burden is not None and nonop_burden_5y is not None) else None
    tree["営業外損益率"] = {
        "現在値": round(nonop_burden, 2) if nonop_burden is not None else None,
        "5年前": round(nonop_burden_5y, 2) if nonop_burden_5y is not None else None,
        "5年変化pt": round(nonop_chg, 2) if nonop_chg is not None else None,
        "評価": "改善" if nonop_chg and nonop_chg > 0 else ("悪化" if nonop_chg and nonop_chg < 0 else "横ばい"),
        "説明": "100%=営業外損益なし。為替差損益・資産売却等の影響",
    }

    tax_burden = None
    tax_burden_5y = None
    if pretax_income and pretax_income != 0 and net_income is not None:
        tax_burden = net_income / pretax_income * 100
    if pretax_income_5y and pretax_income_5y != 0 and net_income_5y is not None:
        tax_burden_5y = net_income_5y / pretax_income_5y * 100
    tax_chg = tax_burden - tax_burden_5y if (tax_burden is not None and tax_burden_5y is not None) else None
    tree["税引後利益率"] = {
        "現在値": round(tax_burden, 2) if tax_burden is not None else None,
        "5年前": round(tax_burden_5y, 2) if tax_burden_5y is not None else None,
        "5年変化pt": round(tax_chg, 2) if tax_chg is not None else None,
        "評価": "改善" if tax_chg and tax_chg > 0 else ("悪化" if tax_chg and tax_chg < 0 else "横ばい"),
        "説明": "100%=税金ゼロ。低いほど税負担が重い（≒1-実効税率）",
    }

    if op_now is not None and int_burden is not None and nonop_burden is not None and tax_burden is not None:
        computed_npm = op_now * (int_burden / 100) * (nonop_burden / 100) * (tax_burden / 100)
        tree["純利益率検算"] = {
            "計算値": round(computed_npm, 2),
            "実績値": round(npm_pct, 2) if npm_pct is not None else None,
        }

    # Sub-decomposition of Asset Turnover
    fixed_assets = d.get("fixed_assets")
    fixed_assets_5y = d.get("fixed_assets_5y")
    ar = d.get("accounts_receivable")
    ar_5y = d.get("accounts_receivable_5y")
    inventory = d.get("inventory")
    inventory_5y = d.get("inventory_5y")

    fixed_turn = safe_div(rev_now, fixed_assets)
    fixed_turn_5y = safe_div(rev_5y, fixed_assets_5y)
    fixed_chg = fixed_turn - fixed_turn_5y if (fixed_turn is not None and fixed_turn_5y is not None) else None
    tree["固定資産回転率"] = {"現在値": fixed_turn, "5年変化": fixed_chg, "評価": "改善" if fixed_chg and fixed_chg > 0 else ("悪化" if fixed_chg and fixed_chg < 0 else "横ばい")}

    ar_turn = safe_div(rev_now, ar)
    ar_turn_5y = safe_div(rev_5y, ar_5y)
    ar_chg = ar_turn - ar_turn_5y if (ar_turn is not None and ar_turn_5y is not None) else None
    tree["売上債権回転率"] = {"現在値": ar_turn, "5年変化": ar_chg, "評価": "改善" if ar_chg and ar_chg > 0 else ("悪化" if ar_chg and ar_chg < 0 else "横ばい")}

    inv_turn = safe_div(cogs, inventory) if cogs else safe_div(rev_now, inventory)
    inv_turn_5y = safe_div(cogs_5y, inventory_5y) if cogs_5y else safe_div(rev_5y, inventory_5y)
    inv_chg = inv_turn - inv_turn_5y if (inv_turn is not None and inv_turn_5y is not None) else None
    tree["棚卸資産回転率"] = {"現在値": inv_turn, "5年変化": inv_chg, "評価": "改善" if inv_chg and inv_chg > 0 else ("悪化" if inv_chg and inv_chg < 0 else "横ばい")}

    equity_ratio = d.get("equity_ratio")
    equity_ratio_5y = d.get("equity_ratio_5y")
    debt_ratio = 100 - equity_ratio if equity_ratio is not None else None
    debt_ratio_5y = 100 - equity_ratio_5y if equity_ratio_5y is not None else None
    eq_chg = equity_ratio - equity_ratio_5y if (equity_ratio is not None and equity_ratio_5y is not None) else None
    dr_chg = debt_ratio - debt_ratio_5y if (debt_ratio is not None and debt_ratio_5y is not None) else None
    tree["自己資本比率"] = {"現在値": equity_ratio, "5年変化": eq_chg, "評価": "改善" if eq_chg and eq_chg > 0 else ("悪化" if eq_chg and eq_chg < 0 else "横ばい")}
    tree["負債比率"] = {"現在値": debt_ratio, "5年変化": dr_chg, "評価": "改善" if dr_chg and dr_chg < 0 else ("悪化" if dr_chg and dr_chg > 0 else "横ばい")}

    # ROE Contribution ranking (DuPont factors)
    roe_contributions = {}
    if npm_chg is not None and at is not None and em is not None:
        roe_contributions["純利益率"] = npm_chg * 0.01 * (at_5y or at) * (em_5y or em) * 100
    if at_chg is not None and npm is not None and em is not None:
        roe_contributions["総資産回転率"] = at_chg * (npm_5y or npm) * (em_5y or em) * 100
    if em_chg is not None and npm is not None and at is not None:
        roe_contributions["財務レバレッジ"] = em_chg * (npm_5y or npm) * (at_5y or at) * 100

    ranked = sorted(
        [(k, v) for k, v in roe_contributions.items() if v is not None],
        key=lambda x: abs(x[1]), reverse=True
    )
    tree["貢献度ランキング"] = [{"順位": i + 1, "指標": k, "改善寄与度": v, "評価": "改善" if v > 0 else "悪化"} for i, (k, v) in enumerate(ranked)]

    return tree


def compute_pbr_contribution(roe_tree, screening, data, benchmark=None):
    """PBR = ROE × PER / 100 の変化要因を分解"""
    th = generate_dynamic_thresholds(benchmark)
    per_data = screening.get("C-1_PER", {})
    pbr_data = screening.get("C-2_PBR", {})
    per_now = per_data.get("実績値")
    pbr_now = pbr_data.get("実績値")

    per_5y = data.get("per_5y")
    pbr_5y = data.get("pbr_5y")

    # PBR フォールバック: PBR = ROE × PER / 100
    roe_info_pre = roe_tree.get("ROE", {})
    roe_now_pre = roe_info_pre.get("現在値")
    if pbr_now is None and per_now is not None and roe_now_pre is not None:
        pbr_now = round(roe_now_pre * per_now / 100, 2)
    if pbr_5y is None and per_5y is not None:
        roe_chg_pre = roe_info_pre.get("5年変化pt")
        roe_5y_pre = roe_now_pre - roe_chg_pre if (roe_now_pre is not None and roe_chg_pre is not None) else None
        if roe_5y_pre is not None:
            pbr_5y = round(roe_5y_pre * per_5y / 100, 2)

    per_chg = per_now - per_5y if (per_now is not None and per_5y is not None) else None
    pbr_chg = pbr_now - pbr_5y if (pbr_now is not None and pbr_5y is not None) else None

    roe_info = roe_tree.get("ROE", {})
    roe_now = roe_info.get("現在値")
    roe_chg = roe_info.get("5年変化pt")

    pbr_factors = {}
    roe_ranking = roe_tree.get("貢献度ランキング", [])
    roe_5y_val = roe_now - roe_chg if (roe_now is not None and roe_chg is not None) else None

    if per_5y is not None:
        for item in roe_ranking:
            name = item["指標"]
            roe_contrib = item["改善寄与度"]
            pbr_impact = roe_contrib * per_5y / 100
            pbr_factors[name] = {
                "pbr_impact": round(pbr_impact, 4),
                "roe_contrib": round(roe_contrib, 2),
                "category": "ROE要因",
            }

    if per_chg is not None and roe_5y_val is not None:
        per_pbr_impact = roe_5y_val * per_chg / 100
        if roe_chg is not None:
            per_pbr_impact += roe_chg * per_chg / 100
        pbr_factors["PER変動"] = {
            "pbr_impact": round(per_pbr_impact, 4),
            "roe_contrib": None,
            "category": "市場評価要因",
        }

    up_ranked = sorted(
        [(k, v) for k, v in pbr_factors.items() if v["pbr_impact"] > 0],
        key=lambda x: x[1]["pbr_impact"], reverse=True,
    )
    down_ranked = sorted(
        [(k, v) for k, v in pbr_factors.items() if v["pbr_impact"] < 0],
        key=lambda x: x[1]["pbr_impact"],
    )

    def build_list(ranked_items):
        return [
            {
                "順位": i + 1,
                "指標": name,
                "PBR寄与度": info["pbr_impact"],
                "ROE寄与度pt": info["roe_contrib"],
                "カテゴリ": info["category"],
            }
            for i, (name, info) in enumerate(ranked_items)
        ]

    pbr_lo = th["pbr_lo"]
    pbr_hi = th["pbr_hi"]
    if pbr_now is not None:
        if pbr_now < pbr_lo:
            pbr_eval = "割安"
        elif pbr_now < pbr_lo * 1.5:
            pbr_eval = "やや割安"
        elif pbr_now <= pbr_hi * 0.7:
            pbr_eval = "適正水準"
        elif pbr_now <= pbr_hi:
            pbr_eval = "やや割高"
        else:
            pbr_eval = "割高"
    else:
        pbr_eval = "データなし"

    return {
        "up_ranking": build_list(up_ranked),
        "down_ranking": build_list(down_ranked),
        "pbr_now": pbr_now,
        "pbr_5y": pbr_5y,
        "pbr_change": round(pbr_chg, 4) if pbr_chg is not None else None,
        "per_now": per_now,
        "per_5y": per_5y,
        "per_change": round(per_chg, 2) if per_chg is not None else None,
        "roe_now": roe_now,
        "roe_5y_change": roe_chg,
        "pbr_eval": pbr_eval,
        "pbr_range": f"{pbr_lo}～{pbr_hi}x",
    }
