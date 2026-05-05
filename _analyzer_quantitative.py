"""
定量分析モジュール — analyze_quantitative() のみを公開する。
依存: _analyzer_helpers, _analyzer_thresholds
"""
from _analyzer_helpers import safe_div, rate_change, consecutive_increase, get_latest_value
from _analyzer_thresholds import generate_evaluation_criteria


def analyze_quantitative(d, benchmark=None):
    results = {}
    evaluation_criteria = generate_evaluation_criteria(benchmark)
    rev = d.get("revenue")
    fcf = d.get("fcf")
    eps = d.get("eps")
    roe = d.get("roe")
    roa = d.get("roa")
    equity_ratio = d.get("equity_ratio")
    quick_ratio = d.get("quick_ratio")
    current_ratio = d.get("current_ratio")
    op_cf = d.get("operating_cf")
    op_margin = d.get("op_margin")
    ebitda_margin = d.get("ebitda_margin")
    debt_fcf = d.get("debt_fcf")
    ev = d.get("ev")
    nopat = d.get("nopat")
    nopat_5y = d.get("nopat_5y")
    invested_capital = d.get("invested_capital")
    invested_capital_5y = d.get("invested_capital_5y")
    wacc = d.get("wacc")
    ar = d.get("accounts_receivable")
    ar_5y = d.get("accounts_receivable_5y")
    inventory = d.get("inventory")
    inventory_5y = d.get("inventory_5y")
    ap = d.get("accounts_payable")
    ap_5y = d.get("accounts_payable_5y")
    rev_5y = rev[3] if rev and len(rev) > 3 else None
    cogs = d.get("cogs")
    cogs_5y = d.get("cogs_5y")

    # 売上高推移
    if rev and len(rev) >= 2:
        N = min(len(rev), 5)
        N_idx = N - 1
        latest_rev, rev_idx = get_latest_value(rev)
        data_warning = (rev_idx or 0) > 0
        rate = rate_change(latest_rev, rev[N_idx]) if (latest_rev is not None and rev[N_idx] is not None) else None
        consec = consecutive_increase(rev[:N])
        if rate is None:
            ev_rev = "×"
        elif rate >= 10 and consec:
            ev_rev = "◎"
        elif rate >= 5:
            ev_rev = "○"
        elif rate >= 0:
            ev_rev = "▲"
        else:
            ev_rev = "×"
        results["売上高推移"] = {
            "最新値": latest_rev, "5年変化率": rate, "_years": N,
            "3年変化率": rate_change(latest_rev, rev[min(2, N_idx)]) if latest_rev is not None else None,
            "3年連続増加": consecutive_increase(rev[:min(3, N)]),
            "5年連続増加": consec,
            "評価": ev_rev,
            "data_warning": data_warning,
        }
    elif rev and len(rev) == 1:
        latest_rev, _ = get_latest_value(rev)
        results["売上高推移"] = {"最新値": latest_rev, "評価": "未入力", "欠損理由": "時系列データが1期分のみ（比較不可）"}
    else:
        results["売上高推移"] = {"評価": "未入力", "欠損理由": "売上高データなし"}

    # FCF推移
    if fcf and len(fcf) >= 2:
        N = min(len(fcf), 5)
        N_idx = N - 1
        f0 = fcf[0]
        fN = fcf[N_idx]
        rate = rate_change(f0, fN) if (f0 is not None and fN is not None) else None
        consec = consecutive_increase(fcf[:N])
        if rate is None:
            ev_fcf = "×"
        elif rate >= 10 and consec:
            ev_fcf = "◎"
        elif rate >= 5:
            ev_fcf = "○"
        elif rate >= 0:
            ev_fcf = "▲"
        else:
            ev_fcf = "×"
        results["FCF推移"] = {
            "最新値": f0, "5年変化率": rate, "_years": N,
            "3年変化率": rate_change(f0, fcf[min(2, N_idx)]) if f0 is not None else None,
            "3年連続増加": consecutive_increase(fcf[:min(3, N)]),
            "5年連続増加": consec,
            "評価": ev_fcf,
        }
    elif fcf and len(fcf) == 1:
        results["FCF推移"] = {"最新値": fcf[0], "評価": "未入力", "欠損理由": "時系列データが1期分のみ（比較不可）"}
    else:
        results["FCF推移"] = {"評価": "未入力", "欠損理由": "FCFデータなし"}

    # EPS推移
    if eps and len(eps) >= 2 and eps[0] is not None:
        N = min(len(eps), 5)
        N_idx = N - 1
        # Find deepest valid comparison point
        while N_idx > 0 and eps[N_idx] is None:
            N_idx -= 1
        if N_idx == 0:
            # No valid base → only current
            results["EPS推移"] = {"最新値": eps[0], "評価": "未入力", "欠損理由": "時系列データが1期分のみ（比較不可）"}
        else:
            e0 = eps[0]
            e_base = eps[N_idx]
            N = N_idx + 1
            consec = consecutive_increase(eps[:N])
            if e_base > 0 and e0 > 0:
                rate = rate_change(e0, e_base)
                ev_eps = ("◎" if (rate is not None and rate >= 10 and consec) else
                          ("○" if (rate is not None and rate >= 5) else
                           ("▲" if (rate is not None and rate >= 0) else "×")))
            elif e_base < 0 and e0 > 0:
                rate = None
                ev_eps = "○"
            elif e_base < 0 and e0 < 0:
                rate = None
                ev_eps = "▲" if abs(e0) < abs(e_base) else "×"
            else:
                rate = None
                ev_eps = "×"
            results["EPS推移"] = {
                "最新値": e0, "5年変化率": rate, "_years": N,
                "3年変化率": rate_change(e0, eps[min(2, N_idx)]) if (e0 is not None and N_idx >= 2) else None,
                "3年連続増加": consecutive_increase(eps[:min(3, N)]),
                "5年連続増加": consec,
                "評価": ev_eps,
            }
    elif eps and len(eps) == 1 and eps[0] is not None:
        results["EPS推移"] = {"最新値": eps[0], "評価": "未入力", "欠損理由": "時系列データが1期分のみ（比較不可）"}
    else:
        results["EPS推移"] = {"評価": "未入力", "欠損理由": "EPSデータなし"}

    # Debt/FCF
    if debt_fcf is not None:
        ev_dfc = "○" if debt_fcf < 3 else ("▲" if debt_fcf < 5 else "×")
        results["Debt/FCF"] = {"現在値": debt_fcf, "評価": ev_dfc}
    else:
        results["Debt/FCF"] = {"評価": "未入力"}

    # EV/FCF
    fcf_latest = fcf[0] if fcf else None
    ev_fcf_ratio = safe_div(ev, fcf_latest)
    if ev_fcf_ratio is not None:
        ev_efcf = "○" if ev_fcf_ratio < 15 else ("▲" if ev_fcf_ratio < 25 else "×")
        results["EV/FCF"] = {"現在値": ev_fcf_ratio, "評価": ev_efcf}
    else:
        results["EV/FCF"] = {"評価": "未入力"}

    # ROE
    if roe and len(roe) >= 3:
        roe_now, roe_idx = get_latest_value(roe)
        roe_data_warning = (roe_idx or 0) > 0
        roe_3y, roe_5y_val = roe[1], roe[2]
        roe_growth = d.get("roe_growth_rate")
        c3 = roe_now > roe_3y if (roe_now is not None and roe_3y is not None) else None
        c5 = roe_now > roe_5y_val if (roe_now is not None and roe_5y_val is not None) else None
        rg = roe_growth if roe_growth is not None else (roe_now - roe_5y_val if roe_now and roe_5y_val else None)

        if roe_now is None:
            ev_roe = "未入力"
        elif roe_now < 0:
            ev_roe = "×"  # 赤字企業は常に×（ベンチマーク有無に関わらず）
        elif benchmark and "ROE" in evaluation_criteria:
            roe_median = evaluation_criteria["ROE"].get("業種中央値")
            if roe_median and roe_median > 0:
                roe_rel = roe_now / roe_median
                if roe_rel >= 1.5 and rg is not None and rg >= 5:
                    ev_roe = "◎"
                elif roe_rel >= 1.0 and rg is not None and rg >= 3:
                    ev_roe = "○"
                elif roe_rel >= 0.5:
                    ev_roe = "▲"
                else:
                    ev_roe = "×"
            else:
                # ベンチマークに中央値なし→絶対基準にフォールバック
                ev_roe = "◎" if (roe_now >= 15 and rg is not None and rg >= 5 and c3) else ("○" if (roe_now >= 10 and rg is not None and rg >= 3) else ("▲" if roe_now >= 5 else "×"))
        else:
            if roe_now >= 15 and rg is not None and rg >= 5 and c3:
                ev_roe = "◎"
            elif roe_now >= 10 and rg is not None and rg >= 3:
                ev_roe = "○"
            elif roe_now >= 0:
                ev_roe = "▲"
            else:
                ev_roe = "×"

        results["ROE"] = {
            "現在値": roe_now,
            "3年変化pt": roe_now - roe_3y if (roe_now is not None and roe_3y is not None) else None,
            "5年変化pt": roe_now - roe_5y_val if (roe_now is not None and roe_5y_val is not None) else None,
            "評価": ev_roe,
            "評価軸": evaluation_criteria.get("ROE", {}).get("評価軸", ""),
            "data_warning": roe_data_warning,
        }
    else:
        results["ROE"] = {"評価": "未入力"}

    # ROA
    if roa and len(roa) >= 3:
        roa_now, roa_idx = get_latest_value(roa)
        roa_data_warning = (roa_idx or 0) > 0
        roa_3y, roa_5y_val = roa[1], roa[2]
        ev_roa = "×"
        if roa_now is not None:
            if roa_now >= 15:
                ev_roa = "◎"
            elif roa_now >= 8:
                ev_roa = "○"
            elif roa_now >= 0:
                ev_roa = "▲"
            else:
                ev_roa = "×"
        results["ROA"] = {
            "現在値": roa_now,
            "3年変化pt": roa_now - roa_3y if (roa_now is not None and roa_3y is not None) else None,
            "5年変化pt": roa_now - roa_5y_val if (roa_now is not None and roa_5y_val is not None) else None,
            "評価": ev_roa,
            "評価軸": evaluation_criteria.get("ROA", {}).get("評価軸", ""),
            "data_warning": roa_data_warning,
        }
    else:
        results["ROA"] = {"評価": "未入力"}

    # 配当利回り
    div_yield = d.get("dividend_yield")
    div_yield_5y = d.get("dividend_yield_5y")
    if div_yield is not None:
        ev_dy = "◎" if div_yield >= 4 else ("○" if div_yield >= 2 else ("▲" if div_yield >= 1 else "×"))
        dy_chg = div_yield - div_yield_5y if div_yield_5y is not None else None
        results["配当利回り"] = {"現在値": div_yield, "5年変化pt": dy_chg, "評価": ev_dy}
    else:
        results["配当利回り"] = {"評価": "未入力"}

    # 配当性向
    payout = d.get("payout_ratio")
    payout_5y = d.get("payout_ratio_5y")
    if payout is not None:
        ev_po = "◎" if 20 <= payout <= 50 else ("○" if 10 <= payout <= 70 else ("▲" if 0 <= payout <= 100 else "×"))
        po_chg = payout - payout_5y if payout_5y is not None else None
        results["配当性向"] = {"現在値": payout, "5年変化pt": po_chg, "評価": ev_po}
    else:
        results["配当性向"] = {"評価": "未入力"}

    # 自己資本比率
    equity_ratio_5y = d.get("equity_ratio_5y")
    if equity_ratio is not None:
        ev_eq = "◎" if equity_ratio >= 50 else ("○" if equity_ratio >= 40 else ("▲" if equity_ratio >= 20 else "×"))
        eq_chg = equity_ratio - equity_ratio_5y if equity_ratio_5y is not None else None
        results["自己資本比率"] = {"現在値": equity_ratio, "5年変化pt": eq_chg, "評価": ev_eq}
    else:
        results["自己資本比率"] = {"評価": "未入力"}

    # 当座比率
    quick_ratio_5y = d.get("quick_ratio_5y")
    if quick_ratio is not None:
        ev_qr = "◎" if quick_ratio >= 150 else ("○" if quick_ratio >= 100 else ("▲" if quick_ratio >= 80 else "×"))
        qr_chg = quick_ratio - quick_ratio_5y if quick_ratio_5y is not None else None
        results["当座比率"] = {"現在値": quick_ratio, "5年変化pt": qr_chg, "評価": ev_qr}
    else:
        results["当座比率"] = {"評価": "未入力"}

    # 流動比率
    current_ratio_5y = d.get("current_ratio_5y")
    if current_ratio is not None:
        ev_cr = "◎" if current_ratio >= 200 else ("○" if current_ratio >= 100 else ("▲" if current_ratio >= 80 else "×"))
        cr_chg = current_ratio - current_ratio_5y if current_ratio_5y is not None else None
        results["流動比率"] = {"現在値": current_ratio, "5年変化pt": cr_chg, "評価": ev_cr}
    else:
        results["流動比率"] = {"評価": "未入力"}

    # 営業CF
    if op_cf and len(op_cf) >= 1:
        latest_cf = op_cf[0]
        ev_cf = "○" if latest_cf and latest_cf > 0 else "×"
        cf_result = {"最新値": latest_cf, "評価": ev_cf}
        if len(op_cf) >= 2:
            _N_cf = min(len(op_cf), 5)
            cf_result["5年変化率"] = rate_change(op_cf[0], op_cf[_N_cf - 1]) if op_cf[_N_cf - 1] is not None else None
            cf_result["_years"] = _N_cf
        results["営業CF"] = cf_result
    else:
        results["営業CF"] = {"評価": "未入力"}

    # 投資CF
    inv_cf = d.get("investing_cf", [])
    if inv_cf and len(inv_cf) >= 1:
        latest_inv = inv_cf[0]
        ev_inv = "○" if latest_inv is not None and latest_inv < 0 else "▲"
        inv_result = {"最新値": latest_inv, "評価": ev_inv}
        if len(inv_cf) >= 2:
            _N_iv = min(len(inv_cf), 5)
            _inv_base = inv_cf[_N_iv - 1]
            inv_result["5年変化率"] = rate_change(abs(inv_cf[0]) if inv_cf[0] else None, abs(_inv_base) if _inv_base else None)
            inv_result["_years"] = _N_iv
        results["投資CF"] = inv_result
    else:
        results["投資CF"] = {"評価": "未入力"}

    # 財務CF
    fin_cf = d.get("financing_cf", [])
    if fin_cf and len(fin_cf) >= 1:
        latest_fin = fin_cf[0]
        ev_fin = "○" if latest_fin is not None and latest_fin < 0 else "▲"
        fin_result = {"最新値": latest_fin, "評価": ev_fin}
        if len(fin_cf) >= 2:
            _N_fn = min(len(fin_cf), 5)
            _fin_base = fin_cf[_N_fn - 1]
            fin_result["5年変化率"] = rate_change(abs(fin_cf[0]) if fin_cf[0] else None, abs(_fin_base) if _fin_base else None)
            fin_result["_years"] = _N_fn
        results["財務CF"] = fin_result
    else:
        results["財務CF"] = {"評価": "未入力"}

    # CF構成分析 (営業+, 投資-, 財務- = 理想型)
    ocf_ok = op_cf and len(op_cf) >= 1 and op_cf[0] is not None and op_cf[0] > 0
    icf_ok = inv_cf and len(inv_cf) >= 1 and inv_cf[0] is not None and inv_cf[0] < 0
    fcf_ok = fin_cf and len(fin_cf) >= 1 and fin_cf[0] is not None and fin_cf[0] < 0
    cf_pattern_ideal = ocf_ok and icf_ok and fcf_ok
    results["CF構成分析"] = {
        "営業CF符号": "+" if ocf_ok else ("-" if op_cf and len(op_cf) >= 1 and op_cf[0] is not None else "N/A"),
        "投資CF符号": "-" if icf_ok else ("+" if inv_cf and len(inv_cf) >= 1 and inv_cf[0] is not None else "N/A"),
        "財務CF符号": "-" if fcf_ok else ("+" if fin_cf and len(fin_cf) >= 1 and fin_cf[0] is not None else "N/A"),
        "理想型": cf_pattern_ideal,
        "評価": "◎" if cf_pattern_ideal else "⚠",
    }

    # ROIC
    roic = safe_div(nopat, invested_capital)
    roic_5y = safe_div(nopat_5y, invested_capital_5y)
    if roic is not None:
        roic_pct = roic * 100
        ev_roic = "◎" if roic_pct >= 15 else ("○" if roic_pct >= 10 else ("▲" if roic_pct >= 0 else "×"))
        rev_latest = rev[0] if rev else None
        ic_turnover = safe_div(rev_latest, invested_capital)
        roic_chg = (roic - roic_5y) * 100 if roic_5y is not None else None
        results["ROIC"] = {
            "ROIC": roic_pct,
            "投下資本回転率": ic_turnover,
            "ROIC_5年変化pt": roic_chg,
            "評価": ev_roic,
        }
    else:
        results["ROIC"] = {"評価": "未入力"}

    # EBITDAマージン推移
    ebitda_margin_5y = d.get("ebitda_margin_5y")
    if ebitda_margin is not None:
        em_chg = ebitda_margin - ebitda_margin_5y if ebitda_margin_5y is not None else None
        results["EBITDAマージン"] = {"現在値": ebitda_margin, "5年変化pt": em_chg}

    # 営業利益率推移
    if op_margin and len(op_margin) >= 1:
        opm_now, opm_idx = get_latest_value(op_margin)
        opm_data_warning = (opm_idx or 0) > 0
        opm_5y = op_margin[4] if len(op_margin) >= 5 else None
        opm_chg = opm_now - opm_5y if (opm_now is not None and opm_5y is not None) else None
        results["営業利益率Q"] = {"現在値": opm_now, "5年変化pt": opm_chg, "data_warning": opm_data_warning}

    # Debt/FCF推移
    debt_fcf_5y = d.get("debt_fcf_5y")
    if debt_fcf is not None:
        df_chg = debt_fcf - debt_fcf_5y if debt_fcf_5y is not None else None
        results["Debt/FCF_detail"] = {"現在値": debt_fcf, "5年変化": df_chg}

    # WACC
    if wacc is not None:
        ev_wacc = "◎" if wacc <= 6 else ("○" if wacc <= 9 else ("▲" if wacc <= 12 else "×"))
        results["WACC"] = {"現在値": wacc, "評価": ev_wacc}
    else:
        results["WACC"] = {"評価": "未入力"}

    # ROIC vs WACC
    if roic is not None and wacc is not None:
        spread = roic * 100 - wacc
        ev_spread = "○" if spread > 0 else ("▲" if spread > wacc * (-0.2) else "×")
        results["ROIC_vs_WACC"] = {"スプレッド": spread, "評価": ev_spread}

    # CCC分析（業種別閾値）
    # 業種によりCCCの正常値が大きく異なるため、業種特性を反映した閾値を使用
    _industry = (d.get("industry") or "").lower()
    _is_finance = any(k in _industry for k in ["bank", "financial", "insurance", "finance", "金融", "銀行", "保険"])
    _is_retail = any(k in _industry for k in ["retail", "grocer", "小売", "スーパー", "コンビニ"])
    _is_pharma = any(k in _industry for k in ["pharma", "drug", "biotech", "医薬", "製薬", "バイオ"])
    # 業種別CCC閾値 (◎, ○, ▲ の上限日数)
    if _is_finance:
        # 金融業: CCC概念が適用外 → 評価を省略
        _ccc_thresholds = None
    elif _is_retail:
        _ccc_thresholds = (0, 20, 40)   # 小売: 非常に短い
    elif _is_pharma:
        _ccc_thresholds = (60, 120, 180)  # 医薬: 在庫期間が長い
    else:
        _ccc_thresholds = (30, 60, 90)   # 製造・サービス・技術: 標準

    rev_now = rev[0] if rev else None
    if _ccc_thresholds is None:
        # 金融業はCCC評価不要
        results["CCC分析"] = {"評価": "N/A", "欠損理由": "金融業にはCCCは適用されません"}
    elif all(v is not None for v in [ar, inventory, ap, rev_now, cogs]):
        dso = safe_div(ar, rev_now) * 365
        dio = safe_div(inventory, cogs) * 365
        dpo = safe_div(ap, rev_now) * 365
        ccc = dso + dio - dpo if (dso is not None and dio is not None and dpo is not None) else None
        dso_5y_v = safe_div(ar_5y, rev_5y) * 365 if (ar_5y and rev_5y) else None
        dio_5y_v = safe_div(inventory_5y, cogs_5y) * 365 if (inventory_5y and cogs_5y) else None
        dpo_5y_v = safe_div(ap_5y, rev_5y) * 365 if (ap_5y and rev_5y) else None
        ccc_5y = dso_5y_v + dio_5y_v - dpo_5y_v if (dso_5y_v is not None and dio_5y_v is not None and dpo_5y_v is not None) else None
        t_great, t_good, t_warn = _ccc_thresholds
        if ccc is None:
            ev_ccc = "未入力"
        elif ccc < t_great:
            ev_ccc = "◎"
        elif ccc < t_good:
            ev_ccc = "○"
        elif ccc < t_warn:
            ev_ccc = "▲"
        else:
            ev_ccc = "×"
        results["CCC分析"] = {
            "CCC": ccc, "CCC_5年変化": ccc - ccc_5y if ccc_5y else None,
            "DSO": dso, "DIO": dio, "DPO": dpo,
            "評価": ev_ccc,
        }
    else:
        results["CCC分析"] = {"評価": "未入力"}

    return results
