"""
スクリーニング分析モジュール — analyze_screening() のみを公開する。
依存: _analyzer_helpers, _analyzer_thresholds
"""
from _analyzer_helpers import get_latest_value
from _analyzer_thresholds import generate_dynamic_thresholds, INVESTOR_PROFILES


def analyze_screening(d, q_results, benchmark=None, investor_profile='balanced'):
    th = generate_dynamic_thresholds(benchmark, profile=investor_profile)
    prof = INVESTOR_PROFILES.get(investor_profile, INVESTOR_PROFILES['balanced'])
    weights = prof['weights']
    verdict_cfg = prof['verdict']
    immediate_sell_xs = prof['immediate_sell_xs']

    avg_roe = None
    if benchmark:
        avg_roe = benchmark.get('roe_japan')
        if avg_roe is None:
            avg_roe = benchmark.get('roe_global')
        if avg_roe is not None:
            avg_roe = avg_roe * 100

    results = {}

    # Section A: 財務健全性
    eq_r = d.get("equity_ratio")
    a1 = "未入力" if eq_r is None else ("×" if eq_r < th["equity_ratio_x"] else ("▲" if eq_r < th["equity_ratio_tri"] else "○"))
    a1_basis = f"○≧{th['equity_ratio_tri']}% ▲≧{th['equity_ratio_x']}% ×<{th['equity_ratio_x']}%（業界平均の30%/60%基準）"
    results["A-1_自己資本比率"] = {"実績値": eq_r, "閾値_x": th["equity_ratio_x"], "閾値_tri": th["equity_ratio_tri"], "判定": a1, "基準": a1_basis}

    op_cf = d.get("operating_cf", [])
    if not op_cf:
        a2, a2_val = "未入力", None
    else:
        cf0, cf1, cf2 = (op_cf + [None, None, None])[:3]
        if cf0 is None:
            a2, a2_val = "未入力", None
        elif cf0 > 0:
            a2, a2_val = "○", "最新プラス"
        elif cf0 < 0 and cf1 is not None and cf1 < 0 and cf2 is not None and cf2 < 0:
            a2, a2_val = "×", "3期連続マイナス"
        elif cf0 < 0 and cf1 is not None and cf1 < 0:
            a2, a2_val = "▲", "前年2期マイナス"
        else:
            a2, a2_val = "▲", "約1期マイナス"
    a2_basis = "○=最新期プラス ▲=1-2期マイナス ×=3期連続マイナス"
    results["A-2_営業CF"] = {"状況": a2_val, "判定": a2, "基準": a2_basis}

    roe = d.get("roe", [])
    roa_d = d.get("roa", [])
    roe_now = roe[0] if roe else None
    roa_now = roa_d[0] if roa_d else None
    if roe_now is None:
        a3 = "未入力"
    elif avg_roe is not None:
        if roe_now < 0:
            a3 = "×"
        elif roe_now < avg_roe * 0.5:
            a3 = "▲"
        else:
            a3 = "○"
    else:
        if roe_now is not None and roa_now is not None:
            if roe_now < 0 and roa_now < 0:
                a3 = "×"
            elif roe_now < 0 or roa_now < 0:
                a3 = "▲"
            else:
                a3 = "○"
        else:
            a3 = "未入力"
    if avg_roe is not None:
        a3_basis = f"○≧業界平均の50%({avg_roe*0.5:.1f}%) ▲<{avg_roe*0.5:.1f}% ×=マイナス（業界平均ROE: {avg_roe:.1f}%）"
    else:
        a3_basis = "○=ROEプラス ▲=一方マイナス ×=両方マイナス"
    results["A-3_ROE_ROA"] = {"ROE最新": roe_now, "ROA最新": roa_now, "判定": a3, "基準": a3_basis}

    a_keys = ["A-1_自己資本比率", "A-2_営業CF", "A-3_ROE_ROA"]
    a_xs = sum(1 for k in a_keys if results[k]["判定"] == "×")
    a_tris = sum(1 for k in a_keys if results[k]["判定"] == "▲")
    if a_xs >= immediate_sell_xs:
        a_eval = "×NG（即時回避）"
    elif a_tris >= 2:
        a_eval = "▲要注意"
    elif a_tris >= 1:
        a_eval = "▲要確認"
    else:
        a_eval = "◎全項通過"
    a_score = (a_xs * 1 + a_tris * 0.5) * weights['A']
    results["SectionA評価"] = a_eval
    results["SectionAスコア"] = a_score

    # Section B: 成長性・収益性
    rev = d.get("revenue", [])
    if len(rev) >= 3 and rev[0] and rev[2]:
        cagr3 = (rev[0] / rev[2]) ** (1 / 3) - 1
        cagr3_pct = cagr3 * 100
        b1 = "×" if cagr3_pct < th["revenue_cagr_x"] else ("▲" if cagr3_pct < th["revenue_cagr_tri"] else "○")
    else:
        cagr3_pct, b1 = None, "未入力"
    b1_basis = f"○≧{th['revenue_cagr_tri']}% ▲≧{th['revenue_cagr_x']}% ×<{th['revenue_cagr_x']}%（業界期待成長率の±50%基準）"
    results["B-1_売上高CAGR"] = {"3年CAGR": cagr3_pct, "判定": b1, "基準": b1_basis}

    em = d.get("ebitda_margin")
    b2 = "未入力" if em is None else ("×" if em < th["ebitda_margin_x"] else ("▲" if em < th["ebitda_margin_tri"] else "○"))
    b2_basis = f"○≧{th['ebitda_margin_tri']}% ▲≧{th['ebitda_margin_x']}% ×<{th['ebitda_margin_x']}%（業界平均の30%/60%基準）"
    results["B-2_EBITDAマージン"] = {"実績値": em, "閾値_x": th["ebitda_margin_x"], "閾値_tri": th["ebitda_margin_tri"], "判定": b2, "基準": b2_basis}

    op_m = d.get("op_margin", [])
    if len(op_m) >= 3:
        op_now, op_idx = get_latest_value(op_m)
        op_data_warning = (op_idx or 0) > 0
        op_1y, op_2y = op_m[1], op_m[2]
        if op_now is not None and op_1y is not None and op_2y is not None:
            if op_now < op_1y and op_1y < op_2y:
                b3_val, b3 = "連続低下（3期）", "×"
            elif op_now < th["op_margin_x"]:
                b3_val, b3 = op_now, "×"
            elif op_now < th["op_margin_tri"]:
                b3_val, b3 = op_now, "▲"
            else:
                b3_val, b3 = op_now, "○"
        else:
            b3_val, b3 = None, "未入力"
    else:
        b3_val, b3 = None, "未入力"
        op_data_warning = False
    b3_basis = f"○≧{th['op_margin_tri']}% ▲≧{th['op_margin_x']}% ×<{th['op_margin_x']}%または3期連続低下（業界平均の30%/60%基準）"
    results["B-3_営業利益率"] = {"状況": b3_val, "判定": b3, "基準": b3_basis, "data_warning": op_data_warning}

    eps = d.get("eps", [])
    eps_gr = None
    if len(eps) >= 4 and eps[0] is not None and eps[3] is not None:
        e0, e3 = eps[0], eps[3]
        if e3 > 0 and e0 > 0:
            eps_gr = (e0 / e3 - 1) * 100
            b4 = "×" if eps_gr < th["eps_growth_x"] else ("▲" if eps_gr < th["eps_growth_tri"] else "○")
        elif e3 < 0 and e0 > 0:
            # 赤字→黒字転換: ポジティブだがCAGR計算不可
            eps_gr = None
            b4 = "○"
        elif e3 < 0 and e0 < 0:
            # 両方赤字: 損失縮小なら▲、拡大なら×
            b4 = "▲" if abs(e0) < abs(e3) else "×"
        elif e3 > 0 and e0 <= 0:
            # 黒字→赤字転換: 即×
            eps_gr = None
            b4 = "×"
        else:
            b4 = "未入力"
    else:
        b4 = "未入力"
    b4_basis = f"○≧{th['eps_growth_tri']}% ▲≧{th['eps_growth_x']}% ×<{th['eps_growth_x']}%（業界期待成長率の±50%基準）"
    results["B-4_EPS成長率"] = {"5年成長率": eps_gr, "判定": b4, "基準": b4_basis}

    b_keys = ["B-1_売上高CAGR", "B-2_EBITDAマージン", "B-3_営業利益率", "B-4_EPS成長率"]
    b_xs = sum(1 for k in b_keys if results[k]["判定"] == "×")
    b_tris = sum(1 for k in b_keys if results[k]["判定"] == "▲")
    b_eval = "×投資魅力低下" if b_xs >= 2 else ("▲要追加調査" if b_xs >= 1 else ("▲要注意" if b_tris >= 2 else "◎全項通過"))
    b_score = (b_xs * 1 + b_tris * 0.5) * weights['B']
    results["SectionB評価"] = b_eval
    results["SectionBスコア"] = b_score

    # Section C: バリュエーション・リスク
    per = d.get("per")
    pbr = d.get("pbr")
    nd_ebitda = d.get("nd_ebitda")
    cur_r = d.get("current_ratio")

    if per is None:
        c1 = "未入力"
    elif per < 0:
        c1 = "×"  # 赤字企業（EPS < 0）: PERは定義不能
    elif per > th["per_hi"] or per < th["per_lo"]:
        c1 = "×"
    elif per > th["per_hi"] * 0.8 or per < th["per_lo"] * 1.5:
        c1 = "▲"
    else:
        c1 = "○"
    c1_basis = f"○={th['per_lo']}～{th['per_hi']}x ▲=やや範囲外 ×<{th['per_lo']}xまたは>{th['per_hi']}x（EPS<0の場合は×）"
    results["C-1_PER"] = {"実績値": per, "閾値_上限": th["per_hi"], "閾値_下限": th["per_lo"], "判定": c1, "基準": c1_basis}

    c2 = "未入力" if pbr is None else ("×" if (pbr > th["pbr_hi"] or pbr < th["pbr_lo"]) else ("▲" if (pbr > th["pbr_hi"] * 0.7 or pbr < th["pbr_lo"] * 1.5) else "○"))
    c2_basis = f"○={th['pbr_lo']}～{th['pbr_hi']}x ▲=やや範囲外 ×<{th['pbr_lo']}xまたは>{th['pbr_hi']}x（業界平均PBRの0.15～2.5倍基準）"
    results["C-2_PBR"] = {"実績値": pbr, "閾値_上限": th["pbr_hi"], "閾値_下限": th["pbr_lo"], "判定": c2, "基準": c2_basis}

    c3 = "未入力" if nd_ebitda is None else ("×" if nd_ebitda > th["nd_ebitda_x"] else ("▲" if nd_ebitda > th["nd_ebitda_tri"] else "○"))
    c3_basis = f"○≦{th['nd_ebitda_tri']}x ▲≦{th['nd_ebitda_x']}x ×>{th['nd_ebitda_x']}x（業界平均の1.5/2.5倍基準）"
    results["C-3_NetDebt_EBITDA"] = {"実績値": nd_ebitda, "閾値_x": th["nd_ebitda_x"], "閾値_tri": th["nd_ebitda_tri"], "判定": c3, "基準": c3_basis}

    c4 = "未入力" if cur_r is None else ("×" if cur_r < th["current_ratio_x"] else ("▲" if cur_r < th["current_ratio_tri"] else "○"))
    c4_basis = f"○≧{th['current_ratio_tri']}% ▲≧{th['current_ratio_x']}% ×<{th['current_ratio_x']}%"
    results["C-4_流動比率"] = {"実績値": cur_r, "閾値_x": th["current_ratio_x"], "閾値_tri": th["current_ratio_tri"], "判定": c4, "基準": c4_basis}

    c_keys = ["C-1_PER", "C-2_PBR", "C-3_NetDebt_EBITDA", "C-4_流動比率"]
    c_xs = sum(1 for k in c_keys if results[k]["判定"] == "×")
    c_tris = sum(1 for k in c_keys if results[k]["判定"] == "▲")
    c_eval = "×タイミング要見直し" if c_xs >= 3 else ("▲要コスト評価" if c_xs >= 1 else ("▲要注意" if c_tris >= 2 else "◎全項通過"))
    c_score = (c_xs * 1 + c_tris * 0.5) * weights['C']
    results["SectionC評価"] = c_eval
    results["SectionCスコア"] = c_score

    # Section D: 定性・ESG
    d1 = d.get("d1_mgmt_change", "未入力")
    d2 = d.get("d2_ownership", "未入力")
    d3 = d.get("d3_esg", "未入力")
    d1_basis = "○=安定経営陣 ▲=一部変更 ×=大幅刷新・不祥事"
    results["D-1_経営陣変更"] = {"判定": d1, "基準": d1_basis}
    d2_basis = "○=安定株主構成 ▲=一部変動 ×=敵対的買収・大量売却"
    results["D-2_株主構造"] = {"判定": d2, "基準": d2_basis}
    d3_basis = "○=ESGリスクなし ▲=軽微なリスク ×=重大なESG・規制リスク"
    results["D-3_ESG"] = {"判定": d3, "基準": d3_basis}

    d_xs = sum(1 for v in [d1, d2, d3] if v == "×")
    d_tris = sum(1 for v in [d1, d2, d3] if v == "▲")
    d_eval = "×重要リスク確認" if d_xs >= 2 else ("▲リスク注意" if d_xs >= 1 else ("▲要確認" if d_tris >= 1 else "◎リスクなし"))
    d_score = (d_xs * 1 + d_tris * 0.5) * weights['D']
    results["SectionD評価"] = d_eval
    results["SectionDスコア"] = d_score

    # 最終投資判定
    total_score = a_score + b_score + c_score + d_score
    if a_xs >= immediate_sell_xs or total_score >= verdict_cfg['sell']:
        final = "SELL"
    elif total_score <= verdict_cfg['buy']:
        final = "BUY"
    else:
        final = "HOLD"
    results["総合スコア"] = total_score
    results["最終投資判定"] = final
    results["適用プロファイル"] = investor_profile

    return results
