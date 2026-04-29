"""
汎用ヘルパー関数 — analyzer内のどのモジュールからも依存なしでインポート可能。
"""


def safe_div(a, b):
    try:
        if b is None or b == 0:
            return None
        return a / b
    except (TypeError, ZeroDivisionError):
        return None


def rate_change(current, base):
    r = safe_div(current, base)
    if r is None:
        return None
    return (r - 1) * 100


def consecutive_increase(vals):
    """データが新しい順（vals[0]=最新）に並んでいることを前提に、
    時系列が連続して増加しているか（最新 > 前年 > 前々年…）を確認する。
    """
    clean = [v for v in vals if v is not None]
    if len(clean) < 2:
        return None
    return all(clean[i] > clean[i + 1] for i in range(len(clean) - 1))


def get_latest_value(vals):
    """配列から最初の非None値を返す（最新利用可能データ）。
    Returns: (value, index) — index > 0 の場合は最新年のデータが欠損。
    """
    if not vals:
        return None, None
    for idx, v in enumerate(vals):
        if v is not None:
            return v, idx
    return None, None
