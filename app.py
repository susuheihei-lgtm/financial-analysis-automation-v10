"""
個別株式分析ウェブアプリケーション
Flask ベースのダッシュボード
"""
import logging
import os
import re
import secrets
import sys
import json
import pathlib
import tempfile
import traceback
import uuid

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# ── .env 読み込み（python-dotenv が無くても動作するよう try/except）──
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, '.env'))
except ImportError:
    pass

from flask import Flask, render_template, request, jsonify, session
from flask_caching import Cache
from analyzer import run_full_analysis, INDUSTRY_LIST, generate_dynamic_thresholds
from excel_parser import parse_excel, scan_available_metrics, extract_custom_timeseries
from yfinance_parser import parse_yfinance

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, 'templates'),
    static_folder=os.path.join(BASE_DIR, 'static'),
    instance_path=os.path.join(BASE_DIR, 'instance'),
)
app.config['MAX_CONTENT_LENGTH'] = 80 * 1024 * 1024  # 80MB (5 Excel files)

# ── C2: SECRET_KEY — 本番では必ず環境変数で設定。未設定時はセキュアなランダム値を生成。
_secret = os.environ.get('SECRET_KEY')
_is_debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
if not _secret:
    if not _is_debug:
        # 本番環境で SECRET_KEY が未設定の場合、起動を拒否する
        raise RuntimeError(
            "SECRET_KEY 環境変数が設定されていません。"
            "本番環境では `python -c \"import secrets; print(secrets.token_hex(32))\"` で生成した"
            "値を SECRET_KEY に設定してください。"
        )
    # 開発環境: 起動ごとにランダム生成（セッションはプロセス再起動で無効化される）
    _secret = secrets.token_hex(32)
    logging.warning("開発用: SECRET_KEY をランダム生成しました。本番では環境変数で固定してください。")
app.secret_key = _secret

# ── キャッシュ設定（REDIS_URL があれば Redis、なければメモリ）──────────────────
_redis_url = os.environ.get('REDIS_URL')
if _redis_url:
    app.config['CACHE_TYPE'] = 'RedisCache'
    app.config['CACHE_REDIS_URL'] = _redis_url
else:
    app.config['CACHE_TYPE'] = 'SimpleCache'
app.config['CACHE_DEFAULT_TIMEOUT'] = 3600  # 1時間
cache = Cache(app)

# ── L1: Cookieセキュリティフラグ ──────────────────────────────────────────────
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
# HTTPS環境では True に変更すること
app.config['SESSION_COOKIE_SECURE'] = not _is_debug

# ── H3: セキュリティレスポンスヘッダー ─────────────────────────────────────────
@app.after_request
def set_security_headers(response):
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    response.headers['Permissions-Policy'] = 'camera=(), microphone=(), geolocation=()'
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' https://cdn.tailwindcss.com https://cdn.jsdelivr.net "
        "https://cdnjs.cloudflare.com https://html2canvas.hertzen.com 'unsafe-inline'; "
        "style-src 'self' https://fonts.googleapis.com 'unsafe-inline'; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' https://images.unsplash.com data: blob:; "
        "connect-src 'self';"
    )
    return response

# ── C4: セッションに保存する一時ファイルの管理（パストラバーサル対策）──────────────
# ファイルパスをセッションに直接保存せず、サーバーサイドのUUID→パスマップで管理する
_temp_file_registry: dict[str, str] = {}  # {token: absolute_path}
_TEMP_DIR = tempfile.gettempdir()

def _register_temp_file(path: str) -> str:
    """一時ファイルパスをサーバーサイドに登録し、セッション用トークンを返す。"""
    token = str(uuid.uuid4())
    _temp_file_registry[token] = path
    return token

def _resolve_temp_file(token: str) -> str | None:
    """トークンから一時ファイルパスを取得。パストラバーサル検証付き。"""
    path = _temp_file_registry.get(token)
    if not path:
        return None
    # パストラバーサル防止: tempdir 配下のパスのみ許可
    try:
        resolved = str(pathlib.Path(path).resolve())
        if not resolved.startswith(_TEMP_DIR):
            logging.warning("不正なパスへのアクセス試行: %s", path)
            return None
    except Exception:
        return None
    return path if os.path.exists(path) else None

# ── H4: ティッカーシンボルのバリデーション ────────────────────────────────────
_TICKER_RE = re.compile(r'^[A-Z0-9.\-\^]{1,20}$', re.IGNORECASE)

def _validate_ticker(symbol: str) -> str | None:
    """ティッカーを検証。有効なら正規化したシンボルを返し、無効なら None を返す。"""
    s = symbol.strip().upper()
    if _TICKER_RE.match(s):
        return s
    return None

DATA_DIR = os.path.join(BASE_DIR, 'data')

# Load Damodaran industry data at startup
_damodaran_data = {}
_damodaran_path = os.path.join(DATA_DIR, 'damodaran_industry.json')
if os.path.exists(_damodaran_path):
    with open(_damodaran_path, 'r', encoding='utf-8') as _f:
        _damodaran_raw = json.load(_f)
        _damodaran_data = _damodaran_raw.get('industries', {})


_VALID_RATINGS = {'○', '▲', '×'}

def _validate_qualitative(val: str, fallback: str = '○') -> str:
    """d1/d2/d3の定性評価値が有効か検証し、無効なら fallback を返す。"""
    return val if val in _VALID_RATINGS else fallback


def load_sample_data():
    path = os.path.join(DATA_DIR, 'stock_data.json')
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _build_analysis_response(data, ts_data, benchmark, investor_profile):
    """共通の分析実行・レスポンス構築ヘルパー。"""
    try:
        result = run_full_analysis(data, benchmark=benchmark, investor_profile=investor_profile)
    except ValueError as e:
        # データ不足・型エラーなど想定内の失敗
        raise
    except Exception as e:
        app.logger.exception("run_full_analysis unexpected error: %s", e)
        raise RuntimeError("分析エンジンで予期しないエラーが発生しました") from e

    if ts_data:
        result['timeseries'] = ts_data
    if data.get('current_price') is not None:
        result['current_price'] = data['current_price']
    for key in ('analyst_recommendation', 'analyst_mean', 'analyst_count',
                'analyst_target_mean', 'analyst_target_high', 'analyst_target_low'):
        if data.get(key) is not None:
            result[key] = data[key]
    if benchmark:
        try:
            result['dynamic_thresholds'] = generate_dynamic_thresholds(benchmark, profile=investor_profile)
        except Exception as e:
            app.logger.warning("generate_dynamic_thresholds failed: %s", e)
    return result


# ── C3: 安全なエラーレスポンスヘルパー ───────────────────────────────────────
def _error_response(user_msg: str, status: int = 500, exc: Exception | None = None) -> tuple:
    """ユーザー向けメッセージと内部ログを分離する。スタックトレースはサーバー側のみ出力。"""
    if exc is not None:
        app.logger.exception("Internal error [%d]: %s", status, user_msg)
    return jsonify({'error': user_msg}), status


@app.route('/')
def index():
    return render_template('index.html', industries=INDUSTRY_LIST)


@app.route('/api/damodaran_industries')
def damodaran_industries():
    """Return list of industry names from Damodaran data."""
    names = sorted(_damodaran_data.keys())
    return jsonify(names)


@app.route('/api/industry_benchmark')
def industry_benchmark():
    """Return benchmark data for a given industry."""
    industry = request.args.get('industry', '')
    if industry in _damodaran_data:
        return jsonify(_damodaran_data[industry])
    return jsonify({'error': 'Industry not found'}), 404


@app.route('/api/analyze', methods=['POST'])
def analyze():
    ts_data = None

    try:
        if 'file' in request.files:
            f = request.files['file']
            if f.filename:
                ext = os.path.splitext(f.filename)[1].lower()
                if ext in ('.xlsx', '.xls'):
                    currency = request.form.get('currency', 'JPY')
                    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                        f.save(tmp.name)
                        try:
                            data, ts_data = parse_excel(tmp.name, currency=currency)
                        finally:
                            os.unlink(tmp.name)
                    company = request.form.get('company', '').strip()
                    ticker = request.form.get('ticker', '').strip()
                    industry = request.form.get('industry', '製造・サービス')

                    if company:
                        data['company'] = company
                    elif not data.get('company'):
                        data['company'] = os.path.splitext(f.filename)[0]

                    if ticker:
                        data['ticker'] = ticker
                    elif not data.get('ticker'):
                        match = re.search(r'^(\d{4,5})|[-_](\d{4,5})[-_]', f.filename)
                        if match:
                            data['ticker'] = match.group(1) or match.group(2)

                    data['industry'] = industry
                    data['d1_mgmt_change'] = _validate_qualitative(request.form.get('d1', '○'))
                    data['d2_ownership'] = _validate_qualitative(request.form.get('d2', '○'))
                    data['d3_esg'] = _validate_qualitative(request.form.get('d3', '○'))
                elif ext == '.json':
                    data = json.load(f)
                else:
                    return jsonify({'error': 'サポートされていないファイル形式です'}), 400
            else:
                return jsonify({'error': 'ファイルが選択されていません'}), 400
        elif request.is_json:
            data = request.get_json()
        else:
            return jsonify({'error': 'データが提供されていません'}), 400

        selected_industry = request.form.get('damodaran_industry', '')
        benchmark = _damodaran_data.get(selected_industry)
        investor_profile = request.form.get('investor_profile', 'balanced')

        response_data = _build_analysis_response(data, ts_data, benchmark, investor_profile)
        return jsonify(response_data)

    except ImportError as e:
        return _error_response('必要なライブラリが不足しています。管理者にお問い合わせください。', 500, e)
    except ValueError as e:
        return jsonify({'error': f'データ検証エラー: {str(e)}'}), 400
    except RuntimeError as e:
        return _error_response(str(e), 500, e)
    except Exception as e:
        err_msg = str(e)
        if 'openpyxl does not support' in err_msg or '.xls' in err_msg:
            return jsonify({'error': '.xlsファイルの読み込みに失敗しました。xlrdライブラリをインストールしてください。'}), 500
        return _error_response('分析中にエラーが発生しました。入力データを確認してください。', 500, e)


@app.route('/api/fetch_ticker', methods=['POST'])
def fetch_ticker():
    """ティッカーシンボルからyfinanceでデータを取得して分析"""
    body = request.get_json() or {}
    raw_symbol = body.get('ticker', '').strip()

    if not raw_symbol:
        return jsonify({'error': 'ティッカーシンボルを入力してください'}), 400

    # H4: ティッカー入力バリデーション
    symbol = _validate_ticker(raw_symbol)
    if not symbol:
        return jsonify({
            'error': '無効なティッカー形式です。'
                     '日本株は "7203.T"、米国株は "AAPL" のように入力してください（英数字・ドット・ハイフンのみ）。'
        }), 400

    # キャッシュから返す（同一ティッカーは1時間再利用）
    cache_key = f'ticker:{symbol}'
    cached = cache.get(cache_key)
    if cached:
        return jsonify(cached)

    try:
        data, ts_data = parse_yfinance(symbol)

        industry = body.get('industry', '')
        if industry:
            data['industry'] = industry
        data['d1_mgmt_change'] = _validate_qualitative(body.get('d1', '○'))
        data['d2_ownership'] = _validate_qualitative(body.get('d2', '○'))
        data['d3_esg'] = _validate_qualitative(body.get('d3', '○'))

        damodaran_industry = body.get('damodaran_industry', '')
        benchmark = _damodaran_data.get(damodaran_industry)
        investor_profile = body.get('investor_profile', 'balanced')

        return jsonify(_build_analysis_response(data, ts_data, benchmark, investor_profile))

    except ValueError as e:
        err_msg = str(e)
        hint = (
            '日本株は "7203.T"（末尾に .T）、米国株は "AAPL" のように入力してください。'
            'ティッカーシンボルは Yahoo Finance で確認できます。'
        )
        if 'データを取得できません' in err_msg or 'No data' in err_msg:
            return jsonify({'error': f'"{symbol}" のデータが見つかりませんでした。{hint}'}), 400
        return jsonify({'error': f'データ検証エラー: {err_msg}'}), 400

    except RuntimeError as e:
        # _build_analysis_response が wrap した分析エンジンエラー
        return _error_response(str(e), 500, e)

    except Exception as e:
        err_msg = str(e).lower()
        if any(kw in err_msg for kw in ('no data', 'delisted', 'not found', '404')):
            return jsonify({
                'error': (
                    f'"{symbol}" のデータが見つかりません。'
                    '上場廃止・シンボル変更の可能性があります。'
                    '日本株は末尾に ".T"（例: 7203.T）、米国株はそのまま（例: AAPL）。'
                )
            }), 400
        if 'too many requests' in err_msg or 'rate limit' in err_msg or '429' in err_msg:
            return jsonify({
                'error': 'データ取得の上限に達しました（yfinanceレート制限）。1〜2分後に再試行してください。'
            }), 429
        if 'timeout' in err_msg or 'connection' in err_msg:
            return jsonify({
                'error': 'データ取得がタイムアウトしました。しばらく待ってから再試行してください。'
            }), 503
        return _error_response(
            f'"{symbol}" の分析中にエラーが発生しました。しばらく待ってから再度お試しください。',
            500, e
        )


@app.route('/api/sample')
def sample():
    data = load_sample_data()
    excel_path = os.path.join(DATA_DIR, '6269-financials.xlsx')
    ts_data = None
    if os.path.exists(excel_path):
        _, ts_data = parse_excel(excel_path)
    selected_industry = request.args.get('damodaran_industry', '')
    benchmark = _damodaran_data.get(selected_industry)
    investor_profile = request.args.get('investor_profile', 'balanced')
    return jsonify(_build_analysis_response(data, ts_data, benchmark, investor_profile))


@app.route('/api/competitor_analyze', methods=['POST'])
def competitor_analyze():
    """最大5社のExcel/ティッカーを受け取り、比較分析データを返す"""
    try:
        slot_types = request.form.getlist('types[]')

        # M1: サーバーサイドで最大件数を強制
        if len(slot_types) > 5:
            return jsonify({'error': '比較できるのは最大5社までです'}), 400

        names = request.form.getlist('names[]')
        tickers = request.form.getlist('tickers[]')
        files = request.files.getlist('files[]')
        industry = request.form.get('damodaran_industry', '')
        benchmark = _damodaran_data.get(industry)
        investor_profile = request.form.get('investor_profile', 'balanced')

        companies = []
        file_idx = 0
        ticker_idx = 0

        for i, slot_type in enumerate(slot_types):
            name = names[i] if i < len(names) and names[i].strip() else ''

            if slot_type == 'ticker':
                raw_sym = tickers[ticker_idx] if ticker_idx < len(tickers) else ''
                ticker_idx += 1
                symbol = _validate_ticker(raw_sym)
                if not symbol:
                    continue
                data, ts_data = parse_yfinance(symbol)
                if not name:
                    name = data.get('company', symbol)
            else:
                if file_idx >= len(files):
                    continue
                f = files[file_idx]
                file_idx += 1
                if not f.filename:
                    continue
                ext = os.path.splitext(f.filename)[1].lower()
                if ext not in ('.xlsx', '.xls'):
                    continue
                comp_currency = request.form.get('currency', 'JPY')
                with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                    f.save(tmp.name)
                    try:
                        data, ts_data = parse_excel(tmp.name, currency=comp_currency)
                    finally:
                        os.unlink(tmp.name)
                if not name:
                    name = os.path.splitext(f.filename)[0]

            data['industry'] = industry or ''
            result = run_full_analysis(data, benchmark=benchmark, investor_profile=investor_profile)

            companies.append({
                'name': name,
                'timeseries': ts_data,
                'screening': result.get('screening', {}),
                'quantitative': result.get('quantitative', {}),
            })

        if len(companies) < 2:
            return jsonify({'error': '比較には2社以上が必要です'}), 400

        resp = {'companies': companies}
        if benchmark:
            resp['dynamic_thresholds'] = generate_dynamic_thresholds(benchmark, profile=investor_profile)
        return jsonify(resp)
    except ValueError as e:
        return jsonify({'error': 'データが無効です。入力内容を確認してください。'}), 400
    except Exception as e:
        return _error_response('比較分析中にエラーが発生しました。', 500, e)


@app.route('/api/scan_metrics', methods=['POST'])
def scan_metrics():
    """アップロードされたExcelから可視化可能なメトリクスをスキャン"""
    if 'file' not in request.files:
        return jsonify({'error': 'ファイルが提供されていません'}), 400
    f = request.files['file']
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in ('.xlsx', '.xls'):
        return jsonify({'error': 'Excelファイルのみ対応'}), 400

    tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
    tmp_path = tmp.name
    f.save(tmp_path)
    tmp.close()

    try:
        metrics = scan_available_metrics(tmp_path)
        # C4: パスではなくUUIDトークンをセッションに保存
        token = _register_temp_file(tmp_path)
        session['excel_token'] = token
        return jsonify({'metrics': metrics})
    except Exception as e:
        # M4: エラー時も一時ファイルを削除
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        return _error_response('メトリクスのスキャン中にエラーが発生しました。', 500, e)


@app.route('/api/custom_analysis', methods=['POST'])
def custom_analysis():
    """選択されたメトリクスのカスタム分析データを返す"""
    body = request.get_json()
    selected = body.get('selected', [])
    if not selected:
        return jsonify({'error': '指標が選択されていません'}), 400

    # C4: セッションのトークン経由でパスを解決（パストラバーサル検証済み）
    token = session.get('excel_token')
    excel_path = _resolve_temp_file(token) if token else None

    if not excel_path:
        # フォールバック: サンプルデータ
        excel_path = os.path.join(DATA_DIR, '6269-financials.xlsx')
        if not os.path.exists(excel_path):
            return jsonify({'error': 'Excelファイルが見つかりません'}), 400

    try:
        ts = extract_custom_timeseries(excel_path, selected)
        return jsonify({'timeseries': ts, 'selected': selected})
    except Exception as e:
        return _error_response('カスタム分析中にエラーが発生しました。', 500, e)


@app.route('/api/scan_sample')
def scan_sample():
    """サンプルExcelのメトリクスをスキャン"""
    excel_path = os.path.join(DATA_DIR, '6269-financials.xlsx')
    if not os.path.exists(excel_path):
        return jsonify({'metrics': []})
    try:
        metrics = scan_available_metrics(excel_path)
        return jsonify({'metrics': metrics})
    except Exception as e:
        return _error_response('サンプルスキャン中にエラーが発生しました。', 500, e)


if __name__ == '__main__':
    os.chdir(BASE_DIR)
    # C1: debug は環境変数 FLASK_DEBUG で制御（デフォルト: off）
    app.run(
        debug=_is_debug,
        port=int(os.environ.get("PORT", 5050)),
        load_dotenv=False,
    )
