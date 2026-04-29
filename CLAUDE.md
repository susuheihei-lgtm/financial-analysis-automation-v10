# Financial Analysis Automation v5

Flask製の個別株式分析ウェブアプリ。ティッカー入力またはExcelアップロードで財務データを取得し、スコアリング・チャート・競合比較を1画面で提供。

## Commands

| Command | Description |
|---------|-------------|
| `.venv/bin/python3 app.py` | 開発サーバー起動（port 5050） |
| `.venv/bin/python3 -m pip install -r requirements.txt` | 依存パッケージインストール |
| `lsof -i :5050 -t \| xargs kill -9` | ポート5050を強制解放 |
| `git push origin main` | GitHub v5リポジトリへpush |

## Architecture

```
v5/
  app.py                    # Flaskルート定義（API + セッション管理）
  analyzer.py               # 分析エンジン（薄いオーケストレーター）
  _analyzer_helpers.py      # 共通ユーティリティ（safe_div, rate_change等）
  _analyzer_quantitative.py # 定量スコアリングロジック
  _analyzer_screening.py    # スクリーニング判定
  _analyzer_thresholds.py   # 投資家プロファイル・しきい値定義
  _analyzer_trees.py        # ROA/ROEツリー分解
  yfinance_parser.py        # yfinance + SEC EDGAR + EDINET + IR BANK データ取得
  edinet_parser.py          # EDINET XBRL パーサー（日本株10年分）
  irbank_parser.py          # IR BANK CSV パーサー（日本株4〜5年分、EDINET非設定時）
  excel_parser.py           # Excelファイルパーサー（.xls/.xlsx、日本語対応）
  templates/index.html      # シングルページアプリ（HTML/CSS/JS）
  static/css/, static/js/   # 静的アセット
  data/                     # サンプルExcelデータ / EDINETキャッシュ / IR BANKキャッシュ
  .env                      # ローカル環境変数（gitignore済み）
```

## Key Files

- `app.py:178` — `/api/analyze` メインエンドポイント（POST, Excel or tickerデータ受け取り）
- `app.py:240` — `/api/fetch_ticker` yfinance/EDGAR でティッカー情報取得
- `yfinance_parser.py:645` — `parse_yfinance()` エントリポイント
- `yfinance_parser.py:218` — `_get_sec_annual_series()` SEC EDGAR年次データ取得
- `_analyzer_thresholds.py` — 投資家プロファイル5種（conservative/balanced/growth/income/aggressive）
- `templates/index.html` — V0エディトリアルデザイン（Bebas Neue + IBM Plex Mono + #D4852A）

## Environment

```bash
# .env（必須 — ないとSECRET_KEY RuntimeErrorで起動不可）
FLASK_DEBUG=true
SECRET_KEY=<32バイトhex>    # python -c "import secrets; print(secrets.token_hex(32))"
EDINET_API_KEY=<APIキー>    # 任意。設定すると日本株が10年分取得可能
                             # 無料取得: https://disclosure.edinet-fsa.go.jp/
```

- `python-dotenv` が venv にインストールされている必要あり（`pip install python-dotenv`）
- `PORT` 環境変数でポート変更可能（デフォルト: 5050）

## Gotchas

- **yfinance は年次データ4年まで**の仕様制限。財務諸表は SEC/IR BANK が代替、市場データは yfinance 必須
- **米国株**: SEC EDGAR（10年分）→ yfinance（市場データ補完）
- **日本株**: IR BANK（4〜5年分、自動DL）→ yfinance（市場データ補完）
- **EDINET**: APIキー設定済みだが接続問題のため現在スキップ。IR BANK がフォールバック
- **SEC EDGARは米国株のみ**。日本株（`.T`）はIR BANK / yfinance
- **AU（AngloGold）はSEC EDGARにus-gaapデータなし** — yfinanceフォールバックで処理される
- **`major_holders` フォーマット変更（yfinance新版）**: string index + 'Value'列。旧コードの`iloc[str, 1]`はクラッシュする — `_assess_ownership()`で対応済み
- **起動前にObsidianを開く必要あり** — MCP Tools（REST API Plugin）がObsidian起動を要求
- **venvのpythonを使うこと** — システムpythonではパッケージが見つからない
- **テンプレートは`templates/index.html`の1ファイルのみ** — CSS/JSもすべてインライン

## Design System (V0 Editorial)

- フォント: `Bebas Neue`（見出し）、`IBM Plex Mono`（ラベル/ボタン）
- アクセントカラー: `#D4852A`（CSS変数: `--accent-v0`）
- コーナー: `border-radius: 0`（角丸禁止）
- 背景: グリッドパターン60px + ノイズオーバーレイ

## Chrome MCP 操作ルール

- Chrome MCP（`mcp__Claude_in_Chrome__*`）が起動しても、**エラー再現のためにブラウザを自動操作してはならない**
- 起動後は必ずユーザーの操作を待ち、ユーザーが該当画面に遷移するまで保留する
- ユーザーから「この画面を確認して」「今の画面を見て」など明示的な許可が出た後に、現在表示されている画面上で調査を開始する
- `navigate`, `form_input`, `shortcuts_execute` など画面遷移・入力を伴うツールは、ユーザーの明示的指示なしに使用禁止

## GitHub

- v5: `https://github.com/susuheihei-lgtm/financial-analysis-app-v5`
- v4: `https://github.com/susuheihei-lgtm/financial-analysis-app-v4`（v5と同内容、Render.comデプロイ用）
