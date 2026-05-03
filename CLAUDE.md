# Financial Analysis Automation v10

Flask製の個別株式分析ウェブアプリ。v9ベース。ティッカー入力またはExcelアップロードで財務データを取得し、スコアリング・チャート・競合比較を1画面で提供。

## Commands

| Command | Description |
|---------|-------------|
| `.venv/bin/python3 app.py` | 開発サーバー起動（port 5050） |
| `.venv/bin/python3 -m pip install -r requirements.txt` | 依存パッケージインストール |
| `lsof -i :5050 -t \| xargs kill -9` | ポート5050を強制解放 |
| `git push origin main` | GitHub v10リポジトリへpush |

## Architecture

```
v10/
  app.py                    # Flaskルート定義（API + セッション管理）
  analyzer.py               # 分析エンジン（薄いオーケストレーター）
  _analyzer_helpers.py      # 共通ユーティリティ（safe_div, rate_change等）
  _analyzer_quantitative.py # 定量スコアリングロジック
  _analyzer_screening.py    # スクリーニング判定
  _analyzer_thresholds.py   # 投資家プロファイル・しきい値定義
  _analyzer_trees.py        # ROA/ROEツリー分解
  yfinance_parser.py        # yfinance + SEC EDGAR + EDINET + IR BANK データ取得
  edinet_parser.py          # EDINET XBRL パーサー（日本株10年分）
  irbank_parser.py          # IR BANK スクレイピング（年次CSV + 四半期HTML）
  excel_parser.py           # Excelファイルパーサー（.xls/.xlsx、日本語対応）
  templates/index.html      # シングルページアプリ（HTML/CSS/JS）
  static/css/, static/js/   # 静的アセット
  data/                     # サンプルExcelデータ / EDINETキャッシュ / IR BANKキャッシュ
  .env                      # ローカル環境変数（gitignore済み）
```

## Key Files

- `app.py:178` — `/api/analyze` メインエンドポイント
- `app.py:274` — `/api/fetch_ticker` yfinance/EDGAR でティッカー情報取得
- `yfinance_parser.py:645` — `parse_yfinance()` エントリポイント
- `edinet_parser.py` — `parse_edinet()` EDINET 10年分取得（secCodeベース）
- `irbank_parser.py` — `parse_irbank()` 年次 / `parse_irbank_quarterly()` 四半期HTML
- `templates/index.html` — V0エディトリアルデザイン（Bebas Neue + IBM Plex Mono + #D4852A）

## Environment

```bash
# .env（必須）
FLASK_DEBUG=true
SECRET_KEY=<32バイトhex>
EDINET_API_KEY=<APIキー>  # 任意。設定すると日本株が10年分取得可能
```

## Gotchas

- **日本株10年**: EDINET XBRL（2ファイル取得で10年分）→ IR BANK年次CSV（5年）→ yfinance補完
- **日本株四半期**: IR BANK `irbank.net/{code}/quarter` HTMLスクレイピング（q-data.csv は全銘柄404）
  - 単位自動判定: 億円(×1e8) / 百万円(×1e6)
  - 実績（co_red）行のみ取得、Q4は四半期履歴に含まれないためNoneになる（仕様）
  - Toyota等の半期報告会社は2Qのみ取得される（仕様）
- **米国株四半期**: SEC EDGAR 10-Q + yfinance
- **SEC EDGARは米国株のみ**。日本株（`.T`）はIR BANK / yfinance
- **venvのpythonを使うこと** — システムpythonではパッケージが見つからない
- **テンプレートは`templates/index.html`の1ファイルのみ** — CSS/JSもすべてインライン
- **beautifulsoup4 必須** — IR BANK四半期スクレイピングに使用

## Design System (V0 Editorial)

- フォント: `Bebas Neue`（見出し）、`IBM Plex Mono`（ラベル/ボタン）
- アクセントカラー: `#D4852A`（CSS変数: `--accent-v0`）
- コーナー: `border-radius: 0`（角丸禁止）
- 背景: グリッドパターン60px + ノイズオーバーレイ

## Chrome MCP 操作ルール

- ユーザーの明示的許可後のみ操作
- `navigate`, `form_input` 等は自動実行禁止

## GitHub

- v10: `https://github.com/susuheihei-lgtm/financial-analysis-automation-v10`
- v9:  `https://github.com/susuheihei-lgtm/financial-analysis-app-v9`
