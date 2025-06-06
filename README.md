# 仕様書解析ツール

PDF形式の仕様書から情報を抽出し、構造化されたデータとして出力するツールです。

## 機能

- PDFファイルからのテキスト抽出
- 大項目、中項目、小項目の自動認識
- 試験内容、試験条件、判定要領の抽出
- 抽出ルールのカスタマイズ
- マスタデータの管理
- 処理履歴の記録とエクスポート

## 必要条件

- Python 3.8以上
- Tesseract OCR（画像からのテキスト抽出に必要）

## インストール

1. リポジトリをクローン：
```bash
git clone https://github.com/yourusername/spec-tool.git
cd spec-tool
```

2. 仮想環境を作成して有効化：
```bash
python -m venv venv
source venv/bin/activate  # Linuxの場合
venv\Scripts\activate     # Windowsの場合
```

3. 必要なパッケージをインストール：
```bash
pip install -r requirements.txt
```

## 使用方法

1. アプリケーションを起動：
```bash
python app.py
```

2. ブラウザで http://localhost:7865 にアクセス

3. PDFファイルをアップロードして処理を開始

## ライセンス

MIT License
