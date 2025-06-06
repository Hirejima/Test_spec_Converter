#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import pdfplumber
from typing import Dict, List, Optional, Tuple
import gradio as gr
import docx
from PIL import Image
import pytesseract
import io
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sqlite3
from dataclasses import dataclass, asdict
import re
import logging
from queue import Queue
import traceback

MASTER_DATA_PATH = Path("master_data.json")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)
HISTORY_DB = Path("history.db")
RULES_DIR = Path("rules")
RULES_DIR.mkdir(exist_ok=True)
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f"app_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# グローバル変数
processing_status = {"current": 0, "total": 0, "message": ""}
processing_lock = threading.Lock()
stop_processing = threading.Event()
log_queue = Queue()

@dataclass
class ExtractionRule:
    """抽出ルールの設定"""
    name: str = "デフォルト"
    major_pattern: str = r'^■.*$'
    middle_pattern: str = r'^\d+\.'
    minor_pattern: str = r'^\d+-\d+）'
    skip_pattern: str = r'表\d+'
    content_keywords: List[str] = None
    condition_keywords: List[str] = None
    judgment_keywords: List[str] = None

    def __post_init__(self):
        if self.content_keywords is None:
            self.content_keywords = ["試験条件及び方法"]
        if self.condition_keywords is None:
            self.condition_keywords = ["試験項目"]
        if self.judgment_keywords is None:
            self.judgment_keywords = ["確認項目"]

    def save(self):
        """ルールをJSONファイルとして保存"""
        rule_path = RULES_DIR / f"{self.name}.json"
        with open(rule_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self), f, ensure_ascii=False, indent=2)
        logging.info(f"ルールを保存しました: {self.name}")

    @classmethod
    def load(cls, name: str) -> Optional['ExtractionRule']:
        """ルールをJSONファイルから読み込み"""
        rule_path = RULES_DIR / f"{name}.json"
        if not rule_path.exists():
            return None
        try:
            with open(rule_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return cls(**data)
        except Exception as e:
            logging.error(f"ルールの読み込みに失敗しました: {str(e)}")
            return None

    @classmethod
    def list_rules(cls) -> List[str]:
        """保存されているルールの一覧を取得"""
        return [f.stem for f in RULES_DIR.glob("*.json")]

def log_message(message: str, level: str = "info"):
    """ログメッセージをキューに追加"""
    log_queue.put({
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": message
    })
    if level == "error":
        logging.error(message)
    elif level == "warning":
        logging.warning(message)
    else:
        logging.info(message)

def get_logs() -> str:
    """キューからログメッセージを取得"""
    logs = []
    while not log_queue.empty():
        log = log_queue.get()
        logs.append(f"[{log['timestamp']}] {log['level'].upper()}: {log['message']}")
    return "\n".join(logs)

def export_history(format: str = "excel") -> str:
    """処理履歴をエクスポート"""
    try:
        df = get_history()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if format == "excel":
            output_path = OUTPUT_DIR / f"history_{timestamp}.xlsx"
            df.to_excel(output_path, index=False)
        else:  # CSV
            output_path = OUTPUT_DIR / f"history_{timestamp}.csv"
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        log_message(f"履歴をエクスポートしました: {output_path}")
        return str(output_path)
    except Exception as e:
        error_msg = f"履歴のエクスポートに失敗しました: {str(e)}"
        log_message(error_msg, "error")
        return error_msg

class MasterDataManager:
    """マスターデータを管理するクラス"""
    
    def __init__(self, data_file: str = "master_data.json"):
        """初期化
        
        Args:
            data_file (str): マスターデータファイルのパス
        """
        self.data_file = Path(data_file)
        self.data = {"mappings": {}}  # デフォルトの初期値
        self._load_data()  # 既存のデータがあれば読み込む
    
    def _load_data(self) -> None:
        """マスターデータを読み込む"""
        if not self.data_file.exists():
            return
        
        try:
            with open(self.data_file, "r", encoding="utf-8") as f:
                loaded_data = json.load(f)
                if isinstance(loaded_data, dict) and "mappings" in loaded_data:
                    self.data = loaded_data
                else:
                    print("警告: マスターデータの形式が不正です。新しいファイルを作成します。")
        except json.JSONDecodeError:
            print("警告: マスターデータファイルが破損しています。新しいファイルを作成します。")
        except Exception as e:
            print(f"警告: マスターデータの読み込み中にエラーが発生しました: {e}")
    
    def save(self) -> None:
        """マスターデータを保存する"""
        try:
            with open(self.data_file, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"エラー: マスターデータの保存に失敗しました: {e}")
            sys.exit(1)
    
    def add_mapping(self, standard: str, varied: str) -> None:
        """マッピングを追加する
        
        Args:
            standard (str): 標準用語
            varied (str): バラバラ用語
        """
        if not standard or not varied:
            print("エラー: 標準用語とバラバラ用語は空にできません")
            return
        
        if standard not in self.data["mappings"]:
            self.data["mappings"][standard] = []
        
        if varied in self.data["mappings"][standard]:
            print(f"警告: このマッピングは既に存在します: {standard} - {varied}")
            return
        
        self.data["mappings"][standard].append(varied)
        self.save()
        print(f"マッピングを追加しました: {standard} - {varied}")
    
    def remove_mapping(self, standard: str, varied: str) -> None:
        """マッピングを削除する
        
        Args:
            standard (str): 標準用語
            varied (str): バラバラ用語
        """
        if standard not in self.data["mappings"]:
            print(f"警告: 標準用語が見つかりません: {standard}")
            return
        
        if varied not in self.data["mappings"][standard]:
            print(f"警告: バラバラ用語が見つかりません: {varied}")
            return
        
        self.data["mappings"][standard].remove(varied)
        if not self.data["mappings"][standard]:
            del self.data["mappings"][standard]
        
        self.save()
        print(f"マッピングを削除しました: {standard} - {varied}")
    
    def list_mappings(self) -> None:
        """マッピングの一覧を表示する"""
        if not self.data["mappings"]:
            print("マスターデータが設定されていません")
            return
        
        print("\n現在のマスターデータ:")
        for standard, varied_list in self.data["mappings"].items():
            print(f"\n標準用語: {standard}")
            for varied in varied_list:
                print(f"  - {varied}")

class PDFProcessor:
    """PDFファイルを処理するクラス"""
    
    def __init__(self, output_dir: str = "output"):
        """初期化
        
        Args:
            output_dir (str): 出力ディレクトリのパス
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def process(self, pdf_path: str) -> Optional[pd.DataFrame]:
        """PDFファイルを処理する
        
        Args:
            pdf_path (str): PDFファイルのパス
        """
        try:
            with pdfplumber.open(pdf_path) as pdf:
                all_data = []
                current_major = None
                current_middle = None
                current_minor_data = {}
                
                for page_num, page in enumerate(pdf.pages, 1):
                    if page_num <= 3:  # 最初の3ページはスキップ
                        continue
                    
                    text = page.extract_text()
                    if not text:
                        continue
                        
                    for line in text.split('\n'):
                        line = line.strip()
                        if not line:
                            continue
                        
                        # 大項目の検出
                        if line.startswith('■') and not any(f"表{i}" in line for i in range(1, 100)):
                            if current_minor_data:
                                all_data.append(current_minor_data)
                                current_minor_data = {}
                            current_major = line
                            current_middle = None
                            continue
                        
                        # 表タイトルのスキップ
                        if any(f"表{i}" in line for i in range(1, 100)):
                            continue
                        
                        # 中項目の検出
                        if line[0].isdigit() and '.' in line[:3]:
                            if current_minor_data:
                                all_data.append(current_minor_data)
                                current_minor_data = {}
                            current_middle = line
                            continue
                        
                        # 小項目の検出
                        if line[0].isdigit() and '-' in line[:3] and '）' in line[:5]:
                            if current_minor_data:
                                all_data.append(current_minor_data)
                            
                            current_minor_data = {
                                '大項目': current_major,
                                '中項目': current_middle if current_middle else '',
                                '小項目': line,
                                '試験内容': '',
                                '試験条件': '',
                                '判定要領': ''
                            }
                            continue
                        
                        if current_minor_data:
                            if "試験条件及び方法" in line:
                                current_minor_data['試験内容'] = line.split("試験条件及び方法", 1)[-1].strip()
                            elif "試験項目" in line:
                                current_minor_data['試験条件'] = line.split("試験項目", 1)[-1].strip()
                            elif "確認項目" in line:
                                current_minor_data['判定要領'] = line.split("確認項目", 1)[-1].strip()
                            else:
                                if current_minor_data['試験内容'] and not current_minor_data['試験条件'] and not current_minor_data['判定要領']:
                                    current_minor_data['試験内容'] += " " + line

                if current_minor_data:
                    all_data.append(current_minor_data)
                
                if not all_data:
                    print("警告: データが見つかりませんでした")
                    return None
                
                df = pd.DataFrame(all_data)
                return df[['大項目', '中項目', '小項目', '試験内容', '試験条件', '判定要領']]

        except Exception as e:
            print(f"エラー: PDFの処理中にエラーが発生しました: {e}")
            return None

    def save_to_excel(self, df: pd.DataFrame, pdf_path: str) -> None:
        """DataFrameをExcelファイルとして保存する
        
        Args:
            df (pd.DataFrame): 保存するDataFrame
            pdf_path (str): 元のPDFファイルのパス
        """
        try:
            output_file = self.output_dir / f"{Path(pdf_path).stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            df.to_excel(output_file, index=False)
            print(f"Excelファイルが生成されました: {output_file}")
        except Exception as e:
            print(f"エラー: Excelファイルの保存に失敗しました: {e}")

def print_help() -> None:
    """ヘルプメッセージを表示する"""
    print("使用法:")
    print("1. マスターデータの表示:")
    print("   python app.py list")
    print("\n2. マッピングの追加:")
    print("   python app.py add <標準用語> <バラバラ用語>")
    print("\n3. マッピングの削除:")
    print("   python app.py remove <標準用語> <バラバラ用語>")
    print("\n4. PDFの処理:")
    print("   python app.py process <PDFファイルのパス>")

def main() -> None:
    """メイン関数"""
    if len(sys.argv) < 2:
        print_help()
        return
    
    command = sys.argv[1].lower()
    
    try:
        if command == "list":
            master = MasterDataManager()
            master.list_mappings()
        
        elif command == "add":
            if len(sys.argv) != 4:
                print("エラー: 標準用語とバラバラ用語を指定してください")
                print_help()
                return
            master = MasterDataManager()
            master.add_mapping(sys.argv[2], sys.argv[3])
        
        elif command == "remove":
            if len(sys.argv) != 4:
                print("エラー: 標準用語とバラバラ用語を指定してください")
                print_help()
                return
            master = MasterDataManager()
            master.remove_mapping(sys.argv[2], sys.argv[3])
        
        elif command == "process":
            if len(sys.argv) != 3:
                print("エラー: PDFファイルのパスを指定してください")
                print_help()
                return
            
            processor = PDFProcessor()
            df = processor.process(sys.argv[2])
            if df is not None:
                processor.save_to_excel(df, sys.argv[2])
        
        else:
            print(f"エラー: 不明なコマンド '{command}'")
            print_help()
    
    except Exception as e:
        print(f"エラー: 予期せぬエラーが発生しました: {e}")
        sys.exit(1)

def load_master_data():
    if not MASTER_DATA_PATH.exists():
        return {}
    try:
        with open(MASTER_DATA_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_master_data(data):
    with open(MASTER_DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def list_mappings() -> pd.DataFrame:
    data = load_master_data()
    rows = []
    for std, varieds in data.get("mappings", {}).items():
        for varied in varieds:
            rows.append({"標準用語": std, "バラバラ用語": varied})
    return pd.DataFrame(rows)

def add_mapping(standard: str, varied: str) -> pd.DataFrame:
    data = load_master_data()
    if "mappings" not in data:
        data["mappings"] = {}
    if standard not in data["mappings"]:
        data["mappings"][standard] = []
    if varied and varied not in data["mappings"][standard]:
        data["mappings"][standard].append(varied)
        save_master_data(data)
    return list_mappings()

def remove_mapping(standard: str, varied: str) -> pd.DataFrame:
    data = load_master_data()
    if standard in data.get("mappings", {}):
        if varied in data["mappings"][standard]:
            data["mappings"][standard].remove(varied)
            if not data["mappings"][standard]:
                del data["mappings"][standard]
            save_master_data(data)
    return list_mappings()

def extract_text_from_docx(file_path: str) -> str:
    """Wordファイルからテキストを抽出"""
    try:
        doc = docx.Document(file_path)
        return "\n".join([paragraph.text for paragraph in doc.paragraphs])
    except Exception as e:
        print(f"Wordファイルの処理中にエラーが発生しました: {e}")
        return ""

def extract_text_from_image(file_path: str) -> str:
    """画像ファイルからテキストを抽出（OCR）"""
    try:
        image = Image.open(file_path)
        return pytesseract.image_to_string(image, lang='jpn')
    except Exception as e:
        print(f"画像ファイルの処理中にエラーが発生しました: {e}")
        return ""

def extract_text_from_pdf(file_path: str) -> str:
    """PDFからテキストを抽出（最適化版）"""
    try:
        text = ""
        start_time = time.time()
        with pdfplumber.open(file_path) as pdf:
            total_pages = len(pdf.pages)
            log_message(f"PDFファイルを開きました。総ページ数: {total_pages}")
            
            # メモリ使用量を抑えるため、ページごとに処理
            for i, page in enumerate(pdf.pages, 1):
                # タイムアウトチェック（5分）
                if time.time() - start_time > 300:
                    log_message("処理がタイムアウトしました（5分）", "error")
                    return text
                
                try:
                    # 進捗表示（5ページごと）
                    if i % 5 == 0:
                        elapsed_time = time.time() - start_time
                        log_message(f"ページ {i}/{total_pages} を処理中... (経過時間: {elapsed_time:.1f}秒)")
                    
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                    else:
                        log_message(f"ページ {i} からテキストを抽出できませんでした", "warning")
                    
                    # メモリ解放
                    del page_text
                except Exception as e:
                    log_message(f"ページ {i} の処理中にエラー: {str(e)}", "warning")
                    continue
            
            if not text:
                log_message("PDFからテキストを抽出できませんでした。スキャンされたPDFの可能性があります。", "error")
                return ""
            
            elapsed_time = time.time() - start_time
            log_message(f"PDF処理が完了しました。処理時間: {elapsed_time:.1f}秒")
            return text
    except Exception as e:
        error_msg = f"PDFの処理中にエラーが発生しました: {str(e)}"
        log_message(error_msg, "error")
        return ""

def process_text(text: str, rule: ExtractionRule = None) -> Optional[pd.DataFrame]:
    """テキストを処理してデータフレームを生成（改善版）"""
    try:
        if not text:
            log_message("テキストが空です", "error")
            return None
        
        # テキストを行ごとに分割
        lines = text.split('\n')
        log_message(f"テキストを{len(lines)}行に分割しました")
        
        # データを格納するリスト
        data = []
        current_major = ""
        current_middle = ""
        current_minor = ""
        current_content = []
        current_condition = []
        current_judgment = []
        
        # 各行を処理
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # 大項目の判定
            major_match = re.search(r'^(\d+\.\s*[^\n]+)', line)
            if major_match:
                # 前の項目があれば保存
                if current_content or current_condition or current_judgment:
                    data.append({
                        '大項目': current_major,
                        '中項目': current_middle,
                        '小項目': current_minor,
                        '試験内容': '\n'.join(current_content),
                        '試験条件': '\n'.join(current_condition),
                        '判定要領': '\n'.join(current_judgment)
                    })
                    current_content = []
                    current_condition = []
                    current_judgment = []
                
                current_major = major_match.group(1)
                current_middle = ""
                current_minor = ""
                continue
            
            # 中項目の判定
            middle_match = re.search(r'^(\d+\.\d+\.\s*[^\n]+)', line)
            if middle_match:
                # 前の項目があれば保存
                if current_content or current_condition or current_judgment:
                    data.append({
                        '大項目': current_major,
                        '中項目': current_middle,
                        '小項目': current_minor,
                        '試験内容': '\n'.join(current_content),
                        '試験条件': '\n'.join(current_condition),
                        '判定要領': '\n'.join(current_judgment)
                    })
                    current_content = []
                    current_condition = []
                    current_judgment = []
                
                current_middle = middle_match.group(1)
                current_minor = ""
                continue
            
            # 小項目の判定
            minor_match = re.search(r'^(\d+\.\d+\.\d+\.\s*[^\n]+)', line)
            if minor_match:
                # 前の項目があれば保存
                if current_content or current_condition or current_judgment:
                    data.append({
                        '大項目': current_major,
                        '中項目': current_middle,
                        '小項目': current_minor,
                        '試験内容': '\n'.join(current_content),
                        '試験条件': '\n'.join(current_condition),
                        '判定要領': '\n'.join(current_judgment)
                    })
                    current_content = []
                    current_condition = []
                    current_judgment = []
                
                current_minor = minor_match.group(1)
                continue
            
            # 内容の分類
            if current_major:  # 大項目が設定されている場合のみ内容を追加
                if '試験条件' in line or '条件' in line:
                    current_condition.append(line)
                elif '判定要領' in line or '判定' in line:
                    current_judgment.append(line)
                else:
                    current_content.append(line)
        
        # 最後の項目を保存
        if current_content or current_condition or current_judgment:
            data.append({
                '大項目': current_major,
                '中項目': current_middle,
                '小項目': current_minor,
                '試験内容': '\n'.join(current_content),
                '試験条件': '\n'.join(current_condition),
                '判定要領': '\n'.join(current_judgment)
            })
        
        if not data:
            log_message("有効なデータが見つかりませんでした", "warning")
            return None
        
        # データフレームの作成
        df = pd.DataFrame(data)
        log_message(f"データフレームを作成しました: {len(df)}行")
        
        # 空の列を削除
        df = df.replace('', pd.NA).dropna(how='all', axis=1)
        
        # 重複行の削除
        df = df.drop_duplicates()
        
        return df
    except Exception as e:
        error_msg = f"テキスト処理中にエラーが発生しました: {str(e)}"
        log_message(error_msg, "error")
        return None

def update_status(current: int, total: int, message: str):
    """処理状態を更新"""
    with processing_lock:
        processing_status["current"] = current
        processing_status["total"] = total
        processing_status["message"] = message

def get_status():
    """現在の処理状態を取得"""
    with processing_lock:
        return processing_status

def process_single_file(file_path: str, file_ext: str, rule: ExtractionRule = None) -> Tuple[Optional[pd.DataFrame], str]:
    """単一ファイルを処理（最適化版）"""
    try:
        start_time = time.time()
        log_message(f"ファイル処理を開始: {os.path.basename(file_path)}")
        
        # ファイルサイズチェック
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB単位
        if file_size > 50:  # 50MB以上の場合
            log_message(f"警告: ファイルサイズが大きいです ({file_size:.1f}MB)", "warning")
        
        # テキスト抽出
        if file_ext == "pdf":
            text = extract_text_from_pdf(file_path)
        elif file_ext in ["docx", "doc"]:
            text = extract_text_from_docx(file_path)
        elif file_ext in ["png", "jpg", "jpeg", "bmp", "tiff"]:
            text = extract_text_from_image(file_path)
        else:
            return None, f"未対応のファイル形式です: {file_ext}"
        
        if not text:
            return None, "テキストの抽出に失敗しました。ファイルが正しく読み込めないか、テキストが含まれていない可能性があります。"
        
        # テキスト処理
        df = process_text(text, rule)
        if df is None or df.empty:
            return None, "データの抽出に失敗しました。ファイルの内容が期待される形式と異なる可能性があります。"
        
        elapsed_time = time.time() - start_time
        log_message(f"ファイル処理が完了: {os.path.basename(file_path)} (処理時間: {elapsed_time:.1f}秒)")
        return df, "処理が完了しました"
    except Exception as e:
        error_msg = f"処理中にエラーが発生しました: {str(e)}"
        log_message(error_msg, "error")
        return None, error_msg

def filter_dataframe(df: pd.DataFrame, filter_text: str) -> pd.DataFrame:
    """DataFrameをフィルタリング"""
    if not filter_text:
        return df
    try:
        return df[df.astype(str).apply(lambda x: x.str.contains(filter_text, case=False)).any(axis=1)]
    except:
        return df

def process_files_parallel(file_objs: List, output_format: str, rule: ExtractionRule = None) -> Tuple[pd.DataFrame, str]:
    """複数ファイルを並列処理（改善版）"""
    if not file_objs:
        return None, "ファイルが選択されていません"
    
    stop_processing.clear()
    total_files = len(file_objs)
    start_time = time.time()
    update_status(0, total_files, "処理を開始します...")
    log_message(f"処理を開始: {total_files}ファイル")
    
    all_data = []
    errors = []
    
    # 並列処理のワーカー数を制限（大きなファイルの場合は1つに制限）
    max_workers = 1 if any(os.path.getsize(f.name) > 10 * 1024 * 1024 for f in file_objs) else min(4, os.cpu_count(), total_files)
    log_message(f"並列処理ワーカー数: {max_workers}")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_single_file, file_obj.name, file_obj.name.lower().split('.')[-1], rule): file_obj
            for file_obj in file_objs
        }
        
        completed = 0
        for future in as_completed(future_to_file):
            if stop_processing.is_set():
                log_message("処理を中断しました", "warning")
                break
            
            # タイムアウトチェック（10分）
            if time.time() - start_time > 600:
                log_message("全体の処理がタイムアウトしました（10分）", "error")
                break
                
            completed += 1
            file_obj = future_to_file[future]
            elapsed_time = time.time() - start_time
            progress = int((completed / total_files) * 100)
            update_status(progress, total_files, 
                         f"処理中: {os.path.basename(file_obj.name)} (経過時間: {elapsed_time:.1f}秒)")
            
            try:
                df, status = future.result()
                if df is not None and not df.empty:
                    all_data.append(df)
                    output_path = OUTPUT_DIR / f"combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{output_format.lower()}"
                    add_history_entry(
                        os.path.basename(file_obj.name),
                        "成功",
                        str(output_path)
                    )
                    log_message(f"ファイル処理成功: {os.path.basename(file_obj.name)}")
                else:
                    errors.append(f"{os.path.basename(file_obj.name)}: {status}")
                    add_history_entry(
                        os.path.basename(file_obj.name),
                        "失敗",
                        error_message=status
                    )
                    log_message(f"ファイル処理失敗: {os.path.basename(file_obj.name)} - {status}", "error")
            except Exception as e:
                error_msg = f"{os.path.basename(file_obj.name)}: {str(e)}"
                errors.append(error_msg)
                add_history_entry(
                    os.path.basename(file_obj.name),
                    "失敗",
                    error_message=str(e)
                )
                log_message(f"ファイル処理エラー: {error_msg}", "error")
    
    if not all_data:
        return None, "処理に成功したファイルがありません。\nエラー:\n" + "\n".join(errors)
    
    # 全データを結合
    log_message("データの結合を開始します...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # 出力ファイルの保存
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if output_format == "Excel":
        output_path = OUTPUT_DIR / f"combined_{timestamp}.xlsx"
        log_message(f"Excelファイルの保存を開始します... 保存先: {output_path}")
        combined_df.to_excel(output_path, index=False)
    else:  # CSV
        output_path = OUTPUT_DIR / f"combined_{timestamp}.csv"
        log_message(f"CSVファイルの保存を開始します... 保存先: {output_path}")
        combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    # エラーメッセージの作成
    error_msg = "\n".join(errors) if errors else "なし"
    total_time = time.time() - start_time
    success_msg = f"処理が完了しました。\n保存先: {output_path}\n処理時間: {total_time:.1f}秒\n\nエラー:\n{error_msg}"
    
    update_status(100, total_files, f"処理が完了しました (合計時間: {total_time:.1f}秒)")
    log_message(f"処理が完了しました。合計時間: {total_time:.1f}秒")
    return combined_df, success_msg

def preview_file(file_obj) -> Tuple[Optional[pd.DataFrame], str]:
    """ファイルのプレビューを表示"""
    if file_obj is None:
        return None, "ファイルが選択されていません"
    
    file_path = file_obj.name
    file_ext = file_path.lower().split('.')[-1]
    
    df, status = process_single_file(file_path, file_ext)
    if df is None:
        return None, status
    
    return df, "プレビューを表示しています"

def process_with_rule(file_objs, output_format):
    """ファイルを処理"""
    if not file_objs:
        return "エラー: ファイルが選択されていません", pd.DataFrame(), "エラー"

    try:
        # ファイルオブジェクトのリストを取得
        files = [f.name for f in file_objs]
        log_message(f"処理開始: {len(files)}ファイル")
        
        # 抽出ルールの作成
        rule = ExtractionRule(
            name="default",
            major_pattern=r'^\d+\.\s*[^\n]+',
            middle_pattern=r'^\d+\.\d+\.\s*[^\n]+',
            minor_pattern=r'^\d+\.\d+\.\d+\.\s*[^\n]+',
            content_keywords=["試験内容", "内容"],
            condition_keywords=["試験条件", "条件"],
            judgment_keywords=["判定要領", "判定"]
        )
        
        # ファイル処理の実行
        df, status = process_files_parallel(file_objs, output_format, rule)
        
        if df is None:
            return status, pd.DataFrame(), "エラー"
        
        # 処理履歴の追加
        for file_obj in file_objs:
            add_history_entry(
                os.path.basename(file_obj.name),
                "成功",
                str(OUTPUT_DIR / f"combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{output_format.lower()}")
            )
        
        return "処理完了", df, "成功"
    except Exception as e:
        error_msg = f"処理中にエラーが発生しました: {str(e)}"
        log_message(error_msg, "error")
        return error_msg, pd.DataFrame(), "エラー"

def save_rule(rule_name, major_pattern, middle_pattern, minor_pattern,
             content_keywords, condition_keywords, judgment_keywords):
    """抽出ルールを保存"""
    try:
        rule = ExtractionRule(
            name=rule_name,
            major_pattern=major_pattern,
            middle_pattern=middle_pattern,
            minor_pattern=minor_pattern,
            content_keywords=[k.strip() for k in content_keywords.split(",")],
            condition_keywords=[k.strip() for k in condition_keywords.split(",")],
            judgment_keywords=[k.strip() for k in judgment_keywords.split(",")]
        )
        rule.save()
        return f"ルール '{rule_name}' を保存しました"
    except Exception as e:
        return f"ルールの保存に失敗しました: {str(e)}"

def load_rule(rule_name):
    """抽出ルールを読み込み"""
    try:
        rule = ExtractionRule.load(rule_name)
        if rule:
            return [
                rule.major_pattern,
                rule.middle_pattern,
                rule.minor_pattern,
                ",".join(rule.content_keywords),
                ",".join(rule.condition_keywords),
                ",".join(rule.judgment_keywords),
                f"ルール '{rule_name}' を読み込みました"
            ]
        return ["", "", "", "", "", "", f"ルール '{rule_name}' が見つかりません"]
    except Exception as e:
        return ["", "", "", "", "", "", f"ルールの読み込みに失敗しました: {str(e)}"]

def list_rules():
    """保存済みルールの一覧を表示"""
    try:
        rules = ExtractionRule.list_rules()
        if rules:
            return "保存済みルール:\n" + "\n".join(rules)
        return "保存済みルールはありません"
    except Exception as e:
        return f"ルール一覧の取得に失敗しました: {str(e)}"

def export_history():
    """処理履歴をエクスポート"""
    try:
        history = get_history()
        if not history:
            return "履歴がありません"
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = OUTPUT_DIR / f"history_{timestamp}.xlsx"
        
        df = pd.DataFrame(history)
        df.to_excel(output_path, index=False)
        return f"履歴をエクスポートしました: {output_path}"
    except Exception as e:
        return f"履歴のエクスポートに失敗しました: {str(e)}"

def save_master_data(master_df):
    """マスタデータを保存"""
    try:
        if master_df is None or master_df.empty:
            return "マスタデータが空です"
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = OUTPUT_DIR / f"master_{timestamp}.xlsx"
        
        master_df.to_excel(output_path, index=False)
        return f"マスタデータを保存しました: {output_path}"
    except Exception as e:
        return f"マスタデータの保存に失敗しました: {str(e)}"

def load_master_data():
    """マスタデータを読み込み"""
    try:
        # 最新のマスタファイルを探す
        master_files = list(OUTPUT_DIR.glob("master_*.xlsx"))
        if not master_files:
            return pd.DataFrame(columns=['大項目', '中項目', '小項目']), "マスタファイルが見つかりません"
        
        latest_file = max(master_files, key=lambda x: x.stat().st_mtime)
        df = pd.read_excel(latest_file)
        return df, f"マスタデータを読み込みました: {latest_file}"
    except Exception as e:
        return pd.DataFrame(columns=['大項目', '中項目', '小項目']), f"マスタデータの読み込みに失敗しました: {str(e)}"

def update_progress():
    """進捗状況を更新"""
    while True:
        status = get_status()
        if status["total"] > 0:
            progress = (status["current"] / status["total"]) * 100
            yield progress, status["message"]
        time.sleep(0.1)

def update_logs():
    """ログを更新"""
    return get_logs()

def create_ui():
    """Gradio UIを作成する"""
    with gr.Blocks(title="仕様書解析ツール") as demo:
        gr.Markdown("# 仕様書解析ツール")
        
        with gr.Row():
            with gr.Column():
                file_input = gr.File(
                    label="PDFファイルをアップロード",
                    file_types=[".pdf"],
                    file_count="multiple"
                )
                output_format = gr.Radio(
                    choices=["Excel", "CSV"],
                    label="出力形式",
                    value="Excel"
                )
                process_btn = gr.Button("処理開始")
                status = gr.Textbox(label="処理状態")
                log_output = gr.Textbox(label="ログ", lines=10)
            
            with gr.Column():
                result_df = gr.Dataframe(
                    label="処理結果",
                    headers=["大項目", "中項目", "小項目", "試験内容", "試験条件", "判定要領"],
                    datatype=["str", "str", "str", "str", "str", "str"]
                )
        
        with gr.Tabs():
            with gr.TabItem("マスタデータ管理"):
                with gr.Row():
                    with gr.Column():
                        master_file = gr.File(
                            label="マスタデータファイル",
                            file_types=[".xlsx", ".csv"]
                        )
                        load_master_btn = gr.Button("マスタデータ読み込み")
                    with gr.Column():
                        master_df = gr.Dataframe(label="マスタデータ")
            
            with gr.TabItem("抽出ルール管理"):
                with gr.Row():
                    with gr.Column():
                        rule_name = gr.Textbox(label="ルール名")
                        rule_file = gr.File(
                            label="ルールファイル",
                            file_types=[".json"]
                        )
                        save_rule_btn = gr.Button("ルール保存")
                        load_rule_btn = gr.Button("ルール読み込み")
                    with gr.Column():
                        rule_df = gr.Dataframe(label="ルール一覧")
            
            with gr.TabItem("処理履歴"):
                history_df = gr.Dataframe(label="処理履歴")
                export_history_btn = gr.Button("履歴エクスポート")
        
        # イベントハンドラの設定
        process_btn.click(
            fn=process_with_rule,
            inputs=[file_input, output_format],
            outputs=[status, result_df, log_output]
        )
        
        load_master_btn.click(
            fn=load_master_data,
            inputs=[],
            outputs=[master_df]
        )
        
        save_rule_btn.click(
            fn=save_rule,
            inputs=[rule_name, rule_file],
            outputs=[rule_df]
        )
        
        load_rule_btn.click(
            fn=load_rule,
            inputs=[rule_name],
            outputs=[rule_df]
        )
        
        export_history_btn.click(
            fn=export_history,
            inputs=[],
            outputs=[history_df]
        )
    
    return demo

# データベース初期化
def init_database():
    """処理履歴のデータベースを初期化"""
    conn = sqlite3.connect(HISTORY_DB)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS processing_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            filename TEXT,
            status TEXT,
            output_path TEXT,
            error_message TEXT
        )
    ''')
    conn.commit()
    conn.close()

def add_history_entry(filename: str, status: str, output_path: str = "", error_message: str = ""):
    """処理履歴を追加"""
    conn = sqlite3.connect(HISTORY_DB)
    c = conn.cursor()
    c.execute('''
        INSERT INTO processing_history (timestamp, filename, status, output_path, error_message)
        VALUES (?, ?, ?, ?, ?)
    ''', (datetime.now().isoformat(), filename, status, output_path, error_message))
    conn.commit()
    conn.close()

def get_history() -> pd.DataFrame:
    """処理履歴を取得"""
    try:
        conn = sqlite3.connect(HISTORY_DB)
        df = pd.read_sql_query("SELECT * FROM processing_history ORDER BY timestamp DESC", conn)
        conn.close()
        return df
    except Exception as e:
        log_message(f"履歴の取得に失敗しました: {str(e)}", "error")
        return pd.DataFrame(columns=['timestamp', 'filename', 'status', 'output_path', 'error_message'])

if __name__ == "__main__":
    # 出力ディレクトリの作成
    os.makedirs("output", exist_ok=True)
    
    # データベースの初期化
    init_database()
    
    # UIの作成と起動
    demo = create_ui()
    demo.queue()
    demo.launch(
        server_name="127.0.0.1",
        server_port=7865,  # 7864から7865に変更
        share=True,
        show_api=False,
        show_error=True,
        quiet=True
    ) 