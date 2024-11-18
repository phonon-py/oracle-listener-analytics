# src/main.py
import logging
import sys
from pathlib import Path

from data_processors.oracle_merger import OracleMerger
from parsers.listener_log_parser import ListenerLogParser


def setup_logging():
    """ログ設定を初期化"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def process_logs(raw_dir, processed_dir, parser):
    """ログファイルの処理を実行"""
    processed_files = []
    
    for log_file in raw_dir.rglob('*.log'):
        try:
            logging.info(f"Processing {log_file.name}")
            
            # 出力ファイル名の作成（ディレクトリ構造を維持）
            relative_path = log_file.relative_to(raw_dir)
            output_file = processed_dir / relative_path.parent / f"{log_file.stem}_processed.csv"
            
            # 出力ディレクトリの作成
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # ログファイルの処理
            df = parser.process_and_save(log_file, output_file)
            logging.info(f"Processed {len(df)} records from {log_file.name}")
            
            processed_files.append(output_file)
            
        except Exception as e:
            logging.error(f"Failed to process {log_file.name}: {str(e)}")
            continue
            
    return processed_files

def merge_with_oracle(processed_files, merged_dir):
    """処理済みファイルとOracleデータをマージ"""
    try:
        merger = OracleMerger()
        
        for processed_file in processed_files:
            try:
                # 出力ファイル名の作成
                output_file = merged_dir / f"{processed_file.stem}_merged.csv"
                
                # マージ処理の実行
                merger.process_and_save(processed_file, output_file)
                logging.info(f"Successfully merged {processed_file.name} with Oracle data")
                
            except Exception as e:
                logging.error(f"Failed to merge {processed_file.name}: {str(e)}")
                continue
                
    except Exception as e:
        logging.error(f"Failed to initialize OracleMerger: {str(e)}")

def main():
    setup_logging()
    
    # Windows環境での文字化け対策
    if sys.platform == 'win32':
        import locale
        sys.stdout.reconfigure(encoding=locale.getpreferredencoding())
    
    # パスの設定
    data_dir = Path('data')
    raw_dir = data_dir / 'raw' / 'ACH-RFID1' / '本番環境'
    processed_dir = data_dir / 'processed'
    merged_dir = data_dir / 'merged'
    
    # 必要なディレクトリの作成
    processed_dir.mkdir(exist_ok=True)
    merged_dir.mkdir(exist_ok=True)
    
    # パーサーのインスタンス化
    parser = ListenerLogParser()
    
    # ログファイルの処理
    processed_files = process_logs(raw_dir, processed_dir, parser)
    
    # Oracleデータとのマージ
    if processed_files:
        merge_with_oracle(processed_files, merged_dir)

if __name__ == "__main__":
    main()