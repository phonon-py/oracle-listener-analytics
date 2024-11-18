from pathlib import Path
import logging
import sys
from parsers.listener_log_parser import ListenerLogParser

def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

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
    
    # 出力ディレクトリの作成
    processed_dir.mkdir(exist_ok=True)
    
    # パーサーのインスタンス化
    parser = ListenerLogParser()
    
    # すべての.logファイルを再帰的に検索して処理
    for log_file in raw_dir.rglob('*.log'):
        try:
            logging.info(f"Processing {log_file.name}")
            
            # 出力ファイル名の作成（ディレクトリ構造を維持）
            relative_path = log_file.relative_to(raw_dir)
            output_file = processed_dir / relative_path.parent / f"{log_file.stem}_processed.csv"
            
            # 出力ディレクトリの作成
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            df = parser.process_and_save(log_file, output_file)
            logging.info(f"Processed {len(df)} records from {log_file.name}")
            
        except Exception as e:
            logging.error(f"Failed to process {log_file.name}: {str(e)}")
            continue

if __name__ == "__main__":
    main()