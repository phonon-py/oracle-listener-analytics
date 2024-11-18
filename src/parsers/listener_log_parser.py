import pandas as pd
import re
from pathlib import Path
import logging

class ListenerLogParser:
    def __init__(self):
        # HOSTとUSERを抽出するための正規表現パターン
        self.host_pattern = re.compile(r'\(HOST=([^)]+)\)')
        self.user_pattern = re.compile(r'\(USER=([^)]+)\)')

    def parse_log_line(self, line):
        """1行のログからHOSTとUSERの情報を抽出"""
        try:
            # HOST情報の抽出
            host_match = self.host_pattern.search(line)
            host = host_match.group(1) if host_match else None

            # USER情報の抽出
            user_match = self.user_pattern.search(line)
            user = user_match.group(1) if user_match else None

            if host or user:  # どちらかの情報が取得できた場合
                return {
                    'HOST': host,
                    'USER': user
                }
            return None

        except Exception as e:
            logging.debug(f"Error parsing line: {line}")
            logging.debug(f"Error details: {str(e)}")
            return None

    def parse_file(self, file_path):
        """ログファイルを解析してDataFrameを返す"""
        parsed_data = []
        file_path = Path(file_path)
        
        try:
            with open(file_path, 'r', encoding='cp932') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parsed_line = self.parse_log_line(line)
                    if parsed_line:
                        parsed_data.append(parsed_line)
                        
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {str(e)}")
            raise
            
        return pd.DataFrame(parsed_data)

    def process_and_save(self, input_file, output_file):
        """ログを解析してCSVとして保存"""
        df = self.parse_file(input_file)
        if not df.empty:
            df.to_csv(output_file, index=False, encoding='utf-8')
            logging.info(f"Extracted {len(df)} records with HOST and USER information")
        return df