import configparser
import logging
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class OracleMerger:
    def __init__(self, config_path=r"notebooks\research\.env.ini"):
        """
        Oracleデータとログデータをマージするためのクラス
        
        Parameters:
        -----------
        config_path : str
            設定ファイルのパス
        """
        self.config = self._load_config(config_path)
        self.engine = self._create_engine()
        
    def _load_config(self, config_path):
        """設定ファイルを読み込む"""
        config = configparser.ConfigParser()
        config.read(config_path)
        return config

    def _create_engine(self):
        """SQLAlchemyエンジンを作成"""
        try:
            username = self.config["ORACLE_ACH_CSYDB"]["DB_USERNAME"]
            password = self.config["ORACLE_ACH_CSYDB"]["DB_PASSWORD"]
            hostname = self.config["ORACLE_ACH_CSYDB"]["DB_HOSTNAME"]
            port = self.config["ORACLE_ACH_CSYDB"]["DB_PORT"]
            service_name = self.config["ORACLE_ACH_CSYDB"]["DB_SERVICE_NAME"]
            
            DATABASE_URL = f"oracle+cx_oracle://{username}:{password}@{hostname}:{port}/?service_name={service_name}"
            return create_engine(DATABASE_URL, echo=False)
            
        except Exception as e:
            logging.error(f"Failed to create database engine: {str(e)}")
            raise

    def get_personal_info(self):
        """Oracleから個人情報を取得"""
        query = """
        SELECT cd_person, 
               nm_person_last_n, 
               nm_person_first_n, 
               nm_workshop, 
               no_extension1, 
               nm_mail_address1
        FROM WPUSER.WXAAF010
        """
        
        try:
            df = pd.read_sql_query(query, self.engine)
            
            # 文字列カラムの後続スペースを削除
            string_columns = df.select_dtypes(include=['object']).columns
            for col in string_columns:
                df[col] = df[col].str.strip()
            
            # 空白を NaN に変換
            df = df.replace(r'^\s*$', pd.NA, regex=True)
            
            return df
            
        except Exception as e:
            logging.error(f"Failed to fetch personal info: {str(e)}")
            raise

    @staticmethod
    def extract_user_id(user_string):
        """ユーザー文字列から末尾6桁の数字を抽出"""
        if pd.isna(user_string):
            return user_string
            
        pattern = r'\d{6}$'
        match = re.search(pattern, str(user_string))
        
        return match.group(0) if match else user_string

    def process_listener_log(self, log_df):
        """リスナーログのユーザーIDを処理"""
        log_df = log_df.copy()
        log_df['USER'] = log_df['USER'].apply(self.extract_user_id)
        return log_df

    def merge_data(self, log_df):
        """ログデータとOracleデータをマージ"""
        try:
            # Oracleからデータを取得
            oracle_df = self.get_personal_info()
            
            # ログデータを処理
            processed_log_df = self.process_listener_log(log_df)
            
            # データのマージ
            merged_df = processed_log_df.merge(
                oracle_df,
                left_on='USER',
                right_on='cd_person',
                how='left'
            )
            
            return merged_df
            
        except Exception as e:
            logging.error(f"Failed to merge data: {str(e)}")
            raise

    def process_and_save(self, input_file, output_file):
        """ログファイルを読み込み、マージして保存"""
        try:
            # ログファイルの読み込み
            log_df = pd.read_csv(input_file)
            
            # データのマージ
            merged_df = self.merge_data(log_df)
            
            # 結果の保存
            merged_df.to_csv(output_file, index=False, encoding='cp932')
            logging.info(f"Successfully merged and saved data to {output_file}")
            
            return merged_df
            
        except Exception as e:
            logging.error(f"Failed to process and save file: {str(e)}")
            raise