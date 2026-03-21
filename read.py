# coding: utf-8
import os
import pandas as pd
from sqlalchemy import create_engine
# mariadb の代わりに psycopg2 を使うことが一般的ですが、
# sqlalchemy が裏で動くので import 形式を整理します。

class Read:
    def __init__(self):
        # Renderの環境変数から情報を取得
        self.user = os.environ.get("DB_USER", "postgres")
        self.password = os.environ.get("DB_PASSWORD", "1111")
        self.host = os.environ.get("DB_HOST", "localhost")
        self.database = os.environ.get("DB_NAME", "sample_db")
        
        # 1. 接続URLの校正 (mysql+mysqlconnector -> postgresql)
        # ※RenderのExternal Connection Stringを参考にします
        engine_url = f"postgresql://{self.user}:{self.password}@{self.host}/{self.database}"
        self.engine = create_engine(engine_url)
        print("チェック完了：PostgreSQLに接続しました。")

    def install(self):
        try:
            csv_file = "sample.csv"
            # CSVの読み込み（ここはそのまま）
            df = pd.read_csv(csv_file, sep=r',(?![^()]*\))', engine='python', index_col=False)
            
            # 2. データベースへ書き込み
            # Postgresではテーブル名を小文字にするのが無難です
            df.to_sql('access_db', con=self.engine, if_exists='replace', index=False)
            print("PostgreSQLにCSVデータを同期しました！")
            
        except Exception as e:
            print(f"エラーが発生しました: {e}")
    
    def get_data(self):
        self.install()
        
        # 3. SQLAlchemyエンジンから直接データを読み込む（mariadbコネクタを使わない方法）
        # この方がライブラリの依存が少なくて済みます
        try:
            with self.engine.connect() as connection:
                # SQL実行（テーブル名は小文字に合わせます）
                query = "SELECT * FROM access_db ORDER BY 日時 DESC"
                df = pd.read_sql(query, connection)
                
                # HTMLで扱いやすいように辞書のリスト形式に変換
                rows = df.to_dict(orient='records')
                return rows
        except Exception as e:
            print(f"データ取得エラー: {e}")
            return []

# --- 実行 ---
if __name__ == "__main__":
    db_tool = Read()
    db_tool.install()