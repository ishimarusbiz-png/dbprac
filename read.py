# coding: utf-8
import os
import mariadb
import pandas as pd
from sqlalchemy import create_engine

class Read:
    def __init__(self):
        # Renderの設定画面から自動で情報を取ってくる（コードにはパスワードを書かない！）
        self.user = os.environ.get("DB_USER", "root")
        self.password = os.environ.get("DB_PASSWORD", "1111") # ローカルでは空、Renderでは自動取得
        self.host = os.environ.get("DB_HOST", "localhost")
        self.database = os.environ.get("DB_NAME", "sample_db")
        
        # 1. データベースの自動作成（Renderの外部DBを使う場合は不要なことが多いですが、念のため）
        self.create_db_if_not_exists()
        
        # 2. SQLAlchemyエンジンの作成
        engine_url = f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}"
        self.engine = create_engine(engine_url)
        print("チェック完了：環境変数を使ってデータベースに接続しました。")

    def create_db_if_not_exists(self):
        # localhost（自分のPC）の時だけ実行するようにガードをかける
        if self.host == "localhost":
            conn = mariadb.connect(user=self.user, password=self.password, host=self.host)
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.execute(f"USE {self.database}") #これを使うと指示
            conn.commit()
            cursor.close()
            conn.close()

    def install(self):
        try:
            csv_file = "sample.csv"
            df = pd.read_csv(csv_file, sep=r',(?![^()]*\))', engine='python', index_col=False)
            df.to_sql(f'access_DB', con=self.engine, if_exists='replace', index=False)
            print("Render上のデータベースにCSVデータを同期しました！")
            conn = mariadb.connect(
                user=self.user, 
                password=self.password,
                host=self.host,
                database=self.database)
            cursor = conn.cursor()
            print("カーソル準備完了")
            # 修正後：テーブル名（access_DB）を指定する！
            cursor.execute("SELECT * FROM access_DB ORDER BY 日時 DESC")
            print("CSVファイルの初期並び替えが完了しました！")
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"エラーが発生しました: {e}")
    
    def get_data(self): # 1. データベースからデータを取ってくる
        self.install() # ここでデータの同期と並び替えが行われる
        
        # 2. MariaDBに接続して全データをリストで取得
        import mariadb
        conn = mariadb.connect(
            user=db_tool.user, 
            password=db_tool.password, 
            host=db_tool.host, 
            database=db_tool.database
        )
        cursor = conn.cursor(dictionary=True) # 辞書形式で取るとHTMLで扱いやすい！
        cursor.execute("SELECT * FROM access_DB ORDER BY 日時 DESC")
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()

    # 3. 取ってきたデータを HTML に放り込む！
        return rows

# --- 実行 ---
db_tool = Read()
db_tool.install()
