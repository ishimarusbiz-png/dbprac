import mariadb
import os
import pandas as pd
from dotenv import load_dotenv

class LDM():
    def __init__(self):
        load_dotenv()
        self.config = {
            "host": os.getenv("DB_HOST"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_NAME")
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        """データベースに接続する"""
        try:
            self.conn = mariadb.connect(**self.config)
            self.cursor = self.conn.cursor()
            print("MariaDBに接続しました。")
        except mariadb.Error as e:
            print(f"接続エラー: {e}")
            raise

    def setup_database(self):
        """DBとテーブルを初期化する"""
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.config['database']}")
        self.cursor.execute("DROP TABLE IF EXISTS ips")
        self.cursor.execute("""
            CREATE TABLE ips( 
                id INT AUTO_INCREMENT PRIMARY KEY ,
                IpId VARCHAR(100) NOT NULL,
                TimeStamp BIGINT NOT NULL,
                Uri TEXT NOT NULL,
                HttpMethod VARCHAR(100) NOT NULL,
                ResponseCode INT NOT NULL,
                Bytes INT NOT NULL,
                Referrer TEXT NOT NULL,
                UserAgent TEXT NOT NULL)
        """)#上の決まりをスキーマという
        #primary key =table一意の値。主キーとも
        print("テーブルをリセットして作成しました。")

    def import_csv(self, csv_file, chunk_size=10000):
        """CSVデータをインポートする"""
        print(f"CSVのカラム名: {pd.read_csv(csv_file, nrows=0).columns.tolist()}")
        
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            chunk = chunk.fillna('')
 
            # TimeStampの型変換
            if 'TimeStamp' in chunk.columns:
                chunk['TimeStamp'] = pd.to_numeric(chunk['TimeStamp'], errors='coerce').fillna(0).astype(int)

            
            
            data = list(chunk[target_columns].itertuples(index=False, name=None))
            sql = """INSERT INTO ips (IpId, TimeStamp, Uri, HttpMethod, ResponseCode, Bytes, Referrer, UserAgent) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
            
            try:
                self.cursor.executemany(sql, data)
                self.conn.commit()
                print(f"{len(chunk)} 件のデータをインポートしました。")
            except mariadb.Error as e:
                print(f"挿入エラー: {e}")

    def fetch_recent_logs(self, limit=5):
        """最新のログを取得して表示する"""
        self.cursor.execute("SELECT * FROM ips LIMIT ?", (limit,))
        return self.cursor.fetchall()

    def close(self):
        """リソースを解放する"""
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()
        print("接続を閉じました。")

# ==========================================
# メイン処理
# ==========================================
if __name__ == "__main__":#直接実行時以下の命令
    db_manager = LDM()
    
    try:
        db_manager.connect()
        db_manager.setup_database()
        db_manager.import_csv("eclog.csv")
        
        print("\n--- インポート後のデータ確認 ---")
        logs = db_manager.fetch_recent_logs(5)
        for row in logs:
            print(f"ID: {row[0]} | IP: {row[1]} | Method: {row[4]} | URI: {row[3]} | Status: {row[5]}")
            
    finally:
        db_manager.close()
