import mariadb
import os
import pandas as pd
import time
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
        #==IpIdをindexに指定
        
        print("テーブルをリセットして作成しました。")
        try:
            db_manager.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ipid ON ips (IpId);")
        except Exception as e:
            print(f"エラー：{e}")

    def import_csv(self, csv_file, chunk_size=10000):
        """CSVデータをインポートする"""
        print(f"CSVのカラム名: {pd.read_csv(csv_file, nrows=0).columns.tolist()}")
        
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            chunk = chunk.fillna('')
 
            # TimeStampの型変換
            if 'TimeStamp' in chunk.columns:
                chunk['TimeStamp'] = pd.to_numeric(chunk['TimeStamp'], errors='coerce').fillna(0).astype(int);

            target_columns=["IpId", "TimeStamp", "Uri", "HttpMethod", "ResponseCode", "Bytes", "Referrer", "UserAgent"]
            
            data = list(chunk[target_columns].itertuples(index=False, name=None));
            sql = """INSERT INTO ips (IpId, TimeStamp, Uri, HttpMethod, ResponseCode, Bytes, Referrer, UserAgent) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
            
            try:
                self.cursor.executemany(sql, data);
                self.conn.commit();
                print(f"{len(chunk)} 件のデータをインポートしました。")
            except mariadb.Error as e:
                print(f"挿入エラー: {e}");

    #INSERT実行メソッド
    def insert_data(self):
        print("DBのインサートを開始します")

    def fetch_recent_logs(self, limit=5):
        """最新のログを取得して表示する"""
        self.cursor.execute("SELECT * FROM ips LIMIT ?", (limit,));
        return self.cursor.fetchall();

    def close(self):
        """リソースを解放する"""
        if self.cursor: self.cursor.close();
        if self.conn: self.conn.close();
        print("接続を閉じました。");
    #===
    #★（練習用）
    #===
    def input_testdata(self):
        """DBより100件のデータをランダムに取得し（練習用）"""
        print("テスト用テーブルをリセットします")
        self.cursor.execute("DROP TABLE ips_test ");
        self.cursor.execute("CREATE TABLE ips_test LIKE ips");
        #確認
        self.cursor.execute('SHOW TABLES')
        print(f"＞現在のテーブル一覧: {self.cursor.fetchall()}");

        print("ipsテーブルの上位100件をips_testにインポートします");
        print("＞インポート処理")
        start=time.perf_counter()
        try:

            sql_input = """INSERT INTO ips_test (IpId, TimeStamp, Uri, HttpMethod, ResponseCode, Bytes, Referrer, UserAgent)VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
            #＜没処理案＞:書き換えたコードにすることで処理速度が500倍になりました
            # self.cursor.execute("SELECT * FROM ips")
            # target_datas=self.cursor.fetchmany(100)
            # cleaned_data = self.cursor.execute("SELECT * FROM ips")
            self.cursor.execute("SELECT * FROM ips limit 100")
            target_datas=self.cursor.fetchall();
            try:
                cleaned_data=[]
                cleaned_data = [row[1:] for row in target_datas];
            except Exception as e:
                print(f"加工エラーが発生：{e}");
            self.cursor.executemany(sql_input,cleaned_data);
            self.conn.commit();
        except Exception as e:
            print(f"エラーが発生：{e}");
        finally:
            goal=time.perf_counter();
            print(f"{goal-start}秒 検索が終了しました");
        self.cursor.execute("SELECT * FROM ips_test");
        print(f"入力完了、データ：{self.cursor.fetchall()}");
        self.cursor.execute("SELECT COUNT(*) FROM ips_test")
        total_count=self.cursor.fetchone()[0];
        print(f"全件数：{total_count}件");

    def insert(self,list):
        print("==INSERT開始==")
        print(f"入力データ：{list}");
        print("IDを追加")
        self.cursor.execute("SELECT COUNT(*)  FROM ips_test");
        total=self.cursor.fetchone();
        data_total=total[0]
        print(data_total)
        data_total+=1;
        list.insert(0,data_total);
        print(f"ID追加後のデータ：{list}")

        data_tuple=tuple(list)
        print(f"タプル化：{data_tuple}");

        #タプルをDBに入れる
        try:
            self.cursor.execute("SELECT * FROM ips_test")
            self.cursor.execute(f"INSERT INTO ips_test VALUES {data_tuple}")
            self.cursor.execute("SELECT * FROM ips_test")
            inserted_data=self.cursor.fetchall()
            print(inserted_data);
        except Exception as e:
            print(f"エラー：{e}")

        return inserted_data


    

# ==========================================
# メイン処理
# ==========================================
if __name__ == "__main__":#直接実行時以下の命令
    db_manager = LDM()
    
    try:
        db_manager.connect()
        #db_manager.setup_database()
        #db_manager.import_csv("eclog.csv")
        db_manager.input_testdata()
        
        # print("\n--- インポート後のデータ確認 ---")
        # logs = db_manager.fetch_recent_logs(5)
        # for row in logs:
        #     print(f"ID: {row[0]} | IP: {row[1]} | Method: {row[4]} | URI: {row[3]} | Status: {row[5]}")
            
    finally:
        db_manager.close()
