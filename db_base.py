import mariadb
import os
import pandas as pd
from dotenv import load_dotenv

def Pu():
    def __init__
    #ファイル名を指定
    csv="eclog.csv"

    # .envファイルを読み込む
    load_dotenv()

    config = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME")
    }

    #mariadbに接続
    try:
        conn = mariadb.connect(**config)
        print("MariaDBにセキュアに接続しました！")

    except mariadb.Error as e:
        print(f"接続エラー: {e}")

    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
    print("DBを作成しました！")

    cursor.execute("""CREATE TABLE IF NOT EXISTS ips(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    IpId VARCHAR(100) NOT NULL,
                    TimeStamp BIGINT NOT NULL,
                    Uri VARCHAR(100) NOT NULL,
                    HttpMethod VARCHAR(100) NOT NULL,
                    ResponseCode INT NOT NULL,
                    Bytes INT NOT NULL,
                    Referrer TEXT NOT NULL,
                    UserAgent TEXT NOT NULL)""")
    print("テーブルを作成しました！")

    #データを分割して取り込み
    chunk_size = 10000  # 各チャンクのサイズ
    print("CSVのカラム名一覧:", pd.read_csv(csv, nrows=0).columns.tolist())

    for chunk in pd.read_csv(csv, chunksize=chunk_size):
    # --- 追加: 既存のテーブルをリセット（構造変更を反映させるため） ---
        cursor.execute("DROP TABLE IF EXISTS ips")
        cursor.execute("""CREATE TABLE ips(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    IpId VARCHAR(100) NOT NULL,
                    TimeStamp BIGINT NOT NULL,  -- ここが重要
                    Uri TEXT NOT NULL,
                    HttpMethod VARCHAR(100) NOT NULL,
                    ResponseCode INT NOT NULL,
                    Bytes INT NOT NULL,
                    Referrer TEXT NOT NULL,
                    UserAgent TEXT NOT NULL)""")
    print("テーブルをリセットして作成しました！")

    # --- データの取り込み ---
    for chunk in pd.read_csv(csv, chunksize=chunk_size):
        # 欠損値を埋める
        chunk = chunk.fillna('')

        # 重要：TimeStampが指数表記(float)で読み込まれるのを防ぎ、整数(int)に変換
        if 'TimeStamp' in chunk.columns:
            chunk['TimeStamp'] = pd.to_numeric(chunk['TimeStamp'], errors='coerce').fillna(0).astype(int)

        target_columns = [
            'IpId', 'TimeStamp', 'Uri', 'HttpMethod', 
            'ResponseCode', 'Bytes', 'Referrer', 'UserAgent'
        ]
        
        # リスト化
        data = list(chunk[target_columns].itertuples(index=False, name=None))

        sql = """INSERT INTO ips (IpId, TimeStamp, Uri, HttpMethod, ResponseCode, Bytes, Referrer, UserAgent) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
        
        try:
            cursor.executemany(sql, data)
            conn.commit()
            print(f"{len(chunk)} 件のデータをインポートしました。")
        except mariadb.Error as e:
            print(f"データ挿入エラー: {e}")

    # ここでもう一度 execute を使っても、上書きしていなければ正常に動きます
    print("このDBとテーブルに格納されました",end="")
    cursor.execute("SHOW TABLES") 
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    cursor.execute("""
        SELECT * FROM ips
        LIMIT 5
    """)

    for row in cursor.fetchall():
        print(f"ID: {row[0]} | IP: {row[1]} | Method: {row[4]} | URI: {row[3]} | Status: {row[5]}")

    # 最後にリソースを解放
    cursor.close()
    conn.close()