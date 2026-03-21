# coding: utf-8
import mariadb  # 1. ライブラリは一番上で読み込む

class Read:  # 2. クラス名はシンプルに
    def __init__(self):
        # 初期化の時にメッセージを出す
        print("データベース操作クラスを準備しました")

    def install(self):
        # データベース接続情報
        config = {
            "host": "localhost",
            "user": "root",
            "password": "", # ここに決めたパスワードを入れる
            "database": "sample_db"
        }

        try:
            # MariaDBに接続
            conn = mariadb.connect(**config)
            print("MariaDBに接続しました！")

            cursor = conn.cursor()
            
            # ここで何か作業をする（今は閉じるだけ）
            cursor.close()
            conn.close()

        except mariadb.Error as e:
            print(f"接続エラー: {e}")

# --- 使い方 ---
# 3. インスタンスを作って
db_tool = Read()
# 4. 接続を実行する！
db_tool.install()

