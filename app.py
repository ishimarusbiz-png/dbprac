#coding:utf-8
import mariadb
import sys
import pandas
import pymysql
from flask import Flask, render_template, request, redirect, url_for
import os
from db import LDM

# Flaskアプリケーションのインスタンス化
app = Flask(__name__)
CORRECT_PASSWORD = "4311"

# ルーティング設定（ルートURL "/" にアクセスした時の処理）
@app.route("/", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user_input = request.form.get("password")
        
        if user_input == CORRECT_PASSWORD:
            return redirect(url_for('main_page'))
        else:
            error = "パスワードが違います。もう一度試してください。"
            # ここは「パスワードを間違えた時」の表示
            return render_template("login.html", error=error)

    # 【重要】ここに一行追加！ 
    # POSTじゃない時（最初にサイトに来た時）に表示する画面
    return render_template("login.html", error=None)


# 23行目以降をこれに書き換え
@app.route("/main")
def main_page():
    # ログイン成功後に表示したいHTMLファイルを指定（例: index.html）
    return render_template("index.html")

db_manager = LDM()

@app.route("/apppage",methods=["GET", "POST"])
def app_page():
    print("通常検索")
    db_manager.connect()
    # 辞書形式で取得できるように設定（Cursorクラスを変更するか手動変換）
    db_manager.cursor.execute("SELECT IpId, TimeStamp, Uri, HttpMethod, ResponseCode, Bytes, Referrer, UserAgent FROM ips ORDER BY IpId LIMIT 15",)
    #もし複数の列（Uri または UserAgent など）から探したい場合は、以下のように OR でつなぐ必要 X [*]
    rows = db_manager.cursor.fetchall()
    
    # テンプレートに渡すデータ（辞書のリストにする例）
    log_data = []
    for r in rows:
        log_data.append({
            "ipid": r[0],
            "timestamp": r[1],
            "uri": r[2],
            "method": r[3],
            "status": r[4],
            "bytes": r[5],
            "referrer": r[6],
            "ua": r[7]
        })
    
    return render_template("apppage.html", log=log_data)

@app.route("/result",methods=["GET", "POST"])
def result():
    print("関数が呼ばれました")
    try:
        s_words=request.form.get("search")
        print(request.form.get("search"))
        word=f"%{s_words}%"
        print(f"検索用に変換：{word}")
    except Exception as e:
        print("正常に検索ができませんでした")
    db_manager.cursor.execute("SELECT IpId, TimeStamp, Uri, HttpMethod, ResponseCode, Bytes, Referrer, UserAgent FROM ips WHERE IpId or TimeStamp or Uri or HttpMethod or ResponseCode or Bytes or Referrer or UserAgent like ? ORDER BY IpId LIMIT 15",(s_words,))
    
    rows = db_manager.cursor.fetchall()
    
    # テンプレートに渡すデータ（辞書のリストにする例）
    log_data = []
    for r in rows:
        log_data.append({
            "ipid": r[0],
            "timestamp": r[1],
            "uri": r[2],
            "method": r[3],
            "status": r[4],
            "bytes": r[5],
            "referrer": r[6],
            "ua": r[7]
        })
    return render_template("result.html", log=log_data)

# サーバーの起動
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9000)