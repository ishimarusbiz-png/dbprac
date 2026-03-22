#coding:utf-8
import mariadb
import sys
import pandas
import pymysql
from flask import Flask, render_template, request, redirect, url_for
import os

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

@app.route("/apppage")
def app_page():
    # ログイン成功後に表示したいHTMLファイルを指定（例: index.html）
    return render_template("apppage.html")


# サーバーの起動
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9000)