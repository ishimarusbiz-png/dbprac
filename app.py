#coding:utf-8
import sys
import pandas as pd
from sqlalchemy import create_engine
from read import Read #read.py を読み込む
from flask import Flask, render_template, request, redirect, url_for

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
    db_tool=Read()
    rows=db_tool.get_data()
    return render_template("apppage.html",datalist=rows)




if __name__ == "__main__":
    # Renderは環境変数 PORT を指定してくるので、それを読み取る
    # 指定がない場合は 10000 を使う（Renderのデフォルトに合わせる）
    port = int(os.environ.get("PORT", 10000))
    # host="0.0.0.0" は必須！これがないと外から繋がらない
    app.run(host="0.0.0.0", port=port)