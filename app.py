#coding:utf-8
import mariadb
import sys
import pandas
import pymysql
from flask import Flask, render_template, request, redirect, url_for,session
import os
from db import LDM
import time
import math

# Flaskアプリケーションのインスタンス化
app = Flask(__name__)
app.secret_key = 'anything_is_ok'
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
    #
    start=time.perf_counter()

    
    # 辞書形式で取得できるように設定（Cursorクラスを変更するか手動変換）
    db_manager.cursor.execute("SELECT IpId, TimeStamp, Uri, HttpMethod, ResponseCode, Bytes, Referrer, UserAgent FROM ips LIMIT 15",)
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
    goal=time.perf_counter()
    print(f"{goal-start}秒 検索が終了しました")
    return render_template("apppage.html", log=log_data)

@app.route("/result",methods=["GET", "POST"])
def result():
    print("関数が呼ばれました")
    rows=[]
    try:
        start=time.perf_counter()
        print(start)
        s_word=request.form.get("s_words")
        print(f"{s_word}")
        word=f"{s_word}"
        db_manager.cursor.execute(f"SELECT IpId, TimeStamp, Uri, HttpMethod, ResponseCode, Bytes, Referrer, UserAgent FROM ips WHERE IpId = ? LIMIT 15",(word,))
        #課題：fstring以外でできないか？
        goal=time.perf_counter()
        print(f"{goal-start}秒 検索が終了しました")
    except Exception as e:
        print("正常に検索ができませんでした")
        print(e)
    
    
    try:
        rows = db_manager.cursor.fetchall()
        #課題：処理を軽くするためにループ処理にできないか
    except:
        print("結果処理ができません")
        print(e)
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

# データの新規登録画面を表示する
@app.route("/insert",methods=["GET", "POST"])
def insert_page():
    

        #入力されたデータを受け取る

    return render_template("insert.html")

@app.route("/insert_result",methods=["POST"])
def insert_result(): 
#★INSERTするデータをリストで取得する
    #新規登録画面で入力されたデータを受け取るための空リストを準備
    input_data=[]
    print("データ受取準備完了")
    if request.method == "POST":
        print("入力データを読み取ります")
    try:
        input_str=request.form.get("insert_d")
        print("入力された文字列データを受け取りました");
        print(input_str)
    except Exception as e:
        print(f"インプットエラー：{e}");
    #文字列にしたデータを、,で区切ってリストにする
    input_data=input_str.split(",")
    print("受け取り文字列をリスト化しました");
    print(input_data)
    #==DB処理==
    db_manager.insert(input_data);
    print(f"DB挿入後のリストの中身: {input_data}") # ★ここを確認！
    print(f"要素の数: {len(input_data)}")
    #辞書の形にする
    print("リストを表示用の辞書に変換します");
    try:
        r=input_data
        rows_inserted = {
            "id":        r[0], # 101
            "ipid":      r[1], # '12DE'
            "timestamp": r[2], # '637108...'
            "uri":       r[3], # '/wyszukiwanie...'
            "method":    r[4], # 'GET'
            "status":    r[5], # '200'
            "bytes":     r[6], # '9666'
            "referrer":  r[7], # '-'
            "ua":        r[8]  # 要素が9個あるのでr[8]まで取得可能
        }
        print(f"作成された辞書: {rows_inserted}")
    except IndexError as e:
        print(f"インデックスエラーが発生しました。要素数が足りません: {e}")
        rows_inserted = {"error": "データの形式が正しくありません"}
    return render_template("insert_result.html",inserted_data=rows_inserted)

@app.route("/delete_select",methods=["GET", "POST"])
def delete_select_page():

    print("DBを表示")
    #
    start=time.perf_counter()

    
    # 辞書形式で取得できるように設定（Cursorクラスを変更するか手動変換）
    db_manager.cursor.execute("SELECT id,IpId, TimeStamp, Uri, HttpMethod, ResponseCode, Bytes, Referrer, UserAgent FROM ips_test ",)
    #もし複数の列（Uri または UserAgent など）から探したい場合は、以下のように OR でつなぐ必要 X [*]
    rows = db_manager.cursor.fetchall()
    # テンプレートに渡すデータ（辞書のリストにする例）
    log_data = []
    for r in rows:
        log_data.append({
            "id" :r[0],
            "ipid": r[1],
            "timestamp": r[2],
            "uri": r[3],
            "method": r[4],
            "status": r[5],
            "bytes": r[6],
            "referrer": r[7],
            "ua": r[8]
        })
    goal=time.perf_counter()
    print(f"{goal-start}秒 検索が終了しました")
    return render_template("delete_select.html", log=log_data)

@app.route("/delete_result", methods=["GET", "POST"])
def delete_result():
    # 1. データが送られてきた時 (JavaScriptの fetch から)
    if request.method == "POST":
        target_id = request.form.get('id')
        session['target_id'] = target_id # セッションに保存
        return "OK", 200 # JavaScriptに「受け取ったよ」と返事をして終了

    # 2. 画面を表示する時 (window.location.href から)
    # ここは POST 以外の時（つまり GET の時）だけ実行されます
    display_id = session.get('target_id')
    print(f"削除対象ID：{display_id}")
    query = "SELECT * FROM ips_test WHERE id = %s"
    db_manager.cursor.execute(query, (display_id,))

    # 1行取得
    item_selected = db_manager.cursor.fetchone()

    return render_template("delete_result.html", id_selected=display_id,log=item_selected)





db_manager = LDM()


db_manager.connect()
# サーバーの起動
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9000)