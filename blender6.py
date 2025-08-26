from flask import Flask, jsonify, request, send_from_directory
import pendulum
import sqlite3
from pathlib import Path
import pandas as pd
from flask_socketio import SocketIO, emit

app = Flask(__name__, template_folder='dist')
# socketio = SocketIO(app, cors_allowed_origins="http://127.0.0.1:8000")

# SQLite 数据库文件路径
DATABASE = 'db.sqlite3'  # 请替换为你的数据库文件路径

delay = {"510300": 0, "510500": 0}

DIST_DIR = Path.cwd() / 'templates'

@app.route('/')
def serve_index():
    return send_from_directory(DIST_DIR, 'index.html')

@app.route('/assets/<path:filename>')
def serve_assets(filename):
    return send_from_directory(DIST_DIR / 'assets', filename)

@app.route('/<path:filename>')
def serve_static(filename):
    return send_from_directory(DIST_DIR, filename)

@app.route('/api/test')
def test():
    tick = request.args.get('tick')
    symbol = request.args.get('symbol')
    if symbol == None:
        print("symbol None")
        symbol = "510300"
    interval= 3
    now = pendulum.now("Asia/Shanghai")
    if now < now.at(9, 30, 0):
        wait_time = remain(now, 9,30,0)
        return {"time":now.to_iso8601_string(), "remain": wait_time, "data": []}
    elif not Path("db.sqlite3").exists():
        wait_time = -1
    elif now < now.at(11, 30, 0):
        current_second = now.second + now.microsecond / 1_000_000  # 当前秒数（带小数）
        wait_time = interval - (current_second % interval)
    elif now < now.at(13, 0, 0):
        wait_time = remain(now, 13, 0, 0)
    elif now < now.at(15, 0, 0):
        current_second = now.second + now.microsecond / 1_000_000  # 当前秒数（带小数）
        wait_time = interval - (current_second % interval)
    else:
        wait_time = remain(now, 23, 59, 59)

    res_data = get_db(symbol, tick)
    # print(res_data)
    for item in res_data:
        item["chg"] *= 10
        # item["arrow"] *= 10
    if res_data == []:
        payload = {"time":now.to_iso8601_string(), "remain": wait_time, "data": res_data}
    else:
        payload = {"time":now.to_iso8601_string(), "remain": wait_time, "data": res_data, "latest": res_data[-1]}
    print("===============================================")
    # print(payload)
    return payload

@app.route("/notify", methods=["GET"])
def notify():
    # blender2 完成一次下载数据后 立刻 触发这个，从而决定是否发送emit给前端
    # tick 是当前已经保存的最新ckpt的字符串形式  http://127.0.0.1:5000/notify?tick=2025-04-11%2014%3A59%3A57
    tick = request.args.get("tick")
    print(tick)
    if tick == None:
        emit({"title": "Vol test", "msg": "Up or Down"})
        return "ceshi", 200

    df = load_db(tick)
    calc_300(df)
    # calc_500(df)
    print(delay)
    return "通知已处理", 200


# @socketio.on("connect")
# def handle_connect():
#     print("客户端已连接")


# @socketio.on("disconnect")
# def handle_disconnect():
#     print("客户端已断开")

def load_db(tick):
    # tick = "2025-04-11 14:59:57"
    with sqlite3.connect("db.sqlite3") as conn:
        query = """
            SELECT * FROM pilot WHERE ckpt <= ?
        """

        # 传 tick 参数给 SQL 查询
        df = pd.read_sql(query, conn, params=(tick,))
    return df

def get_db(symbol, tick=None, db_name="db.sqlite3"):
    if tick == None or tick == "undefined":
        # tick = str(pendulum.now("Asia/Shanghai").at(9,30,0))
        with sqlite3.connect(db_name) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT ckpt FROM pilot WHERE symbol = ? ORDER BY ckpt ASC LIMIT 1", (symbol,))
            row = cursor.fetchone()
            tick = row['ckpt'] if row else str(pendulum.now("Asia/Shanghai").at(9,30,0))
    with sqlite3.connect(db_name) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT ckpt, chg, pcr, berry, berry_ma, vol, vol_inc, oi, oi_inc, oi_call, oi_put FROM pilot WHERE symbol = ? AND ckpt > ?",  (symbol, tick))  # 假设表名为 pilot
        rows = cursor.fetchall()
    result_list = []  # 创建一个空列表
    for row in rows:
        result_list.append(dict(row))  # 将每一行转换为字典并添加到列表中
    return result_list

# @app.route('/api/pilot')
# def get_pilot_data():
#     tick = request.args.get('tick')
#     res = get_db("510300", tick)
#     return jsonify(res)


def get_datetime():
    with sqlite3.connect("db.sqlite3") as conn:
        cursor = conn.cursor()

        # 查询数据库并限制返回一行
        query = f"""
            SELECT DATETIME(ckpt) AS ckpt
            FROM stock
            WHERE symbol = '510300'
            ORDER BY ckpt DESC
            LIMIT 1;
        """

        cursor.execute(query)
        row = cursor.fetchone()  # 获取一行结果

    if row:
        return jsonify(row[0])
    else:
        return None




def calc_300(df):
    symbol = "510300"
    df = df[df["symbol"] == symbol]
    print(df["vol_inc"].iloc[-1], df["oi_call"].iloc[-1], delay[symbol])
    if df["vol_inc"].iloc[-1] > 10 and df["oi_call"].iloc[-1] > 2 and delay[symbol] <= 0:
        delay[symbol] = 10
        emit({"title": "Vol" + symbol, "msg": "Up"})
    elif df["vol_inc"].iloc[-1] > 10 and df["oi_put"].iloc[-1] > 2 and delay[symbol] <= 0:
        delay[symbol] = 10
        emit({"title": "Vol" + symbol, "msg": "Down"})
    else:
        delay[symbol] -= 1
        return "已通知，此次delay，已处理", 200


def calc_500(df):
    symbol = "510500"
    df = df[df["symbol"] == symbol]
    if df["vol_inc"].iloc[-1] > 10 and df["oi_call"].iloc[-1] > 2 and delay[symbol] <= 0:
        delay[symbol] = 10
        emit({"title": "Vol" + symbol, "msg": "Up"})
    elif df["vol_inc"].iloc[-1] > 10 and df["oi_put"].iloc[-1] > 2 and delay[symbol] <= 0:
        delay[symbol] = 10
        emit({"title": "Vol" + symbol, "msg": "Down"})
    else:
        delay[symbol] -= 1
        return "已通知，此次delay，已处理", 200


def emit(data):
    if data:
        # socketio.emit("update_data", {"data": data}, namespace="/")
        print(f"收到通知，已推送数据: {data}")
        return "通知已处理", 200
    else:
        return "获取数据失败", 500


def remain(now, hour, minute, second):
    wait_time = (now.at(hour, minute, second) - now).total_seconds()
    print(f"Waiting until {hour}:{minute}:{second} ... ({wait_time} seconds)")
    return wait_time

if __name__ == '__main__':
    app.run(debug=True, host="127.0.0.1", port=8000)
    # socketio.run(app, host="127.0.0.1", port=5000, debug=True)