import sqlite3
import re
import json
import time
import pendulum
import pandas as pd
from urllib.parse import quote
import numpy as np

# import threading
from tenacity import retry, stop_after_attempt
import blender

# pd.set_option("display.max_rows",None)
# import warnings
# import urllib3
# warnings.filterwarnings("ignore", category=urllib3.exceptions.NotOpenSSLWarning)
import requests

TICK = None
DFP = None
DFO = None
DFS = None
DFF = None
OPLIST = []
BASE = {"510050":2.778,"510300":3.98,"510500":5.711}


def launch():
    """
    在给定的时间间隔（秒）上执行函数 func，每次在 interval 的整数倍时间点对齐
    判断是否等待，然后执行，最后等待interval
    """
    interval = 3
    while True:
        now = pendulum.now("Asia/Shanghai")
        print("=============")
        print(now)
        if now < now.at(9, 29, 50):
            wait(now, 9, 29, 50)
            blender.reset_table()
            blender.category_etf()
            time.sleep(2)
            create_all_table()
            load_db_init()
            continue  # 检查跳转后的时间
        elif now < now.at(9, 30, 0):
            # check holiday
            if is_working_day():
                wait(now, 9, 30, 0)
                continue
            else:
                print("holiday")
                wait(now, 23, 59, 59)
                exit(0)
        elif now < now.at(11, 30, 0):
            pass
        elif now < now.at(13, 0, 0):
            wait(now, 13, 0, 0)
            continue  # 检查跳转后的时间
        elif now < now.at(15, 0, 0):
            pass
        elif now < now.at(20, 0, 0):
            wait(now, 20, 0, 0)
            continue  # 检查跳转后的时间
        else:
            wait(now, 23, 59, 59)
            mv_pilot()
            exit(0)
        remove_hist(now)
        fetch_online()
        pilot()
        send_notify(now)

        now = pendulum.now("Asia/Shanghai")
        current_second = now.second + now.microsecond / 1_000_000  # 当前秒数（带小数）

        # 计算等待时间，使执行对齐到 interval 的整数倍
        wait_time = interval - (current_second % interval)
        time.sleep(wait_time)  # 等待到下一个 interval 的倍数


def create_all_table(db_name="db.sqlite3"):
    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        # 删除旧表
        # cursor.execute("DROP TABLE IF EXISTS option")
        # 创建 option 表
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS option (
                name TEXT,
                symbol TEXT,
                ckpt TEXT,
                ckptol TEXT,
                vol_buy INTEGER,
                price_buy REAL,
                price REAL,
                price_sell REAL,
                vol_sell INTEGER,
                oi REAL,
                inc REAL,
                price_exec REAL,
                price_yest REAL,
                price_open REAL,
                price_high REAL,
                price_low REAL,
                vol INTEGER,
                obv REAL,
                type TEXT,
                expiration TEXT,
                expire INTEGER
            )
        """
        )
        conn.commit()
    print(f"Table 'option' in database '{db_name}' has been reset.")

    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        # 删除旧表
        # cursor.execute("DROP TABLE IF EXISTS stock")
        # 创建 option 表
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stock (
                name TEXT,
                symbol TEXT,
                ckpt TEXT,
                ckptol TEXT,
                chg REAL,
                price_open REAL,
                price_yest REAL,
                price REAL,
                price_high REAL,
                price_low REAL,
                price_buy REAL,
                price_sell REAL,
                vol INTEGER,
                obv INTEGER
            )
        """
        )
        conn.commit()

    print(f"Table 'stock' in database '{db_name}' has been reset.")

    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        # 删除旧表
        # cursor.execute("DROP TABLE IF EXISTS future")
        # 创建 option 表
        # id INTEGER PRIMARY KEY AUTOINCREMENT,
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS future (
                name TEXT,
                symbol TEXT,
                ckpt TEXT,
                ckptol TEXT,
                price_open REAL,
                price_high REAL,
                price_low REAL,
                price REAL,
                vol INTEGER,
                obv REAL,
                oi REAL
            )
        """
        )
        conn.commit()
    print(f"Table 'future' in database '{db_name}' has been reset.")

    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        # 删除旧表
        # cursor.execute("DROP TABLE IF EXISTS pilot")
        # 创建 option 表
        # id INTEGER PRIMARY KEY AUTOINCREMENT,
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pilot (
                symbol TEXT,
                ckpt TEXT,
                chg REAL,
                chg_ma REAL,
                pcr REAL,
                berry REAL,
                berry_ma REAL,
                vol REAL,
                vol_inc REAL,
                oi REAL,
                oi_inc REAL,
                oi_call REAL,
                oi_put REAL
            )
        """
        )
        conn.commit()

    print(f"Table 'pilot in database '{db_name}' has been reset.")


def load_db_init():
    global DFO
    global DFS
    global DFF
    global DFP
    global OPLIST
    with sqlite3.connect("db.sqlite3") as conn:

        cursor = conn.cursor()
        cursor.execute("SELECT op_symbol FROM category WHERE op_symbol LIKE 'CON%'")
        # if market == "cffex":
        #     cursor.execute("SELECT op_symbol FROM category WHERE op_symbol NOT LIKE 'CON%'")
        OPLIST = [row[0] for row in cursor.fetchall()]

        query = """
            SELECT *
            FROM option
            """
        df = pd.read_sql(query, conn)
        df["ckpt"] = pd.to_datetime(df["ckpt"])
        df["ckptol"] = pd.to_datetime(df["ckptol"])
        df["vol"] = df["vol"].astype(int)
        df["oi"] = df["oi"].astype(int)
        DFO = df

        query = """
            SELECT *
            FROM stock
            """
        df = pd.read_sql(query, conn)
        df["ckpt"] = pd.to_datetime(df["ckpt"])
        df["ckptol"] = pd.to_datetime(df["ckptol"])
        DFS = df

        query = """
            SELECT *
            FROM future
            """
        df = pd.read_sql(query, conn)
        df["ckpt"] = pd.to_datetime(df["ckpt"])
        df["ckptol"] = pd.to_datetime(df["ckptol"])
        DFF = df

        query = """
            SELECT *
            FROM pilot
            """
        df = pd.read_sql(query, conn)
        df["ckpt"] = pd.to_datetime(df["ckpt"])
        # df["ckptol"] = pd.to_datetime(df["ckptol"])
        DFP = df



def fetch_online():
    now = pendulum.now("Asia/Shanghai")
    now_str = str(now)[:19]
    print("start fetch_online")

    # option
    try:
        response_text = fetch_data(OPLIST)
    except RuntimeError as e:
        print(f"Error fetching data: {e}")
        return
    data_lines = response_text.strip().split("\n")
    parsed_data = []
    for line in data_lines:
        try:
            parsed_dict = parse_response(line, "option")
            parsed_dict["ckpt"] = now_str
            parsed_data.append(parsed_dict)
        except ValueError as e:
            print(f"Error parsing line: {line}, {e}")
    save_to_db(parsed_data, "option", "db.sqlite3")
    save_to_df(parsed_data, "option")

    # stock
    symbols = ["sh000016", "sh000300", "sh000905", "sh000852"]
    try:
        response_text = fetch_data(symbols)
    except RuntimeError as e:
        print(f"Error fetching data: {e}")
        return
    data_lines = response_text.strip().split("\n")
    parsed_data = []
    for line in data_lines:
        try:
            parsed_dict = parse_response(line, "stock")
            parsed_dict["ckpt"] = now_str
            parsed_data.append(parsed_dict)
        except ValueError as e:
            print(f"Error parsing line: {line}, {e}")
    save_to_db(parsed_data, "stock", "db.sqlite3")
    save_to_df(parsed_data, "stock")

    # future
    symbols = ["nf_IH0", "nf_IF0", "nf_IC0", "nf_IM0", "nf_T0"]
    try:
        response_text = fetch_data(symbols)
    except RuntimeError as e:
        print(f"Error fetching data: {e}")
        return
    data_lines = response_text.strip().split("\n")
    parsed_data = []
    for line in data_lines:
        try:
            parsed_dict = parse_response(line, "future")
            parsed_dict["ckpt"] = now_str
            parsed_data.append(parsed_dict)
        except ValueError as e:
            print(f"Error parsing line: {line}, {e}")
    save_to_db(parsed_data, "future", "db.sqlite3")
    save_to_df(parsed_data, "future")

    print(pendulum.now())
    print("finish fetch_online")


# 下载数据
@retry(stop=stop_after_attempt(2))
def fetch_data(symbols):
    url = "http://hq.sinajs.cn/list=" + ",".join(symbols)
    headers = {"Referer": "http://vip.stock.finance.sina.com.cn/"}
    response = requests.get(url, headers=headers, timeout=5)

    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch data: HTTP {response.status_code}")

    return response.text


# 保存数据到 `option` 表
def save_to_db(data, pattern, db_name="db.sqlite3"):
    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        # 插入新数据
        for payload in data:
            # payload_converted = {k: int(v) if isinstance(v, np.int64) else v for k, v in payload.items()}
            columns = ", ".join(payload.keys())  # 动态生成列名
            placeholders = ", ".join(["?"] * len(payload))  # 动态生成占位符
            values = tuple(payload.values())  # 动态获取值

            cursor.execute(
                f"""
                INSERT INTO {pattern} ({columns})
                VALUES ({placeholders})
            """,
                values,
            )
        conn.commit()

    print(f"Saved {len(data)} records to {pattern} table.")

def mv_pilot(db_name="db.sqlite3"):
    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        # 检查 history 表是否存在
        if not table_exists(cursor, 'history'):
            # 从 pilot 表复制结构和数据创建 history 表
            cursor.execute('CREATE TABLE history AS SELECT * FROM pilot')

        # 将 pilot 表的数据追加到 history 表
        cursor.execute('INSERT INTO history SELECT * FROM pilot')

        # # 删除 pilot 表的数据
        # cursor.execute('DELETE FROM pilot')
        # 删除整个 pilot 表
        cursor.execute('DROP TABLE pilot')
        cursor.execute('DROP TABLE category')
        cursor.execute('DROP TABLE future')
        cursor.execute('DROP TABLE option')
        cursor.execute('DROP TABLE stock')

        conn.commit()

    print(f"Backup pilot to history table.")

# 保存数据到  全局DF
def save_to_df(data, pattern):
    global DFO
    global DFS
    global DFF
    global DFP
    if pattern == "option":
        df = DFO
    elif pattern == "stock":
        df = DFS
    elif pattern == "future":
        df = DFF
    elif pattern == "pilot":
        df = DFP
    else:
        raise
    """
    将字典列表 data 追加到 DataFrame dfo 中，保持数据类型。

    Args:
        data (list of dict): 要追加的数据，每个字典代表一行。
        dfo (pd.DataFrame): 目标 DataFrame。

    Returns:
        pd.DataFrame: 追加数据后的 DataFrame。
    """
    if not data:
        return df  # 如果 data 为空，直接返回原始 DFO

    new_df = pd.DataFrame(data)  # 将字典列表转换为 DataFrame

    # 确保新 DataFrame 的列与 DFO 的列一致
    for col in df.columns:
        if col not in new_df.columns:
            new_df[col] = None  # 如果列不存在，添加空列

    new_df = new_df[df.columns]  # 确保列的顺序一致

    # 尝试将新 DataFrame 的数据类型转换为 DFO 的数据类型
    try:
        new_df = new_df.astype(df.dtypes)
    except ValueError as e:
        print(f"数据类型转换失败：{e}")
        return df  # 如果转换失败，返回原始 DFO

    # 使用 concat 追加 DataFrame
    df = pd.concat([df, new_df], ignore_index=True)
    # 将更新后的 df 赋值回全局变量
    if pattern == "option":
        DFO = df
    elif pattern == "stock":
        DFS = df
    elif pattern == "future":
        DFF = df
    elif pattern == "pilot":
        DFP = df
    return df


# 数据解析函数
def parse_response(line, pattern):
    # 匹配双引号中的内容
    match = re.search(r'"(.*?)"', line)
    if not match:
        raise ValueError("Response line does not contain double-quoted content")
    # 解析为字典
    content = match.group(1).split(",")
    if pattern == "option":
        # print(content)
        payload = {
            "name": content[37],
            "symbol": content[36],
            "vol": content[41],
            "obv": content[42],
            "vol_buy": content[0],
            "price_buy": content[1],
            "price": content[2],
            "price_sell": content[3],
            "vol_sell": content[4],
            "oi": content[5],
            "inc": content[6],
            "price_exec": content[7],
            "price_yest": content[8],
            "price_open": content[9],
            "ckptol": content[32],
            "price_high": content[39],
            "price_low": content[40],
            "type": "call" if content[45] == "C" else "put",
            "expiration": content[46],
            "expire": content[47],
        }
    elif pattern == "stock":
        symbol_dict = {
            "上证50": "510050",
            "沪深300": "510300",
            "中证500": "510500",
            "中证1000": "512100",
        }
        symbol = symbol_dict.get(content[0], "未知")
        payload = {
            "name": content[0],
            "symbol": symbol,
            "ckptol": f"{content[30]} {content[31]}",
            "chg": (float(content[3]) - float(content[2])) / float(content[2]) * 100,
            "price_open": content[1],
            "price_yest": content[2],
            "price": content[3],
            "price_high": content[4],
            "price_low": content[5],
            "price_buy": content[6],
            "price_sell": content[7],
            "vol": content[8],
            "obv": content[9],
        }
    elif pattern == "future":
        symbol_dict = {
            "上证50指数期货连续": "510050",
            "沪深300指数期货连续": "510300",
            "中证500指数期货连续": "510500",
            "中证1000指数期货连续": "512100",
            "10年期国债期货连续": "T0",
        }
        symbol = symbol_dict.get(content[49], "未知")
        payload = {
            "name": content[49],
            "symbol": symbol,
            "ckptol": f"{content[36]} {content[37]}",
            "price_open": content[0],
            "price_high": content[1],
            "price_low": content[2],
            "price": content[3],
            "vol": content[4],
            "obv": content[5],
            "oi": content[6],
        }
    else:
        raise ValueError("parse response with none pattern")
    return payload


# option
# 0 买量(◎)，买价，最新价，卖价，卖量，持仓量，涨幅，行权价，昨收价，开盘价，
# 10 涨停价，跌停价(11)，申卖 价五，申卖量五，申卖价四，申卖量四，申卖价三，申卖量三，申卖价二，申卖量二，
# 20 中卖价一，申卖量一，申买价一，申买量一 ，申买价二，申买量二，电买价三，申买量三，申买价四，电买量四，
# 30 申买价五，申买量五，行情时间，主力合约标识，状态码，标的证券类型，标的股票，期权合约简称，振幅(38)，最高价，
# 40 最低价，成交量，成交额，分红调整标志，昨结算价，认购认沽标志，到期日，剩余天数，虚实值标志，内在价值，
# 50 时间价值

# stock
# 0 名字， 开盘价, 昨曰收盘价, 最近成交价, 最高成交价,最低成交价,买入价卖出价,成交数量,成交金额,
# 10 买数量一, 买价位一,买数量二,买价位二,买数量三,买价位三,买数量四，买价位四,买数量五,买价位五,
# 20 卖数量一, 卖价位一,卖数量二,卖价位二,卖数量三,卖价位三,卖数量四,卖价位四,卖数量五,卖价位五,
# 30 行情曰期, 行情时间,停牌状态

# feature
# 0 今开盘价， 最高价，最低价， 最新价，成交量，成交额 持仓量， 最新价？，未知，涨停价
# 10 跌停价， 未知， 未知 ， 2580， 2577， 56036， 2595.8 ， 5， 未知， 0，
# 20 未知0
# 30              日期（36） 时间37
# 40    name 49
#


def fetch_tick_latest(table="stock"):
    # 最新的本地数据时间
    # 数据库连接
    with sqlite3.connect("db.sqlite3") as conn:
        cursor = conn.cursor()

        # 查询数据库并限制返回一行
        query = f"""
            SELECT DATETIME(ckpt) AS ckpt
            FROM {table}
            WHERE symbol = '510300'
            ORDER BY ckpt DESC
            LIMIT 1;
        """

        cursor.execute(query)
        row = cursor.fetchone()  # 获取一行结果

    if row:
        columns = ["ckpt"]
        # return dict(zip(columns, row))["ckpt"]
        return pendulum.parse(row[0], tz="Asia/Shanghai")
    else:
        return None


def fetch_option(symbol, type, tick):
    global DFO
    tick_start = tick.add(minutes=-30).to_datetime_string()
    df = DFO[(DFO["symbol"] == symbol) & (DFO["type"] == type)]

    # 按 ckpt 分组
    df_grouped = df.groupby("ckpt")[["vol", "oi"]].sum().reset_index()
    # df_grouped = df_grouped.tail(300)

    return df_grouped


def create_berry(symbol, df):
    if symbol == "510300":
        res_se = 10 * df["chg"] + df["pcr"] - 92
    elif symbol == "510500":
        res_se = 10 * df["chg"] + df["pcr"] - 102
    return res_se


def create_inc(se, window):
    ma_se = se.rolling(window, min_periods=1).mean().values
    inc_se = se - ma_se
    return inc_se


def create_pilot(symbol, tick):
    tick_str = tick.to_datetime_string()
    call_df = fetch_option(symbol, "call", tick)
    put_df = fetch_option(symbol, "put", tick)

    # 同步两个df保证一致
    latest_call = call_df["ckpt"].max()
    latest_put = put_df["ckpt"].max()

    if latest_call != latest_put:
        print(f"警告: call ({latest_call}) 和 put ({latest_put}) 的最新 ckpt 不一致")
        # 可选择等待或丢弃最新一行
        if latest_call > latest_put:
            call_df = call_df[call_df["ckpt"] <= latest_put]
        else:
            put_df = put_df[put_df["ckpt"] <= latest_call]
        print(call_df)
        print(put_df)
        raise

    pcr_df = pd.merge(call_df, put_df, on="ckpt", suffixes=("_call", "_put"))
    pcr_df["pcr"] = pcr_df["vol_put"] / pcr_df["vol_call"] * 100
    pcr_df["vol"] = (pcr_df["vol_put"] + pcr_df["vol_call"]).astype(float)
    pcr_df["oi"] = pcr_df["oi_put"] + pcr_df["oi_call"]
    pcr_df["oi"] = pcr_df["oi"].astype(float)

    if pcr_df.empty:
        return None

    mdf = pd.merge(
        pcr_df, DFS[DFS["symbol"] == symbol][["ckpt", "chg"]], on="ckpt", how="left"
    )
    mdf["berry"] = create_berry(symbol, mdf)
    mdf["berry_ma"] = mdf["berry"].rolling(300, min_periods=1).mean().values
    mdf["chg_ma"] = mdf["chg"].rolling(300, min_periods=1).mean().values
    mdf["vol_inc"] = create_inc(mdf["vol"], 47) / 1000
    mdf["oi_inc"] = create_inc(mdf["oi"], 47) / 1000
    mdf["oi_call"] = create_inc(mdf["oi_call"], 47) / 1000
    mdf["oi_put"] = create_inc(mdf["oi_put"], 47) / 1000

    latest_row = mdf.iloc[-1]
    pilot_dict = {}
    pilot_dict["symbol"] = symbol
    pilot_dict["ckpt"] = tick_str
    pilot_dict["chg"] = latest_row["chg"]
    pilot_dict["chg_ma"] = latest_row["chg_ma"]
    pilot_dict["pcr"] = latest_row["pcr"]
    pilot_dict["berry"] = latest_row["berry"]
    pilot_dict["berry_ma"] = latest_row["berry_ma"]
    pilot_dict["vol"] = latest_row["vol"]
    pilot_dict["vol_inc"] = latest_row["vol_inc"]
    pilot_dict["oi"] = latest_row["oi"]
    pilot_dict["oi_inc"] = latest_row["oi_inc"]
    pilot_dict["oi_call"] = latest_row["oi_call"]
    pilot_dict["oi_put"] = latest_row["oi_put"]

    # pilot_dict["arrow"] = create_arrow(symbol, tick_str)

    if latest_row["vol_inc"] < 0:
        print(pilot_dict)
        print(latest_row)
        print(call_df)
        print(put_df)
        print(pcr_df)
        print(mdf)
        raise
    # fix 9:40
    tick = pendulum.parse(tick_str)
    end_tick = tick.at(9, 40, 0)
    if tick < end_tick:
        duration = (end_tick - tick).in_seconds()
        if duration > 600 or duration < 0:
            print("=================")
            print(f"tick {tick} , end_tick {end_tick}")
            raise
        pilot_dict["pcr"] = pilot_dict["pcr"] * (600 - duration) / 600
        pilot_dict["berry"] = pilot_dict["berry"] * (600 - duration) / 600
        pilot_dict["berry_ma"] = pilot_dict["berry_ma"] * (600 - duration) / 600
        pilot_dict["vol"] = pilot_dict["vol"] * (600 - duration) / 600
        pilot_dict["vol_inc"] = pilot_dict["vol_inc"] * (600 - duration) / 600
        pilot_dict["oi"] = pilot_dict["oi"] * (600 - duration) / 600
        pilot_dict["oi_inc"] = pilot_dict["oi_inc"] * (600 - duration) / 600
        # pilot_dict["pcr_inc"] = pilot_dict["pcr_inc"] * (600 - duration) / 600

    return pilot_dict

def synthesize_futures_price(call_price, put_price, strike_price, risk_free_rate, time_to_expiry):
    """
    Calculate the synthetic futures price using Put-Call Parity.

    Parameters:
    - call_price: Price of the call option
    - put_price: Price of the put option
    - strike_price: Strike price of the options
    - risk_free_rate: Annualized risk-free interest rate (as a decimal, e.g., 0.05 for 5%)
    - time_to_expiry: Time to option expiry in years (e.g., 0.5 for 6 months)

    Returns:
    - Synthetic futures price
    """
    # Put-Call Parity: C - P = F * e^(-rT) - K
    # Rearrange to solve for F (futures price): F = (C - P + K) * e^(rT)
    discount_factor = np.exp(risk_free_rate * time_to_expiry)
    synthetic_futures = (call_price - put_price + strike_price) * discount_factor
    return synthetic_futures

def create_arrow(symbol, tick):
    # load_db_init()
    base_price = BASE[symbol]
    print("=================================")
    print(symbol, tick)
    df = DFO[(DFO["ckpt"] == tick) & (DFO["symbol"] == symbol)]
    print(df)
    df['name'] = df['name'].str.replace('购', '', regex=False)
    df['name'] = df['name'].str.replace('沽', '', regex=False)
    call_df = df[df["type"] == "call"]
    put_df = df[df["type"] == "put"]
    print(call_df)
    merged_df = pd.merge(call_df, put_df, on="name", suffixes=("_call", "_put"))
    print(merged_df)
    cell_df = pd.DataFrame()
    cell_df["name"] = merged_df["name"]
    cell_df["price_call"] = merged_df["price_call"]
    cell_df["price_put"] = merged_df["price_put"]
    cell_df["price_exec"] = merged_df["price_exec_call"]
    cell_df["vol"] = merged_df["vol_call"] + merged_df["vol_put"]
    cell_df["expire"] = merged_df["expire_call"]
    futures_prices = []
    for _, row in cell_df.iterrows():
        price = synthesize_futures_price(
            call_price=row['price_call'],
            put_price=row['price_put'],
            strike_price=row['price_exec'],
            risk_free_rate=0.05,
            time_to_expiry=row["expire"] / 365
        )
        futures_prices.append(price)
    cell_df["futures_price"] = futures_prices
    # cell_df["price_vol"] = cell_df['futures_price'] * cell_df['vol'] 
    # weighted_avg_price = cell_df['price_vol'].sum() / cell_df['vol'].sum()
    cell_df["price_vol"] = cell_df['futures_price'] * cell_df['vol'] / (cell_df['expire']+1)
    weighted_avg_price = cell_df['price_vol'].sum() / (cell_df['vol'] / (cell_df['expire']+1)).sum()
    print(cell_df) 
    print(weighted_avg_price)
    chg = (weighted_avg_price - base_price) / base_price * 100
    return chg

def wait(now, hour, minute, second):
    wait_time = (now.at(hour, minute, second) - now).total_seconds()
    print(f"Waiting until {hour}:{minute}:{second} ... ({wait_time} seconds)")
    if wait_time >= 5:
        time.sleep(wait_time - 3)
        print(f"Waiting until {hour}:{minute}:{second} ... (3 seconds)")
        time.sleep(1)
        print(f"Waiting until {hour}:{minute}:{second} ... (2 seconds)")
        time.sleep(1)
        print(f"Waiting until {hour}:{minute}:{second} ... (1 second)")
        time.sleep(1)
        print(f"Waiting until {hour}:{minute}:{second} ... (Start)")
    else:
        time.sleep(wait_time)


def pilot():
    tick = fetch_tick_latest("stock")

    pilot_list = []
    pilot_list.append(create_pilot("510300", tick))
    pilot_list.append(create_pilot("510500", tick))

    save_to_db(pilot_list, "pilot", "db.sqlite3")
    save_to_df(pilot_list, "pilot")


def is_working_day():
    # should run on 9:30:00
    now = pendulum.now("Asia/Shanghai")
    market_open_time = pendulum.now("Asia/Shanghai").set(hour=9, minute=29, second=0)
    # 下载数据
    try:
        response_text = fetch_data(["sh000300"])
    except RuntimeError as e:
        print(f"Error fetching data: {e}")
    data_lines = response_text.strip().split("\n")
    try:
        parsed_dict = parse_response(data_lines[0], "stock")
    except ValueError as e:
        print(f"Error parsing line: market state, {e}")
        raise
    ckptol = parsed_dict["ckptol"]
    ckpt_dt = pendulum.parse(ckptol)
    if now < market_open_time:
        print("not ready time")
        return None
    if now.day == ckpt_dt.day:
        return True
    else:
        return False

def send_notify(tick):
    base_url = "http://127.0.0.1:5000"
    encoded_tick = quote(str(tick)[:19], safe="")
    url = f"{base_url}/notify?tick={encoded_tick}"
    try:
        response = requests.get(url, timeout=5)
    except:
        pass

def table_exists(cursor, table_name):
    cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone()[0] > 0

def remove_hist(tick):
    global DFO
    global DFS
    global DFF
    global DFP

    ts = tick.add(minutes=-30).replace(tzinfo=None)
    ts_pd = pd.to_datetime(ts)
    DFO = DFO[DFO["ckpt"] >= ts_pd]
    DFS = DFS[DFS["ckpt"] >= ts_pd]
    DFF = DFF[DFF["ckpt"] >= ts_pd]
    DFP = DFP[DFP["ckpt"] >= ts_pd]


if __name__ == "__main__":
    # re run @ opening , load data
    now = pendulum.now("Asia/Shanghai")
    if now > now.at(9, 30, 0):
        create_all_table()
        load_db_init()
    # blender.reset_table()
    # blender.category_etf()
    # load_db_init()
    # fetch_online()
    # pilot()
    # print(DFP)
    launch()
