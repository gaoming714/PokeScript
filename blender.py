from itertools import product
import requests
import sqlite3
import re
import json
import time

from bs4 import BeautifulSoup

# 数据库初始化（删除旧表并重新创建）
def reset_table(db_name="db.sqlite3"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    print("func")
    # 删除旧表
    cursor.execute("DROP TABLE IF EXISTS category")

    # 创建新表
    cursor.execute('''
        CREATE TABLE category (
            op_symbol TEXT NOT NULL,
            op_asset TEXT NOT NULL,
            op_type TEXT NOT NULL,
            op_expire TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()
    print(f"Table 'options' in database '{db_name}' has been reset.")

# 数据插入函数
def save_to_db(op_data, db_name="db.sqlite3"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO category (op_symbol, op_asset, op_type, op_expire)
        VALUES (?, ?, ?, ?)
    ''', op_data)
    conn.commit()
    conn.close()
    print(f"Added {len(op_data)} records to database '{db_name}'.")

# 数据解析函数
def parse_response(response_text):
    # 匹配双引号中的内容
    match = re.search(r'"(.*?)"', response_text)
    if not match:
        raise ValueError("Response does not contain double-quoted content")
    
    # 以逗号分割内容，形成数组
    content = match.group(1)
    return content.split(',')


def fetch_con(asset, expire):
    op_data = []
    for op_type, op_str in [("call", "UP"), ("put", "DOWN")]:
        url = f"http://hq.sinajs.cn/list=OP_{op_str}_{asset}{expire}"
        headers = {"Referer": "http://vip.stock.finance.sina.com.cn/"}
        
        # 发起 HTTP 请求
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch data, HTTP status code: {response.status_code}")
            return

        # 解析内容
        try:
            op_symbols = parse_response(response.text)
        except ValueError as e:
            print(f"Error parsing response: {e}")
            return

        # 组装数据
        op_data.extend([(symbol, asset, op_type, expire) for symbol in op_symbols if symbol.strip()])

    return op_data

def option_expire() -> list:
    base_name = "300ETF"
    url = (
        "http://stock.finance.sina.com.cn/futures/api/openapi.php/StockOptionService.getStockName?exchange=null&cate="
        + base_name
    )
    headers = {"Referer": "http://vip.stock.finance.sina.com.cn/"}
    # 发起 HTTP 请求
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch data, HTTP status code: {response.status_code}")
        return

    res_dict = json.loads(response.text)
    month_list = list(set(res_dict["result"]["data"]["contractMonth"]))
    expire_list = []
    for month in month_list:
        pretty_month = month[2:4] + month[5:7]
        expire_list.append(pretty_month)
    expire_list.sort()
    return expire_list


def option_expire_index() -> list:
    url = "https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php/ho/cffex"
    headers = {"Referer": "http://vip.stock.finance.sina.com.cn/"}

    # 发送GET请求
    response = requests.get(url, headers=headers)

    # 检查请求是否成功
    if response.status_code == 200:
        # 使用BeautifulSoup解析网页内容
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 查找id为option_suffix的元素
        option_suffix_element = soup.find(id="option_suffix")
        
        # 如果找到该元素，获取所有li标签中的内容
        if option_suffix_element:
            # 获取所有li标签
            li_elements = option_suffix_element.find_all('li')
            
            # 提取每个li标签中的文本，并转换为列表
            li_values = [li.get_text()[-4:] for li in li_elements]
            
            sorted_values = sorted(li_values, key=int)

            # 输出结果
            print(sorted_values)
        else:
            print(f"请求失败，状态码: {response.status_code}")
    return sorted_values


def fetch_p(asset, expire):
    """
    output : symbol, asset, type, expire_month
    """
    if asset == "000016":
        url = "https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php/ho/cffex"
        shot = "ho"
    elif asset == "000300":
        url = "https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php/io/cffex"
        shot = "io"
    elif asset == "000852":
        url = "https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php/mo/cffex"
        shot = "mo"
    else:
        raise


    url = "https://stock.finance.sina.com.cn/futures/api/openapi.php/OptionService.getOptionData"
    params = {
        "type": "futures",
        "product": f"{shot}",
        "exchange": "cffex",
        "pinzhong": f"{shot}{expire}",
    }
    headers = {"Referer": "http://vip.stock.finance.sina.com.cn/"}

    # 发送GET请求
    response = requests.get(url, params=params, headers=headers)

    # 检查请求是否成功
    if response.status_code == 200:
         data_text = response.text
         data_json = json.loads(data_text[data_text.find("{") : data_text.rfind("}") + 1])

    else:
        print(f"请求失败，状态码: {response.status_code}")
    up_data = data_json["result"]["data"]["up"]
    down_data = data_json["result"]["data"]["down"]
    result_up_list = [item[8] for item in up_data]
    result_down_list = [item[7] for item in down_data]
    op_data = []
    op_data.extend([("P_OP_" + symbol, asset, "call", expire ) for symbol in result_up_list])
    op_data.extend([("P_OP_" + symbol, asset, "put", expire ) for symbol in result_down_list])

    return op_data

# 主逻辑
def category_etf():
    all_op_category = []
    expire_month = option_expire()

    nested_list = [["510050", "510300", "510500", "512100"], expire_month]
    for op_asset, op_expire in product(*nested_list):
        all_op_category.extend(fetch_con(op_asset, op_expire))
    
    # 保存到 SQLite
    save_to_db(all_op_category)

def category_index():
    all_op_category = []
    expire_month = option_expire_index()

    nested_list = [["000016", "000300", "000852"], expire_month]
    for op_asset, op_expire in product(*nested_list):
        all_op_category.extend(fetch_p(op_asset, op_expire))
    
    # 保存到 SQLite
    save_to_db(all_op_category)


if __name__ == "__main__":
    reset_table()
    category_etf()
    time.sleep(2)
    # category_index()
