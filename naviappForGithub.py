import asyncio
import aiohttp
import pandas as pd
import os
import io
import json
from datetime import datetime, timedelta
import gspread
import time


# --- 設定項目 ---
SPREADSHEET_NAME = 'naviapp_sheet' 
TEI_MASTER_FILE = 'tei.20260326.csv'
BASE_URL = "https://jik.nishitetsu.jp/jikoku/naviapp/busnavi"
AWS_BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/nishitetsu-api/kaishibi4"
MAX_CONCURRENT_REQUESTS = 150  # 高速化のため150のままにします

BUS_RANGES = [
    (351, 352, 4), (371, 377, 3), (21, 26, 2), (27, 27, 3),
    (101, 107, 4), (201, 212, 4), (218, 221, 4), (701, 704, 4),
    (2002, 2005, 4), (2010, 2099, 4), (2101, 2120, 4), (2130, 2133, 4),
    (2261, 2279, 4), (2301, 2301, 4), (2350, 2358, 4), (2401, 2434, 4),
    (2501, 2536, 4), (2601, 2616, 4), (2660, 2694, 4), (2698, 2701, 4), (2710, 2814, 4),
    (2830, 2886, 4), (2902, 2944, 4), (2958, 2983, 4), 
    (1004, 1010, 4), (1020, 1031, 4), (1037, 1055, 4), (1071, 1113, 4),
    (1121, 1132, 4), (1138, 1143, 4), (1157, 1172, 4), (1185, 1196, 4), 
    (1237, 1281, 4), (1291, 1308, 4), (1330, 1356, 4), (1440, 1464, 4),
    (1801, 1826, 4), (1903, 1912, 4), (1914, 1925, 4), (2013, 2025, 4),
    (4403, 4403, 4), (4501, 4502, 4), (4601, 4616, 4), (3630, 3672, 4), 
    (4701, 4706, 4), (4720, 4726, 4), (4730, 4733, 4), 
    (4830, 4845, 4), (4850, 4879, 4), (4905, 4948, 4), (3016, 3025, 4), 
    (3033, 3037, 4), (3043, 3069, 4), (3126, 3140, 4), (3144, 3158, 4), 
    (3201, 3212, 4), (3217, 3240, 4), (3244, 3244, 4), (3250, 3253, 4), (3268, 3299, 4), 
    (3330, 3351, 4), (3420, 3443, 4), (3701, 3710, 4), 
    (5689, 5689, 4), (5766, 5771, 4), (5775, 5779, 4), (5822, 5898, 4),
    (9001, 9035, 4), (9039, 9041, 4), (9053, 9102, 4), (9118, 9127, 4), (9136, 9146, 4), 
    (9201, 9240, 4), (9260, 9353, 4), (9368, 9376, 4), 
    (9406, 9406, 4), (9413, 9417, 4), (9423, 9437, 4), (9441, 9441, 4), (9450, 9492, 4), (9496, 9525, 4),   
    (9601, 9606, 4), (9612, 9617, 4), (9624, 9628, 4), (9635, 9645, 4), (9650, 9692, 4), (9720, 9724, 4), 
    (9801, 9903, 4), (9913, 9914, 4), (9920, 9920, 4), (9925, 9928, 4), (9937, 9960, 4),
    (5900, 6013, 4), (6024, 6024, 4), (6050, 6050, 4), (6102, 6265, 4),
    (8000, 8005, 4), (8008, 8010, 4), (8050, 8050, 4),
    (8015, 8016, 4), (8402, 8410, 4), (8501, 8529, 4), (8534, 8546, 4),
    (8604, 8606, 4), (7610, 7625, 4), (7702, 7743, 4), (7777, 7777, 4), 
    (7801, 7844, 4), (7905, 7927, 4), (7937, 7946, 4), (8017, 8048, 4),
    (8103, 8135, 4), (8203, 8231, 4), (8301, 8317, 4), (8421, 8440, 4),
    (8553, 8559, 4), (8650, 8658, 4), (8750, 8754, 4), (8801, 8812, 4),
    (8903, 8904, 4), (8053, 8099, 4),
    ("K1141", "K1269", 4), ("K2141", "K2269", 4), ("K9450", "K9450", 4), ("F400", "F699", 3), 
    (400, 426, 3), (710, 770, 3), ("B550", "B999", 3), ("B1000", "B1200", 4)
]

def load_tei_master():
    if not os.path.exists(TEI_MASTER_FILE): return {}
    for enc in ['utf-8-sig', 'cp932', 'utf-8']:
        try:
            tei_df = pd.read_csv(TEI_MASTER_FILE, encoding=enc, header=None)
            return dict(zip(tei_df[1].astype(str), tei_df[2].astype(str)))
        except: continue
    return {}

TEI_MASTER = load_tei_master()

def format_time(t_str):
    if not t_str or pd.isna(t_str): return ""
    s = str(t_str).zfill(4)
    return f"{s[:2]}:{s[2:]}"

async def get_human_readable_info(session, raw_data):
    try:
        part1, part2 = raw_data.split('] ')
        des_label = part1 + ']'
        keito, bin_no, jigyosha, use_date = part2.split(',')
        route_code = keito[:3]
        run_id = f"{keito}-{bin_no}"
        clean_date = use_date.replace('-', '')
        
        #ここを変える！！！！
        csv_url = f"{AWS_BASE_URL}/{clean_date}/{jigyosha}/{keito}_2.csv"
        
        async with session.get(csv_url, timeout=5) as response:
            if response.status != 200: 
                return f"{raw_data} {{{run_id}}}", route_code
            content = await response.read()
            csv_df = pd.read_csv(io.BytesIO(content), dtype=str)
            if bin_no not in csv_df.columns: 
                return f"{raw_data} {{{run_id}}}", route_code
            
            start_stop_cd = csv_df.iloc[0]['busstop_cd']
            end_stop_cd = csv_df.iloc[-1]['busstop_cd']
            start_name = TEI_MASTER.get(start_stop_cd, start_stop_cd)
            end_name = TEI_MASTER.get(end_stop_cd, end_stop_cd)
            t_start = format_time(csv_df.iloc[0][bin_no])
            t_end = format_time(csv_df.iloc[-1][bin_no])
            
            return f"{des_label}{start_name}{t_start}→{end_name}{t_end} {{{run_id}}}", route_code
    except:
        return raw_data, "000"

async def fetch_bus_data(session, bus_no, semaphore):
    params = {'bus_no': bus_no, 'lang': 'ja', 'site_cd': '0006', 'ver': '3'}
    async with semaphore:
        try:
            async with session.get(BASE_URL, params=params, timeout=10) as response:
                if response.status != 200: return bus_no, None, None
                data = await response.json()
                if data.get("response") != "200": return bus_no, None, None
                
                des_val = data.get("des_no") or data.get("operate_name") or "無番"
                res_str = f"[{des_val}] {data.get('keito_cd','')},{data.get('bin_no','')},{data.get('jigyosha_cd','')},{data.get('use_st_date','')}"
                readable_info, route_code = await get_human_readable_info(session, res_str)
                return bus_no, readable_info, route_code
        except:
            return bus_no, None, None

async def run_scraping_job():
    # 💡【新機能】0:05 〜 4:25 の間はスクレイピングを実行せずに終了する
    now = datetime.now()
    current_time = now.time()
    
    # 判定用の時間オブジェクトを作成 (0時5分 と 4時55分)
    start_skip = datetime.strptime("00:05", "%H:%M").time()
    end_skip = datetime.strptime("04:25", "%H:%M").time()
    
    # 現在の時刻が 0:05 以降、かつ 4:55 以前であるか判定
    if start_skip <= current_time <= end_skip:
        print(f"Skipping job: Current time ({now.strftime('%H:%M')}) is within the maintenance window (00:05 - 04:25).")
        return "Success: Skipped within maintenance window"
    #ここからは既存処理
    raw_bus_list = []
    for s, e, d in BUS_RANGES:
        if isinstance(s, str):
            prefix = s[0]
            start_num = int(s[1:])
            end_num = int(e[1:])
        else:
            prefix = ""
            start_num = s
            end_num = e
        for n in range(start_num, end_num + 1):
            bus_no = f"{prefix}{str(n).zfill(d)}"
            raw_bus_list.append(bus_no)

    bus_list = list(dict.fromkeys(raw_bus_list))

    credentials_json = os.environ.get("GCP_CREDENTIALS")
    if not credentials_json:
        print("Error: GCP_CREDENTIALS not set")
        return
    
    creds_dict = json.loads(credentials_json)
    gc = gspread.service_account_from_dict(creds_dict)
    sh = gc.open(SPREADSHEET_NAME)
    
    # 朝3時日付変更線ロジック
    target_date = (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d')
    
    try:
        worksheet = sh.worksheet(target_date)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=target_date, rows="100", cols="2000")

    existing_data = worksheet.get_all_values()
    if existing_data:
        df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        df = df.replace('', None)
    else:
        df = pd.DataFrame()

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_bus_data(session, bus_no, semaphore) for bus_no in bus_list]
        results = await asyncio.gather(*tasks)

    results_dict = {r[0]: (r[1], r[2]) for r in results if r[1] is not None}
    has_update = False
    
    for bus_no, (current_val, route_code) in results_dict.items():
        last_val = None
        if bus_no in df.columns and len(df) > 0:
            actual_data = df[bus_no].iloc[1:].dropna()
            if not actual_data.empty:
                last_val = actual_data.iloc[-1]
            
        if current_val != last_val:
            has_update = True
            if bus_no not in df.columns:
                df[bus_no] = [None] * (len(df) if len(df) > 0 else 1)
            
            idx = df[bus_no].iloc[1:].count() + 1
            if idx >= len(df):
                new_empty_row = pd.Series([None] * len(df.columns), index=df.columns)
                df = pd.concat([df, new_empty_row.to_frame().T], ignore_index=True)
            
            df.at[idx, bus_no] = current_val

    if has_update or not df.empty:
        sorting_keys = []
        for bus_no in df.columns:
            if bus_no in results_dict:
                df.at[0, bus_no] = results_dict[bus_no][1]
            
            rep_route = str(df.at[0, bus_no]).zfill(3)
            sorting_keys.append((bus_no, rep_route))

        sorted_columns = [k[0] for k in sorted(sorting_keys, key=lambda x: (x[1], x[0]))]
        df = df.reindex(columns=sorted_columns)
        
        df_to_save = df.fillna('')
        data_to_write = [df_to_save.columns.values.tolist()] + df_to_save.values.tolist()
        
        # 💡【通信エラー対策】Google側が切断しても3回まで自動リトライする仕組み
        for attempt in range(3):
            try:
                worksheet.clear()
                worksheet.update(data_to_write)
                print(f"Success: Updated sheet [{target_date}]")
                break  # 正常に書き込めたら、リトライのループを抜けて終了します
            except Exception as e:
                if attempt < 2:
                    print(f"Google通信エラーのため、5秒後に再試行します... (リトライ {attempt + 1}/2): {str(e)}")
                    time.sleep(5)
                else:
                    print("3回リトライしましたが、Googleへの書き込みに失敗しました。")
                    raise e  # 3回とも失敗した場合は、諦めてエラーログを残します
    else:
        print("No update needed")

if __name__ == "__main__":
    # 直接非同期ジョブを実行
    asyncio.run(run_scraping_job())
