import requests
import json
import pandas as pd
from datetime import datetime
import time
from supabase import create_client, Client
import numpy as np
import os

def main_task():
    def get_bfx_funding_rate(market_id):
        url = "https://api.bfx.trade/markets/fundingrate"
        params = {"market_id": market_id}

        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error BFX: {response.status_code}")
            return None

    def get_all_funding_rates():
        # BFX
        market_pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]
        bfx_data = []

        for pair in market_pairs:
            funding_rate_data = get_bfx_funding_rate(pair)
            if funding_rate_data and funding_rate_data['result']:
                first_result = funding_rate_data['result'][0]
                market_id = first_result['market_id'].replace('-USD', '')
                funding_rate = round(float(first_result['funding_rate']), 8)
                bfx_data.append({'name': market_id, 'funding_bfx': funding_rate})
            time.sleep(1)

        bfx = pd.DataFrame(bfx_data)

        # Bluefin
        url = "https://dapi.api.sui-prod.bluefin.io/fundingRate?symbol="
        headers = {"accept": "application/json"}
        symbols = ['SOL-PERP', 'ETH-PERP', 'BTC-PERP']
        bluefin_data = []

        for symbol in symbols:
            try:
                response = requests.get(url + symbol, headers=headers)
                if response.status_code == 200:
                    json_data = response.json()
                    name = json_data['symbol'].split('-')[0]
                    funding_rate = float(json_data['fundingRate']) / 1000000000000000000
                    bluefin_data.append({'name': name, 'funding_bluefin': funding_rate})
            except requests.exceptions.RequestException as e:
                print(f"Error Bluefin {symbol}: {e}")

        bluefin = pd.DataFrame(bluefin_data)

        # Orderly
        url = "https://api-evm.orderly.network/v1/public/funding_rates"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            rows = data['data']['rows']

            orderly_data = []
            for row in rows:
                symbol = row['symbol']
                start_index = symbol.find("PERP_") + len("PERP_")
                end_index = symbol.find("_USDC")
                clean_symbol = symbol[start_index:end_index]

                if clean_symbol in ['BTC', 'ETH', 'SOL']:
                    orderly_data.append({
                        'name': clean_symbol,
                        'funding_orderly': float(row['last_funding_rate'])/8
                    })

            orderly = pd.DataFrame(orderly_data)
        else:
            orderly = pd.DataFrame()

        # Hyperliquid
        url = "https://api.hyperliquid.xyz/info"
        headers = {"Content-Type": "application/json"}
        data = {"type": "metaAndAssetCtxs"}

        response = requests.post(url, headers=headers, json=json.dumps(data))

        if response.status_code == 200:
            response_data = response.json()
            universe = response_data[0]["universe"]
            asset_data = response_data[1]

            hyperliquid_data = []
            for i, asset in enumerate(universe):
                if asset["name"] in ["BTC", "ETH", "SOL"]:
                    hyperliquid_data.append({
                        'name': asset["name"],
                        'funding_hyperliquid': asset_data[i]["funding"]
                    })

            hyperliquid = pd.DataFrame(hyperliquid_data)
        else:
            hyperliquid = pd.DataFrame()

        # Merge all dataframes
        dfs = {
            'bfx': bfx,
            'bluefin': bluefin,
            'orderly': orderly,
            'hyperliquid': hyperliquid
        }

        merged = list(dfs.values())[0]
        for name, df in list(dfs.items())[1:]:
            merged = pd.merge(merged, df, on='name', how='outer')

        timestamp = datetime.now().timestamp()
        merged['id'] = merged.apply(lambda row: f"{row['name']}_{timestamp}", axis=1)
        merged['timestamp'] = timestamp

        return merged

    try:
        # Get funding rates
        merged_data = get_all_funding_rates()

        # Supabase connection
        SUPABASE_URL = os.environ.get('SUPABASE_URL')
        SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
        TABLE_NAME = 'Perpetuos'

        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

        # Convert DataFrame to records
        records = merged_data.to_dict(orient='records')

        # Insert into Supabase
        result = supabase.table(TABLE_NAME).upsert(records, on_conflict=['id']).execute()
        print(f"Task executed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"Error in main task: {e}")
        # Aquí puedes añadir tu lógica de alerta si lo deseas
        alert_data = {
            "message": "🚨 ALERTA 🚨 - Script caído"
        }
        requests.post("https://hook.eu2.make.com/yiddb9e1cm82pm756kd7tsbsfc12pe4t", json=alert_data)

if __name__ == "__main__":
    main_task()
