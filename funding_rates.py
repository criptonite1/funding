import requests
import json
import pandas as pd
from datetime import datetime
import time
from supabase import create_client, Client
import numpy as np
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_bfx_funding_rate(market_id):
    try:
        url = "https://api.bfx.trade/markets/fundingrate"
        params = {"market_id": market_id}

        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except Exception as e:
        logger.error(f"Error in BFX API call: {e}")
        return None

def get_all_funding_rates():
    try:
        # BFX
        market_pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]
        bfx_data = []

        for pair in market_pairs:
            funding_rate_data = get_bfx_funding_rate(pair)
            if funding_rate_data and funding_rate_data.get('result'):
                first_result = funding_rate_data['result'][0]
                market_id = first_result['market_id'].replace('-USD', '')
                funding_rate = round(float(first_result['funding_rate']), 8)
                bfx_data.append({'name': market_id, 'funding_bfx': funding_rate})
            time.sleep(1)

        logger.info(f"BFX data collected: {len(bfx_data)} entries")
        bfx = pd.DataFrame(bfx_data)

        # Bluefin
        url = "https://dapi.api.sui-prod.bluefin.io/fundingRate?symbol="
        headers = {"accept": "application/json"}
        symbols = ['SOL-PERP', 'ETH-PERP', 'BTC-PERP']
        bluefin_data = []

        for symbol in symbols:
            try:
                response = requests.get(url + symbol, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                name = json_data['symbol'].split('-')[0]
                funding_rate = float(json_data['fundingRate']) / 1000000000000000000
                bluefin_data.append({'name': name, 'funding_bluefin': funding_rate})
            except Exception as e:
                logger.error(f"Error Bluefin {symbol}: {e}")

        logger.info(f"Bluefin data collected: {len(bluefin_data)} entries")
        bluefin = pd.DataFrame(bluefin_data)

        # Orderly
        url = "https://api-evm.orderly.network/v1/public/funding_rates"
        try:
            response = requests.get(url)
            response.raise_for_status()
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
            logger.info(f"Orderly data collected: {len(orderly_data)} entries")
        except Exception as e:
            logger.error(f"Error in Orderly API: {e}")
            orderly = pd.DataFrame()

        # Hyperliquid
        url = "https://api.hyperliquid.xyz/info"
        headers = {"Content-Type": "application/json"}
        data = {"type": "metaAndAssetCtxs"}

        try:
            response = requests.post(url, headers=headers, json=json.dumps(data))
            response.raise_for_status()
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
            logger.info(f"Hyperliquid data collected: {len(hyperliquid_data)} entries")
        except Exception as e:
            logger.error(f"Error in Hyperliquid API: {e}")
            hyperliquid = pd.DataFrame()

        # Merge all dataframes
        dfs = {
            'bfx': bfx,
            'bluefin': bluefin,
            'orderly': orderly,
            'hyperliquid': hyperliquid
        }

        # Log DataFrame shapes before merge
        for name, df in dfs.items():
            logger.info(f"{name} DataFrame shape: {df.shape}")

        # Check if we have any data before merging
        if all(df.empty for df in dfs.values()):
            raise ValueError("No data available from any API")

        # Start with the first non-empty DataFrame
        merged = next(df for df in dfs.values() if not df.empty)

        # Merge with remaining DataFrames
        for name, df in dfs.items():
            if not df.empty and not df.equals(merged):
                merged = pd.merge(merged, df, on='name', how='outer')

        timestamp = datetime.now().timestamp()
        merged['id'] = merged.apply(lambda row: f"{row['name']}_{timestamp}", axis=1)
        merged['timestamp'] = timestamp

        logger.info(f"Final merged DataFrame shape: {merged.shape}")
        return merged

    except Exception as e:
        logger.error(f"Error in get_all_funding_rates: {e}")
        raise

def main_task():
    try:
        # Get funding rates
        merged_data = get_all_funding_rates()

        if merged_data.empty:
            raise ValueError("No data collected from APIs")

        # Supabase connection
        SUPABASE_URL = os.environ.get('SUPABASE_URL')
        SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Supabase credentials not found in environment variables")

        TABLE_NAME = 'Perpetuos'
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

        # Convert DataFrame to records
        records = merged_data.to_dict(orient='records')

        # Insert into Supabase
        result = supabase.table(TABLE_NAME).upsert(records, on_conflict=['id']).execute()
        logger.info(f"Task executed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        logger.error(f"Error in main task: {e}")
        # Alert logic
        alert_data = {
            "message": f"🚨 ALERTA 🚨 - Script caído: {str(e)}"
        }
        try:
            requests.post("https://hook.eu2.make.com/yiddb9e1cm82pm756kd7tsbsfc12pe4t", json=alert_data)
        except Exception as alert_error:
            logger.error(f"Error sending alert: {alert_error}")
        raise

if __name__ == "__main__":
    main_task()
