import datetime
from celery import shared_task, Task
import redis
import asyncio
import aiohttp
import json

redisClient = redis.StrictRedis(host='localhost',port=6379,db=0)

class RetryTask(Task):
    autoretry_for = (Exception,)
    retry_kwargs = {'max_retries': 5}
    retry_backoff = 2
    retry_jitter = False
    task_acks_late = True
    worker_concurrency = 4
    prefetch_multiplier = 1


@shared_task(bind=True, base=RetryTask)
def send_candle_time(update_time):
    asyncio.run(get_candle_data(update_time))

def get_candle_time(minute=1):
    now = datetime.datetime.now()
    start = now - datetime.timedelta(minutes=minute)
    return start.timestamp(), now.timestamp()

start, end = get_candle_time()


async def get_candle_data(update):
    symbol_list ='BTC-BTC, ETH-BTC, LTC-BTC, EOS-BTC, XRP-BTC, KCS-BTC, DIA-BTC, VET-BTC, DASH-BTC, DOT-BTC, XTZ-BTC, ZEC-BTC, BSV-BTC, ADA-BTC, ATOM-BTC, LINK-BTC, LUNA-BTC, NEO-BTC, UNI-BTC, ETC-BTC, BNB-BTC, TRX-BTC, XLM-BTC, BCH-BTC, USDC-BTC, GRT-BTC, 1INCH-BTC, AAVE-BTC, SNX-BTC, API3-BTC, CRV-BTC, MIR-BTC, SUSHI-BTC, COMP-BTC, ZIL-BTC, YFI-BTC, OMG-BTC, XMR-BTC, WAVES-BTC, MKR-BTC, COTI-BTC, SXP-BTC, THETA-BTC, ZRX-BTC, DOGE-BTC, LRC-BTC, FIL-BTC, DAO-BTC, BTT-BTC, KSM-BTC, BAT-BTC, ROSE-BTC, CAKE-BTC, CRO-BTC, XEM-BTC, MASK-BTC, FTM-BTC, IOST-BTC, ALGO-BTC, DEGO-BTC, CHR-BTC, CHZ-BTC, MANA-BTC, ENJ-BTC, IOST-BTC, ANKR-BTC, ORN-BTC, SAND-BTC, VELO-BTC, AVAX-BTC, DODO-BTC, WIN-BTC, ONE-BTC, SHIB-BTC, ICP-BTC, MATIC-BTC, CKB-BTC, SOL-BTC, VRA-BTC, DYDX-BTC, ENS-BTC, NEAR-BTC, SLP-BTC, AXS-BTC, TLM-BTC, ALICE-BTC, IOTX-BTC, QNT-BTC, SUPER-BTC, HABR-BTC, RUNE-BTC, EGLD-BTC, AR-BTC, RNDR-BTC, LTO-BTC, YGG-BTC'.replace('-BTC','-USDT').split(', ')
    async with aiohttp.ClientSession() as session:
        for symbol in symbol_list:
            print(symbol)
            async with session.get(f'https://api.kucoin.com/api/v1/market/candles?type={update}min&symbol={symbol}&startAt={int(start)}&endAt={int(end)}') as response:

                response = await response.json()
                # print(response)
                # with open("j_file.json", "a+") as jf:
                #     json.dump(response, jf)

                time_delta = int(response['data'][0][0]) if response.get('data') else 0
                data = str(response['data'][0]) if response.get('data') else ''
                redisClient.zadd(f"{symbol}_{update}min", {data:time_delta})


asyncio.run(get_candle_data(1))
