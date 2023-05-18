#!/usr/bin/env python

from collections import defaultdict
from typing import DefaultDict, Dict, Tuple
from utc_bot import UTCBot, start_bot
import math
import proto.utc_bot as pb
import betterproto
import asyncio
import re

import pandas as pd 
import numpy as np
from scipy.stats import linregress

futures_2020 = pd.read_csv("futures_2020_clean.csv")
futures_2021 = pd.read_csv("futures_2021_clean.csv")
weather_2020 = pd.read_csv("weather_2020.csv")
weather_2021 = pd.read_csv("weather_2021.csv")


DAYS_IN_MONTH = 21
DAYS_IN_YEAR = 252
INTEREST_RATE = 0.02
NUM_FUTURES = 14
TICK_SIZE = 0.00001
FUTURE_CODES = [chr(ord('A') + i) for i in range(NUM_FUTURES)] # Suffix of monthly future code
CONTRACTS = ['SBL'] +  ['LBS' + c for c in FUTURE_CODES] + ['LLL']

#Load Prices
df_prices = pd.concat([futures_2020, futures_2021], axis=0)
df_prices = df_prices.drop(columns=['Unnamed: 0'])
df_prices = df_prices.reset_index(drop=True)

#Load Weather
weather_prices = pd.concat([weather_2020, weather_2021], axis=0)
weather_prices = weather_prices.drop(columns=['Unnamed: 0'])
weather_prices = weather_prices.reset_index(drop=True)
df = df_prices.join(weather_prices)


class Case1Bot(UTCBot):
    """
    An example bot
    """
    etf_suffix = ''
    async def create_etf(self, qty: int):
        '''
        Creates qty amount the ETF basket
        DO NOT CHANGE
        '''
        if len(self.etf_suffix) == 0:
            return pb.SwapResponse(False, "Unsure of swap")
        return await self.swap("create_etf_" + self.etf_suffix, qty)

    async def redeem_etf(self, qty: int):
        '''
        Redeems qty amount the ETF basket
        DO NOT CHANGE
        '''
        if len(self.etf_suffix) == 0:
            return pb.SwapResponse(False, "Unsure of swap")
        return await self.swap("redeem_etf_" + self.etf_suffix, qty) 
    
    async def days_to_expiry(self, asset):
        '''
        Calculates days to expiry for the future
        '''
        future = ord(asset[-1]) - ord('A')
        expiry = 21 * (future + 1)
        return self._day - expiry

    async def handle_exchange_update(self, update: pb.FeedMessage):
        '''
        Handles exchange updates
        '''
        kind, _ = betterproto.which_one_of(update, "msg")
        #Competition event messages
        if kind == "generic_msg":
            msg = update.generic_msg.message
            
            # Used for API DO NOT TOUCH
            if 'trade_etf' in msg:
                self.etf_suffix = msg.split(' ')[1]
                
            # Updates current weather
            if "Weather" in update.generic_msg.message:
                msg = update.generic_msg.message
                weather = float(re.findall("\d+\.\d+", msg)[0])
                self._weather_log.append(weather)
                
            # Updates date
            if "Day" in update.generic_msg.message:
                self._day = int(re.findall("\d+", msg)[0])
                            
            # Updates positions if unknown message (probably etf swap)
            else:
                resp = await self.get_positions()
                if resp.ok:
                    self.positions = resp.positions
                    
        elif kind == "market_snapshot_msg":
            for asset in CONTRACTS:
                book = update.market_snapshot_msg.books[asset]
                while (True):
                    await self.place_order(asset, pb.OrderSpecType.MARKET, pb.OrderSpecSide.ASK, qty = 2)
                    await self.place_order(asset, pb.OrderSpecType.MARKET, pb.OrderSpecSide.ASK, qty = 2)
                    await self.place_order(asset, pb.OrderSpecType.MARKET, pb.OrderSpecSide.ASK, qty = 2)
                    
                #self._best_bid[asset] = float(book.bids[0].px)
                #self._best_ask[asset] = float(book.bids[0].px)
                #self.calculate_fair_price(self, asset, book)
                #self.make_market_asset(self, asset="SBL")
            

    


if __name__ == "__main__":
    start_bot(Case1Bot)
