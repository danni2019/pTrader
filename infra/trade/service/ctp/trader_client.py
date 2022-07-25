import json
import pandas as pd
import requests
from typing import Any
from utils.tool.logger import log

logger = log(__name__, "infra")


class Trader:
    def __init__(self, host: str, port: str):
        self.url = f"{host}:{port}" if 'http' in host else f"http://{host}:{port}"

    def query_position(self, contract: str = None):
        resp_ = requests.post(
            f"{self.url}/query_position",
            data={'contract': contract}
        )
        if resp_.status_code == 200:
            return json.loads(resp_.content)
        else:
            logger.error(f"query position err: {resp_.status_code}")

    def query_account(self):
        resp_ = requests.get(
            f"{self.url}/query_account",
        )
        if resp_.status_code == 200:
            return json.loads(resp_.content)
        else:
            logger.error(f"query account err: {resp_.status_code}")

    def trade(
            self,
            trade_action: str,
            instrument_contract: str,
            exchange: str,
            limit_price: Any,
            volume: Any,
            ioc: int,
            mv_required: Any = 1
    ):
        """
        ioc: 0 / 1  stands for false / true
        """
        resp_ = requests.post(
            f"{self.url}/{trade_action}",
            data={
                'instrument_contract': instrument_contract,
                'exchange': exchange,
                'limit_price': limit_price,
                'volume': volume,
                'ioc': ioc,
                'mv_required': mv_required
            }
        )
        if resp_.status_code == 200:
            return json.loads(resp_.content)
        else:
            logger.error(f"trade action err: {resp_.status_code}")

    def revoke_order(
            self,
            instrument_contract: str,
            exchange: str,
            order_ref: str
    ):
        resp_ = requests.post(
            f"{self.url}/revoke_order",
            data={
                'instrument_contract': instrument_contract,
                'exchange': exchange,
                'order_ref': order_ref
            }
        )
        if resp_.status_code == 200:
            return json.loads(resp_.content)
        else:
            logger.error(f"order revoke err: {resp_.status_code}")
