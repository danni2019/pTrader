"""
Naming conventions and rules for each exchange.
"""
from time import sleep
from datetime import datetime


class TradeRules:

    @classmethod
    def standard_contract_to_trade_code(cls, contract: str, exchange: str):
        if len(contract) <= 6:
            if exchange.lower() in ['shfe', 'ine', 'dce']:
                return contract.lower()
            elif exchange.lower() == 'cffex':
                return contract.upper()
            elif exchange.lower() == 'czce':
                return contract[:-4].upper() + contract[-3:]
            else:
                raise ValueError(f"Please check your exchange input, got {exchange}. ")
        else:
            if exchange.lower() in ['shfe', 'ine']:
                c_ = contract.split('-')
                return f"{c_[0].lower()}{c_[1].upper()}{c_[2]}"
            elif exchange.lower() == 'dce':
                c_ = contract.split('-')
                return f"{c_[0].lower()}-{c_[1].upper()}-{c_[2]}"
            elif exchange.lower() == 'cffex':
                c_ = contract.split('-')
                return f"{c_[0].upper()}-{c_[1].upper()}-{c_[2]}"
            elif exchange.lower() == 'czce':
                c_ = contract.split('-')
                return f"{c_[0][:-4].upper()}{c_[0][-3:]}{c_[1].upper()}{c_[2]}"
            else:
                raise ValueError(f"Please check your exchange input, got {exchange}. ")

    @classmethod
    def request_id_generator(cls):
        sleep(0.2)
        return int(datetime.now().timestamp())

    def is_trading(self, symbol: str, exchange: str):
        """
        check if given instrument is still trading at this time.
        """
        # Todo! user can define customized method here.
        return True

    def exchange_open(self):
        """
        Check if currently the exchange is open.
        """
        # Todo! user can define customized method here.
        return True

