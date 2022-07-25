import os
import sys
import json
from time import sleep
import infra.trade.api.resource._ctp.thostmduserapi as mapi
from infra.tool import rules
from utils.buffer.redis_handle import RedisMsg
from utils.tool.logger import log
from utils.tool.configer import Config


logger = log(__file__, "infra")


class _CTPMdSpi(mapi.CThostFtdcMdSpi):

    def __init__(
            self,
            md_api,
            broker_id: str,
            user_id: str,
            pwd: str,
            nRequestId: int,
            user_product_info: str,
    ):
        super().__init__()
        self.md_api = md_api
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = pwd
        self.user_product_info = user_product_info
        self.status = 0
        self.session_id = None
        self.front_id = None

        self.nReqId = nRequestId

        self.latest_md_buf = {}

    def OnFrontConnected(self) -> "void":
        logger.info("OnFrontConnected")
        authfield = mapi.CThostFtdcReqUserLoginField()
        authfield.BrokerID = self.broker_id
        authfield.UserID = self.user_id
        authfield.Password = self.password
        authfield.UserProductInfo = self.user_product_info
        self.md_api.ReqUserLogin(authfield, self.nReqId)
        logger.info(f"Send ReqUserLogin: {self.nReqId} - ok")

    def OnRspUserLogin(
            self,
            pRspUserLogin: 'CThostFtdcRspUserLoginField',
            pRspInfo: 'CThostFtdcRspInfoField',
            nRequestID: 'int',
            bIsLast: 'bool'
    ) -> "void":
        if not pRspInfo.ErrorID:
            logger.info(
                f"OnRspUserLogin - {nRequestID} Succeeded: {pRspInfo.ErrorID} - {pRspInfo.ErrorMsg} - "
                f"LoginTime: {pRspUserLogin.LoginTime} - UserID: {pRspUserLogin.UserID} - "
                f"BrokerID: {pRspUserLogin.BrokerID} - FrontID: {pRspUserLogin.FrontID}"
            )
            self.status = 1
        else:
            logger.error(
                f"OnRspUserLogin - {nRequestID} Failed: {pRspInfo.ErrorID} - {pRspInfo.ErrorMsg} - "
                f"LoginTime: {pRspUserLogin.LoginTime} - UserID: {pRspUserLogin.UserID} - "
                f"BrokerID: {pRspUserLogin.BrokerID} - FrontID: {pRspUserLogin.FrontID}"
            )
            sys.exit(0)

    def OnRtnDepthMarketData(self, pDepthMarketData: 'CThostFtdcDepthMarketDataField') -> "void":
        md_data = {
            "TradingDay": pDepthMarketData.TradingDay,
            "InstrumentID": pDepthMarketData.InstrumentID,
            "ExchangeID": pDepthMarketData.ExchangeID,
            "ExchangeInstID": pDepthMarketData.ExchangeInstID,
            "LastPrice": pDepthMarketData.LastPrice,
            "PreSettlementPrice": pDepthMarketData.PreSettlementPrice,
            "PreClosePrice": pDepthMarketData.PreClosePrice,
            "PreOpenInterest": pDepthMarketData.PreOpenInterest,
            "OpenPrice": pDepthMarketData.OpenPrice,
            "HighestPrice": pDepthMarketData.HighestPrice,
            "LowestPrice": pDepthMarketData.LowestPrice,
            "Volume": pDepthMarketData.Volume,
            "Turnover": pDepthMarketData.Turnover,
            "OpenInterest": pDepthMarketData.OpenInterest,
            "ClosePrice": pDepthMarketData.ClosePrice,
            "SettlementPrice": pDepthMarketData.SettlementPrice,
            "UpperLimitPrice": pDepthMarketData.UpperLimitPrice,
            "LowerLimitPrice": pDepthMarketData.LowerLimitPrice,
            "PreDelta": pDepthMarketData.PreDelta,
            "CurrDelta": pDepthMarketData.CurrDelta,
            "UpdateTime": pDepthMarketData.UpdateTime,
            "UpdateMillisec": pDepthMarketData.UpdateMillisec,
            "BidPrice1": pDepthMarketData.BidPrice1,
            "BidVolume1": pDepthMarketData.BidVolume1,
            "AskPrice1": pDepthMarketData.AskPrice1,
            "AskVolume1": pDepthMarketData.AskVolume1,
            "AveragePrice": pDepthMarketData.AveragePrice,
            "ActionDay": pDepthMarketData.ActionDay
        }
        self.latest_md_buf[pDepthMarketData.InstrumentID] = md_data

    def OnRspSubMarketData(
            self,
            pSpecificInstrument: 'CThostFtdcSpecificInstrumentField',
            pRspInfo: 'CThostFtdcRspInfoField',
            nRequestID: 'int',
            bIsLast: 'bool'
    ) -> "void":
        logger.info(
            f"OnRspSubMarketData - {nRequestID}: Sub: {pSpecificInstrument.InstrumentID} - "
            f"{pRspInfo.ErrorID} - {pRspInfo.ErrorMsg}"
        )


class CtpMd:
    def __init__(
            self,
            front_addr: str,
            broker_id: str,
            investor_id: str,
            pwd: str,
            user_product_info: str,
            msg_db: int,
            channel_name: str,
    ):
        _conf = Config()
        self.pth = _conf.path

        self.front_addr = front_addr
        self.broker_id = broker_id
        self.user_id = self.investor_id = investor_id
        self.password = pwd
        self.trade_rule = rules.TradeRules()
        self.nReqId = rules.TradeRules.request_id_generator()
        self.user_product_info = user_product_info

        self.ctp_api = mapi

        self.md_api = mapi.CThostFtdcMdApi_CreateFtdcMdApi(
            os.path.join(
                self.pth,
                os.sep.join(["infra", "trade", "api", "temp", f"MD_{self.nReqId}"])
            )
        )

        self.md_spi = _CTPMdSpi(
            self.md_api,
            broker_id=self.broker_id,
            user_id=self.user_id,
            pwd=self.password,
            nRequestId=self.nReqId,
            user_product_info=self.user_product_info,
        )
        self.md_api.RegisterSpi(self.md_spi)
        self.md_api.RegisterFront(self.front_addr)

        self.msg = RedisMsg(db=msg_db, channel=channel_name)

    def __enter__(self):
        if not self.trade_rule.exchange_open():
            logger.warning("Market closed. Ctp exit.")
            sys.exit(0)
        self.md_api.Init()
        while not self.md_spi.status:
            sleep(0.01)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # self.md_api.Join()
        self.md_api.Release()

    def subscribe_md(self, contracts: list):
        self.md_api.SubscribeMarketData([i.encode('utf-8') for i in contracts], len(contracts))

