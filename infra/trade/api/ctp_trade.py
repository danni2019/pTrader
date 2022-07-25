import os
import sys
from time import sleep
from typing import Any
import infra.trade.api.resource._ctp.thosttraderapi as tapi
from infra.tool import rules
from utils.database.unified_db_control import UnifiedControl
from utils.tool.logger import log
from utils.tool.configer import Config


logger = log(__file__, "infra")


class _CtpTradeSpi(tapi.CThostFtdcTraderSpi):
    def __init__(
            self,
            trade_api,
            broker_id: str,
            user_id: str,
            pwd: str,
            app_id: str,
            auth_code: str,
            nRequestId: int,
            user_product_info: str,
    ):
        super().__init__()
        self.trade_api = trade_api
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = pwd
        self.app_id = app_id
        self.auth_code = auth_code
        self.user_product_info = user_product_info
        self.status = 0
        self.session_id = None
        self.front_id = None

        self.private_db = UnifiedControl(db_type='base')
        self.private_db._switch_client()

        self.nReqId = nRequestId

        self.order_rtn_detail = {}
        self.trade_rtn_detail = {}

        self.instrument_dict = {}

        self.account_detail = {}
        self.req_position_detail = []
        self.__req_pos_detail_intermediator = []
        self.req_position_record = {}

    def _init_containers(self):
        self.req_position_detail, self.__req_pos_detail_intermediator = [], []
        self.account_detail = {}

    def OnFrontConnected(self) -> "void":
        logger.info("OnFrontConnected")
        authfield = tapi.CThostFtdcReqAuthenticateField()
        authfield.BrokerID = self.broker_id
        authfield.UserID = self.user_id
        authfield.AppID = self.app_id
        authfield.AuthCode = self.auth_code
        r = self.trade_api.ReqAuthenticate(authfield, self.nReqId)
        logger.info(f"Send ReqAuthenticate: {self.nReqId} ret: {r} - ok")

    def OnRspAuthenticate(
            self,
            pRspAuthenticateField: 'CThostFtdcRspAuthenticateField',
            pRspInfo: 'CThostFtdcRspInfoField',
            nRequestID: 'int',
            bIsLast: 'bool'
    ) -> "void":
        logger.info(
            "Authenticate Details: "
            f"{pRspAuthenticateField.BrokerID} - "
            f"{pRspAuthenticateField.UserID} - "
            f"{pRspAuthenticateField.AppID} - "
            f"{pRspAuthenticateField.UserProductInfo}"
        )
        if not pRspInfo.ErrorID:
            loginfield = tapi.CThostFtdcReqUserLoginField()
            loginfield.BrokerID = self.broker_id
            loginfield.UserID = self.user_id
            loginfield.Password = self.password
            loginfield.UserProductInfo = self.user_product_info
            self.trade_api.ReqUserLogin(loginfield, 0)
            logger.info(f"Send ReqUserLogin: {self.nReqId} - ok")
        else:
            logger.error(f"Recv RspAuthenticate Err: {pRspInfo.ErrorID} - {pRspInfo.ErrorMsg}")
            sys.exit(0)

    def OnRspUserLogin(
            self,
            pRspUserLogin: 'CThostFtdcRspUserLoginField',
            pRspInfo: 'CThostFtdcRspInfoField',
            nRequestID: 'int',
            bIsLast: 'bool'
    ) -> "void":
        if not pRspInfo.ErrorID:
            logger.info(
                f"OnRspUserLogin - {self.nReqId} - "
                f"TradingDay: {pRspUserLogin.TradingDay} - "
                f"Session: {pRspUserLogin.SessionID} - LoginTime: {pRspUserLogin.LoginTime}"
            )
            qryinfofield = tapi.CThostFtdcQrySettlementInfoField()
            qryinfofield.BrokerID = self.broker_id
            qryinfofield.InvestorID = self.user_id
            qryinfofield.TradingDay = pRspUserLogin.TradingDay
            self.trade_api.ReqQrySettlementInfo(qryinfofield, self.nReqId)
            self.session_id = pRspUserLogin.SessionID
            self.front_id = pRspUserLogin.FrontID
            logger.info(f"Send ReqQrySettlementInfo: {self.nReqId} - ok")
        else:
            logger.error(f"Recv Login Err: {pRspInfo.ErrorID} - {pRspInfo.ErrorMsg}")
            sys.exit(0)

    def OnRspQrySettlementInfo(
            self,
            pSettlementInfo: 'CThostFtdcSettlementInfoField',
            pRspInfo: 'CThostFtdcRspInfoField',
            nRequestID: 'int',
            bIsLast: 'bool'
    ) -> "void":
        if pSettlementInfo is not None:
            logger.info(f"SettlementInfo: {pSettlementInfo.Content}")
        else:
            logger.info("SettlementInfo: Null")
        pSettlementInfoConfirm = tapi.CThostFtdcSettlementInfoConfirmField()
        pSettlementInfoConfirm.BrokerID = self.broker_id
        pSettlementInfoConfirm.InvestorID = self.user_id
        self.trade_api.ReqSettlementInfoConfirm(pSettlementInfoConfirm, self.nReqId)
        logger.info(f"Send ReqSettlementInfoConfirm: {self.nReqId} - ok")

    def OnRspSettlementInfoConfirm(
            self,
            pSettlementInfoConfirm: 'CThostFtdcSettlementInfoConfirmField',
            pRspInfo: 'CThostFtdcRspInfoField',
            nRequestID: 'int',
            bIsLast: 'bool'
    ) -> "void":
        if not pRspInfo.ErrorID:
            logger.info(f"Settlement Confirmed: {pRspInfo.ErrorID} {pRspInfo.ErrorMsg} Trader Good2GO.")
            self.status = 1
        else:
            pass

    def OnRspQryInstrument(self,
                           pInstrument: 'CThostFtdcInstrumentField',
                           pRspInfo: 'CThostFtdcRspInfoField',
                           nRequestID: int, bIsLast: bool
                           ) -> "Void":
        self.instrument_dict[pInstrument.ProductID] = {
            "ExchangeID": pInstrument.ExchangeID,
            "InstrumentID": pInstrument.InstrumentID,
            "OpenDate": pInstrument.OpenDate,
            "ExpDate": pInstrument.ExpireDate,
        }

    def OnRtnOrder(self, pOrder: 'CThostFtdcOrderField') -> "void":
        logger.info(
            f"OnRtnOrder OrderSysID: {pOrder.OrderSysID} - Ref: {pOrder.OrderRef} "
            f"Status: {pOrder.OrderStatus} - {pOrder.StatusMsg} "
            f"{pOrder.InstrumentID}@{pOrder.LimitPrice} Vol: {pOrder.VolumeTotal} Direction: {pOrder.Direction}"
        )
        self.order_rtn_detail[pOrder.OrderRef] = {
            "BrokerID": pOrder.BrokerID,
            "InvestorID": pOrder.InvestorID,
            "UserID": pOrder.UserID,
            "OrderRef": pOrder.OrderRef,
            "OrderSysID": pOrder.OrderSysID,
            "OrderLocalID": pOrder.OrderLocalID,
            "OrderSubmitStatus": pOrder.OrderSubmitStatus,
            "OrderStatus": pOrder.OrderStatus,
            "OrderType": pOrder.OrderType,
            "StatusMsg": pOrder.StatusMsg,
            "Direction": pOrder.Direction,
            "OffsetFlag": pOrder.CombOffsetFlag,
            "HedgeFlag": pOrder.CombHedgeFlag,
            "OrderPriceType": pOrder.OrderPriceType,
            "LimitPrice": pOrder.LimitPrice,
            "VolumeTotalOriginal": pOrder.VolumeTotalOriginal,           # 数量
            "TimeCondition": pOrder.TimeCondition,
            "GTDDate": pOrder.GTDDate,
            "VolumeCondition": pOrder.VolumeCondition,
            "MinVolume": pOrder.MinVolume,
            "ContingentCondition": pOrder.ContingentCondition,
            "StopPrice": pOrder.StopPrice,
            "IsAutoSuspend": pOrder.IsAutoSuspend,
            "RequestID": pOrder.RequestID,
            "VolumeTotal": pOrder.VolumeTotal,                           # 剩余数量
            "VolumeTraded": pOrder.VolumeTraded,                         # 今交易数量
            "InsertDate": pOrder.InsertDate,
            "InsertTime": pOrder.InsertTime,
            "ActiveTime": pOrder.ActiveTime,
            "SuspendTime": pOrder.SuspendTime,
            "Updatetime": pOrder.UpdateTime,
            "CancelTime": pOrder.CancelTime,
            "TradingDay": pOrder.TradingDay,
            "InstrumentID": pOrder.InstrumentID,
            "ExchangeID": pOrder.ExchangeID,
            "AccountID": pOrder.AccountID,
            "FrontID": pOrder.FrontID,
            "SessionID": pOrder.SessionID,
        }

    def OnRspOrderInsert(
            self,
            pInputOrder: 'CThostFtdcInputOrderField',
            pRspInfo: 'CThostFtdcRspInfoField',
            nRequestID: 'int',
            bIsLast: 'bool'
    ) -> "void":
        """报单填写字段错误从该spi返回"""
        logger.error(f"OnRspOrderInsert Err (Check Order Filing Inputs): {pRspInfo.ErrorID} - {pRspInfo.ErrorMsg}")

    def OnRtnTrade(self, pTrade: 'CThostFtdcTradeField') -> "void":
        logger.info(
            f"OnRtnTrade OrderSysID: {pTrade.OrderSysID} - Ref: {pTrade.OrderRef} "
            f"{pTrade.InstrumentID}@{pTrade.Price} Vol: {pTrade.Volume} Direction: {pTrade.Direction} | "
            f"Trade at: {pTrade.TradeDate} {pTrade.TradeTime}"
        )
        self.trade_rtn_detail[pTrade.OrderRef] = {
            "BrokerID": pTrade.BrokerID,
            "InvestorID": pTrade.InvestorID,
            "UserID": pTrade.UserID,
            "OrderRef": pTrade.OrderRef,
            "OrderSysID": pTrade.OrderSysID,
            "OrderLocalID": pTrade.OrderLocalID,
            "TradeType": pTrade.TradeType,
            "Direction": pTrade.Direction,
            "OffsetFlag": pTrade.OffsetFlag,
            "HedgeFlag": pTrade.HedgeFlag,
            "Price": pTrade.Price,
            "Volume": pTrade.Volume,
            "TradeDate": pTrade.TradeDate,
            "TradeTime": pTrade.TradeTime,
            "PriceSource": pTrade.PriceSource,
            "SequenceNo": pTrade.SequenceNo,
            "TradingDay": pTrade.TradingDay,
            "ExchangeID": pTrade.ExchangeID,
        }

    def OnRspQryTradingAccount(
            self,
            pTradingAccount: 'CThostFtdcTradingAccountField',
            pRspInfo: 'CThostFtdcRspInfoField',
            nRequestID: 'int',
            bIsLast: 'bool'
    ) -> "void":
        logger.info(
            f"OnRspQryTradingAccount({nRequestID}) "
            f"Broker: {pTradingAccount.BrokerID} - Account: {pTradingAccount.AccountID} "
            f"Cap. Avail.: {pTradingAccount.Available} Balance: {pTradingAccount.Balance} "
            f"CloseProfit: {pTradingAccount.CloseProfit} PositionProfit{pTradingAccount.PositionProfit} "
            f"Commission: {pTradingAccount.Commission} "
            f"SettlementID: {pTradingAccount.SettlementID} "
        )
        self.account_detail = {
            "nRequestID": nRequestID,
            "detail": {
                "BrokerID": pTradingAccount.BrokerID,
                "AccountID": pTradingAccount.AccountID,
                "PreDeposit": pTradingAccount.PreDeposit,
                "PreBalance": pTradingAccount.PreBalance,
                "PreMargin": pTradingAccount.PreMargin,
                "Deposit": pTradingAccount.Deposit,
                "Withdraw": pTradingAccount.Withdraw,
                "FrozenMargin": pTradingAccount.FrozenMargin,
                "FrozenCash": pTradingAccount.FrozenCash,
                "FrozenCommission": pTradingAccount.FrozenCommission,
                "CurrMargin": pTradingAccount.CurrMargin,
                "CashIn": pTradingAccount.CashIn,
                "Commission": pTradingAccount.Commission,
                "CloseProfit": pTradingAccount.CloseProfit,
                "PositionProfit": pTradingAccount.PositionProfit,
                "Balance": pTradingAccount.Balance,
                "Available": pTradingAccount.Available,
                "WithdrawQuota": pTradingAccount.WithdrawQuota,
                "Reserve": pTradingAccount.Reserve,
                "TradingDay": pTradingAccount.TradingDay,
                "SettlementID": pTradingAccount.SettlementID,
                "nRequestID": nRequestID,
            }
        }

    def OnRspQryInvestorPosition(
            self,
            pInvestorPosition: 'CThostFtdcInvestorPositionField',
            pRspInfo: 'CThostFtdcRspInfoField',
            nRequestID: 'int',
            bIsLast: 'bool'
    ) -> "void":
        try:
            logger.info(
                f"OnRspQryInvestorPosition({nRequestID}) "
                f"Broker: {pInvestorPosition.BrokerID} - Account: {pInvestorPosition.InvestorID} "
                f"YesterdayPosition: {pInvestorPosition.YdPosition} CurrentPosition: {pInvestorPosition.Position} "
                f"PositionDirection: {pInvestorPosition.PosiDirection} "
                f"PositionCost: {pInvestorPosition.PositionCost} MarginUsed: {pInvestorPosition.UseMargin} "
                f"CloseProfit: {pInvestorPosition.CloseProfit} PositionProfit: {pInvestorPosition.PositionProfit} "
                f"InstrumentID: {pInvestorPosition.InstrumentID}"
            )
        except Exception as err:
            logger.error(err.args[0])
            self.req_position_record[nRequestID] = True
        else:
            self.__req_pos_detail_intermediator.append(
                {
                    "BrokerID": pInvestorPosition.BrokerID,
                    "InvestorID": pInvestorPosition.InvestorID,
                    "PosiDirection": pInvestorPosition.PosiDirection,
                    "HedgeFlag": pInvestorPosition.HedgeFlag,
                    "PositionDate": pInvestorPosition.PositionDate,
                    "YdPosition": pInvestorPosition.YdPosition,
                    "Position": pInvestorPosition.Position,
                    "LongFrozen": pInvestorPosition.LongFrozen,
                    "ShortFrozen": pInvestorPosition.ShortFrozen,
                    "LongFrozenAmount": pInvestorPosition.LongFrozenAmount,
                    "ShortFrozenAmount": pInvestorPosition.ShortFrozenAmount,
                    "OpenVolume": pInvestorPosition.OpenVolume,
                    "CloseVolume": pInvestorPosition.CloseVolume,
                    "OpenAmount": pInvestorPosition.OpenAmount,
                    "CloseAmount": pInvestorPosition.CloseAmount,
                    "PositionCost": pInvestorPosition.PositionCost,
                    "PreMargin": pInvestorPosition.PreMargin,
                    "UseMargin": pInvestorPosition.UseMargin,
                    "FrozenMargin": pInvestorPosition.FrozenMargin,
                    "FrozenCash": pInvestorPosition.FrozenCash,
                    "FrozenCommission": pInvestorPosition.FrozenCommission,
                    "CashIn": pInvestorPosition.CashIn,
                    "Commission": pInvestorPosition.Commission,
                    "CloseProfit": pInvestorPosition.CloseProfit,
                    "PositionProfit": pInvestorPosition.PositionProfit,
                    "PreSettlementPrice": pInvestorPosition.PreSettlementPrice,
                    "SettlementPrice": pInvestorPosition.SettlementPrice,
                    "TradingDay": pInvestorPosition.TradingDay,
                    "SettlementID": pInvestorPosition.SettlementID,
                    "OpenCost": pInvestorPosition.OpenCost,
                    "ExchangeMargin": pInvestorPosition.ExchangeMargin,
                    "CombPosition": pInvestorPosition.CombPosition,
                    "CombLongFrozen": pInvestorPosition.CombLongFrozen,
                    "CombShortFrozen": pInvestorPosition.CombShortFrozen,
                    "CloseProfitByDate": pInvestorPosition.CloseProfitByDate,
                    "CloseProfitByTrade": pInvestorPosition.CloseProfitByTrade,
                    "TodayPosition": pInvestorPosition.TodayPosition,
                    "MarginRateByMoney": pInvestorPosition.MarginRateByMoney,
                    "MarginRateByVolume": pInvestorPosition.MarginRateByVolume,
                    "StrikeFrozen": pInvestorPosition.StrikeFrozen,
                    "StrikeFrozenAmount": pInvestorPosition.StrikeFrozenAmount,
                    "AbandonFrozen": pInvestorPosition.AbandonFrozen,
                    "ExchangeID": pInvestorPosition.ExchangeID,
                    "YdStrikeFrozen": pInvestorPosition.YdStrikeFrozen,
                    "TasPosition": pInvestorPosition.TasPosition,
                    "TasPositionCost": pInvestorPosition.TasPositionCost,
                    "InstrumentID": pInvestorPosition.InstrumentID,
                    "nRequestID": nRequestID,
                }
            )
            self.req_position_record[nRequestID] = bIsLast
            if bIsLast:
                self.req_position_detail = self.__req_pos_detail_intermediator
                self.__req_pos_detail_intermediator = []


class CtpTrade:
    def __init__(
            self,
            front_addr: str,
            broker_id: str,
            investor_id: str,
            pwd: str,
            app_id: str,
            auth_code: str,
            user_product_info: str,
            mode: int,
    ):
        """
        mode:
            1: tapi.THOST_TERT_QUICK,
            2: tapi.THOST_TERT_RESUME,
            3: tapi.THOST_TERT_RESTART,
            4: tapi.THOST_TERT_NONE,
        """
        _conf = Config()
        self.pth = _conf.path

        self.order_req_id = None
        self.trade_rule = rules.TradeRules()

        self.front_addr = front_addr
        self.broker_id = broker_id
        self.user_id = self.investor_id = investor_id
        self.password = pwd
        self.app_id = app_id
        self.auth_code = auth_code
        self.nReqId = self.req_id
        self.user_product_info = user_product_info

        self.ctp_api = tapi

        _mode_dict = {
            1: tapi.THOST_TERT_QUICK,
            2: tapi.THOST_TERT_RESUME,
            3: tapi.THOST_TERT_RESTART,
            4: tapi.THOST_TERT_NONE,
        }

        self.mode = _mode_dict[mode]

        self.trade_api = tapi.CThostFtdcTraderApi_CreateFtdcTraderApi(
            os.path.join(
                self.pth,
                os.sep.join(["infra", "trade", "api", "temp", f"TRADE_{self.nReqId}"])
            )
        )

        self.trade_spi = _CtpTradeSpi(
            self.trade_api,
            broker_id=self.broker_id,
            user_id=self.user_id,
            pwd=self.password,
            app_id=self.app_id,
            auth_code=self.auth_code,
            nRequestId=self.nReqId,
            user_product_info=self.user_product_info,
        )
        self.trade_api.RegisterSpi(self.trade_spi)
        self.trade_api.SubscribePrivateTopic(self.mode)
        self.trade_api.SubscribePublicTopic(self.mode)
        self.trade_api.RegisterFront(self.front_addr)

    @property
    def req_id(self):
        # request_id generate with no duplicates
        order_id = rules.TradeRules.request_id_generator()
        while self.order_req_id is not None and order_id <= self.order_req_id:
            order_id = rules.TradeRules.request_id_generator()
        self.order_req_id = order_id
        return order_id

    def __enter__(self):
        if not self.trade_rule.exchange_open():
            logger.warning("Market closed. Ctp exit.")
            sys.exit(0)
        self.trade_api.Init()
        while not self.trade_spi.status:
            sleep(0.1)
        else:
            # 规避查询到空值的情况，也是初始化的一部分
            # self.query_position()         # Todo 为什么实盘查询所有持仓会报错？
            self.query_account()
            self.trade_spi._init_containers()
            logger.info("Let's rock!")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        显式调用关闭api
        """
        # 实际业务逻辑用不到Join
        self.trade_api.Release()

    """
    Trade api：
            交易(trade) - 报撤单
    """

    def _trade_order_submit(self, **kwargs):
        """
        fill out and submit order.
        :param kwargs:
                BrokerID=BROKERID
                ExchangeID=EXCHANGEID
                InstrumentID=INSTRUMENTID
                UserID=USERID
                InvestorID=USERID
                Direction=DIRECTION
                LimitPrice=PRICE
                VolumeTotalOriginal=VOLUME
                OrderPriceType=api.THOST_FTDC_OPT_LimitPrice
                ContingentCondition = api.THOST_FTDC_CC_Immediately
                TimeCondition = api.THOST_FTDC_TC_GFD
                VolumeCondition = api.THOST_FTDC_VC_AV
                CombHedgeFlag="1"
                CombOffsetFlag=OFFSET
                GTDDate=""
                orderfieldRef="1"
                MinVolume = 0
                ForceCloseReason = api.THOST_FTDC_FCC_NotForceClose
                IsAutoSuspend = 0
        :return:
        """
        order_fields = self.ctp_api.CThostFtdcInputOrderField()
        order_id = self.req_id
        order_ref = str(order_id)
        for k, v in kwargs.items():
            order_fields.__setattr__(k, v)
        order_fields.__setattr__("OrderRef", order_ref)
        self.trade_api.ReqOrderInsert(order_fields, order_id)
        logger.info(f"Order submit {order_id} -- {order_ref} filed.")
        return order_ref

    def _trade_order_revoke(self, **kwargs):
        revoked_order_fields = self.ctp_api.CThostFtdcInputOrderActionField()
        order_id = self.req_id
        for k, v in kwargs.items():
            revoked_order_fields.__setattr__(k, v)
        self.trade_api.ReqOrderAction(revoked_order_fields, order_id)
        logger.info(f"Order revoke {revoked_order_fields.ActionFlag} filed.")

    def query_available_contracts(self, **kwargs):
        qry_instrument_fld = self.ctp_api.CThostFtdcQryInstrumentField()
        order_id = self.req_id
        for k, v in kwargs.items():
            qry_instrument_fld.__setattr__(k, v)
        self.trade_api.ReqQryInstrument(qry_instrument_fld, order_id)

    def buy_open(
            self,
            instrument_contract: str,
            exchange: str,
            limit_price: Any,
            volume: Any,
            ioc: Any = False,
            mv_required: Any = 1,
    ):
        tc_ = self.ctp_api.THOST_FTDC_TC_IOC if bool(int(ioc)) else self.ctp_api.THOST_FTDC_TC_GFD
        if int(mv_required) > 1:
            vc_ = self.ctp_api.THOST_FTDC_VC_MV
        else:
            vc_ = self.ctp_api.THOST_FTDC_VC_AV
        return self._trade_order_submit(
            InstrumentID=instrument_contract,
            ExchangeID=exchange,
            LimitPrice=float(limit_price),
            VolumeTotalOriginal=int(volume),
            BrokerID=self.broker_id,
            UserID=self.user_id,
            InvestorID=self.investor_id,
            Direction=self.ctp_api.THOST_FTDC_D_Buy,
            OrderPriceType=self.ctp_api.THOST_FTDC_OPT_LimitPrice,
            ContingentCondition=self.ctp_api.THOST_FTDC_CC_Immediately,
            TimeCondition=tc_,
            VolumeCondition=vc_,
            MinVolume=int(mv_required),
            CombHedgeFlag="1",
            CombOffsetFlag=self.ctp_api.THOST_FTDC_OF_Open,
            ForceCloseReason=self.ctp_api.THOST_FTDC_FCC_NotForceClose,
            IsAutoSuspend=0,
        )

    def sell_open(
            self,
            instrument_contract: str,
            exchange: str,
            limit_price: Any,
            volume: Any,
            ioc: Any = False,
            mv_required: Any = 1,
    ):
        tc_ = self.ctp_api.THOST_FTDC_TC_IOC if bool(int(ioc)) else self.ctp_api.THOST_FTDC_TC_GFD
        if int(mv_required) > 1:
            vc_ = self.ctp_api.THOST_FTDC_VC_MV
        else:
            vc_ = self.ctp_api.THOST_FTDC_VC_AV
        return self._trade_order_submit(
            InstrumentID=instrument_contract,
            ExchangeID=exchange,
            LimitPrice=float(limit_price),
            VolumeTotalOriginal=int(volume),
            BrokerID=self.broker_id,
            UserID=self.user_id,
            InvestorID=self.investor_id,
            Direction=self.ctp_api.THOST_FTDC_D_Sell,
            OrderPriceType=self.ctp_api.THOST_FTDC_OPT_LimitPrice,
            ContingentCondition=self.ctp_api.THOST_FTDC_CC_Immediately,
            TimeCondition=tc_,
            VolumeCondition=vc_,
            MinVolume=int(mv_required),
            CombHedgeFlag="1",
            CombOffsetFlag=self.ctp_api.THOST_FTDC_OF_Open,
            ForceCloseReason=self.ctp_api.THOST_FTDC_FCC_NotForceClose,
            IsAutoSuspend=0,
        )

    def buy_close(
            self,
            instrument_contract: str,
            exchange: str,
            limit_price: Any,
            volume: Any,
            ioc: Any = False,
            mv_required: Any = 1,
            td_opt: Any = 0,
    ):
        """
        :param instrument_contract:  traded contract
        :param exchange:  exchange abbr
        :param limit_price: price
        :param volume: volume traded
        :param ioc: True for IOC order / False for GFD order
        :param mv_required: min_volume traded, default 1 or 0.
                            if set above 1, a minimum of mv_required Volume is needed for trade to be executed.
        :param td_opt:
                    0: Close Position
                    1: Close Today
                    2: Close Yesterday
        :return: None
        """
        tc_ = self.ctp_api.THOST_FTDC_TC_IOC if bool(int(ioc)) else self.ctp_api.THOST_FTDC_TC_GFD
        if int(mv_required) > 1:
            vc_ = self.ctp_api.THOST_FTDC_VC_MV
        else:
            vc_ = self.ctp_api.THOST_FTDC_VC_AV
        if int(td_opt) == 0:
            offset_ = self.ctp_api.THOST_FTDC_OF_Close
        elif int(td_opt) == 1:
            offset_ = self.ctp_api.THOST_FTDC_OF_CloseToday
        elif int(td_opt) == 2:
            offset_ = self.ctp_api.THOST_FTDC_OF_CloseYesterday
        else:
            raise AttributeError(f"td offset value can only be 0, 1 or 2. Got {td_opt}. ")
        return self._trade_order_submit(
            InstrumentID=instrument_contract,
            ExchangeID=exchange,
            LimitPrice=float(limit_price),
            VolumeTotalOriginal=int(volume),
            BrokerID=self.broker_id,
            UserID=self.user_id,
            InvestorID=self.investor_id,
            Direction=self.ctp_api.THOST_FTDC_D_Buy,
            OrderPriceType=self.ctp_api.THOST_FTDC_OPT_LimitPrice,
            ContingentCondition=self.ctp_api.THOST_FTDC_CC_Immediately,
            TimeCondition=tc_,
            VolumeCondition=vc_,
            MinVolume=int(mv_required),
            CombHedgeFlag="1",
            CombOffsetFlag=offset_,
            ForceCloseReason=self.ctp_api.THOST_FTDC_FCC_NotForceClose,
            IsAutoSuspend=0,
        )

    def sell_close(
            self,
            instrument_contract: str,
            exchange: str,
            limit_price: Any,
            volume: Any,
            ioc: Any = False,
            mv_required: Any = 1,
            td_opt: Any = 0,
    ):
        """
        :param instrument_contract:  traded contract
        :param exchange:  exchange abbr
        :param limit_price: price
        :param volume: volume traded
        :param ioc: True for IOC order / False for GFD order
        :param mv_required: min_volume traded, default 1 or 0.
                            if set above 1, a minimum of mv_required Volume is needed for trade to be executed.
        :param td_opt:
                    0: Close Position
                    1: Close Today
                    2: Close Yesterday
        :return: None
        """
        tc_ = self.ctp_api.THOST_FTDC_TC_IOC if bool(int(ioc)) else self.ctp_api.THOST_FTDC_TC_GFD
        if int(mv_required) > 1:
            vc_ = self.ctp_api.THOST_FTDC_VC_MV
        else:
            vc_ = self.ctp_api.THOST_FTDC_VC_AV
        if int(td_opt) == 0:
            offset_ = self.ctp_api.THOST_FTDC_OF_Close
        elif int(td_opt) == 1:
            offset_ = self.ctp_api.THOST_FTDC_OF_CloseToday
        elif int(td_opt) == 2:
            offset_ = self.ctp_api.THOST_FTDC_OF_CloseYesterday
        else:
            raise AttributeError(f"td offset value can only be 0, 1 or 2. Got {td_opt}. ")
        return self._trade_order_submit(
            InstrumentID=instrument_contract,
            ExchangeID=exchange,
            LimitPrice=float(limit_price),
            VolumeTotalOriginal=int(volume),
            BrokerID=self.broker_id,
            UserID=self.user_id,
            InvestorID=self.investor_id,
            Direction=self.ctp_api.THOST_FTDC_D_Sell,
            OrderPriceType=self.ctp_api.THOST_FTDC_OPT_LimitPrice,
            ContingentCondition=self.ctp_api.THOST_FTDC_CC_Immediately,
            TimeCondition=tc_,
            VolumeCondition=vc_,
            MinVolume=int(mv_required),
            CombHedgeFlag="1",
            CombOffsetFlag=offset_,
            ForceCloseReason=self.ctp_api.THOST_FTDC_FCC_NotForceClose,
            IsAutoSuspend=0,
        )

    def withdraw_order(
            self,
            instrument_contract: str,
            exchange: str,
            order_ref: str,
    ):
        sys_id = self.trade_spi.order_rtn_detail[order_ref]['OrderSysID']
        self._trade_order_revoke(
            BrokerID=self.broker_id,
            ExchangeID=exchange,
            InstrumentID=instrument_contract,
            UserID=self.user_id,
            InvestorID=self.investor_id,
            OrderSysID=sys_id,
            ActionFlag=self.ctp_api.THOST_FTDC_AF_Delete
        )
        return order_ref

    def _qry_account(self, **kwargs):
        acc_fields = self.ctp_api.CThostFtdcQryTradingAccountField()
        req_id = self.req_id
        for k, v in kwargs.items():
            acc_fields.__setattr__(k, v)
        self.trade_api.ReqQryTradingAccount(acc_fields, req_id)
        logger.info(f"Qry account {req_id} filed.")
        return req_id

    def _qry_position(self, **kwargs):
        pos_fields = self.ctp_api.CThostFtdcQryInvestorPositionField()
        req_id = self.req_id
        for k, v in kwargs.items():
            pos_fields.__setattr__(k, v)
        self.trade_api.ReqQryInvestorPosition(pos_fields, req_id)
        logger.info(f"Qry position {req_id} filed.")
        return req_id

    def query_account(self):
        return self._qry_account(
            BrokerID=self.broker_id,
            InvestorID=self.investor_id,
            CurrencyID="CNY"
        )

    def query_position(self, instrument_contract: str = None):
        if instrument_contract is None:
            return self._qry_position(
                BrokerID=self.broker_id,
                InvestorID=self.investor_id,
            )
        else:
            return self._qry_position(
                BrokerID=self.broker_id,
                InvestorID=self.investor_id,
                InstrumentID=instrument_contract
            )
