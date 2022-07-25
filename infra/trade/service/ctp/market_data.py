from infra.trade.api.ctp_md import *
from utils.tool.configer import Config

config = Config()
trade_conf = config.get_trade_conf


def md_broadcast(
        broker_name: str = 'SIM',
        channel: str = 'test',
        contract_list: list = None,
        gap: float = 1
):
    m = CtpMd(
        trade_conf.get(broker_name, "md_front_addr"),
        trade_conf.get(broker_name, "broker_id"),
        trade_conf.get(broker_name, "investor_id"),
        trade_conf.get(broker_name, "pwd"),
        trade_conf.get(broker_name, "product_info"),
        msg_db=trade_conf.getint(broker_name, "md_msg_db"),
        channel_name=channel,
    )
    sub_ls = [
        m.trade_rule.standard_contract_to_trade_code(*i) for i in contract_list
    ]
    with m:
        m.subscribe_md(sub_ls)
        while m.trade_rule.exchange_open():
            m.msg.pub(json.dumps(m.md_spi.latest_md_buf))
            sleep(gap)
        else:
            logger.warning("Market closed. Ending ctp data broadcasting.")
        sys.exit(0)


if __name__ == "__main__":
    md_broadcast(
        broker_name='SIM',
        channel='test',
    )
