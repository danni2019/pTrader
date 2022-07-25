import os
import configparser


class Config:
    def __init__(self):
        _fp = os.path.dirname(__file__)
        self.path = os.path.realpath(os.path.join(_fp,  os.sep.join(['..', '..'])))

    @property
    def get_conf(self):
        _conf_path = os.path.join(self.path, 'config.ini')
        config = configparser.ConfigParser()
        config.read(_conf_path)
        return config

    @property
    def get_trade_conf(self):
        _conf_path = os.path.join(self.path, 'trade.ini')
        config = configparser.ConfigParser()
        config.read(_conf_path)
        return config
