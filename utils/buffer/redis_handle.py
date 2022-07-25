import redis
from typing import Any, Union
from utils.tool.configer import Config
from utils.tool.logger import log
from utils.tool.decorator import auto_retry

conf_ = Config()
config = conf_.get_conf


class Redis:
    def __init__(self):
        self.host = config.get('Redis', 'host')
        self.port = int(config.get('Redis', 'port'))
        self.pwd = config.get('Redis', 'password')

    def create_handle(self, db: int, decode: bool = True):
        conn = redis.ConnectionPool(host=self.host, port=self.port, password=self.pwd, db=db, decode_responses=decode)
        rds = redis.StrictRedis(connection_pool=conn, decode_responses=decode)
        return rds

    @auto_retry()
    def set_key(self, db: int, k: Any, v: Any, **kwargs):
        rds = self.create_handle(db)
        rds.set(k, v, **kwargs)

    @auto_retry()
    def get_key(self, db: int, k: Any, decode: bool):
        rds = self.create_handle(db, decode=decode)
        return rds.get(k)

    @auto_retry()
    def set_hash(self, db: int, name: Any, k: Any = None, v: Any = None, mapping: Any = None):
        rds = self.create_handle(db)
        if k is None:
            rds.hmset(name, mapping)
        else:
            rds.hset(name, k, v, mapping)

    @auto_retry()
    def get_hash(self, db: int, name: Any, decode: bool, k: Any = None):
        rds = self.create_handle(db, decode=decode)
        if k is None:
            return rds.hgetall(name)
        else:
            return rds.hget(name, k)

    @auto_retry()
    def key_exist(self, db: int, k: Any):
        rds = self.create_handle(db)
        return rds.exists(k)

    @auto_retry()
    def del_key(self, db: int, keys: list):
        rds = self.create_handle(db)
        rds.delete(*keys)

    @auto_retry()
    def flush(self, db: Union[int, None] = None):
        if db is None:
            rds = self.create_handle(0)
            rds.flushall()
        else:
            rds = self.create_handle(db)
            rds.flushdb()


logger_msg = log(__file__, "utils")


class RedisMsg(Redis):
    """
    A msg queue on redis.
    message queue by default will not decode any responses.
    """
    def __init__(self, db: int, channel: str = 'default'):
        super().__init__()
        self.channel = channel
        self.db = db

    @auto_retry()
    def push_msg(self, msg: bytes):
        rds = self.create_handle(self.db, decode=False)
        rds.lpush(self.channel, msg)

    @auto_retry()
    def get_msg(self, timeout: int = 0):
        rds = self.create_handle(self.db, decode=False)
        msg = rds.brpop(self.channel, timeout=timeout)
        return msg[1] if msg is not None else None

    @auto_retry()
    def pub(self, msg: Any):
        rds = self.create_handle(self.db, decode=False)
        sub_num = rds.publish(channel=self.channel, message=msg)
        logger_msg.info(f"{self.channel} publishing: {sub_num} subscribers.")
        return sub_num

    def __sub(self, subscriber):
        for news in subscriber.listen():
            yield news

    @auto_retry()
    def sub(self):
        rds = self.create_handle(self.db, decode=False)
        sub = rds.pubsub()
        sub.subscribe(self.channel)
        try:
            for item in self.__sub(sub):
                yield item
        except KeyboardInterrupt or SystemExit:
            sub.unsubscribe()
        else:
            pass
