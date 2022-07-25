import time
from functools import wraps
import traceback
from utils.tool.logger import log
from utils.tool.configer import Config

logger = log(__file__, "utils")

configer = Config()


def auto_retry(
        show_err: bool = True,
        max_retries: int = 3,
):
    def func_wrapper(func):
        @wraps(func)
        def __function(*args, **kwargs):
            max_t = 0
            while True:
                try:
                    res = func(*args, **kwargs)
                except Exception:
                    max_t += 1
                    if show_err:
                        msg_ = f"\ndetails: \n{str(traceback.format_exc())}"
                        logger.error(msg_)
                    else:
                        pass
                    if max_t < max_retries:
                        time.sleep(5)
                        continue
                    else:
                        break
                else:
                    return res
        return __function
    return func_wrapper
