import os.path
import sys
import utils
from utils.tool.configer import Config
from utils.tool.logger import log

logger = log(__file__, "Setup")


def run_setup(project: str):
    conf_ = Config()
    root_path = conf_.path
    for folder in ['infra', 'utils']:
        if os.path.exists(os.path.join(root_path, folder)):
            pass
        else:
            os.mkdir(os.path.join(root_path, folder))
    logger.info("Completing file system.")

    sys_path = sys.path
    p_ = [i for i in sys_path if os.path.split(i)[-1] == 'site-packages']
    for p in p_:
        with open(os.path.join(p, f'{project}.pth'), mode='w+', encoding='utf8') as f:
            f.write(root_path)
    logger.info(f"Root path: {root_path} added into system path.")


if __name__ == '__main__':
    run_setup('pTrader')
