from pathlib import Path
import os

PATH_MAIN = Path(os.path.dirname(__file__))
PATH_STATICS = PATH_MAIN / "statics"
PATH_IMAGES = PATH_STATICS / "images"
PATH_TESTS = PATH_MAIN / "tests"
PATH_TESTS_INPUTS = PATH_TESTS / "inputs"

IDX_COL_IN = "SOC"
IND_COL_QRY = "SUBORDEN"
ADDRESS_COL = "D_ADDRESS_1"

SPANISH_SPECIAL = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n"}

if __name__ == '__main__':
    print("the main path is: {}".format(PATH_MAIN))
    vars = locals().copy()
    paths = {}
    for k, v in vars.items():
        if k.startswith("PATH_"):
            path = Path(v)
            if path.is_dir():
                print("directory {} already exists".format(v))
            else:
                os.mkdir(path)
                print("directory {} created".format(v))
