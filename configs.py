from pathlib import Path
import os
import datetime

PATH_MAIN = Path(os.path.dirname(__file__))
PATH_STATICS = PATH_MAIN / "statics"
PATH_IMAGES = PATH_STATICS / "images"
PATH_TESTS = PATH_MAIN / "tests"
PATH_TESTS_INPUTS = PATH_TESTS / "inputs"

IDX_COL_IN = "SOC"
IND_COL_QRY = "SUBORDEN"
ADDRESS_COL = "D_ADDRESS_1"
DATE_COL = "RANGOFECHAPACTADA"

SPANISH_SPECIAL = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n"}
EPOCH = datetime.datetime(1900, 1, 1)

BULDING_KWS = ["departamento",
               "dpto",
               "depto",
               "dp",
               "depa",
               "dep",
               "apto",
               "torre",
               "torres",
               "piso",
               "pisos",
               "edi",
               "edificio",
               "oficina",
               "local",
               "comercial",
               "of",
               "ofi",
               "loc"
               "condominio",
               "pasaje",
               "psje",
               "pje",
               "int",
               "block",
               "bloque",
               "interior",
               "casa",
               "cas",
               "cabaña",
               "cond"]

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
