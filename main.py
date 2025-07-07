from classes.extractor import Extractor
from classes.exceltransformer import ExcelTransformer
from classes.grouper import Grouper
from v8_utilities.paths import PathV8
from pprint import pprint

import os

def main():
    #path_handle = PathV8()

    site_name = "FIDCS"

    extractor = Extractor(site_name)

    path_to_download = "FIDCs Investidos/Relatórios/Planilhas de Monitoramentos/2025/Relatórios Abril"

    fidcs_files = extractor.list_files(path_file=path_to_download)
    #print(fidcs_files)

    for fidc in fidcs_files:
        # complicar coisas desnecessariamente dps... (ajeitar os paths)
        # adaptar isso para o mês

        name_ = "FIDC_" + fidc + "_2025_04_01.xlsx"  # TEM QUE VER A QUESTÃO DA DATA DEPOIS
        path_ = path_to_download + "/" + fidc
        path_target = "./examples/" + name_

        # HANDLE caso ocorra erro e pular o fidc

        extractor.download_file(path_, name_, path_target)

        transformer = ExcelTransformer(path_target, fidc)
        transformer.transform_table()

    grouper = Grouper()

    grouper.read_csvs("./out")

    grouper.group_fidcs("2025-04-01")

if __name__ == "__main__":
    main()
# TODO TEM QUE VER A QUESTÃO DOS DIAS CORRIDOS E DIAS ÚTEIS
# TODO CHECAR QUESTÕES DOS VALORES NEGATIVOS
# TODO CRIAR AS COLUNAS QUE PEDIRAM