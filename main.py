from classes.extractor import Extractor
from classes.exceltransformer import ExcelTransformer
from classes.grouper import Grouper
from classes.logger import LogFIDC

from pprint import pprint

import os

logger = LogFIDC()

def main():
    #path_handle = PathV8()

    site_name = "FIDCS"

    extractor = Extractor(site_name)

    path_to_download = "FIDCs Investidos/Relatórios/Planilhas de Monitoramentos/2025/Relatórios Março"

    fidcs_files = extractor.list_files(path_file=path_to_download)
    #botar o raise para caso o arquivo não seja achado
    #print(fidcs_files)

    for fidc in fidcs_files:
        try:
            name_ = "FIDC_" + fidc + "_2025_03_01.xlsx"
            path_ = path_to_download + "/" + fidc
            path_target = "./examples/" + name_

            extractor.download_file(path_, name_, path_target)

            transformer = ExcelTransformer(path_target, fidc)
            transformer.transform_table()
        except Exception as e:
            logger.error(f"O FIDC {fidc} será pulado devido ao erro: {e}")
            continue

    grouper = Grouper()

    grouper.read_csvs("./out")

    grouped_data = grouper.group_fidcs("2025-03-01")

if __name__ == "__main__":
    main()
# TODO TEM QUE VER A QUESTÃO DOS DIAS CORRIDOS E DIAS ÚTEIS
# TODO CHECAR QUESTÕES DOS VALORES NEGATIVOS
# TODO CRIAR AS COLUNAS QUE PEDIRAM
# TODO DETECTAR ERRO DO ITEM FALTANTE
# TODO VER FUNDO SOBERANO NO CALCULO DO CAIXAPL
# complicar coisas desnecessariamente dps... (ajeitar os paths)
# adaptar isso para o mês

# AQUI É ACHAR ERROS E TRATAR O MÁXIMO DE TODOS PRA SEMPRE TER ALGUM DIAGNOSTICO
