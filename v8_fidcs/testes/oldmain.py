from classes.extractor import Extractor
from classes.exceltransformer import ExcelTransformer
from classes.grouper import Grouper
from classes.logger import LogFIDC

import os
import datetime

def main():
    logger = LogFIDC()
    folder_root = os.path.abspath(os.path.join(os.getcwd(), "data"))
    raw_path = os.path.join(folder_root, "RAW")
    parsed_path = os.path.join(folder_root, "PARSED")

    # Dados fixos para teste
    site_name = "FIDCS"
    extractor = Extractor(site_name)

    # Caminho fixo para o mês de março de 2025
    path_to_download = "FIDCs Investidos/Relatórios/Planilhas de Monitoramentos/2024/Relatórios Outubro"

    # FIDCs que deseja testar
    fidcs_files = extractor.list_files(path_file=path_to_download)
    """
    for fidc in fidcs_files:
        try:
            name_ = f"FIDC_{fidc}_2024_10_01.xlsx"
            path_ = f"{path_to_download}/{fidc}"
            path_target = os.path.join(raw_path, name_)

            extractor.download_file(path_, name_, path_target)

            transformer = ExcelTransformer(path_target, fidc)
            transformer.transform_table(folder_root)

        except Exception as e:
            logger.error(f"O FIDC {fidc} será pulado devido ao erro: {e}")
            continue
    """
    try:
       
        grouper = Grouper()
        grouper.read_csvs(parsed_path)
        grouped_data = grouper.group_fidcs("2025_02_01")
        print("Dados agrupados com sucesso.")
    except Exception as e:
        logger.error(f"Erro no agrupamento: {e}")

if __name__ == "__main__":
    main()
