from classes.extractor import Extractor
from classes.exceltransformer import ExcelTransformer
from classes.grouper import Grouper
from dotenv import load_dotenv

import os

def main():
    load_dotenv()
    tenant_id = os.getenv("TENANT_ID")
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    authority_url = os.getenv("AUTHORITY_URL")
    site_domain = os.getenv("SITE_DOMAIN")
    site_name = os.getenv("SITE_NAME")
    site_id = os.getenv("SITE_ID")

    extrator = Extractor(tenant_id, client_id, client_secret, authority_url, site_domain, site_name, site_id)

    # Extrai o arquivo FIDC
    path_sharepoint = "FIDCs Investidos/Relatórios/Planilhas de Monitoramentos/2025/Relatórios Abril"
    fidcs_files = extrator.list_files(path_sharepoint)

    path_download = "./examples"
    for fidc in fidcs_files:
        name_ = "FIDC_" + fidc + "_2025_04_30.xlsx" # TEM QUE VER A QUESTÃO DA DATA DEPOIS
        path_ = path_sharepoint + "/" + fidc + "/" + name_
        path_target = "./examples/" + name_

        extrator.download_file(path_, name_, path_target)

        transformer = ExcelTransformer(path_target, fidc)
        transformer.transform_table()
    #fidc_path = extrator.extract_fidcs(path="", fidcs_list=["FIDC1", "FIDC2"])
    grouper = Grouper()

    grouper.read_csvs("./out")

    grouper.group_fidcs("2025-04-30")


if __name__ == "__main__":
    main()
# TODO TEM QUE VER A QUESTÃO DOS DIAS CORRIDOS E DIAS ÚTEIS
# TODO CHECAR QUESTÕES DOS VALORES NEGATIVOS