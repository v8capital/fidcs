from v8_utilities.sharepoint import SharePoint
from classes.exceltransformer import ExcelTransformer
from classes.grouper import Grouper
from dotenv import load_dotenv

import os

def main():
    """excel = Excel_Transformer()

    arquivos = [
        ("./examples/Relatório - FIDC PAGOL - 2025.04.xlsx", "PAGOL"),
        ("./examples/M8 Asset_PHD FIDC_Informação aos Investidores_Abril 2025.xlsx", "PHD"),
        ("./examples/ÁRTICO FIDC - Histórico de Performance_Até_maio_2025.xlsx", "ARTICO"),
        ("./examples/ALFA FIDC - Dados Basicos - Informações de Investimentos - Abril2025.xlsx", "ALFA"),
        ("./examples/Dados Básicos - Análise Investimento - Barcelona.xlsx", "BARCELONA"),
        ("./examples/Modelo MultiAsset - Acompanhamento Resultados Fundo_v8.xlsx", "MULTIASSET"),
        ("./examples/Dados - Análise Investimento MULTIPLIKE FIDC ABRIL 2025.xlsx", "MULTIPLIKE"),
        ("./examples/Monitoramento FIDC Multissetorial ONE7 LP - Abr 2025.xlsx", "ONE7"),
        ("./examples/2025.04 Acompanhamento - FIDC Appaloosa (1).xlsx", "APPALOOSA"),
        ("./examples/Monitoramento_Solar_202504.xlsx", "SOLAR"),
    ]

    for path, sheet in arquivos:
        excel.read_excel(path, sheet)
        excel.transform_table()"""


    #grp = Grouper()

    #grp.read_csvs("./out/")

    #grp.group_FIDCs("2025-04-30")

    """

    load_dotenv()

    tenant_id = os.getenv("TENANT_ID")
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    authority_url = os.getenv("AUTHORITY_URL")
    site_domain = os.getenv("SITE_DOMAIN")
    site_name = os.getenv("SITE_NAME")
    site_id = os.getenv("SITE_ID")


    extr = SharePoint(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        authority_url=authority_url,
        site_domain=site_domain,
        site_name=site_name,
        site_id=site_id
    )

    extr.download_file(
        "FIDCs Investidos/Relatórios/Planilhas de Monitoramentos/2025/Relatórios Abril/SOLAR/FIDC_SOLAR_2025_04_30.xlsx",
       "FIDC_SOLAR_2025_04_30.xlsx", "./teste.xlsx")
       
    """

    arquivos = [
        ("./examples/Relatório - FIDC PAGOL - 2025.04.xlsx", "PAGOL"),
        ("./examples/M8 Asset_PHD FIDC_Informação aos Investidores_Abril 2025.xlsx", "PHD"),
        ("./examples/ÁRTICO FIDC - Histórico de Performance_Até_maio_2025.xlsx", "ARTICO"),
        ("./examples/ALFA FIDC - Dados Basicos - Informações de Investimentos - Abril2025.xlsx", "ALFA"),
        ("./examples/Dados Básicos - Análise Investimento - Barcelona.xlsx", "BARCELONA"),
        ("./examples/Modelo MultiAsset - Acompanhamento Resultados Fundo_v8.xlsx", "MULTIASSET"),
        ("./examples/Dados - Análise Investimento MULTIPLIKE FIDC ABRIL 2025.xlsx", "MULTIPLIKE"),
        ("./examples/Monitoramento FIDC Multissetorial ONE7 LP - Abr 2025.xlsx", "ONE7"),
        ("./examples/2025.04 Acompanhamento - FIDC Appaloosa (1).xlsx", "APPALOOSA"),
        ("./examples/Monitoramento_Solar_202504.xlsx", "SOLAR"),
    ]

    for path, sheet in arquivos:
        excel = ExcelTransformer(path, sheet)
        excel.transform_table()

    grp = Grouper()

    grp.read_csvs("./out/")

    grp.group_fidcs("2025-04-30")
    # to meio preocupado com a perca de info, mas acho q não vai acontecer

    # TODO AJEITAR O VALOR NEGATIVO DO PDD DA SOLAR, fazer depois
    print()


if __name__ == "__main__":
    main()