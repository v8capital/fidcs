from testes.teste import SharePoint


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

    extr = SharePoint()

    extr.download_file("AURUM FIDC - Informação aos Investidores - Abril 2025.xlsx", "Aurum")

    # to meio preocupado com a perca de info, mas acho q não vai acontecer

    # TODO AJEITAR O VALOR NEGATIVO DO PDD DA SOLAR, fazer depois
    print()


if __name__ == "__main__":
    main()