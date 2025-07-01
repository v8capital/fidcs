import pandas as pd
from classes.FIDC import FIDC
from classes.Excel_Transformer import Excel_Transformer

def main():
    excel = Excel_Transformer()

    #excel.read_excel("./examples/Relatório - FIDC PAGOL - 2025.04.xlsx", "PAGOL")
    #excel.read_excel("./examples/M8 Asset_PHD FIDC_Informação aos Investidores_Abril 2025.xlsx", "PHD")
    #excel.read_excel("./examples/ÁRTICO FIDC - Histórico de Performance_Até_maio_2025.xlsx", "ARTICO")
    #excel.read_excel("./examples/ALFA FIDC - Dados Basicos - Informações de Investimentos - Abril2025.xlsx", "ALFA")
    #excel.read_excel("./examples/Dados Básicos - Análise Investimento - Barcelona.xlsx", "BARCELONA")
    #excel.read_excel("./examples/Modelo MultiAsset - Acompanhamento Resultados Fundo_v8.xlsx", "MULTIASSET")
    #excel.read_excel("./examples/Dados - Análise Investimento MULTIPLIKE FIDC ABRIL 2025.xlsx", "MULTIPLIKE")
    #excel.read_excel("./examples/Monitoramento FIDC Multissetorial ONE7 LP - Abr 2025.xlsx", "ONE7")
    #excel.read_excel("./examples/2025.04 Acompanhamento - FIDC Appaloosa (1).xlsx", "APPALOOSA")
    excel.read_excel("./examples/Monitoramento_Solar_202504.xlsx", "SOLAR")

    excel.transform_table()
    print()

if __name__ == "__main__":
    main()