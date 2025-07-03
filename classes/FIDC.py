import pandas as pd
import numpy as np

from typing import Union
from classes.logger import LogFIDC

import yaml
import logging
import warnings
import re

PATH = './YAMLs/'
def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        dados = yaml.safe_load(f)

    return dados  # Retorna como dicionário: {coluna_final: [possibilidades]}

logger = LogFIDC()

class FIDC():
    def __init__(self, table: pd.DataFrame, raw_table: Union[pd.DataFrame, list], name: str, type: str, pattern: list) -> None:
        patterns_fidcs = read_yaml(PATH + 'FIDCs.yaml')

        self.raw_table = raw_table
        self.table = table
        self.name = name
        self.type = type
        self.pattern = pattern

    def convert_to_double(self, data):
        # Define valores a substituir por NaN
        invalid_entries = ["-", " ", ""]  # você pode adicionar outros se necessário
        data = data.replace(invalid_entries, np.nan).infer_objects(copy=False)

        # Verifica e printa valores inválidos que viraram NaN por não serem numéricos
        for col in data.columns:
            for idx, val in data[col].items():
                # Ignora valores não escalares
                if not pd.api.types.is_scalar(val):
                    continue
                # Se for NaN, continue
                if pd.isna(val):
                    continue
                try:
                    float(val)
                except Exception:
                    logger.debug(f"Valor inválido convertido para NaN: '{val}' | Coluna: '{col}' | Linha: {idx}")
                    data.at[idx, col] = np.nan

        # Converte o restante para float (double)
        return data.astype("double")

    def correct_values(self, data):
        # multiplica todas as colunas no YAML que possuem valor R1000 por 1000
        r1000_cols = [k for d in self.pattern for k, v in d.items() if v == "valueR1000"]
        # Seleciona as colunas presentes no DataFrame
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in r1000_cols)]
        # Multiplica por 1000
        data[cols_to_multiply] = data[cols_to_multiply] * 1000
        return data

    def correct_percentages(self, data, target):
        percent_cols = [k for d in self.pattern for k, v in d.items() if v == "repeatpercent"]
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in percent_cols)]
        for col in cols_to_multiply:
            idxs = [i for i, c in enumerate(data.columns) if c == col]
            for idx in idxs:
                data.iloc[:, idx] = data.iloc[:, idx] * data[target]
        return data


    def clean_column_names(self, data):
        novas_colunas = []
        for nome in data.columns:
            texto_modificado = re.sub(r'(Cedente\s+\d+)\s+\(Vlr Presente-PDD\)', r'\1', nome)
            texto_modificado = re.sub(r'(Sacado\s+\d+)\s+\(Vlr Presente-PDD\)', r'\1', texto_modificado)
            novas_colunas.append(texto_modificado)
        data.columns = novas_colunas
        return data

    def remove_rows_before(self, indexes, date_limit):
        date_limit = pd.to_datetime(date_limit)

        dates = pd.to_datetime(indexes, format='%d/%m/%Y', errors = "coerce")

        rows = dates >= date_limit

        return indexes[rows]

    def create_10_biggests(self, data, target):
        column_name = "Concentrações " + target + "s (R$)"
        columns_to_sum = [target + f' {i}' for i in range(1, 11)]
        data[column_name] = data[columns_to_sum].sum(axis=1)
        return data

    def correct_assets(self, data):
        assets = list({k for d in self.pattern for k, v in d.items() if v == "asset"})
        dc = {k for d in self.pattern for k, v in d.items() if v == "dc"}
        #print(data.columns)

        assets, dc = [
            [val for val in data.columns if any(re.fullmatch(rx, val) for rx in padroes)]
            for padroes in (assets, dc)
        ]

        #print(assets)

        #print(dc)
        data[assets] = data[assets].multiply(data[dc[0]], axis = 0)
        return data

    def create_total_liquid(self, data):
        liquid_days_p = list({k for d in self.pattern for k, v in d.items() if v == "liquids"})
        # print(data.columns)

        liquid_days = [val for val in data.columns if any(re.fullmatch(rx, val) for rx in liquid_days_p)]
        data["Liquidado Total(R$)"] = data[liquid_days].sum(axis=1)

    def convert_date(self, arr):
        month_list = {
            "Janeiro": "January", "Fevereiro": "February", "Março": "March",
            "Abril": "April", "Maio": "May", "Junho": "June", "Julho": "July",
            "Agosto": "August", "Setembro": "September", "Outubro": "October",
            "Novembro": "November", "Dezembro": "December",
            "jan": "January", "fev": "February", "mar": "March",
            "abr": "April", "mai": "May", "jun": "June", "jul": "July",
            "ago": "August", "set": "September", "out": "October",
            "nov": "November", "dez": "December"
        }

        arr_en = []
        for raw in arr:
            if isinstance(raw, (pd.Timestamp,)):
                arr_en.append(raw)
            elif isinstance(raw, str):
                s = re.sub(r"\s+", " ", raw.strip()).replace("-", " ")
                for month in month_list:
                    if month in s:
                        s = s.replace(month, month_list[month])
                arr_en.append(s)
            else:
                arr_en.append(raw)

        # Tenta primeiro com %B %Y, depois com %B %y para os que falharem
        dates = pd.to_datetime(arr_en, format="%B %Y", errors='coerce')
        mask_na = dates.isna()
        if mask_na.any():
            dates2 = pd.to_datetime(pd.Series(arr_en)[mask_na], format="%B %y", errors='coerce')
            dates = pd.Series(dates)
            dates[mask_na] = dates2

        return dates

