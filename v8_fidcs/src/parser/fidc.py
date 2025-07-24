from typing import Union
from v8_fidcs.src.others.logger import LogFIDC

import pandas as pd
import numpy as np

import yaml
import logging
import warnings
import re
import os

PATH = os.path.join(os.getcwd(), 'YAMLs')

def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        dados = yaml.safe_load(f)
    return dados  # Retorna como dicionário: {coluna_final: [possibilidades]}

logger = LogFIDC()

class FIDC():
    def __init__(self, table: pd.DataFrame, raw_table: Union[pd.DataFrame, list], name: str, type: str, pattern: list) -> None:
        patterns_fidcs = read_yaml(os.path.join(PATH, "FIDCs.yaml"))

        self.raw_table = raw_table
        self.table = table
        self.name = name
        self.type = type
        self.pattern = pattern

        self._ptbr_num = re.compile(
            r'''^          # start
                                -?         # optional sign
                                \d{1,3}    # first group of up to 3 digits
                                (?:\.\d{3})*  # zero or more “.###” thousands groups
                                (?:,\d+)?     # optional “,##” decimal part
                                $          # end
                            ''',
            re.VERBOSE,
        )

    def _str_ptbr_to_float(self, s: str) -> float:
        """
        Convert '322.850,74' → 322850.74 (and similar) safely.
        Assumes `s` already matches `_ptbr_num`.
        """


        logger.debug(f"Número em PTBR encontrado e sendo convertido: '{s}'")

        return float(s.replace('.', '').replace(',', '.'))

    # TODO ver a questão dos dias corridos e úteis
    # def correct_days(self, data: pd.DataFrame) -> pd.DataFrame:

    def convert_to_double(self, data: pd.DataFrame) -> pd.DataFrame:
        """Sanitise DataFrame and return it as float64 (“double”)."""

        # 1. Replace obvious “empty” entries with NaN
        data = data.map(lambda x: x.strip() if isinstance(x, str) else x)

        invalid_entries = ["-", " ", ""]
        data = data.replace(invalid_entries, np.nan).infer_objects(copy=False)

        rows_to_drop = set()

        # 2. Row‑wise cleanup (colunas com nomes repetidos tratadas corretamente)
        for i in range(data.shape[1]):
            col = data.columns[i]
            series = data.iloc[:, i]

            for row_pos, (idx, val) in enumerate(series.items()):
                if not pd.api.types.is_scalar(val):
                    continue

                if pd.isna(val) and type(val).__name__ == "NaTType":
                    logger.warning(
                        f"Valor do tipo NaT encontrado | Coluna: '{col}' | Linha: {idx} — linha será removida"
                    )
                    rows_to_drop.add(idx)
                    continue

                if pd.isna(val):
                    continue

                if isinstance(val, str) and self._ptbr_num.match(val):
                    try:
                        data.iat[row_pos, i] = self._str_ptbr_to_float(val)
                        continue
                    except Exception:
                        pass

                try:
                    float(val)
                except Exception:
                    logger.debug(
                        f"Valor inválido convertido para NaN: '{val}' | "
                        f"Coluna: '{col}' | Linha: {idx}"
                    )
                    data.iat[row_pos, i] = np.nan

        # Remoção das linhas marcadas
        if rows_to_drop:
            data = data.drop(index=rows_to_drop)

        # 3. Conversão final para float64 (“double”)
        return data.astype("double")

    def absolute_values(self, data):
        # multiplica todas as colunas no YAML que possuem valor absolute por -1
        absolute_cols = [k for d in self.pattern for k, v in d.items() if v == "absolute"]
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in absolute_cols)]
        data[cols_to_multiply] = data[cols_to_multiply] * -1000 # pq o PDD tá em milhares
        return data

    def correct_values(self, data):
        # multiplica todas as colunas no YAML que possuem valor R1000 por 1000
        r1000_cols = [k for d in self.pattern for k, v in d.items() if v == "valueR1000"]
        # Seleciona as colunas presentes no DataFrame
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in r1000_cols)]
        # Multiplica por 1000
        data[cols_to_multiply] = data[cols_to_multiply] * 1000
        return data

    def correct_percentages(self, data, target):
        percent_cols = [k for d in self.pattern for k, v in d.items() if v == "repeatpercent" or v == "percentrp"]
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in percent_cols)]
        for col in cols_to_multiply:
            idxs = [i for i, c in enumerate(data.columns) if c == col]
            for idx in idxs:
                data.iloc[:, idx] = data.iloc[:, idx] * data[target]
        return data

    def clean_column_names(self, data):
        # acredito que essa função pode ser inserida na correct_column_names mas não farei isso agr
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
        renames = list({k for d in self.pattern for k, v in d.items() if v == "rename"})
        #print(data.columns)

        assets, dc = [
            [val for val in data.columns if any(re.fullmatch(rx, val) for rx in padroes)]
            for padroes in (assets, dc)
        ]

        renames = [val for val in data.columns if any(re.fullmatch(rx, val) for rx in renames)]
        #print(assets)
        #print(dc)
        #print(renames)
        for col in renames:
            data[f"{col} (%)"] = data[col]
            data[col] = np.nan
        if dc:
            data[assets] = data[assets].multiply(data[dc[0]], axis = 0)
        return data

    def correct_column_names(self, data):
        def clean_col(col):
            return re.sub(r'\s*\(.*?\)\s*', '', col).strip()
        par_cols = [k for d in self.pattern for k, v in d.items() if v == "removepar" or v == "percentrp"]
        new_columns = {}
        for col in data.columns:
            if any(re.fullmatch(pat, col) for pat in par_cols):
                new_columns[col] = clean_col(col)
            else:
                new_columns[col] = col  # mantém original

        data = data.rename(columns=new_columns)
        return data


    def _days_to_start_of_month(self, data):
        df = data.copy()
        df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[~df.index.isna()]

        # set index to the first day of each month
        df.index = df.index.to_period('M').to_timestamp(how='start')

        return df.groupby(df.index).first()

    def sum_columns(self, data, sign: str):
        columns_to_sum_regex = list({k for d in self.pattern for k, v in d.items() if v == "repeat" + sign})
        columns_to_sum = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in columns_to_sum_regex)]

        column_regex = next((k for d in self.pattern for k, v in d.items() if v == sign), None)
        column = next((col for col in data.columns if re.fullmatch(column_regex, col)), None)
        print(column, columns_to_sum, data[columns_to_sum].sum(axis = 1))
        data[column] = data[columns_to_sum].sum(axis = 1)

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
            "ago": "August", "set": "September", "oct": "October",
            "nov": "November", "dez": "December"
        }

        arr_en = []
        for raw in arr:
            if isinstance(raw, (pd.Timestamp,)):
                arr_en.append(raw)
            elif isinstance(raw, str):
                s = re.sub(r"\s+", " ", raw.strip()).replace("-"," ")
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

