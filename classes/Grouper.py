import pandas as pd
import numpy as np

from typing import Dict, List
from itertools import chain
from v8_utilities.logv8 import LogV8

import yaml
import re
import os


PATH = './YAMLs/'
def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        dados = yaml.safe_load(f)

    return dados  # Retorna como dicionário: {coluna_final: [possibilidades]}

logger = LogV8()

#ajeitar tudo aqui

class Grouper(object):
    def __init__(self):
        self.equiv_columns =  read_yaml(PATH + "/colunas.YAML")
        self.regex_patterns = read_yaml(PATH + "/regex.YAML")
        self.csv_dict: Dict[str, pd.DataFrame] = {}

    # ------------------------  PROCESSAMENTO DE STRINGS  ------------------------ #
    @staticmethod
    def _verify_pattern(entry: str, patterns: List[str]) -> bool:
        text = str(entry).lower().strip()
        return any(re.search(p, text) for p in patterns)

    def _days_string_processing(self, entry: str | None) -> str | None:

        if not entry or not isinstance(entry, str):
            return None

        entry = entry.lower().strip()
        unwanted_numbers = {6, 16, 31, 61, 91, 121, 151, 181, 366, 721}

        # essa partezinha é bem chatinha de entender mas basicamente to padronizando as strings que se referem a dias
        change_pattern = [
            (r"^1-(\d+)", lambda m: f"0-{m.group(1)} dias"),
            (r"^de\s*(\d+)\s*a\s*(\d+)\s*dias?$", lambda m:
            f"{int(m.group(1)) - 1 if int(m.group(1)) in unwanted_numbers else m.group(1)}-{m.group(2)} dias"),
            (r"^(\d+)\s*(?:a|e|-)\s*(\d+)\s*dias?$", lambda m:
            f"{int(m.group(1)) - 1 if int(m.group(1)) in unwanted_numbers else m.group(1)}-{m.group(2)} dias"),
            (r"^(\d+)\s*(?:a|e|-)\s*(\d+)$", lambda m:
            f"{int(m.group(1)) - 1 if int(m.group(1)) in unwanted_numbers else m.group(1)}-{m.group(2)} dias"),
            (r"^até\s*(\d+)(?:\s*dias?)?$", lambda m: f"<= {m.group(1)} dias"),
            (r"^>\s*(\d+)$", lambda m: f"> {int(m.group(1)) - 1 if int(m.group(1)) == 121 else m.group(1)} dias"),
            (r"^(?:acima\s*de|superior(?:\s*a)?)\s*(\d+)(?:\s*dias?)?$",
             lambda m: f"> {int(m.group(1)) - 1 if int(m.group(1)) == 121 else m.group(1)} dias"),
        ]

        for pattern, format in change_pattern:
            if match := re.fullmatch(pattern, entry):
                return format(match)
        return None

    # ------------------------  DATAFRAME MANIPULAÇÃO  ------------------------ #
    def _days_column_processing(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = pd.Series(df.columns)

        pattern = [
            r"^\d+\s*a\s*\d+$",
            r"^\d+\s*-\s*\d+$",
            r"^\d+\s*-\s*\d+\s*dias$",
            r"^de\s+\d+\s*a\s+\d+\s*dias$",
            r"^\d+\s*e\s*\d+\s*dias$",
            r"^acima\s+de\s+\d+\s*dias$",
            r"^superior\s+a\s+\d+$",
            r"^superior\s+\d+$",
            r"^>\s*\d+$",
            r"^até\s+\d+$",
            r"^até\s+\d+\s*dias$"
        ]

        mask = columns.apply(lambda entry: self._verify_pattern(entry, pattern))
        columns_processed = columns[mask]

        columns[mask] = columns_processed.apply(self._days_string_processing)
        df.columns = columns
        return df

    def _rename_equiv_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_dict = {}
        for wanted_name, possibilities in self.equiv_columns.items():

            if possibilities is None: continue

            for possibility in possibilities:
                if possibility in df.columns:
                    rename_dict[possibility] = wanted_name
        return df.rename(columns=rename_dict)

    def _grouping_days_column(self, df: pd.DataFrame) -> pd.DataFrame:

        # agrupa as colunas de dias pelo qual coluna ela se refere

        pattern = [
            r'(\d+)\s*-\s*(\d+)\s*dias?',  # intervalo: 10 - 20 dias
            r'>\s*(\d+)\s*dias?',  # > 120 dias
            r'<=\s*(\d+)\s*dias?'  # <= 10 dias
        ]

        columns = pd.Series(df.columns)
        indicator = columns.apply(lambda entry: self._verify_pattern(entry, pattern))
        # vetor booleano, ex: [True, False, False, ...]

        last_true_idx = None
        new_columns = []

        for idx, col_name in enumerate(columns):
            if not indicator.iloc[idx]:
                new_columns.append(col_name)
                last_true_idx = idx
            else:
                if last_true_idx is not None:
                    new_name = f"({columns.iloc[last_true_idx]}){col_name}"
                    new_columns.append(new_name)
                else:
                    new_columns.append(col_name)
        df.columns = new_columns
        return df

    # ------------------------  SELEÇÃO DE COLUNAS  ------------------------ #
    def _selecting_columns_by_name(self) -> Dict[str, List[str]]:
        # Usa apenas as chaves como nomes exatos de colunas a buscar
        wanted_columns = list(self.equiv_columns.keys())

        result = {}

        result = {n: [c for c in df.columns if c in wanted_columns] for n, df in self.csv_dict.items()}

        #for name_df, df in self.csv_dict.items():
        #    found_columns = []
        #    for col in df.columns:
        #        if col in wanted_columns:
        #            found_columns.append(col)
        #    result[name_df] = found_columns

        return result

    def _selecting_columns_by_regex(self) -> Dict[str, List[str]]:
        regex_list = list(self.regex_patterns.values())[0]
        compiled = [re.compile(p) for p in regex_list]

        out: Dict[str, List[str]] = {}
        for name, df in self.csv_dict.items():
            matches = [c for c in df.columns if any(p.fullmatch(str(c)) for p in compiled)]
            out[name] = matches
        return out

    def _filter_final_columns(self, columns: Dict[str, List[str]]) -> None:
        self.csv_dict = {k: df[columns[k]] for k, df in self.csv_dict.items()}

    def _group_by_date(self, date: str | pd.Timestamp) -> pd.DataFrame:
        list_dates = []

        for key, df in self.csv_dict.items():
            if date in df.index:
                row = df.loc[date]
                row_df = pd.DataFrame([row], index = [key])
            else:
                row_df = pd.DataFrame([{col: pd.NA for col in df.columns}], index=[key])
            list_dates.append(row_df)

        result = pd.concat(list_dates, axis = 0, sort = True)
        return result

    # ------------------------  AGRUPAMENTOS AUXILIARES  ------------------------ #
    def _days_to_end_of_month(self) -> None:
        csv_dict = {}

        for key, df in self.csv_dict.items():
            df = df.copy()
            df.index = pd.to_datetime(df.index, errors='coerce')
            df = df[~df.index.isna()]
            df.index = df.index.to_period('M').to_timestamp('M')
            df = df.groupby(df.index).first()
            csv_dict[key] = df

        self.csv_dict = csv_dict

    # ------------------------  ORDENADOR DE COLUNAS  ------------------------ #
    def _extract_order(self, item: str):
        match = re.match(r'^\((.*?)\)\s*(>?=?\s*\d+)(?:\s*-\s*(\d+))?', item)
        if match:
            group = match.group(1)
            start_raw = match.group(2)
            end_raw = match.group(3)

            start = int(re.sub(r'\D', '', start_raw)) if start_raw else 0
            end = int(end_raw) if end_raw else (start if '>' not in start_raw else 9999)

            return group, start, end
        else:
            return 'ZZZ', 99999, 99999

    def _reorder_df_columns(self, required_cols: List[str], regex_cols: List[str])-> List[str]:
        # Ordena colunas do tipo (Grupo)X-Y dias
        ordered_rgx_cols = sorted(regex_cols, key= self._extract_order)
        ordered_cols = required_cols + ordered_rgx_cols

        # Regras de reorganização
        if 'Concentração Top 10 Cedentes (R$)' in ordered_cols and 'Cedente 1' in ordered_cols:
            ordered_cols.remove('Concentração Top 10 Cedentes (R$)')
            idx = ordered_cols.index('Cedente 1')
            ordered_cols.insert(idx, 'Concentração Top 10 Cedentes (R$)')

        if 'Concentração Top 10 Sacados (R$)' in ordered_cols and 'Sacado 1' in ordered_cols:
            ordered_cols.remove('Concentração Top 10 Sacados (R$)')
            idx = ordered_cols.index('Sacado 1')
            ordered_cols.insert(idx, 'Concentração Top 10 Sacados (R$)')

        group_pattern = re.compile(r'^\(([^)]+)\)\s*[\d>]')
        groups = {}
        for i, col in enumerate(ordered_cols):
            match = group_pattern.match(col)
            if match:
                group = match.group(1)
                if group not in groups and group in ordered_cols:
                    groups[group] = i

        for group, idx_ref in groups.items():
            if group in ordered_cols:
                ordered_cols.remove(group)
                ordered_cols.insert(idx_ref - 1, group)

        # Mover 'PDD À Vencer' após último '(PDD Total)X-Y dias'
        if 'PDD À Vencer' in ordered_cols:
            padrao_pdd = re.compile(r'^\(PDD Total\)\s*\d+\s*-\s*\d+\s*dias')
            indices_pdd = [i for i, col in enumerate(ordered_cols) if padrao_pdd.match(col)]
            if indices_pdd:
                idx_destino = max(indices_pdd)
                ordered_cols.remove('PDD À Vencer')
                ordered_cols.insert(idx_destino, 'PDD À Vencer')

        return ordered_cols

    def read_csvs(self, path):
        # por enquanto eu vou ler todos os csvs de uma pasta X
        csv_dict = {}

        def remove_suffix(columns):
            new_columns = []
            for col in columns:
                # Remove sufixo do tipo .1, .2, .3 no final da string
                new_col = re.sub(r'\.\d+$', '', col)
                new_columns.append(new_col)
            return new_columns

        # Percorre todos os arquivos na pasta
        for file in os.listdir(path):
            if file.endswith('.csv'):
                path_file = os.path.join(path, file)
                name_file = os.path.splitext(file)[0]  # Remove a extensão
                df = pd.read_csv(path_file, sep=';', encoding='utf-8-sig', index_col="Data", ).astype("float64")
                # ajuste encoding se necessário
                logger.info(f"FIDC {name_file} encontrado para agrupamento.")
                df.columns = remove_suffix(df.columns)
                df = self._days_column_processing(df)
                df = self._rename_equiv_columns(df)
                df = self._grouping_days_column(df)
                csv_dict[name_file] = df
        self.csv_dict = csv_dict

    # -------------------  CALCULO DAS MÉTRICAS PEDIDAS  ------------------ #
    # TO DO

    # ------------------------  AGRUPAMENTO FINAL  ------------------------ #

    def group_fidcs(self, date: str | pd.Timestamp) -> pd.DataFrame:

        logger.info(f"Iniciando agrupamento dos FIDCs para a data: {date}")

        by_name = self._selecting_columns_by_name()
        by_regex = self._selecting_columns_by_regex() # {"ALFA": ["Coluna1", "Coluna2", ...], ...}

        column_names = {k: by_name.get(k, []) + by_regex.get(k, [])
                        for k in by_name.keys()}

        self._filter_final_columns(column_names) # mantendo apenas as colunas de interesse
        self._days_to_end_of_month()

        grouped_data = self._group_by_date(date)

        regex_cols      = list(pd.Series(chain.from_iterable(by_regex.values())).drop_duplicates())
        main_columns    = list(self.equiv_columns.keys())
        ordered         = self._reorder_df_columns(main_columns, regex_cols)
        grouped_data    = grouped_data[ordered]
        grouped_data.index.name = 'FIDC'

        name = "FIDCS_" + str(date).replace("-", "_") + ".csv"
        path_out = "./grouped_data/" + name
        grouped_data.to_csv(path_out , sep=';', encoding='utf-8-sig')
        logger.info(f"Agrupamento feito com Sucesso. Arquivo salvo em {path_out}")

        return grouped_data