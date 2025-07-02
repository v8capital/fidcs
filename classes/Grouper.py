import pandas as pd
import numpy as np

from itertools import chain

import yaml
import re
import os


PATH = './YAMLs/'
def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        dados = yaml.safe_load(f)

    return dados  # Retorna como dicionário: {coluna_final: [possibilidades]}

class Grouper(object):

    def __init__(self):
        self.equiv_columns = read_yaml(PATH + "/colunas.YAML")
        self.regex_patterns = read_yaml(PATH + "/regex.YAML")

    def __days_string_processing(self, entry):
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
            match = re.fullmatch(pattern, entry)
            if match:
                return format(match)
        return None

    def __verify_pattern(self, entry, pattern):
        text = str(entry).lower().strip()
        return any(re.search(p, text) for p in pattern)


    def __days_column_processing(self, df):
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

        columns_in_the_pattern = columns.apply(lambda entry: self.__verify_pattern(entry, pattern))
        columns_processed = columns[columns_in_the_pattern]

        result = columns_processed.apply(lambda entry: self.__days_string_processing(entry))

        columns[columns_in_the_pattern] = result

        df.columns = columns

        return df

    def __rename_equiv_columns(self, df):
        rename_dict = {}
        for wanted_name, possibilities in self.equiv_columns.items():
            if possibilities is None: continue
            for possibility in possibilities:
                if possibility in df.columns:
                    rename_dict[possibility] = wanted_name
        return df.rename(columns=rename_dict)

    def __grouping_days_column(self, df):

        # agrupa as colunas de dias pelo qual coluna ela se refere

        pattern = [
            r'(\d+)\s*-\s*(\d+)\s*dias?',  # intervalo: 10 - 20 dias
            r'>\s*(\d+)\s*dias?',  # > 120 dias
            r'<=\s*(\d+)\s*dias?'  # <= 10 dias
        ]

        columns = pd.Series(df.columns)
        result = columns.apply(lambda entry: self.__verify_pattern(entry, pattern))  # vetor booleano, ex: [True, False, False, ...]
        last_true_idx = None
        new_columns = []

        for idx, col_name in enumerate(columns):
            if not result.iloc[idx]:
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

    def __selecting_columns_by_name(self):
        # Usa apenas as chaves como nomes exatos de colunas a buscar
        wanted_columns = list(self.equiv_columns.keys())

        result = {}

        for name_df, df in self.csv_dict.items():
            found_columns = []
            for col in df.columns:
                if col in wanted_columns:
                    found_columns.append(col)
            result[name_df] = found_columns

        return result

    def __selecting_columns_by_regex(self):
        wanted_columns = list(self.regex_patterns.values())[0]

        compiled_patterns = [re.compile(p) for p in wanted_columns]

        result = {}

        for name_df, df in self.csv_dict.items():
            found_columns = []
            for col in df.columns:
                for pattern in compiled_patterns:
                    if pattern.fullmatch(str(col)):
                        found_columns.append(col)
                        break
            result[name_df] = found_columns

        return result

    def __selecting_final_columns(self, columns):
        data = {}
        #print(columns)
        for key in self.csv_dict.keys():
            data[key] = self.csv_dict[key][columns[key]]
        return data

    def __group_by_date(self, date):
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

    def __days_to_end_of_month(self):
        csv_dict_ = {}

        for key, df in self.csv_dict.items():
            df = df.copy()
            df.index = pd.to_datetime(df.index, errors='coerce')
            df = df[~df.index.isna()]
            df.index = df.index.to_period('M').to_timestamp('M')
            df = df.groupby(df.index).first()
            csv_dict_[key] = df

        return csv_dict_

    def __extract_order(self, item):
        match = re.match(r'^\((.*?)\)\s*(>?=?\s*\d+)(?:\s*-\s*(\d+))?', item)
        if match:
            group = match.group(1)
            start_raw = match.group(2)
            end_raw = match.group(3)

            start = int(re.sub(r'\D', '', start_raw)) if start_raw else 0
            end = int(end_raw) if end_raw else (start if '>' not in start_raw else 9999)

            return (group, start, end)
        else:
            return ('ZZZ', 99999, 99999)

    def __reorder_df_columns(self, wanted_columns_names, data):
        # Ordena colunas do tipo (Grupo)X-Y dias
        ordered_list = sorted(data, key= self.__extract_order)
        result = wanted_columns_names + ordered_list

        # Regras de reorganização
        if 'Concentração Top 10 Cedentes (R$)' in result and 'Cedente 1' in result:
            result.remove('Concentração Top 10 Cedentes (R$)')
            idx = result.index('Cedente 1')
            result.insert(idx, 'Concentração Top 10 Cedentes (R$)')

        if 'Concentração Top 10 Sacados (R$)' in result and 'Sacado 1' in result:
            result.remove('Concentração Top 10 Sacados (R$)')
            idx = result.index('Sacado 1')
            result.insert(idx, 'Concentração Top 10 Sacados (R$)')

        group_pattern = re.compile(r'^\(([^)]+)\)\s*[\d>]')
        groups = {}
        for i, col in enumerate(result):
            match = group_pattern.match(col)
            if match:
                group = match.group(1)
                if group not in groups and group in result:
                    groups[group] = i

        for group, idx_ref in groups.items():
            if group in result:
                result.remove(group)
                result.insert(idx_ref - 1, group)

        # Mover 'PDD À Vencer' após último '(PDD Total)X-Y dias'
        if 'PDD À Vencer' in result:
            padrao_pdd = re.compile(r'^\(PDD Total\)\s*\d+\s*-\s*\d+\s*dias')
            indices_pdd = [i for i, col in enumerate(result) if padrao_pdd.match(col)]
            if indices_pdd:
                idx_destino = max(indices_pdd)
                result.remove('PDD À Vencer')
                result.insert(idx_destino, 'PDD À Vencer')

        return result

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
                print(name_file)
                df.columns = remove_suffix(df.columns)
                df = self.__days_column_processing(df)
                df = self.__rename_equiv_columns(df)
                df = self.__grouping_days_column(df)
                csv_dict[name_file] = df

        self.csv_dict = csv_dict

    def group_FIDCs(self, date):
        #todo:
            # ajeitar as funções que utilizam o self.csv_dict, porque eu to retornando
            # e algumas funções dependentes utilizam ele mas pode dar erro se eu não atualizar
            # calcular os dados que eles pediram

        column_names_1_dict = self.__selecting_columns_by_name()
        column_names_2_dict = self.__selecting_columns_by_regex() # {"ALFA": ["Coluna1", "Coluna2", ...], ...}

        column_names = {k: column_names_1_dict.get(k, []) + column_names_2_dict.get(k, [])
                     for k in column_names_1_dict.keys()}

        # depois mudar pq é ideal nem tudo tá usando o self.csv_dict

        self.csv_dict = self.__selecting_final_columns(column_names) # mantendo apenas as colunas de interesse
        self.csv_dict = self.__days_to_end_of_month()
        grouped_data = self.__group_by_date(date)

        column_names_2_list = pd.Series(chain.from_iterable(column_names_2_dict.values()))
        column_names_2_list = list(column_names_2_list.drop_duplicates())
        main_columns = list(self.equiv_columns.keys())

        final_order = self.__reorder_df_columns(main_columns, column_names_2_list)

        print(grouped_data.info())
        grouped_data = grouped_data[final_order]
        grouped_data.index.name = "FIDC"

        name = "FIDCS_" + str(date).replace("-", "_") + ".csv"
        PATH_OUT = "./grouped_data/" # TTIRAR ISSO DEPOIS

        grouped_data.to_csv(PATH_OUT + name, sep=';', encoding='utf-8-sig')


