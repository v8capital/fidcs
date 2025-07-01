import pandas as pd
import numpy as np

import yaml
import logging
import warnings
import re

PATH = './YAMLs/'
def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        dados = yaml.safe_load(f)

    return dados  # Retorna como dicionário: {coluna_final: [possibilidades]}

class Excel_Transformer(object):

    def __correct_values(self, data):
        # multiplica todas as colunas no YAML que possuem valor R1000 por 1000
        r1000_cols = [k for d in self.pattern for k, v in d.items() if v == "valueR1000"]
        # Seleciona as colunas presentes no DataFrame
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in r1000_cols)]
        # Multiplica por 1000
        data[cols_to_multiply] = data[cols_to_multiply] * 1000
        return data

    def __correct_percentages(self, data, target):
        percent_cols = [k for d in self.pattern for k, v in d.items() if v == "repeatpercent"]
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in percent_cols)]
        print(cols_to_multiply)
        for col in cols_to_multiply:
            idxs = [i for i, c in enumerate(data.columns) if c == col]
            for idx in idxs:
                data.iloc[:, idx] = data.iloc[:, idx] * data[target]
        return data


    def __clean_column_names(self, data):
        novas_colunas = []
        for nome in data.columns:
            texto_modificado = re.sub(r'(Cedente\s+\d+)\s+\(Vlr Presente-PDD\)', r'\1', nome)
            texto_modificado = re.sub(r'(Sacado\s+\d+)\s+\(Vlr Presente-PDD\)', r'\1', texto_modificado)
            novas_colunas.append(texto_modificado)
        data.columns = novas_colunas
        return data

    def __remove_rows_before(self, indexes, date_limit):
        date_limit = pd.to_datetime(date_limit)

        dates = pd.to_datetime(indexes, format='%d/%m/%Y', errors = "coerce")

        rows = dates >= date_limit

        return indexes[rows]

    def __create_10_biggests(self, data, target):
        column_name = "Concentrações " + target + "s (R$)"
        columns_to_sum = [target + f' {i}' for i in range(1, 11)]
        data[column_name] = data[columns_to_sum].sum(axis=1)
        return data

    def __correct_assets(self, data):
        assets = list({k for d in self.pattern for k, v in d.items() if v == "asset"})
        dc = {k for d in self.pattern for k, v in d.items() if v == "dc"}
        #print(data.columns)

        assets, dc = [
            [val for val in data.columns if any(re.fullmatch(rx, val) for rx in padroes)]
            for padroes in (assets, dc)
        ]

        print(assets)

        print(dc)
        data[assets] = data[assets].multiply(data[dc[0]], axis = 0)
        return data

    def __create_total_liquid(self, data):
        liquid_days_p = list({k for d in self.pattern for k, v in d.items() if v == "liquids"})
        # print(data.columns)

        liquid_days = [val for val in data.columns if any(re.fullmatch(rx, val) for rx in liquid_days_p)]
        data["Liquidado Total(R$)"] = data[liquid_days].sum(axis=1)

    def __convert_date(self, arr):
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

    def __check_name(self, name, patterns_fidcs):
        print(name)
        for manager_name, items in patterns_fidcs.items():
            itemskeys = set(key for d in items for key in d.keys())
            if 'FUNDS' in itemskeys:
                if name in items[1]['FUNDS']:
                    print('Nome encontrado no FUNDS')

                    self.type = manager_name
                    self.pattern = patterns_fidcs[manager_name][0]["COLUMNS"]
                    return name
            elif name == manager_name:
                print('Nome encontrado no MANAGER')
                self.pattern = next(iter(patterns_fidcs[manager_name][0].values()))
                self.type = manager_name
                return name
            else:
                print('Nome não encontrado')

        # caso não encontre, retorna excessão
        raise Exception("NOME não encontrado")

    def __check_columns(self, data):
        # acredito que isso será padrão para todos
        expected_columns = self.pattern.copy()
        data.columns = data.columns.astype(str)
        columns = list(data.columns)
        #print("COLUNAS PRESENTES:", columns)
        for item in expected_columns:
            expected_column_name, type_column = next(iter(item.items()))
            #print(columns)
            matched = any(
                re.fullmatch(expected_column_name, col, flags=re.IGNORECASE)
                for col in columns
            )

            if not matched:
                print("⚠️ COLUNA ESPERADA NÃO ENCONTRADA:", expected_column_name)

        for col in columns:
            matched = False
            for i, item in enumerate(expected_columns):
                pattern, value = next(iter(item.items()))
                #print(pattern, col)
                if re.fullmatch(pattern, col, flags=re.IGNORECASE):
                    matched = True
                    if value == 'empty':
                        idx = data.columns.get_loc(col)
                        if isinstance(idx, (np.ndarray, list)):

                            pos = np.where(idx)[0] # apagando pelo final
                            data = data.iloc[:, [j for j in range(data.shape[1]) if j != pos[1]]]

                            #apagando sempre o segundo, mas pode ser o último
                            print("⚠️ COLUNA1 PREVISTA MAS COM VALOR 'empty' REMOVIDA:", col)
                            # apagando pela posição inves de nome
                        else:
                            data = data.drop(col, axis=1)
                            print("⚠️ COLUNA2 PREVISTA MAS COM VALOR 'empty' REMOVIDA:", col)
                    if value == "solarremove":
                        idx = data.columns.get_loc(col)
                        # tratamento de erro besta, por causa da merda do solar que tem 1 exceção e não quero mexer no q ta funcionando
                        pos = np.where(idx)[0]  # apagando pelo começo
                        data = data.iloc[:, [j for j in range(data.shape[1]) if j != pos[0]]]
                        print("⚠️ COLUNA1 PREVISTA MAS COM VALOR 'empty' REMOVIDA:", col)

                    # Consome o item da lista expected_columns
                    if value != 'repeat' and value != 'repeatpercent':
                        del expected_columns[i]
                    break  # encontrou correspondência, não precisa verificar os demais

            if not matched:
                idx = data.columns.get_loc(col)

                if isinstance(idx, (np.ndarray, list)):
                    pos = np.where(idx)[0]  # apagando pelo final
                    data = data.iloc[:, [j for j in range(data.shape[1]) if j != pos[1]]]
                else:
                    data = data.drop(col, axis = 1)
                print("⚠️ COLUNA NÃO PREVISTA NO PADRÃO REMOVIDA:", col)
        #print(data)
        return data

    def read_excel(self, path, name):
        sheet_names =  pd.ExcelFile(path).sheet_names
        patterns_fidcs = read_yaml(PATH + 'FIDCs.yaml')

        if len(sheet_names) > 1:
            #print("OPAAAA")
            tables = []
            for sheet in sheet_names:
                df = pd.read_excel(path, sheet_name=sheet, header = None)
                #print(df)
                df = df.T
                #print(df)
                df.reset_index(drop=True, inplace=True)
                tables.append(df)
            self.raw_table = pd.concat(tables, axis = 1)
            self.raw_table = self.raw_table.dropna(how='all')
            self.table = self.raw_table.copy()
        else:
            self.raw_table = pd.read_excel(path)
            self.table = self.raw_table.T

        self.name = self.__check_name(name, patterns_fidcs)


    def transform_table(self):
        # para não modificar a table o tempo inteiro vou criar uma cópia e modificar ela
        table_copy = self.table.copy()
        #print(self.type)

        # todo: melhorar essa parte do index depois
        # todo: tirar a repetição de código dentro dos IFs
        # respçver warning do indexes

        if self.type == "TERCON":
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Item":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"

            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])

            table_copy = self.__check_columns(table_copy)

            table_copy = table_copy[
                ~(pd.to_datetime(indexes.index, errors='coerce')).isna()
            ] # ver isso aq dps
            # apagando os %PL e a Variação
        elif self.type == "M8":
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Item":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"

            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])

            table_copy = table_copy.dropna(subset=["Item"])

            indexes = indexes.dropna()
            table_copy = self.__check_columns(table_copy)
            table_copy.reset_index(drop=True, inplace=True)

            #print("teste")
            date = self.__convert_date(indexes) # mandando a coluna ITEM
            table_copy.index = pd.Index(date) # talvez colocar name = "Data"

        elif self.type == "ORRAM":
            print(table_copy)
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Item":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"

            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])
            table_copy = table_copy.dropna(subset=["Item"])

            indexes = indexes.dropna()
            table_copy = self.__check_columns(table_copy)

            table_copy = self.__correct_assets(table_copy)
            table_copy.index = indexes
            #table_copy = self.create_total_liquid()
        elif self.type == "PATTERN1":
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Item":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"

            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])

            table_copy = table_copy.dropna(subset=["Item"])

            indexes = indexes.dropna()

            table_copy = self.__check_columns(table_copy)
            table_copy.reset_index(drop=True, inplace=True)

            table_copy.index = pd.Index(indexes) # talvez colocar name = "Data"

            # talvez mudar o nome depois
            table_copy = self.__create_10_biggests(table_copy, "Sacado")
            table_copy = self.__create_10_biggests(table_copy, "Cedente")

        elif self.type == "BARCELONA":
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Item":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"

            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])

            table_copy = table_copy.dropna(subset=["Item"])

            indexes = indexes.dropna()

            table_copy = self.__check_columns(table_copy)
            table_copy.reset_index(drop=True, inplace=True)

            table_copy = self.__correct_assets(table_copy)

            table_copy.index = pd.Index(indexes) # talvez colocar name = "Data"

            # talvez mudar o nome depois
        elif self.type == "MULTIASSET":
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Item":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"


            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])

            table_copy = table_copy.dropna(subset=["Item"])
            #podemelhorar aqui, mas por enquanto fica assim

            indexes = self.__remove_rows_before(indexes, "2021-01-01")
            #print(indexes)

            indexes = indexes.dropna()
            table_copy = self.__check_columns(table_copy)

            table_copy = self.__correct_assets(table_copy)
            table_copy.index = indexes

        elif self.type == "MULTIPLIKE":
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Item":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"

            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])

            table_copy = table_copy.dropna(subset=["Item"])
            # podemelhorar aqui, mas por enquanto fica assim
            #print(indexes)

            indexes = indexes.dropna()
            #print(table_copy.columns)
            table_copy = self.__check_columns(table_copy)

            table_copy = self.__create_10_biggests(table_copy, "Sacado")
            table_copy = self.__create_10_biggests(table_copy, "Cedente")

            table_copy.index = indexes

            table_copy = table_copy[
                ~(pd.to_datetime(table_copy.index, errors='coerce')).isna()
            ]

        elif self.type == "ONE7":
            #TODO AJEITAR O YAML QUE O GPT TÁ TROLL
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Item":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"

            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])

            # podemelhorar aqui, mas por enquanto fica assim
            #print(indexes)

            #print(table_copy.columns)

            table_copy = self.__check_columns(table_copy)

            table_copy = self.__create_10_biggests(table_copy, "Sacado")
            table_copy = self.__create_10_biggests(table_copy, "Cedente")
            #print(table_copy)
            date = self.__convert_date(indexes)  # mandando a coluna ITEM
            table_copy.index = pd.Index(date)

        elif self.type == "VALOREM":
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Descrição/Período":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"
            #print(table_copy)
            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])

            table_copy = table_copy.dropna(subset=["Descrição/Período"])

            indexes = indexes.dropna()

            table_copy = self.__check_columns(table_copy)
            table_copy = self.__clean_column_names(table_copy)
            table_copy.reset_index(drop=True, inplace=True)

            table_copy.index = pd.Index(indexes) # talvez colocar name = "Data"
        elif self.type == "SOLAR":
            #print(table_copy)
            for i in range(table_copy.shape[1]):
                if table_copy.iloc[0, i] == "Item":
                    indexes = table_copy.iloc[1:, i]  # pegando a coluna "itens"
            #print(table_copy)
            table_copy.columns = table_copy.iloc[0, :]  # pegando as colunas, colocando no lugar e apagando dps
            table_copy = table_copy.drop(table_copy.index[0])

            table_copy = table_copy.dropna(subset=["Item"])

            indexes = indexes.dropna()

            table_copy = self.__check_columns(table_copy)
            table_copy.reset_index(drop=True, inplace=True)

            table_copy.index = pd.Index(indexes) # talvez colocar name = "Data"

            table_copy = self.__correct_values(table_copy)
            table_copy = self.__correct_percentages(table_copy, "PL Total Classe (R$ mil)")

            table_copy = self.__create_10_biggests(table_copy, "Sacado")
            table_copy = self.__create_10_biggests(table_copy, "Cedente")

        #print(table_copy.columns)
        path_out = "./out/" + self.name + ".csv" # tem q colocar a data depois
        table_copy.to_csv(path_out, sep = ";",  encoding = "utf-8-sig")
        # so pra ver

        self.table = table_copy.copy()

        #print(table_copy)
        # começar tirando os unnamed