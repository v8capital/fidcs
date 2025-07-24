from v8_fidcs.src.parser.fidc import FIDC
from v8_fidcs.src.others.logger import LogFIDC
from functools import reduce

import pandas as pd
import numpy as np

import yaml
import re
import os

pd.set_option('future.no_silent_downcasting', True)

PATH = os.path.join(os.getcwd(), 'YAMLs')

def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        dados = yaml.safe_load(f)
    return dados  # Retorna como dicionário: {coluna_final: [possibilidades]}

logger = LogFIDC()

class ExcelTransformer(object):

    def __init__(self, path, name):
        sheet_names =  pd.ExcelFile(path).sheet_names
        patterns_fidcs = read_yaml(os.path.join(PATH, "FIDCs.yaml"))
        type, pattern = self._check_name(name, patterns_fidcs)

        if len(sheet_names) > 1 and type in ["ORRAM", "MULTIASSET", "FIRMA"]:
            tables = []
            if len(sheet_names) > 3:
                logger.warning(f"Mais de 3 sheets encontrada no FIDC {name}, serão lidas as 3 primeiras.")
                sheet_names = sheet_names[:-1]  # remove a última planilha
            for sheet in sheet_names:
                df = pd.read_excel(path, sheet_name=sheet, header = None)
                df = df.T
                #print(df)
                df.reset_index(drop=True, inplace=True)
                tables.append(df)
            raw_table = tables.copy()
            table = pd.concat(tables, axis = 1)
            table = table.dropna(how='all')
        elif type == "SOLAR":
            raw_table = pd.read_excel(path, sheet_name = "Dados", header = None)
            table = raw_table.T
        elif len(sheet_names) > 1 and type not in ["ORRAM", "MULTIASSET"]:
            logger.warning(f"Mais de uma sheet encontrada no FIDC {name}, será lida apenas a primeira.")
            raw_table = pd.read_excel(path)
            table = raw_table.T
        else:
            raw_table = pd.read_excel(path)
            table = raw_table.T
        #self.table.to_csv("./PARSED/raw_" + name + ".csv", sep = ";",  encoding = "utf-8-sig")
        self.fidc = FIDC(table = table, raw_table = raw_table, name = name, type = type, pattern = pattern)
        logger.info(f"FIDC {self.fidc.name} da Gestora {self.fidc.type} carregado com sucesso.")

    # --------------------------------------------------------------------- #
    # PEQUENOS HELPERS (sem warnings)
    # --------------------------------------------------------------------- #
    def _extract_indexes_and_prepare(self, tbl, item_label="Item"):
        """Pull the index column, promote first row to header, drop original header row."""
        try:
            found = False
            m = 0
            for i in range(tbl.shape[0]):
                for j in range(tbl.shape[1]):
                    if tbl.iloc[i, j] == item_label:
                        idx = tbl.iloc[i + 1:, j]  # values below header row
                        m = i
                        found = True
                        break  # interrompe o loop interno
                if found:
                    break
            tbl.columns = tbl.iloc[m, :].infer_objects()
            tbl = tbl.drop(tbl.index[:m + 1])

            return tbl, idx
        except Exception as e:
            raise Exception(f"Erro ao extrair índices das datas do FIDC: {e}")

    def _standardize(self, tbl, idx, *, subset = "Item", drop_item=True, do_check=True, reset_index = True, multi_item = False):
        """Optional Item‑column NA cleanup, generic column checks, re‑index reset."""
        if drop_item:
            if multi_item:
                # Mantém apenas a primeira ocorrência da coluna específica
                mask = ~((tbl.columns == subset) & tbl.columns.duplicated(keep='first'))
                tbl = tbl.loc[:, mask]
            tbl = tbl.dropna(subset=[subset])
            idx = idx.dropna()
        if do_check:  # <- NEW FLAG
            tbl = self._check_columns(tbl)
        if reset_index:
            tbl.reset_index(drop=True, inplace=True)
        return tbl, idx

    def _set_index(self, tbl: pd.DataFrame, idx):
        """Define o índice sem disparar FutureWarning."""
        if not isinstance(idx, pd.Series):
            idx = pd.Series(idx)
        idx_clean = idx.infer_objects()
        tbl.index = pd.Index(idx_clean, dtype=idx_clean.dtype)

    # --------------------------------------------------------------------- #
    # YAML & COLUMN CHECKERS
    # --------------------------------------------------------------------- #
    def _check_name(self, name, patterns_fidcs):
        for manager_name, items in patterns_fidcs.items():
            itemskeys = set(key for d in items for key in d.keys())
            if 'FUNDS' in itemskeys:
                if name in items[1]['FUNDS']:
                    logger.info(f"Nome {name} encontrado na Gestora {manager_name}.")

                    type = manager_name
                    pattern = patterns_fidcs[manager_name][0]["COLUMNS"]
                    return type, pattern
            elif name == manager_name:
                logger.info(f"Nome {name} encontrado.")
                pattern = next(iter(patterns_fidcs[manager_name][0].values()))
                type = manager_name
                return type, pattern
        raise Exception(f"Nome {name} não encontrado no YAML de padrões de FIDCs.")


    def _check_columns(self, data):
        # acredito que isso será padrão para todos
        expected_columns = self.fidc.pattern.copy()
        data.columns = data.columns.astype(str)
        columns = list(data.columns)
        #print("COLUNAS PRESENTES:", columns)
        for item in expected_columns:
            expected_column_name, type_column = next(iter(item.items()))
            #print(columns)
            matched = any(
                re.fullmatch(expected_column_name, col)
                for col in columns
            )

            if not matched:
                logger.warning(f" COLUNA NO PADRÃO(REGEX) {expected_column_name} ESPERADA NÃO ENCONTRADA NO FIDC {self.fidc.name}.")
                # se uma coluna que precisamos não for encontrada, vamos parar o código
                # mas como algumas colunas são opcionais, vamos apenas dar um warning, depois fazer um check com atributo "important"
                # raise Exception(f"Coluna esperada '{expected_column_name}' não encontrada no FIDC {self.fidc.name}.")

        for col in columns:
            matched = False
            for i, item in enumerate(expected_columns):
                pattern, value = next(iter(item.items()))
                #print(pattern, col)
                if re.fullmatch(pattern, col, flags=re.IGNORECASE):
                    matched = True
                    if value == 'remove':
                        idx = data.columns.get_loc(col)
                        if isinstance(idx, (np.ndarray, list)):

                            pos = np.where(idx)[0] # apagando pelo final
                            data = data.iloc[:, [j for j in range(data.shape[1]) if j != pos[1]]]

                            #apagando sempre o segundo, mas pode ser o último
                            logger.debug(f" Coluna {col} removida, devido ao atributo 'remove'")
                            # apagando pela posição inves de nome
                        else:
                            data = data.drop(col, axis=1)
                            logger.debug(f" Coluna {col} removida, devido ao atributo 'remove'")
                    if value == "removerepeat":
                        idx = data.columns.get_loc(col)
                        # tratamento de erro besta, por causa da merda do solar que tem 1 exceção e não quero mexer no q ta funcionando
                        pos = np.where(idx)[0]  # apagando pelo começo
                        data = data.iloc[:, [j for j in range(data.shape[1]) if j != pos[0]]]
                        logger.debug(f" Coluna {col} removida, devido ao atributo 'remove'")

                    # Consome o item da lista expected_columns
                    if value not in ('repeat', 'repeatpercent', 'repeatmez', 'repeatsen'):
                        del expected_columns[i]
                    break  # encontrou correspondência, não precisa verificar os demais

            if not matched:
                idx = data.columns.get_loc(col)

                if isinstance(idx, (np.ndarray, list)):
                    pos = np.where(idx)[0]  # apagando pelo final
                    data = data.iloc[:, [j for j in range(data.shape[1]) if j != pos[1]]]
                else:
                    data = data.drop(col, axis = 1)
                logger.debug(f" Coluna {col} removida, pois não era prevista no padrão")
                # coluna nova a gente só dá warning, não vamos parar o código
        #print(data)
        return data

    # --------------------------------------------------------------------- #
    # TRANSFORM
    # --------------------------------------------------------------------- #
    def transform_table(self, path):
        # para não modificar a table o tempo inteiro vou criar uma cópia e modificar ela
        table_copy = self.fidc.table.copy()
        fidc_type = self.fidc.type

        # -------- TERCON -------------------------------------------------- #
        if fidc_type == "TERCON":
            if table_copy.index.equals(pd.RangeIndex(len(table_copy))):
                table_copy.index = table_copy.iloc[:, 0] # se não tiver index, vamos usar a primeira coluna como index
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, _ = self._standardize(table_copy, indexes, drop_item=False, reset_index=False)

            table_copy = table_copy[
                ~(pd.to_datetime(indexes.index, errors="coerce")).isna()
            ]

        # -------- M8 ------------------------------------------------------ #
        elif fidc_type == "M8":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes)
            date = self.fidc.convert_date(indexes)
            self._set_index(table_copy, date)

        # -------- ORRAM --------------------------------------------------- #
        elif fidc_type == "ORRAM":
            def _prep_sheet(sheet):
                sheet, idx = self._extract_indexes_and_prepare(sheet, "Item")
                sheet, _ = self._standardize(sheet, idx, do_check=False)  # <- skip check here
                return sheet

            processed = [
                _prep_sheet(s.dropna(how="all"))  # ignore empty sheets
                for s in self.fidc.raw_table
            ]

            table_copy = reduce(
                lambda l, r: pd.merge(l, r, on="Item", how="inner"),
                processed,
            )
            indexes = table_copy["Item"]
            table_copy = self._check_columns(table_copy)

            if self.fidc.name == "SIFRANPP":
                table_copy = self.fidc.sum_columns(table_copy, "mez")
                table_copy["PL Sênior"] = np.nan
                table_copy = self.fidc.sum_columns(table_copy, "sen")
            table_copy = self.fidc.correct_assets(table_copy)
            self._set_index(table_copy, indexes)

        # -------- ALFA ---------------------------------------------------- #
        elif fidc_type == "ALFA":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes)
            self._set_index(table_copy, indexes)

            table_copy = self.fidc.create_10_biggests(table_copy, "Sacado")
            table_copy = self.fidc.create_10_biggests(table_copy, "Cedente")

        # -------- BARCELONA ----------------------------------------------- #
        elif fidc_type == "BARCELONA":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes)
            table_copy = self.fidc.correct_assets(table_copy)
            self._set_index(table_copy, indexes)

        # -------- MULTIASSET ---------------------------------------------- #
        elif fidc_type == "MULTIASSET":
            def _prep_sheet(sheet):
                sheet, idx = self._extract_indexes_and_prepare(sheet, "Item")
                sheet, _ = self._standardize(sheet, idx, do_check=False)  # <- skip check here
                return sheet

            processed = [
                _prep_sheet(s.dropna(how="all"))  # ignore empty sheets
                for s in self.fidc.raw_table
            ]
            table_copy = reduce(
                lambda l, r: pd.merge(l, r, on="Item", how="inner"),
                processed,
            )

            indexes = table_copy["Item"]

            table_copy = self._check_columns(table_copy)
            table_copy = self.fidc.correct_assets(table_copy)
            self._set_index(table_copy, indexes)

        # -------- MULTIPLIKE ---------------------------------------------- #
        elif fidc_type == "MULTIPLIKE":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes)

            table_copy = self.fidc.create_10_biggests(table_copy, "Sacado")
            table_copy = self.fidc.create_10_biggests(table_copy, "Cedente")
            table_copy.index = indexes
            table_copy = table_copy[
                ~(pd.to_datetime(table_copy.index, errors="coerce")).isna()
            ]

        # -------- ONE7 ---------------------------------------------------- #
        elif fidc_type == "ONE7":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, _ = self._standardize(table_copy, indexes, drop_item=False)
            table_copy = self.fidc.create_10_biggests(table_copy, "Sacado")
            table_copy = self.fidc.create_10_biggests(table_copy, "Cedente")
            date = self.fidc.convert_date(indexes)
            self._set_index(table_copy, date)

        # -------- VALOREM ------------------------------------------------- #
        elif fidc_type == "VALOREM":
            table_copy, indexes = self._extract_indexes_and_prepare(
                table_copy, "Descrição/Período"
            )
            table_copy, indexes = self._standardize(table_copy, indexes, subset = "Descrição/Período")
            table_copy = self.fidc.clean_column_names(table_copy)
            self._set_index(table_copy, indexes)

        # -------- SOLAR --------------------------------------------------- #
        elif fidc_type == "SOLAR":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes)
            self._set_index(table_copy, indexes)
            table_copy = self.fidc.correct_values(table_copy)
            table_copy = self.fidc.absolute_values(table_copy)
            table_copy = self.fidc.correct_percentages(
                table_copy, "PL Total Classe (R$ mil)"
            )
            table_copy = self.fidc.create_10_biggests(table_copy, "Sacado")
            table_copy = self.fidc.create_10_biggests(table_copy, "Cedente")

        # -------- ONIX ---------------------------------------------------- #
        elif fidc_type == "ONIXOLD":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes, multi_item = True)
            self._set_index(table_copy, indexes)

            table_copy = table_copy[
                ~(pd.to_datetime(table_copy.index, errors="coerce")).isna()
            ]
            table_copy = self.fidc.correct_assets(table_copy)
            table_copy = self.fidc.create_10_biggests(table_copy, "Sacado")
            table_copy = self.fidc.create_10_biggests(table_copy, "Cedente")

            self.fidc.name = "ONIX"

        # -------- RAIZES -------------------------------------------------- #
        elif fidc_type == "RAIZES":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes, multi_item=True)
            self._set_index(table_copy, indexes)
            table_copy = self.fidc.create_10_biggests(table_copy, "Sacado")
            table_copy = self.fidc.create_10_biggests(table_copy, "Cedente")

        # -------- FIRMA --------------------------------------------------- #
        elif fidc_type == "FIRMA":
            def _prep_sheet(sheet):
                sheet, idx = self._extract_indexes_and_prepare(sheet, "Item")
                sheet, _ = self._standardize(sheet, idx, do_check=False)  # <- skip check here
                return sheet

            processed = [
                _prep_sheet(s.dropna(how="all"))  # ignore empty sheets
                for s in self.fidc.raw_table
            ]

            table_copy = reduce(
                lambda l, r: pd.merge(l, r, on="Item", how="inner"),
                processed,
            )
            indexes = table_copy["Item"]
            table_copy = self._check_columns(table_copy)
            self._set_index(table_copy, indexes)

        # -------- RNX ----------------------------------------------------- #
        elif fidc_type == "RNX":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes)
            date = self.fidc.convert_date(indexes)
            self._set_index(table_copy, date)
            table_copy = self.fidc.correct_column_names(table_copy)
            table_copy = self.fidc.create_10_biggests(table_copy, "Sacado")
            table_copy = self.fidc.create_10_biggests(table_copy, "Cedente")

        # -------- SABIA --------------------------------------------------- #
        elif fidc_type == "SABIA":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes)
            date = self.fidc.convert_date(indexes)
            self._set_index(table_copy, date)
            table_copy = self.fidc.create_10_biggests(table_copy, "Sacado")
            table_copy = self.fidc.create_10_biggests(table_copy, "Cedente")

        # -------- OXSS ---------------------------------------------------- #
        elif fidc_type == "OXSS":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, _ = self._standardize(table_copy, indexes, drop_item=False, reset_index=False)

            self.fidc.name = "IOXII"

        # -------- IOSAN --------------------------------------------------- #
        elif fidc_type == "IOSAN":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy, "FIDC")
            table_copy, indexes = self._standardize(table_copy, indexes, subset = "FIDC")
            table_copy = self.fidc.correct_percentages(
                table_copy, "PL Total"
            )

            table_copy = self.fidc.correct_column_names(table_copy)

            self._set_index(table_copy, indexes)

        # ----------------------------------------------------------------- #
        # Salvar / atualizar instância
        # ----------------------------------------------------------------- #
        table_copy = self.fidc.convert_to_double(table_copy)
        table_copy = self.fidc._days_to_start_of_month(table_copy)
        table_copy.index.name = "Data"
        #file_name = f"{self.fidc.name}" + "_" + date + ".csv"

        #path_out = os.path.join(path, file_name)
        table_copy.to_csv(path, sep = ";",  encoding = "utf-8-sig")

        self.fidc.table = table_copy.copy()
        logger.info(f"Transformação finalizada e CSV salvo em {path}")

        return table_copy

    # --------------------------------------------------------------------- #
    # DOWNLOAD FIDC_LIST
    # --------------------------------------------------------------------- #




# apenas o check columns e o check name devem ser mantidos na classe
# todos os outros irão para a classe FIDC, junto do self.pattern, self.table e afins