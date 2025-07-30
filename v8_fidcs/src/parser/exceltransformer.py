from v8_utilities.paths import PathV8
from v8_fidcs.src.parser.fidc import FIDC
from v8_fidcs.src.others.logger import LogFIDC
from v8_utilities.yaml_functions import load_yaml
from v8_utilities.anbima_calendar import Calendar

from typing import Tuple, Dict, List, Any

from functools import reduce

import pandas as pd
import numpy as np

import re
import os

pd.set_option('future.no_silent_downcasting', True)

# Caminho do arquivo (onde o script está salvo)
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)

PATH = os.path.join(script_dir, "..","..", "..", 'yamls')

logger = LogFIDC()

class ExcelTransformer(object):

    def __init__(self, path_handle, calendar_handle, path_read, path_save, name):
        self.path_handle = path_handle
        self.calendar_handle = calendar_handle
        self.path_read = path_read
        self.path_save = path_save

        sheet_names =  pd.ExcelFile(self.path_read).sheet_names
        patterns_fidcs = load_yaml(os.path.join(PATH, "fidcs.yaml"))
        type, pattern = self._check_name(name, patterns_fidcs)

        if len(sheet_names) > 1 and type in ["ORRAM", "MULTIASSET", "FIRMA"]:
            tables = []
            if len(sheet_names) > 3:
                logger.warning(f"Mais de 3 sheets encontrada no FIDC {name}, serão lidas as 3 primeiras.")
                sheet_names = sheet_names[:-1]  # remove a última planilha
            for sheet in sheet_names:
                df = pd.read_excel(self.path_read, sheet_name=sheet, header = None)
                df = df.T
                #print(df)
                df.reset_index(drop=True, inplace=True)
                tables.append(df)
            raw_table = tables.copy()
            table = pd.concat(tables, axis = 1)
            table = table.dropna(how='all')
        elif type == "SOLAR":
            raw_table = pd.read_excel(self.path_read, sheet_name = "Dados", header = None)
            table = raw_table.T
        elif len(sheet_names) > 1 and type not in ["ORRAM", "MULTIASSET"]:
            logger.warning(f"Mais de uma sheet encontrada no FIDC {name}, será lida apenas a primeira.")
            raw_table = pd.read_excel(self.path_read)
            table = raw_table.T
        else:
            raw_table = pd.read_excel(self.path_read)
            table = raw_table.T

        #self.table.to_csv("./PARSED/raw_" + name + ".csv", sep = ";",  encoding = "utf-8-sig")
        self.fidc = FIDC(path_handle = self.path_handle, calendar_handle = self.calendar_handle, table = table, raw_table = raw_table, name = name, type = type, pattern = pattern)
        logger.info(f"FIDC {self.fidc.name} da Gestora {self.fidc.type} carregado com sucesso.")

    # --------------------------------------------------------------------- #
    # PEQUENOS HELPERS (sem warnings)
    # --------------------------------------------------------------------- #
    def _extract_indexes_and_prepare(self, tbl: pd.DataFrame, item_label: str = "Item") -> Tuple[
        pd.DataFrame, pd.Series]:
        """
        Extrai a coluna de índice a partir do rótulo `item_label`, promove a primeira linha encontrada como cabeçalho,
        e remove as linhas originais do cabeçalho.

        Passos:
            - Busca a célula que contém `item_label`.
            - Usa a coluna abaixo dessa célula como índice.
            - Promove a linha onde `item_label` foi encontrado como cabeçalho das colunas.
            - Remove as linhas acima e inclusive a linha do cabeçalho antigo.

        Args:
            tbl (pd.DataFrame): Tabela original lida do Excel.
            item_label (str, optional): Texto que identifica a coluna índice. Default é "Item".

        Returns:
            Tuple[pd.DataFrame, pd.Series]:
                - DataFrame ajustado com o cabeçalho correto e linhas desconsideradas removidas.
                - Série contendo os valores da coluna índice extraída.

        Raises:
            Exception: Se ocorrer erro na extração, a exceção é relançada com mensagem descritiva.
        """
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
            logger.error(f"Erro ao extrair índices das datas do FIDC: {e}")
            raise Exception(f"Erro ao extrair índices das datas do FIDC: {e}")

    def _standardize(
            self,
            tbl: pd.DataFrame,
            idx: pd.Series,
            *,
            subset: str = "Item",
            drop_item: bool = True,
            do_check: bool = True,
            reset_index: bool = True,
            multi_item: bool = False
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Realiza limpeza opcional na coluna `subset` (ex: "Item"), checagem genérica de colunas,
        e reset do índice do DataFrame.

        Passos:
            - Remove linhas com NA na coluna `subset` e no índice, caso `drop_item` seja True.
            - Em caso de colunas duplicadas com nome igual a `subset` e `multi_item` True,
              mantém somente a primeira ocorrência.
            - Executa checagem das colunas via métdo `_check_columns` se `do_check` for True.
            - Reseta o índice do DataFrame se `reset_index` for True.

        Args:
            tbl (pd.DataFrame): DataFrame a ser tratado.
            idx (pd.Series): Série correspondente ao índice ou coluna de referência.
            subset (str, optional): Nome da coluna a ser usada para filtragem. Default é "Item".
            drop_item (bool, optional): Se True, remove linhas com NA em `subset` e índice. Default é True.
            do_check (bool, optional): Se True, chama o métdo `_check_columns` para validar as colunas. Default é True.
            reset_index (bool, optional): Se True, reseta o índice do DataFrame. Default é True.
            multi_item (bool, optional): Se True, trata colunas duplicadas em `subset` mantendo a primeira ocorrência. Default é False.

        Returns:
            Tuple[pd.DataFrame, pd.Series]: DataFrame tratado e índice atualizado.
        """
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
    def _check_name(
            self,
            name: str,
            patterns_fidcs: Dict[str, List[Dict[str, Any]]]
    ) -> Tuple[str, Any]:
        """
        Verifica se um nome está presente nas definições de padrões dos FIDCs e retorna seu tipo e padrão.

        Busca o `name` dentro do dicionário `patterns_fidcs` que contém gestores e seus respectivos padrões.
        - Se o nome está listado dentro da chave 'FUNDS' de um gestor, retorna o nome do gestor e seu padrão.
        - Se o nome coincide diretamente com o nome do gestor, retorna o padrão do gestor.

        Args:
            name (str): Nome a ser verificado.
            patterns_fidcs (Dict[str, List[Dict[str, Any]]]): Dicionário contendo os padrões dos FIDCs.

        Returns:
            Tuple[str, Any]: Tupla contendo o tipo (nome do gestor) e o padrão associado.

        Raises:
            Exception: Se o nome não for encontrado no YAML de padrões.
        """
        for manager_name, items in patterns_fidcs.items():
            itemskeys = set(key for d in items for key in d.keys())
            if 'FUNDS' in itemskeys:
                if name in items[1]['FUNDS']:
                    logger.info(f"Nome {name} encontrado na Gestora {manager_name}.")
                    type_ = manager_name
                    pattern = patterns_fidcs[manager_name][0]["COLUMNS"]
                    return type_, pattern
            elif name == manager_name:
                logger.info(f"Nome {name} encontrado.")
                pattern = next(iter(patterns_fidcs[manager_name][0].values()))
                type_ = manager_name
                return type_, pattern
        logger.error(f"Nome {name} não encontrado no YAML de padrões de FIDCs.")
        raise Exception(f"Nome {name} não encontrado no YAML de padrões de FIDCs.")

    def _check_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica e ajusta as colunas do DataFrame com base no padrão definido para o FIDC.

        Funcionalidades:
            - Confirma se colunas esperadas estão presentes, emitindo warning caso contrário.
            - Remove colunas que tenham o atributo 'remove' ou 'removerepeat' no padrão.
            - Remove colunas não previstas no padrão, emitindo debug.
            - Colunas marcadas como repetidas (repeat, repeatpercent, repeatmez, repeatsen) não removem o padrão esperado.

        Args:
            data (pd.DataFrame): DataFrame com colunas a serem verificadas e ajustadas.

        Returns:
            pd.DataFrame: DataFrame com colunas ajustadas conforme o padrão, removendo as inválidas.
        """
        expected_columns = self.fidc.pattern.copy()
        data.columns = data.columns.astype(str)
        columns = list(data.columns)

        # Verificação das colunas esperadas
        for item in expected_columns:
            expected_column_name, type_column = next(iter(item.items()))
            matched = any(re.fullmatch(expected_column_name, col) for col in columns)
            if not matched:
                logger.debug(
                    f"COLUNA NO PADRÃO(REGEX) {expected_column_name} ESPERADA NÃO ENCONTRADA NO FIDC {self.fidc.name}."
                )

        # Verificação e remoção das colunas extras ou com atributo de remoção
        for col in columns:
            matched = False
            for i, item in enumerate(expected_columns):
                pattern, value = next(iter(item.items()))
                if re.fullmatch(pattern, col, flags=re.IGNORECASE):
                    matched = True
                    if value == 'remove':
                        idx = data.columns.get_loc(col)
                        if isinstance(idx, (np.ndarray, list)):
                            pos = np.where(idx)[0]  # apagando pelo final
                            data = data.iloc[:, [j for j in range(data.shape[1]) if j != pos[1]]]
                        else:
                            data = data.drop(col, axis=1)
                        logger.debug(f"Coluna {col} removida, devido ao atributo 'remove'")
                    elif value == "removerepeat":
                        idx = data.columns.get_loc(col)
                        pos = np.where(idx)[0]  # apagando pelo começo
                        data = data.iloc[:, [j for j in range(data.shape[1]) if j != pos[0]]]
                        logger.debug(f"Coluna {col} removida, devido ao atributo 'remove'")

                    # Remove da lista expected_columns se não for um tipo repetido
                    if value not in ('repeat', 'repeatpercent', 'repeatmez', 'repeatsen'):
                        del expected_columns[i]
                    break

            if not matched:
                idx = data.columns.get_loc(col)
                if isinstance(idx, (np.ndarray, list)):
                    pos = np.where(idx)[0]
                    data = data.iloc[:, [j for j in range(data.shape[1]) if j != pos[1]]]
                else:
                    data = data.drop(col, axis=1)
                logger.debug(f"Coluna {col} removida, pois não era prevista no padrão")

        return data

    # --------------------------------------------------------------------- #
    # TRANSFORM
    # --------------------------------------------------------------------- #
    def transform_table(self) -> pd.DataFrame:
        """
            Realiza a transformação da tabela Excel conforme o tipo do FIDC, aplicando diversos tratamentos específicos
            para cada tipo, e salva o resultado final em CSV no caminho especificado.

            O processamento inclui:
                - Extração e preparação dos índices.
                - Padronização da tabela.
                - Ajustes específicos por tipo (ex: TERCON, M8, ORRAM, ALFA, SOLAR, ONIXOLD, etc.).
                - Correções de colunas, valores, percentuais, ativos e criação de somas específicas.
                - Conversão dos dados para float64.
                - Ajuste dos índices para o início do mês.
                - Salvamento do CSV final.

            Returns:
                pd.DataFrame: DataFrame transformado e salvo no arquivo CSV.
            """
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

            if self.fidc.name == "IOXII":
                self.fidc.name = "IOXII(OXSS)"
            elif self.fidc.name == "MULTIASSET(TERCON)":
                self.fidc.name = "MULTIASSET"
            elif self.fidc.name == "DCASH":
                self.fidc.name = "DCASH(MATRIZ)"
            elif self.fidc.name == "MATRIZ":
                self.fidc.name = "DCASH(MATRIZ)"

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
            self.fidc.name = "FLUXASSET(ALFA)"


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

            self.fidc.name = "IOXII(OXSS)"

        # -------- IOSAN --------------------------------------------------- #
        elif fidc_type == "IOXI(IOSAN)":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy, "FIDC")
            table_copy, indexes = self._standardize(table_copy, indexes, subset = "FIDC")
            table_copy = self.fidc.correct_percentages(
                table_copy, "PL Total"
            )
            table_copy = self.fidc.correct_column_names(table_copy)
            self._set_index(table_copy, indexes)
            self.fidc.name = "IOXI(IOSAN)"

        # -------- INTERBANK ------------------------------------------------ #
        elif fidc_type == "INTERBANK":
            table_copy, indexes = self._extract_indexes_and_prepare(table_copy)
            table_copy, indexes = self._standardize(table_copy, indexes, drop_item=False, reset_index=False)
            table_copy = table_copy[
                ~(pd.to_datetime(indexes.index, errors="coerce")).isna()
            ]
            table_copy = self.fidc.rename_columns(table_copy, ["10 Maiores Cedentes (R$)", "Cedente 1", "10 Maiores Sacados (R$)", "Sacado 1", "Antecipado", "D0", "Entre D1-D5", "Entre D6-D15", "Entre D16-D30", "Acima de D30"])

        # ----------------------------------------------------------------- #
        # Salvar / atualizar instância
        # ----------------------------------------------------------------- #
        table_copy = self.fidc.convert_to_double(table_copy)
        table_copy = self.fidc._days_to_start_of_month(table_copy)
        table_copy.index.name = "Data"
        #file_name = f"{self.fidc.name}" + "_" + date + ".csv"

        #path_out = os.path.join(path, file_name)
        table_copy.to_csv(self.path_save, sep = ";",  encoding = "utf-8-sig")

        self.fidc.table = table_copy.copy()
        logger.info(f"Transformação finalizada e CSV salvo em {self.path_save}")

        return table_copy

# apenas o check columns e o check name devem ser mantidos na classe
# todos os outros irão para a classe FIDC, junto do self.pattern, self.table e afins