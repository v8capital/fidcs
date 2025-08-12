import pandas as pd
import numpy as np

from typing import Dict, List, Optional, Tuple
from itertools import chain
from v8_fidcs.src.others.logger import LogFIDC
from v8_utilities.paths import PathV8
from v8_utilities.anbima_calendar import Calendar
from v8_utilities.yaml_functions import load_yaml

import re
import os
import datetime

script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)

PATH = os.path.join(script_dir, "..","..", "..", 'yamls')

logger = LogFIDC()

#ajeitar tudo aqui

class Grouper(object):
    def __init__(self, path_handle: PathV8, calendar_handle: Calendar, folder_root: str = None):
        try:
            self.path_handle = path_handle
            self.calendar_handle = calendar_handle

            if folder_root is None:
                self.folder_root = self.path_handle.FIDCS_RELATORIOS_GERAIS
            else:
                self.folder_root = folder_root

            self.equiv_columns =  load_yaml(os.path.join(PATH, "colunas.yaml"))
            self.regex_patterns = load_yaml(os.path.join(PATH, "regex.yaml"))
            self.csv_dict: Dict[str, pd.DataFrame] = {}
        except:
            logger.error(f"Erro na criação do grouper, arquivos YAML não encontrados.")
            raise (f"Erro na criação do grouper, arquivos YAML não encontrados.")

    # ------------------------  PROCESSAMENTO DE STRINGS  ------------------------ #
    @staticmethod
    def _verify_pattern(entry: str, patterns: List[str]) -> bool:
        """
        Verifica se a string de entrada corresponde a algum dos padrões fornecidos.

        Args:
            entry (str): Texto que será verificado.
            patterns (List[str]): Lista de padrões regex para comparar com o texto.

        Returns:
            bool: True se algum padrão for encontrado no texto, False caso contrário.
        """
        text = str(entry).lower().strip()
        return any(re.search(p, text) for p in patterns)

    def _days_string_processing(self, entry: Optional[str]) -> Optional[str]:
        """
        Padroniza strings que representam intervalos de dias, ajustando formatações variadas para um formato uniforme.

        Exemplos tratados:
            - "1-30" → "0-30 dias"
            - "de 2 a 30 dias" → "1-30 dias"
            - "até 60 dias" → "<= 60 dias"
            - "> 121" → "> 120 dias"

        Args:
            entry (Optional[str]): Texto original representando um intervalo de dias.

        Returns:
            Optional[str]: Texto padronizado ou None se a entrada for inválida ou não casar com nenhum padrão.
        """
        if not entry or not isinstance(entry, str):
            return None

        entry = entry.lower().strip()
        unwanted_numbers = {6, 16, 31, 61, 91, 121, 151, 181, 366, 721}

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
        """
        Padroniza os nomes das colunas de um DataFrame que representam intervalos de dias.

        Detecta colunas com padrões como "1 a 30", "de 2 a 60 dias", "acima de 121 dias", etc.,
        e aplica a transformação usando `_days_string_processing`, renomeando essas colunas.

        Args:
            df (pd.DataFrame): DataFrame com colunas a serem verificadas e possivelmente renomeadas.

        Returns:
            pd.DataFrame: DataFrame com os nomes das colunas de dias padronizados.
        """
        columns = pd.Series(df.columns)

        pattern = [
            r"(?i)^\d+\s*a\s*\d+$",
            r"(?i)^\d+\s*-\s*\d+$",
            r"(?i)^\d+\s*-\s*\d+\s*dias$",
            r"(?i)^de\s+\d+\s*a\s+\d+\s*dias$",
            r"(?i)^\d+\s*e\s*\d+\s*dias$",
            r"(?i)^acima\s+de\s+\d+\s*dias$",
            r"(?i)^superior\s+a\s+\d+$",
            r"(?i)^superior\s+\d+$",
            r"(?i)^>\s*\d+$",
            r"(?i)^até\s+\d+$",
            r"(?i)^até\s+\d+\s*dias$"
        ]

        mask = columns.apply(lambda entry: self._verify_pattern(entry, pattern))
        columns_processed = columns[mask]

        columns[mask] = columns_processed.apply(self._days_string_processing)
        df.columns = columns
        return df

    def _rename_equiv_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renomeia colunas do DataFrame com base em equivalências definidas em `self.equiv_columns`.

        Percorre o dicionário `self.equiv_columns`, onde cada chave é o nome desejado e
        o valor é uma lista de possíveis nomes alternativos que podem estar presentes
        no DataFrame. Se alguma dessas alternativas estiver nas colunas do DataFrame, ela é renomeada.

        Exemplo de `self.equiv_columns`:
            {
                "prazo": ["prazo_medio", "prazos", "prazo médio"],
                "retorno": ["ret", "yield"]
            }

        Args:
            df (pd.DataFrame): DataFrame com colunas a serem renomeadas.

        Returns:
            pd.DataFrame: DataFrame com colunas renomeadas conforme o dicionário de equivalência.
        """
        rename_dict = {}
        for wanted_name, possibilities in self.equiv_columns.items():
            if possibilities is None:
                continue

            for possibility in possibilities:
                if possibility in df.columns:
                    rename_dict[possibility] = wanted_name

        return df.rename(columns=rename_dict)

    def _grouping_days_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrupa colunas relacionadas a intervalos de dias com base em colunas anteriores
        que servem como indicador do tipo de métrica associada.

        Identifica colunas com nomes que representam intervalos de dias (ex: "0-30 dias", "> 90 dias")
        e as renomeia incluindo o contexto da última coluna que não representa intervalo —
        ou seja, se a coluna anterior for "prazo médio", a nova coluna se torna "(prazo médio)0-30 dias".

        Padrões considerados:
            - Intervalo: "10 - 20 dias"
            - Superior: "> 120 dias"
            - Inferior: "<= 10 dias"

        Args:
            df (pd.DataFrame): DataFrame com colunas a serem agrupadas.

        Returns:
            pd.DataFrame: DataFrame com colunas renomeadas para refletir agrupamento contextual.
        """
        pattern = [
            r'(\d+)\s*-\s*(\d+)\s*dias?',  # intervalo: 10 - 20 dias
            r'>\s*(\d+)\s*dias?',  # > 120 dias
            r'<=\s*(\d+)\s*dias?'  # <= 10 dias
        ]

        columns = pd.Series(df.columns)
        indicator = columns.apply(lambda entry: self._verify_pattern(entry, pattern))
        # vetor booleano indicando se a coluna representa um intervalo de dias

        last_true_idx = None
        new_columns = []

        for idx, col_name in enumerate(columns):
            if not indicator.iloc[idx]:
                # coluna normal, usada como "prefixo" para futuras colunas de intervalo
                new_columns.append(col_name)
                last_true_idx = idx
            else:
                # coluna de intervalo, renomeia com base na última coluna não-intervalo
                if last_true_idx is not None:
                    new_name = f"({columns.iloc[last_true_idx]}){col_name}"
                    new_columns.append(new_name)
                else:
                    # caso não haja referência anterior, mantém o nome original
                    new_columns.append(col_name)

        df.columns = new_columns
        return df

    # ------------------------  SELEÇÃO DE COLUNAS  ------------------------ #
    def _selecting_columns_by_name(self) -> Dict[str, List[str]]:
        """
        Seleciona colunas de cada DataFrame no dicionário `csv_dict` com base nos nomes desejados.

        Utiliza as chaves de `self.equiv_columns` como lista de colunas-alvo e
        retorna, para cada DataFrame, quais dessas colunas estão presentes.

        Returns:
            Dict[str, List[str]]: Dicionário onde a chave é o nome do DataFrame (nome do CSV),
                                  e o valor é a lista de colunas encontradas nele que coincidem com os nomes esperados.
        """
        wanted_columns = list(self.equiv_columns.keys())  # nomes exatos que queremos buscar

        # percorre os DataFrames salvos no csv_dict e seleciona as colunas que estão entre as desejadas
        result = {
            name_df: [col for col in df.columns if col in wanted_columns]
            for name_df, df in self.csv_dict.items()
        }

        return result

    def _selecting_columns_by_regex(self) -> Dict[str, List[str]]:
        """
            Seleciona colunas de cada DataFrame em `self.csv_dict` cujos nomes correspondem
            a expressões regulares definidas em `self.regex_patterns`.

            Returns:
                Dict[str, List[str]]: Dicionário onde a chave é o nome do DataFrame e o valor é a lista
                de colunas que correspondem às expressões regulares.
            """
        regex_list = list(self.regex_patterns.values())[0]
        compiled = [re.compile(p) for p in regex_list]

        out: Dict[str, List[str]] = {}
        for name, df in self.csv_dict.items():
            matches = [c for c in df.columns if any(p.fullmatch(str(c)) for p in compiled)]
            out[name] = matches
        return out

    def _filter_final_columns(self, columns: Dict[str, List[str]]) -> None:
        """
           Filtra as colunas dos DataFrames em `self.csv_dict` com base nas colunas informadas.

           Args:
               columns (Dict[str, List[str]]): Dicionário onde a chave é o nome do DataFrame
               e o valor é a lista de colunas a serem mantidas.
           """
        self.csv_dict = {k: df[columns[k]] for k, df in self.csv_dict.items()}

    def _group_by_date(self, date) -> pd.DataFrame:
        """
           Agrupa os DataFrames de `self.csv_dict` por uma data específica, definida em date.
           Cada DataFrame é reduzido a uma linha correspondente a essa data (se existir).

            Args:
                date (str): Data que você quer agrupar

           Returns:
               pd.DataFrame: DataFrame consolidado com uma linha por arquivo, indexado pelo nome do arquivo.

           Raises:
               Exception: Se ocorrer algum erro durante o agrupamento.
           """
        try:
            date_str = date.strftime("%Y-%m-%d")
            list_dates = []

            for key, df in self.csv_dict.items():
                if date_str in df.index:
                    row = df.loc[date_str]
                    row_df = pd.DataFrame([row], index=[key])
                else:
                    row_df = pd.DataFrame([{col: pd.NA for col in df.columns}], index=[key])
                list_dates.append(row_df)
            frames = [
                f.dropna(axis=1, how="all")
                for f in list_dates
                if not f.dropna(how="all").empty
            ]

            result = pd.concat(frames, axis=0, sort=True)
            return result
        except Exception as e:
            logger.error(f"Erro no Agrupamento por data: {e}")
            raise Exception(f"Erro no Agrupamento por data: {e}")


    # ------------------------  ORDENADOR DE COLUNAS  ------------------------ #
    def _extract_order(self, item: str) -> Tuple[str, int, int]:
        """
        Extrai informações de ordenação de uma string no formato "(grupo) valor" ou "(grupo) valor - valor".

        A string deve conter um grupo entre parênteses seguido por um número ou intervalo numérico,
        possivelmente com operadores como `>`, `>=`, `-`, etc.

        Exemplos válidos:
            "(Coluna) >120"
            "(Coluna) 30 - 60"
            "(Dias) <=10"

        Args:
            item (str): A string a ser analisada.

        Returns:
            Tuple[str, int, int]: Uma tupla contendo:
                - O nome do grupo (str)
                - O valor inicial do intervalo (int)
                - O valor final do intervalo (int)

            Caso não seja possível fazer o parse, retorna ('ZZZ', 99999, 99999)
        """
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

    def _reorder_df_columns(self, required_cols: List[str], regex_cols: List[str]) -> List[str]:
        """
        Reordena as colunas do DataFrame combinando colunas obrigatórias e colunas que seguem padrões
        específicos, aplicando regras personalizadas para posicionamento.

        O métdo ordena inicialmente as colunas `regex_cols` usando a função `_extract_order`,
        depois concatena com as `required_cols`. Aplica regras específicas para mover certas colunas
        relacionadas a “Concentração” e grupos entre parênteses. Também posiciona a coluna 'PDD À Vencer'
        após a última coluna que corresponde ao padrão '(PDD Total) X-Y dias'.

        Args:
            required_cols (List[str]): Lista de nomes de colunas obrigatórias que devem aparecer primeiro.
            regex_cols (List[str]): Lista de colunas que seguem um padrão regex e serão ordenadas.

        Returns:
            List[str]: Lista com as colunas ordenadas conforme as regras especificadas.
        """
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

    # -------------------  CALCULO DAS MÉTRICAS PEDIDAS  ------------------ #

    def _create_additional_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Cria colunas adicionais derivadas a partir de colunas existentes, calculando percentuais e
        indicadores financeiros baseados no PL Total e outros valores específicos.

        As colunas calculadas incluem percentuais de subordinação, PDD, concentração de cedentes e sacados,
        recompra, valores liquidados e taxas.

        Args:
            data (pd.DataFrame): DataFrame contendo as colunas originais para cálculo.

        Returns:
            pd.DataFrame: DataFrame com as colunas adicionais criadas ou atualizadas.
        """
        # Função auxiliar para checar existência da coluna
        def has_col(col):
            return col in data.columns

        if has_col("PL Mezanino") and has_col("PL Subordinada Jr") and has_col("PL Total"):
            data["Subordinação (%)"] = (data["PL Mezanino"] + data["PL Subordinada Jr"]) / data["PL Total"]
        if has_col("PL Subordinada Jr") and has_col("PL Total"):
            data["Subordinação Jr (%)"] = data["PL Subordinada Jr"] / data["PL Total"]
        if has_col("PDD Total") and has_col("PL Total"):
            data["PDD Total (PL%)"] = data["PDD Total"] / data["PL Total"]

        if has_col("Vencidos Total") and has_col("PL Total"):
            data["CVNP (PL%)"] = data["Vencidos Total"] / data["PL Total"]
        if "CVNP (PL%)" in data.columns and "PDD Total (PL%)" in data.columns:
            data["CVNP - PDD (PL%)"] = data["CVNP (PL%)"] - data["PDD Total (PL%)"]

        if "Vencidos Total - PDD" in data.columns and has_col("PL Total"):
            if not has_col("CVNP - PDD (PL%)"):
                data["CVNP - PDD (PL%)"] = np.nan
            col = data["Vencidos Total - PDD"] / data["PL Total"]
            data["CVNP - PDD (PL%)"] = data["CVNP - PDD (PL%)"].fillna(col)

        if has_col("Cedente 1") and has_col("PL Total"):
            data["Concentração Maior Cedente (PL%)"] = data["Cedente 1"] / data["PL Total"]
        if has_col("Sacado 1") and has_col("PL Total"):
            data["Concentração Maior Sacado (PL%)"] = data["Sacado 1"] / data["PL Total"]
        if has_col("Concentração Top 10 Cedentes (R$)") and has_col("PL Total"):
            data["Concentração 10 Maiores Cedentes (PL%)"] = data["Concentração Top 10 Cedentes (R$)"] / data[
                "PL Total"]
        if has_col("Concentração Top 10 Sacados (R$)") and has_col("PL Total"):
            data["Concentração 10 Maiores Sacados (PL%)"] = data["Concentração Top 10 Sacados (R$)"] / data[
                "PL Total"]
        if has_col("Recompra (R$)") and has_col("PL Total"):
            data["Recompra (PL%)"] = data["Recompra (R$)"] / data["PL Total"]
        if has_col("Liquidado Total (R$)") and has_col("PL Total"):
            data["Liquidados Total (PL%)"] = data["Liquidado Total (R$)"] / data["PL Total"]

        if has_col("Duplicata (%)"):
            data["Duplicata (PL%)"] = data["Duplicata (%)"]
        elif has_col("Duplicata") and has_col("PL Total"):
            data["Duplicata (PL%)"] = data["Duplicata"] / data["PL Total"]

        if has_col("Taxa Média") and has_col("Taxa Ponderada de Cessão"):
            def calc_taxa_media(row):
                if pd.isna(row["Taxa Média"]):
                    return row["Taxa Ponderada de Cessão"]
                else:
                    return row["Taxa Média"]

            data["Taxa Média"] = data.apply(calc_taxa_media, axis=1)

        if has_col("Volume Operado") and has_col("Valor Pago nas Operações no Mês") and has_col("PL Total"):
            def calc_volume_operado(row):
                if not pd.isna(row["Valor Pago nas Operações no Mês"]):
                    val = row["Valor Pago nas Operações no Mês"]
                else:
                    val = row["Volume Operado"]
                return val / row["PL Total"]

            data["Volume Operado (PL%)"] = data.apply(calc_volume_operado, axis=1)

        if has_col("Caixa/Disponibilidades") and has_col("PL Total"):
            def calc_caixa(row):
                fundo = row["Fundo Soberano"] if has_col("Fundo Soberano") and not pd.isna(
                    row.get("Fundo Soberano")) else 0
                return (row["Caixa/Disponibilidades"] + fundo) / row["PL Total"]

            data["Caixa/Disponibilidades (%PL)"] = data.apply(calc_caixa, axis=1)

        if has_col("PL Subordinada Jr") and has_col("PL Total"):
            data["Alavancagem"] = (data["PL Subordinada Jr"] / data["PL Total"])

        if has_col("PL Subordinada Jr") and has_col("Concentração Top 10 Cedentes (R$)"):
            data["Indicador 1"] = (data["PL Subordinada Jr"] / data["Concentração Top 10 Cedentes (R$)"])

        if has_col("PL Subordinada Jr") and has_col("PL Mezanino") and has_col("Concentração Top 10 Cedentes (R$)"):
            data["Indicador 2"] = (data["PL Subordinada Jr"] + data["PL Mezanino"]) / data["Concentração Top 10 Cedentes (R$)"]

        return data

    def read_csvs(self, date: datetime, fidc_list: list[str] | None = None) -> None:
        """
        Lê arquivos CSV de um diretório, processa os DataFrames aplicando tratamentos específicos e armazena
        os DataFrames processados em um dicionário.

        Se uma lista de nomes `fidc_list` for fornecida, o métdo tentará ler apenas os arquivos cujos nomes
        correspondam aos FIDCs da lista e à data especificada. Se `fidc_list` for None, lê todos os arquivos CSV
        do diretório padrão que contenham a data no nome.

        Para cada arquivo CSV processado, são aplicadas as seguintes operações:
        - Leitura do CSV com separador ';' e codificação UTF-8 BOM, usando a coluna "Data" como índice.
        - Remoção de sufixos numéricos nas colunas.
        - Processamento específico de colunas relacionadas a intervalos de dias.
        - Renomeação de colunas equivalentes com base em mapeamento definido.
        - Agrupamento das colunas de dias conforme padrões.
        - Registro de logs de sucesso ou erro para cada arquivo.

        Args:
            date (datetime): Data para filtrar arquivos.
            fidc_list (list[str] | None): Lista opcional com nomes dos FIDCs para leitura.
                Caso None, lê todos os arquivos CSV do diretório padrão que contenham a data.

        Raises:
            Exception: Em caso de erro geral durante a leitura ou processamento dos arquivos.
        """
        try:
            path = os.path.join(self.folder_root, "01_PARSED")
            date_str = date.strftime("%Y_%m_%d")
            csv_dict = {}

            def remove_suffix(columns):
                return [re.sub(r'\.\d+$', '', col) for col in columns]

            all_files = [f for f in os.listdir(path) if f.endswith('.csv') and f.endswith(f"_{date_str}.csv")]

            files_to_process = []

            if fidc_list is None:
                # Lê todos os arquivos com a data especificada
                files_to_process = all_files
            else:
                for fidc_name in fidc_list:
                    # Arquivo esperado no formato "FIDC_<nome>_<data>.csv"
                    matching_files = [f for f in all_files if re.match(fr"FIDC_{re.escape(fidc_name)}_{date_str}\.csv", f)]
                    files_to_process.extend(matching_files)

            for file in files_to_process:
                try:
                    path_file = os.path.join(path, file)
                    name_file = os.path.splitext(file)[0]
                    name = name_file.split("_")[1]

                    df = pd.read_csv(path_file, sep=';', encoding='utf-8-sig', index_col="Data").astype("float64")
                    logger.info(f"FIDC {name} encontrado para agrupamento.")
                    df.columns = remove_suffix(df.columns)
                    df = self._days_column_processing(df)
                    df = self._rename_equiv_columns(df)
                    df = self._grouping_days_column(df)
                    csv_dict[name] = df
                except Exception as e:
                    logger.error(f"Erro ao processar o arquivo {file}: {e}")

            self.csv_dict = csv_dict
        except Exception as e:
            logger.error(f"Erro ao ler os arquivos CSVs do diretório {path}: {e}")
            raise Exception(f"Erro ao ler os arquivos CSVs do diretório {path}: {e}")

    # ------------------------  AGRUPAMENTO FINAL  ------------------------ #

    def group_fidcs(self, date: datetime) -> pd.DataFrame:
        """
        Realiza o agrupamento dos dados dos FIDCs para uma data específica, combinando colunas selecionadas
        por nome exato e por regex, reorganizando colunas, adicionando colunas adicionais calculadas,
        e salvando o resultado em CSV.

        Passos principais:
        - Seleciona colunas por nome e por regex.
        - Combina as colunas selecionadas.
        - Filtra o DataFrame para manter apenas as colunas finais.
        - Agrupa os dados pela data especificada.
        - Reordena as colunas conforme regra personalizada.
        - Cria colunas adicionais com métricas calculadas.
        - Salva o DataFrame resultante em arquivo CSV.

        Args:
            date (datetime)

        Returns:
            pd.DataFrame: DataFrame resultante do agrupamento com as colunas organizadas e métricas adicionais.

        Raises:
            Exception: Se ocorrer erro durante o processo de agrupamento ou salvamento do arquivo.
        """
        try:
            date_str = date.strftime("%Y_%m_%d")
            logger.info(f"Iniciando agrupamento dos FIDCs para a data: {date_str}")

            by_name = self._selecting_columns_by_name()
            by_regex = self._selecting_columns_by_regex()

            column_names = {k: by_name.get(k, []) + by_regex.get(k, [])
                            for k in by_name.keys()}

            self._filter_final_columns(column_names)
            grouped_data = self._group_by_date(date)

            regex_cols = list(pd.Series(chain.from_iterable(by_regex.values())).drop_duplicates())
            main_columns = list(self.equiv_columns.keys())

            ordered = self._reorder_df_columns(main_columns, regex_cols)
            grouped_data = grouped_data[[col for col in ordered if col in grouped_data.columns]] # para evitar quando tiver colunas faltantes
            grouped_data.index.name = 'FIDC'

            grouped_data = self._create_additional_columns(grouped_data)

            # name = "FIDCS_" + str(self.date).replace("-", "_") + ".csv"
            # path_out = os.path.join(os.getcwd(), "data", "GROUPED", name)

            return grouped_data
        except Exception as e:
            logger.error(f"Erro ao agrupar FIDCs para a data {date_str}: {e}")
            raise Exception(f"Erro ao agrupar FIDCs para a data {date_str}: {e}")


    def run(self, date: datetime, fidc_list: list[str] | None = None) -> None:
        """
        Executa o fluxo completo de leitura dos arquivos CSV, agrupamento dos dados dos FIDCs e salvamento do resultado.

        Passos realizados:
        - Cria os diretórios necessários para salvar os arquivos.
        - Lê os arquivos CSV da pasta especificada.
        - Agrupa os dados conforme a data definida na instância.
        - Salva o arquivo CSV agrupado no caminho de saída especificado.
        - Registra logs de sucesso ou erro no processo.

        Args:
            date(datetime): data no formato de datetime que vai definir o agrupamento

        Returns:
            List[str]: lista que corresponde ao nome dos fidcs que foram agrupados
        """
        try:
            date_str = date.strftime("%Y_%m_%d")

            path_to_read = os.path.join(self.folder_root, "01_PARSED")
            path_to_save = os.path.join(self.folder_root, "02_REPORT")
            file_name_save = f"FIDCS_" + date_str + ".csv"
            file_to_save = os.path.join(path_to_save, file_name_save)

            os.makedirs(os.path.dirname(file_to_save), exist_ok=True)

            self.read_csvs(date, fidc_list)

            grouped_data = self.group_fidcs(date)

            grouped_data.to_csv(file_to_save, sep=';', encoding='utf-8-sig', decimal='.')
            logger.info(f"Arquivo salvo com sucesso em {file_to_save}")
            fidc_list_final = grouped_data.index.to_list()
            return fidc_list_final

        except Exception as e:
            logger.error(f"Problema em Agrupar e salvar os Arquivos {e}.")
            raise Exception(f"Problema em Agrupar e salvar os Arquivos {e}.")
