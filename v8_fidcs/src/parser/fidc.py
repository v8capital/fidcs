from typing import Union, List, Optional
from v8_fidcs.src.others.logger import LogFIDC
#from v8_utilities.yaml_functions import load_yaml

import pandas as pd
import numpy as np

import re

#PATH = os.path.join(os.getcwd(), 'YAMLs')

logger = LogFIDC()

class FIDC():
    def __init__(self, path_handle, calendar_handle, table: pd.DataFrame, raw_table: Union[pd.DataFrame, list], name: str, type: str, pattern: list) -> None:
        #patterns_fidcs = load_yaml(os.path.join(PATH, "fidcs.yaml"))
        self.path_handle = path_handle
        self.calendar_handle = calendar_handle

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
        Converte uma string numérica no formato brasileiro para float.

        Exemplo:
            '322.850,74' → 322850.74

        Assumes que a string já está validada e corresponde ao padrão `_ptbr_num`.

        Args:
            s (str): Valor numérico em formato de string no padrão PT-BR.

        Returns:
            float: Valor convertido para ponto flutuante no padrão internacional.
        """
        logger.debug(f"Número em PTBR encontrado e sendo convertido: '{s}'")
        return float(s.replace('.', '').replace(',', '.'))

    def convert_to_double(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e converte os dados de um DataFrame para o tipo float64 (“double”).

        Operações realizadas:
            1. Remove espaços em branco e substitui entradas vazias por NaN.
            2. Trata valores no formato brasileiro (ex: '1.234,56') e converte para float.
            3. Valida valores inválidos e converte-os para NaN.
            4. Remove linhas com valores incompatíveis ou inválidos como NaT.
            5. Converte todo o DataFrame para o tipo float64 ao final.

        Args:
            data (pd.DataFrame): DataFrame com dados brutos a serem tratados e convertidos.

        Returns:
            pd.DataFrame: DataFrame convertido e sanitizado com valores em float64.
        """
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
                    logger.debug(
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
        return data.astype("double").abs()

    def absolute_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Multiplica por -1000 todas as colunas que são marcadas como 'absolute' no YAML de padrões.

        Essas colunas representam valores absolutos negativos (ex: PDD) e estão originalmente em milhares.

        Args:
            data (pd.DataFrame): DataFrame com os dados brutos a serem ajustados.

        Returns:
            pd.DataFrame: DataFrame com as colunas 'absolute' ajustadas para valores negativos e convertidas de milhar para unidade.
        """
        absolute_cols = [k for d in self.pattern for k, v in d.items() if v == "absolute"]
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in absolute_cols)]
        data[cols_to_multiply] = data[cols_to_multiply] * -1000  # PDD está em milhares
        return data

    def correct_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Multiplica por 1000 todas as colunas que são marcadas como 'valueR1000' no YAML de padrões.

        Essas colunas representam valores que estão originalmente em milhares e devem ser convertidas para unidade.

        Args:
            data (pd.DataFrame): DataFrame com os dados a serem ajustados.

        Returns:
            pd.DataFrame: DataFrame com as colunas 'valueR1000' convertidas de milhar para unidade.
        """
        r1000_cols = [k for d in self.pattern for k, v in d.items() if v == "valueR1000"]
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in r1000_cols)]
        data[cols_to_multiply] = data[cols_to_multiply] * 1000
        return data

    def correct_percentages(self, data: pd.DataFrame, target: str) -> pd.DataFrame:
        """
        Ajusta colunas percentuais que devem ser multiplicadas por uma coluna alvo (`target`).

        Identifica colunas marcadas como 'repeatpercent' ou 'percentrp' no YAML de padrões
        e multiplica seus valores pela coluna de referência informada.

        Args:
            data (pd.DataFrame): DataFrame com os dados brutos.
            target (str): Nome da coluna base usada para aplicar os percentuais.

        Returns:
            pd.DataFrame: DataFrame com as colunas percentuais corrigidas com base na coluna alvo.
        """
        percent_cols = [k for d in self.pattern for k, v in d.items() if v == "repeatpercent" or v == "percentrp"]
        cols_to_multiply = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in percent_cols)]
        for col in cols_to_multiply:
            idxs = [i for i, c in enumerate(data.columns) if c == col]
            for idx in idxs:
                data.iloc[:, idx] = data.iloc[:, idx] * data[target]
        return data

    def clean_column_names(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Remove sufixos específicos dos nomes de colunas relacionados a PDD.

        Substitui padrões como:
            'Cedente 1 (Vlr Presente-PDD)' → 'Cedente 1'
            'Sacado 2 (Vlr Presente-PDD)' → 'Sacado 2'

        Essa limpeza facilita a padronização dos nomes de colunas.

        Args:
            data (pd.DataFrame): DataFrame cujos nomes de colunas serão limpos.

        Returns:
            pd.DataFrame: DataFrame com os nomes de colunas atualizados.
        """
        novas_colunas = []
        for nome in data.columns:
            texto_modificado = re.sub(r'(Cedente\s+\d+)\s+\(Vlr Presente-PDD\)', r'\1', nome)
            texto_modificado = re.sub(r'(Sacado\s+\d+)\s+\(Vlr Presente-PDD\)', r'\1', texto_modificado)
            novas_colunas.append(texto_modificado)
        data.columns = novas_colunas
        return data

    def remove_rows_before(self, indexes: pd.Index, date_limit: Union[str, pd.Timestamp]) -> pd.Index:
        """
        Remove índices (datas) anteriores à data limite informada.

        Converte os índices para datetime com o formato '%d/%m/%Y' e filtra apenas os que são iguais ou posteriores à `date_limit`.

        Args:
            indexes (pd.Index): Índice contendo datas em formato string.
            date_limit (Union[str, pd.Timestamp]): Data limite usada como filtro (inclusive). Pode ser string ou objeto Timestamp.

        Returns:
            pd.Index: Índices filtrados, contendo apenas datas a partir da data limite.
        """
        date_limit = pd.to_datetime(date_limit)
        dates = pd.to_datetime(indexes, format='%d/%m/%Y', errors="coerce")
        rows = dates >= date_limit
        return indexes[rows]

    def create_10_biggests(self, data: pd.DataFrame, target: str) -> pd.DataFrame:
        """
        Cria uma nova coluna com a soma dos 10 maiores valores de uma entidade (ex: Cedente, Sacado).

        A nova coluna será nomeada como:
            "Concentrações <target>s (R$)"

        Exemplo:
            Para target='Cedente', irá somar as colunas 'Cedente 1' até 'Cedente 10'.

        Args:
            data (pd.DataFrame): DataFrame com as colunas de valores a serem somadas.
            target (str): Nome base das colunas que representam os maiores valores.

        Returns:
            pd.DataFrame: DataFrame com a nova coluna adicionada representando a soma dos 10 maiores valores.
        """
        column_name = "Concentrações " + target + "s (R$)"
        columns_to_sum = [target + f' {i}' for i in range(1, 11)]
        data[column_name] = data[columns_to_sum].sum(axis=1)
        return data

    def correct_assets(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Ajusta colunas do DataFrame relacionadas a ativos financeiros, corrigindo valores e renomeando colunas.

        Operações realizadas:
            - Identifica colunas com padrão 'asset', 'dc' e 'rename' conforme definido em `self.pattern`.
            - Para colunas 'rename', cria uma nova coluna com sufixo ' (%)' contendo os valores originais e zera a coluna original.
            - Para colunas 'asset', multiplica seus valores pela coluna 'dc' correspondente (se presente).

        Args:
            data (pd.DataFrame): DataFrame contendo as colunas a serem ajustadas.

        Returns:
            pd.DataFrame: DataFrame ajustado com correções e renomeações aplicadas.
        """
        assets = list({k for d in self.pattern for k, v in d.items() if v == "asset"})
        dc = {k for d in self.pattern for k, v in d.items() if v == "dc"}
        renames = list({k for d in self.pattern for k, v in d.items() if v == "rename"})

        assets, dc = [
            [val for val in data.columns if any(re.fullmatch(rx, val) for rx in padroes)]
            for padroes in (assets, dc)
        ]

        renames = [val for val in data.columns if any(re.fullmatch(rx, val) for rx in renames)]

        for col in renames:
            data[f"{col} (%)"] = data[col]
            data[col] = np.nan

        if dc:
            data[assets] = data[assets].multiply(data[dc[0]], axis=0)

        return data

    def correct_column_names(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Corrige nomes de colunas removendo parênteses e seus conteúdos com base em padrões definidos no YAML.

        Colunas marcadas como 'removepar' ou 'percentrp' terão os parênteses e seus conteúdos removidos.
        As demais colunas permanecem inalteradas.

        Exemplo:
            'Cedente (Ajustado)' → 'Cedente'

        Args:
            data (pd.DataFrame): DataFrame com os nomes de colunas a serem ajustados.

        Returns:
            pd.DataFrame: DataFrame com os nomes de colunas corrigidos conforme os padrões definidos.
        """

        def clean_col(col: str) -> str:
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

    def _days_to_start_of_month(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Ajusta o índice do DataFrame para o primeiro dia do mês correspondente a cada data,
        e agrupa as linhas mantendo apenas a primeira ocorrência de cada mês.

        Passos:
            - Converte o índice para datetime, descartando entradas inválidas.
            - Altera o índice para o primeiro dia do mês (timestamp).
            - Agrupa por este índice mensal e retorna a primeira linha de cada grupo.

        Args:
            data (pd.DataFrame): DataFrame com índice temporal (datas).

        Returns:
            pd.DataFrame: DataFrame agrupado por mês com índice no primeiro dia de cada mês,
                          contendo apenas a primeira linha de cada mês.
        """
        df = data.copy()
        df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[~df.index.isna()]

        # set index to the first day of each month
        df.index = df.index.to_period('M').to_timestamp(how='start')

        return df.groupby(df.index).first()

    def sum_columns(self, data: pd.DataFrame, sign: str) -> pd.DataFrame:
        """
        Soma colunas específicas que correspondem ao padrão 'repeat' + sign no YAML e atribui o resultado
        a uma coluna que corresponde ao padrão sign.

        Args:
            data (pd.DataFrame): DataFrame com as colunas a serem somadas.
            sign (str): Sufixo usado para identificar padrões de colunas (ex: 'value', 'asset').

        Returns:
            pd.DataFrame: DataFrame atualizado com a coluna alvo recebendo a soma das colunas correspondentes.
        """
        columns_to_sum_regex = list({k for d in self.pattern for k, v in d.items() if v == "repeat" + sign})
        columns_to_sum = [col for col in data.columns if any(re.fullmatch(rx, col) for rx in columns_to_sum_regex)]

        column_regex: Optional[str] = next((k for d in self.pattern for k, v in d.items() if v == sign), None)
        column: Optional[str] = next((col for col in data.columns if column_regex and re.fullmatch(column_regex, col)),
                                     None)

        if column:
            data[column] = data[columns_to_sum].sum(axis=1)

        return data

    def create_total_liquid(self, data: pd.DataFrame) -> None:
        """
        Cria uma coluna chamada 'Liquidado Total(R$)' que contém a soma dos valores
        das colunas identificadas como 'liquids' no padrão `self.pattern`.

        Args:
            data (pd.DataFrame): DataFrame contendo as colunas com valores a serem somados.

        Returns:
            None: A função modifica o DataFrame no lugar adicionando a nova coluna.
        """
        liquid_days_p = list({k for d in self.pattern for k, v in d.items() if v == "liquids"})
        liquid_days = [val for val in data.columns if any(re.fullmatch(rx, val) for rx in liquid_days_p)]
        data["Liquidado Total(R$)"] = data[liquid_days].sum(axis=1)

    def convert_date(self, arr: List[Union[str, pd.Timestamp]]) -> pd.Series:
        """
        Converte uma lista de strings ou timestamps contendo datas no formato mês-ano em português
        para objetos datetime do pandas.

        O métdo realiza:
            - Normalização e substituição do nome do mês em português pela forma capitalizada.
            - Tenta converter as datas no formato "%B %Y" (ex: "Janeiro 2023").
            - Para valores que falharem na primeira conversão, tenta "%B %y" (ex: "Janeiro 23").

        Args:
            arr (List[Union[str, pd.Timestamp]]): Lista de datas em string ou pd.Timestamp.

        Returns:
            pd.Series: Série do pandas contendo as datas convertidas em datetime64, com NaT para falhas.
        """
        arr_en = []
        for raw in arr:
            if isinstance(raw, (pd.Timestamp,)):
                arr_en.append(raw)
            elif isinstance(raw, str):
                s = re.sub(r"\s+", " ", raw.strip().replace("-", " "))
                month, year = s.split(" ")
                month_en = self.calendar_handle.get_month_by_name(month, "pt")
                s = s.replace(month, month_en.capitalize())
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