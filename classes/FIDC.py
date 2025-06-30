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

class FIDC():
    def __init__(self, data: pd.DataFrame, name: str) -> None:
        patterns_fidcs = read_yaml(PATH + 'FIDCs.yaml')

        self.name = self.__check_name(name, patterns_fidcs)

        #print(patterns_fidcs)
        columns = self.__check_columns(data, patterns_fidcs)

        self.data = data
        if type == "":
            self.type = name

        self.type = type
        self.columns = "" #TODO: deixar no YAML as colunas daquele TIPO
        #TODO: fazer checagem se todas as colunas do DATA estão presentes e vice-versa

