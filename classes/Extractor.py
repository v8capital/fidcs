from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext

import pandas as pd
import numpy as np

import os
import yaml

PATH = './YAMLs/'
def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        dados = yaml.safe_load(f)
    return dados  # Retorna como dicionário: {coluna_final: [possibilidades]}

class Extractor(object):
    #def __init__(self):

    def request(self):
        url_site = ""
        caminho_arquivo = ""
        usuario = ""
        senha = ""  # Para segurança, prefira usar um cofre de segredos ou entrada segura
        destino_local = "arquivo_baixado.xlsx"

        # Autenticação
        ctx_auth = AuthenticationContext(url_site)
        if ctx_auth.acquire_token_for_user(usuario, senha):
            ctx = ClientContext(url_site, ctx_auth)
            response = ctx.web.get_file_by_server_relative_url(caminho_arquivo).download(destino_local).execute_query()
            print(f"Arquivo baixado com sucesso para: {destino_local}")
        else:
            print("Erro na autenticação")
