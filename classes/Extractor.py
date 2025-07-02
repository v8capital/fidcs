from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext

import pandas as pd
import numpy as np

import os
import yaml
import msal
import requests

PATH = './YAMLs/'
def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        dados = yaml.safe_load(f)
    return dados  # Retorna como dicionário: {coluna_final: [possibilidades]}

class Extractor(object):
    #def __init__(self):
    def __init__(self, tenant_id, client_id, client_secret, authority_url, site_domain, site_name, site_id):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.authority_url = authority_url
        self.site_domain = site_domain
        self.site_name = site_name
        self.site_id = site_id

        # retirar depois os que não forem utilizados

        self.app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority_url,
            client_credential=self.client_secret,
        )

        response = self.app.acquire_token_for_client(scopes = ["https://graph.microsoft.com/.default"])

        if "access_token" in response:
            self.access_token = response["access_token"]
        else:
            raise Exception("Falha ao obter o token de acesso")

        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }

        self.drive_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive"
        response = requests.get(self.drive_url, headers=self.headers)

        if response.status_code == 200:
            self.drive_id = response.json()['id']
        else:
            print(f"Falha ao obter o ID do drive. Status code: {response.status_code}")
            raise Exception("Falha ao obter o ID do drive")

    def __go_trough_path(self, subfolder_name, folder_id=""):
        try:
            if folder_id == "":
                url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root/children"
            else:
                url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/items/{folder_id}/children"

            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            items_folder = response.json().get('value', [])
            dict_IDs = {item['name']: item['id'] for item in items_folder}

            if subfolder_name in dict_IDs:
                return {subfolder_name: dict_IDs[subfolder_name]}
            else:
                print(f"Pasta '{subfolder_name}' não encontrada em '{folder_id}'.")
                return None

        except requests.exceptions.Timeout:
            print(f"Timeout ao acessar a pasta '{subfolder_name}' (folder_id: {folder_id})")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição ao acessar a pasta '{subfolder_name}': {e}")
            return None
        except Exception as e:
            print(f"Erro inesperado ao acessar a pasta '{subfolder_name}': {e}")
            return None

    def __get_item_id(self, path_file):
        try:
            folder_list = path_file.split("/")
            dict_id = {item: "" for item in folder_list}
            folder_id = ""
            returned_dict = None

            for subfolder_name, _ in dict_id.items():
                if returned_dict is not None:
                    folder_id = next(iter(returned_dict.values()))
                returned_dict = self.__go_trough_path(subfolder_name, folder_id)
                if returned_dict is None:
                    print(f"Subpasta '{subfolder_name}' não encontrada no caminho '{path_file}'.")
                    return None
            return returned_dict
        except Exception as e:
            print(f"Erro ao obter o item id para o caminho '{path_file}': {e}")
            return None

    def download_file(self, path_file, file_name, target_path):
        try:
            item_id = self.__get_item_id(path_file)
            if item_id is None:
                raise Exception(f"Item ID não encontrado para o caminho '{path_file}'.")

            item_id_value = next(iter(item_id.values()))
            drive_item_url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id_value}/content"

            response = requests.get(drive_item_url, headers=self.headers, timeout=15)
            response.raise_for_status()

            with open(target_path, 'wb') as f:
                f.write(response.content)
            print(f"Arquivo '{file_name}' baixado com sucesso em '{target_path}'.")

        except requests.exceptions.Timeout:
            print(f"Timeout ao baixar o arquivo '{file_name}'.")
        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição ao baixar o arquivo '{file_name}': {e}")
        except Exception as e:
            print(f"Erro inesperado ao baixar o arquivo '{file_name}': {e}")
