import streamlit as st
import os
import zipfile
import io
import datetime

from classes.extractor import Extractor
from classes.exceltransformer import ExcelTransformer
from classes.grouper import Grouper
from classes.logger import LogFIDC


class Input:
    def __init__(self):
        try:
            self.logger = LogFIDC()
            self.today = datetime.date.today()
            self.folder_root = os.path.abspath(os.path.join(os.getcwd(), "data"))
        except Exception as e:
            st.error("Erro ao inicializar aplicação.")
            raise e

    def _build_reference_dates(self):
        try:
            return [
                (self.today.replace(day=1) - datetime.timedelta(days=30 * i)).replace(day=1)
                for i in range(12)
            ]
        except Exception as e:
            self.logger.error(f"Erro ao construir datas de referência: {e}")
            return []

    def _format_months(self, dates):
        try:
            return [date.strftime("%B de %Y").capitalize() for date in dates]
        except Exception as e:
            self.logger.error(f"Erro ao formatar os meses: {e}")
            return []

    def _build_path(self, date: datetime.date) -> str:
        try:
            meses_pt = [
                "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
            ]
            nome_mes = meses_pt[date.month - 1]
            return os.path.join(
                "FIDCs Investidos",
                "Relatórios",
                "Planilhas de Monitoramentos",
                str(date.year),
                f"Relatórios {nome_mes}"
            ).replace("\\", "/")
        except Exception as e:
            self.logger.error(f"Erro ao construir caminho de download: {e}")
            raise e

    def _clean_folders(self):
        folders_to_clear = ["RAW", "PARSED", "GROUPED"]
        for folder in folders_to_clear:
            folder_path = os.path.join(self.folder_root, folder)
            try:
                if os.path.exists(folder_path):
                    for filename in os.listdir(folder_path):
                        file_path = os.path.join(folder_path, filename)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
            except Exception as e:
                self.logger.error(f"Erro ao limpar arquivos da pasta {folder}: {e}")

    def _process_date(self, date: datetime.date):
        site_name = "FIDCS"
        extractor = Extractor(site_name)

        try:
            path_to_download = self._build_path(date)
            fidcs_files = extractor.list_files(path_file=path_to_download)
        except Exception as e:
            self.logger.error(f"Erro ao acessar arquivos: {e}")
            raise RuntimeError(f"Erro ao listar arquivos na pasta '{path_to_download}': {e}")

        raw_path = os.path.join(self.folder_root, "RAW")

        for fidc in fidcs_files:
            try:
                name_ = f"FIDC_{fidc}_{date.year}_{date.month:02}_01.xlsx"
                path_ = os.path.join(path_to_download, fidc).replace("\\", "/")
                path_target = os.path.join(raw_path, name_)

                extractor.download_file(path_, name_, path_target)

                transformer = ExcelTransformer(path_target, fidc)
                transformer.transform_table(self.folder_root)
            except Exception as e:
                self.logger.error(f"O FIDC {fidc} será pulado devido ao erro: {e}")
                continue

        try:
            parsed_path = os.path.join(self.folder_root, "PARSED")
            grouper = Grouper()
            grouper.read_csvs(parsed_path)
            formatted_date = date.replace(day=1).strftime("%Y-%m-%d")
            return grouper.group_fidcs(formatted_date)
        except Exception as e:
            self.logger.error(f"Erro ao agrupar arquivos: {e}")
            raise e

    def _standardize_and_group_only(self, date: datetime.date):
        try:
            raw_path = os.path.join(self.folder_root, "RAW")
            date_str = date.strftime("%Y_%m_%d")

            for filename in os.listdir(raw_path):
                if filename.endswith(".xlsx") and date_str in filename:
                    fidc = filename.split("_")[1]
                    path_target = os.path.join(raw_path, filename)
                    transformer = ExcelTransformer(path_target, fidc)
                    transformer.transform_table(self.folder_root)

            parsed_path = os.path.join(self.folder_root, "PARSED")
            grouper = Grouper()
            grouper.read_csvs(parsed_path)
            formatted_date = date.replace(day=1).strftime("%Y-%m-%d")
            grouper.group_fidcs(formatted_date)

        except Exception as e:
            self.logger.error(f"Erro ao padronizar e agrupar dados existentes: {e}")
            raise e

    def _display_folders(self):
        try:
            st.subheader("📁 Dados FIDCs disponíveis")
            st.markdown("---")

            if st.toggle("Mostrar pastas"):
                available_folders = ["RAW", "PARSED", "GROUPED"]
                selected_folder = st.selectbox("Pastas:", available_folders)

                if selected_folder:
                    folder_path = os.path.join(self.folder_root, selected_folder)
                    try:
                        files = [
                            f for f in os.listdir(folder_path)
                            if os.path.isfile(os.path.join(folder_path, f))
                        ]
                    except Exception as e:
                        st.error("Erro ao acessar arquivos da pasta.")
                        self.logger.error(f"Erro ao listar arquivos da pasta {selected_folder}: {e}")
                        return

                    st.markdown(f"### 📂 Arquivos da pasta: `{selected_folder}`")

                    if not files:
                        st.info("Nenhum arquivo nessa pasta.")
                    else:
                        cols_per_row = 3
                        rows = [files[i:i + cols_per_row] for i in range(0, len(files), cols_per_row)]

                        for row in rows:
                            cols = st.columns(cols_per_row)
                            for idx, filename in enumerate(row):
                                file_path = os.path.join(folder_path, filename)
                                with cols[idx]:
                                    st.markdown(f"**{filename}**")
                                    try:
                                        with open(file_path, "rb") as file:
                                            st.download_button(
                                                label="📥 Baixar",
                                                data=file,
                                                file_name=filename,
                                                mime="application/octet-stream",
                                                key=f"{selected_folder}_{filename}"
                                            )
                                    except Exception as e:
                                        self.logger.error(f"Erro ao abrir {filename}: {e}")
                                        st.error(f"Erro ao abrir {filename}")

                        st.divider()
                        if st.button(f"📦 Baixar todos de {selected_folder}"):
                            try:
                                zip_buffer = io.BytesIO()
                                with zipfile.ZipFile(zip_buffer, "w") as zipf:
                                    for f in files:
                                        zipf.write(os.path.join(folder_path, f), arcname=f)

                                st.download_button(
                                    label="⬇️ Clique aqui para baixar o ZIP",
                                    data=zip_buffer.getvalue(),
                                    file_name=f"{selected_folder}.zip",
                                    mime="application/zip",
                                    key=f"download_zip_{selected_folder}"
                                )
                            except Exception as e:
                                self.logger.error(f"Erro ao criar ZIP da pasta {selected_folder}: {e}")
                                st.error("Erro ao gerar o ZIP")
        except Exception as e:
            self.logger.error(f"Erro ao exibir pastas: {e}")
            st.error("Erro ao exibir conteúdo das pastas.")

    def run(self):
        try:
            st.set_page_config(page_title="Arquivos por Mês e Pasta", layout="wide")
            st.title("📂 FIDCs")

            col1, col2 = st.columns([1, 3])

            with col1:
                st.subheader("📅 Mês de Referência")
                date_options = self._build_reference_dates()
                formatted_months = self._format_months(date_options)

                if not formatted_months:
                    st.error("Erro ao carregar meses disponíveis.")
                    return

                selected_date_str = st.selectbox("Selecione o mês:", formatted_months)
                selected_date = date_options[formatted_months.index(selected_date_str)]

                if st.button("📤 Enviar data"):
                    with st.spinner("Processando, aguarde..."):
                        try:
                            self._process_date(selected_date)
                            st.success("Processamento concluído com sucesso!")
                        except Exception as e:
                            st.error(f"Erro no processamento: {e}")

                st.markdown("---")
                if st.button("🗑️ Limpar pastas"):
                    self._clean_folders()
                    st.success("Pastas RAW, PARSED e GROUPED limpas com sucesso!")

                st.markdown("---")
                if st.button("🔄 Padronizar e Agrupar dados existentes"):
                    with st.spinner("Padronizando e agrupando dados existentes..."):
                        try:
                            self._standardize_and_group_only(selected_date)
                            st.success("Padronização e agrupamento realizados com sucesso!")
                        except Exception as e:
                            st.error(f"Erro ao padronizar e agrupar: {e}")

            with col2:
                self._display_folders()

        except Exception as e:
            self.logger.error(f"Erro ao rodar a aplicação: {e}")
            st.error("Erro inesperado ao iniciar o aplicativo.")
