import streamlit as st
import os
import zipfile
import io
import datetime
import atexit

from classes.extractor import Extractor
from classes.exceltransformer import ExcelTransformer
from classes.grouper import Grouper
from classes.logger import LogFIDC


class Entry:
    def __init__(self):
        self.logger = LogFIDC()
        self.today = datetime.date.today()
        self.folder_root = os.path.abspath(os.path.join(os.getcwd(), "data"))
        atexit.register(self._clean_folders_on_exit)

    def _build_reference_dates(self):
        return [
            (self.today.replace(day=1) - datetime.timedelta(days=30 * i)).replace(day=1)
            for i in range(12)
        ]

    def _format_months(self, dates):
        return [date.strftime("%B de %Y").capitalize() for date in dates]

    def _build_path(self, date: datetime.date) -> str:
        meses_pt = [
            "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
            "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
        ]
        nome_mes = meses_pt[date.month - 1]
        return f"FIDCs Investidos/Relatórios/Planilhas de Monitoramentos/{date.year}/Relatórios {nome_mes}"

    def _clean_folders_on_exit(self):
        folders_to_clear = ["RAW", "PARSED"]
        for folder in folders_to_clear:
            folder_path = os.path.join(self.folder_root, folder)
            if os.path.exists(folder_path):
                for filename in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, filename)
                    try:
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                    except Exception as e:
                        self.logger.error(f"Erro ao apagar {file_path}: {e}")

    def _process_date(self, date: datetime.date):
        site_name = "FIDCS"
        extractor = Extractor(site_name)
        path_to_download = self._build_path(date)

        try:
            fidcs_files = extractor.list_files(path_file=path_to_download)
        except Exception as e:
            raise RuntimeError(f"Erro ao listar arquivos na pasta '{path_to_download}': {e}")

        raw_path = os.path.join(self.folder_root, "RAW")

        for fidc in fidcs_files:
            try:
                name_ = f"FIDC_{fidc}_{date.year}_{date.month:02}_01.xlsx"
                path_ = os.path.join(path_to_download, fidc)
                path_target = os.path.join(raw_path, name_)

                extractor.download_file(path_, name_, path_target)

                transformer = ExcelTransformer(path_target, fidc)
                transformer.transform_table(self.folder_root)
            except Exception as e:
                self.logger.error(f"O FIDC {fidc} será pulado devido ao erro: {e}")
                continue

        parsed_path = os.path.join(self.folder_root, "PARSED")
        grouper = Grouper()
        grouper.read_csvs(parsed_path)
        formatted_date = date.replace(day=1).strftime("%Y-%m-%d")
        return grouper.group_fidcs(formatted_date)

    def _display_folders(self):
        st.subheader("📁 Dados FIDCs disponíveis")
        st.markdown("---")

        if st.toggle("Mostrar pastas"):
            available_folders = ["RAW", "PARSED", "GROUPED"]
            selected_folder = st.selectbox("Pastas:", available_folders)

            if selected_folder:
                folder_path = os.path.join(self.folder_root, selected_folder)
                files = [
                    f for f in os.listdir(folder_path)
                    if os.path.isfile(os.path.join(folder_path, f))
                ]

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
                                with open(file_path, "rb") as file:
                                    st.download_button(
                                        label="📥 Baixar",
                                        data=file,
                                        file_name=filename,
                                        mime="application/octet-stream",
                                        key=f"{selected_folder}_{filename}"
                                    )

                    st.divider()
                    if st.button(f"📦 Baixar todos de {selected_folder}"):
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

    def run(self):
        st.set_page_config(page_title="Arquivos por Mês e Pasta", layout="wide")
        st.title("📂 FIDCs")

        col1, col2 = st.columns([1, 3])

        with col1:
            st.subheader("📅 Mês de Referência")
            date_options = self._build_reference_dates()
            formatted_months = self._format_months(date_options)

            selected_date_str = st.selectbox("Selecione o mês:", formatted_months)
            selected_date = date_options[formatted_months.index(selected_date_str)]

            if st.button("📤 Enviar data"):
                with st.spinner("Processando, aguarde..."):
                    try:
                        self._process_date(selected_date)
                        st.success("Processamento concluído com sucesso!")
                    except Exception as e:
                        st.error(f"Erro no processamento: {e}")

        with col2:
            self._display_folders()