import streamlit as st
import os
import zipfile
import io
import datetime

from classes.extractor import Extractor
from classes.exceltransformer import ExcelTransformer
from classes.grouper import Grouper
from classes.logger import LogFIDC

logger = LogFIDC()


def construir_path(data: datetime.date) -> str:
    meses_pt = [
        "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
    ]
    ano = data.year
    nome_mes = meses_pt[data.month - 1]
    return f"FIDCs Investidos/Relatórios/Planilhas de Monitoramentos/{ano}/Relatórios {nome_mes}"


def processar_data(data: datetime.date):
    site_name = "FIDCS"
    extractor = Extractor(site_name)

    path_to_download = construir_path(data)
    try:
        fidcs_files = extractor.list_files(path_file=path_to_download)
    except Exception as e:
        raise RuntimeError(f"Erro ao listar arquivos na pasta '{path_to_download}': {e}")

    for fidc in fidcs_files:
        try:
            name_ = f"FIDC_{fidc}_{data.year}_{data.month:02}_01.xlsx"
            path_ = f"{path_to_download}/{fidc}"
            path_target = f"./RAW/{name_}"

            extractor.download_file(path_, name_, path_target)

            transformer = ExcelTransformer(path_target, fidc)
            transformer.transform_table()
        except Exception as e:
            logger.error(f"O FIDC {fidc} será pulado devido ao erro: {e}")
            continue

    grouper = Grouper()
    grouper.read_csvs("./PARSED")
    data_formatada = data.replace(day=1).strftime("%Y-%m-%d")
    grouped_data = grouper.group_fidcs(data_formatada)
    return grouped_data


# --- STREAMLIT APP ---
st.set_page_config(page_title="Arquivos por Mês e Pasta", layout="wide")
st.title("📂 Seleção de Arquivos")

# Layout principal: data à esquerda, pastas à direita
col1, col2 = st.columns([1, 3])

with col1:
    st.subheader("📅 Mês de Referência")
    hoje = datetime.date.today()
    opcoes_data = [
        (hoje.replace(day=1) - datetime.timedelta(days=30 * i)).replace(day=1)
        for i in range(12)
    ]
    meses_formatados = [data.strftime("%B de %Y").capitalize() for data in opcoes_data]
    data_selecionada = st.selectbox("Selecione o mês:", meses_formatados)
    data_obj = opcoes_data[meses_formatados.index(data_selecionada)]

    if st.button("📤 Enviar data"):
        try:
            resultado = processar_data(data_obj)
            st.success("Processamento concluído com sucesso!")
        except Exception as e:
            st.error(f"Erro no processamento: {e}")

with col2:
    st.subheader("📁 Pastas disponíveis")
    st.markdown("---")  # linha divisória
    mostrar_pastas = st.toggle("Mostrar pastas")

    PASTA_RAIZ = os.path.abspath(os.path.join(os.getcwd(), ".."))

    if mostrar_pastas:
        pastas_disponiveis = ["PARSED", "RAW", "GROUPED"]

        pasta_selecionada = st.selectbox("Selecione uma pasta:", pastas_disponiveis)

        if pasta_selecionada:
            caminho_completo = os.path.join(PASTA_RAIZ, pasta_selecionada)
            arquivos = [f for f in os.listdir(caminho_completo) if os.path.isfile(os.path.join(caminho_completo, f))]

            st.markdown(f"### 📂 Arquivos da pasta: `{pasta_selecionada}`")

            if not arquivos:
                st.info("Nenhum arquivo nessa pasta.")
            else:
                colunas_por_linha = 3
                linhas = [arquivos[i:i + colunas_por_linha] for i in range(0, len(arquivos), colunas_por_linha)]

                for linha in linhas:
                    cols = st.columns(colunas_por_linha)
                    for idx, nome in enumerate(linha):
                        caminho_arquivo = os.path.join(caminho_completo, nome)
                        with cols[idx]:
                            st.markdown(f"**{nome}**")
                            with open(caminho_arquivo, "rb") as file:
                                st.download_button(
                                    label="📥 Baixar",
                                    data=file,
                                    file_name=nome,
                                    mime="application/octet-stream",
                                    key=f"{pasta_selecionada}_{nome}"
                                )

                st.divider()

                if st.button(f"📦 Baixar todos de {pasta_selecionada}"):
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, "w") as zipf:
                        for nome in arquivos:
                            zipf.write(os.path.join(caminho_completo, nome), arcname=nome)
                    st.download_button(
                        label="⬇️ Clique aqui para baixar o ZIP",
                        data=zip_buffer.getvalue(),
                        file_name=f"{pasta_selecionada}.zip",
                        mime="application/zip",
                        key=f"download_zip_{pasta_selecionada}"
                    )
