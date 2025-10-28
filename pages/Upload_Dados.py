import streamlit as st
from fire_admin import upload_file_and_store_ref, log_event
from utils.data_processing import process_uploaded_file
import pandas as pd

def upload_data_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    st.title("📤 Upload de Dados de Bionio e Rovema Pay")
    log_event("VIEW_UPLOAD_DATA", "Acessando a página de upload de dados.")

    st.info("Envie aqui os arquivos CSV/Excel de Bionio ou Rovema Pay para que os dados sejam disponibilizados no Dashboard.")

    col_u1, col_u2 = st.columns(2)

    with col_u1:
        st.subheader("Configuração do Upload")
        product = st.selectbox("Produto", ['Bionio', 'RovemaPay'])
        uploaded_file = st.file_uploader(
            f"Fazer Upload do Relatório {product} (CSV ou XLSX)",
            type=['csv', 'xlsx']
        )
        
        if st.button(f"Processar e Armazenar no Firebase"):
            if uploaded_file is not None:
                # 1. Processamento e Validação (Prévia)
                with st.spinner(f"Processando arquivo de {product}..."):
                    success_proc, message_proc, df_preview = process_uploaded_file(uploaded_file, product)
                    
                if not success_proc:
                    st.error(f"Falha no processamento: {message_proc}")
                    return

                # 2. Upload para o Firebase Storage
                with st.spinner(f"Enviando arquivo para o Firebase Storage..."):
                    success_upload, message_upload = upload_file_and_store_ref(uploaded_file, product)
                
                if success_upload:
                    st.success(message_upload)
                else:
                    st.error(message_upload)
                
                # 3. Mostrar Preview
                with col_u2:
                    st.subheader(f"Preview e Validação ({product})")
                    if df_preview is not None and not df_preview.empty:
                        st.dataframe(df_preview.head(10), use_container_width=True)
                        st.success("Processamento inicial bem-sucedido. O arquivo foi armazenado.")

            else:
                st.warning("Por favor, selecione um arquivo para upload.")

upload_data_page()
