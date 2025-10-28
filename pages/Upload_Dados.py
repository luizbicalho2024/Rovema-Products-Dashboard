import streamlit as st
import pandas as pd
from fire_admin import save_processed_data_to_firestore, log_event
from utils.data_processing import process_uploaded_file

def upload_data_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    st.title("📤 Upload de Dados de Bionio e Rovema Pay")
    log_event("VIEW_UPLOAD_DATA", "Acessando a página de upload de dados.")

    st.info("Os dados serão **processados, validados e salvos integralmente no Firestore** (todas as linhas).")

    col_u1, col_u2 = st.columns(2)
    
    with col_u1:
        st.subheader("Configuração do Upload")
        product = st.selectbox("Produto", ['Bionio', 'RovemaPay'], help="Selecione o produto ao qual o relatório se refere.")
        uploaded_file = st.file_uploader(
            f"Fazer Upload do Relatório {product} (CSV ou XLSX)",
            type=['csv', 'xlsx']
        )
        
        if st.button(f"Processar e Salvar no Firestore"):
            if uploaded_file is not None:
                
                # 1. Processamento e Validação
                with st.spinner(f"Validando e processando arquivo de {product}..."):
                    success_proc, message_proc, df_processed = process_uploaded_file(uploaded_file, product)
                
                if not success_proc or df_processed is None:
                    st.error(f"Falha crítica no processamento: {message_proc}")
                    log_event("UPLOAD_FAIL_CRITICAL", f"Falha na estrutura do arquivo: {uploaded_file.name}. Erro: {message_proc}")
                    return # Para a execução

                if df_processed.empty:
                    st.warning("O arquivo foi processado com sucesso, mas não contém dados válidos após a limpeza (0 linhas). Verifique se os dados estão no formato esperado. Nenhuma linha foi salva.")
                    log_event("UPLOAD_WARN_EMPTY", f"Arquivo {uploaded_file.name} processado, mas resultou em 0 linhas.")
                    return # Para a execução
                
                total_rows = len(df_processed)
                log_event("UPLOAD_PROCESS_SUCCESS", f"Arquivo {uploaded_file.name} processado. {total_rows} linhas prontas para salvar.")

                # 2. Salvamento no Firestore (Salva TODAS as LINHAS do DataFrame limpo)
                with st.spinner(f"Salvando {total_rows} registros de {product} no Firestore..."):
                    success_save, message_save = save_processed_data_to_firestore(product, df_processed)
                
                if success_save:
                    # Mensagem de sucesso melhorada
                    st.success(f"Sucesso! {message_save}")
                    # Limpa o cache para forçar o Dashboard a buscar os novos dados
                    st.cache_data.clear() 
                    log_event("UPLOAD_SAVE_SUCCESS", f"Dados de {uploaded_file.name} salvos. {total_rows} linhas.")
                    
                    # 3. Mostrar Preview
                    with col_u2:
                        st.subheader(f"Preview (Primeiras 10 de {total_rows} linhas salvas)")
                        st.dataframe(df_processed.head(10), use_container_width=True)
                        
                else:
                    st.error(f"Falha ao salvar no Banco de Dados: {message_save}")
                    log_event("UPLOAD_SAVE_FAIL", f"Falha ao salvar dados de {uploaded_file.name}. Erro: {message_save}")

            else:
                st.warning("Por favor, selecione um arquivo para upload.")

if st.session_state.get('authenticated'):
    upload_data_page()
else:
    pass
