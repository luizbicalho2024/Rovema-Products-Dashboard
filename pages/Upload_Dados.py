import streamlit as st
import pandas as pd
# IMPORT CORRIGIDO: Agora importa a função de salvamento do Firestore
from fire_admin import save_processed_data_to_firestore, log_event 
from utils.data_processing import process_uploaded_file

def upload_data_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    st.title("📤 Upload de Dados de Bionio e Rovema Pay")
    log_event("VIEW_UPLOAD_DATA", "Acessando a página de upload de dados.")

    st.info("Envie aqui os arquivos CSV/Excel. Os dados serão **processados e salvos diretamente no Firestore** (Banco de Dados).")

    col_u1, col_u2 = st.columns(2)
    df_to_save = None 
    
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
                    success_proc, message_proc, df_preview = process_uploaded_file(uploaded_file, product)
                
                # FLUXO DE CONTROLE CORRIGIDO: Verifica a falha imediatamente
                if not success_proc or df_preview is None:
                    st.error(f"Falha no processamento: {message_proc}")
                    return

                # Se chegou aqui, o processamento foi um sucesso e df_preview é um DataFrame válido
                df_to_save = df_preview.copy() 
                
                if df_to_save.empty:
                    st.warning("O arquivo foi processado, mas não contém dados válidos após a limpeza.")
                    return

                # 2. Salvamento no Firestore
                with st.spinner(f"Salvando {len(df_to_save)} registros no Firestore..."):
                    success_save, message_save = save_processed_data_to_firestore(product, df_to_save)
                
                if success_save:
                    st.success(message_save)
                    st.cache_data.clear() 
                else:
                    st.error(message_save)
                
                # 3. Mostrar Preview
                with col_u2:
                    st.subheader(f"Preview (Primeiras 10 linhas processadas)")
                    st.dataframe(df_to_save.head(10), use_container_width=True)
                    st.info("Dados processados e salvos no Banco de Dados.")

            else:
                st.warning("Por favor, selecione um arquivo para upload.")

if st.session_state.get('authenticated'):
    upload_data_page()
else:
    pass
