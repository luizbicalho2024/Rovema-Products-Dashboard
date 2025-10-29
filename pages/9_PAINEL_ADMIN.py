import streamlit as st
from fire_admin import get_db
import pandas as pd
import etl_processor  # Importa seu script de lógica

st.set_page_config(layout="centered", page_title="Admin")

# --- Verificação de Login ---
if "user" not in st.session_state:
    st.error("🔒 Por favor, faça o login primeiro."); st.stop()

# --- Verificação de Permissão (SIMPLES) ---
# Você deve ter uma lógica de acesso mais robusta (como no seu repo)
if st.session_state.user not in st.secrets.get("admins", ["admin@email.com"]):
    st.error("🚫 Acesso Negado. Esta página é restrita.")
    st.stop()

st.title("⚙️ Painel de Administração e Dados")
db = get_db()

# --- 1. Upload de CSVs (Bionio / RovemaPay) ---
st.header("1. Upload de CSVs (Bionio / RovemaPay)")
st.info("Faça o upload dos CSVs aqui. Isso apenas salva os dados 'brutos'.")

def upload_csv_to_firestore(file, collection_name, key_column):
    """Lê um CSV e salva no Firestore, usando uma coluna como ID."""
    try:
        df = pd.read_csv(file, delimiter=';', dtype=str)
        st.dataframe(df.head())
        
        if st.button(f"Confirmar Upload {collection_name}", key=f"btn_{collection_name}"):
            with st.spinner(f"Carregando {len(df)} linhas para {collection_name}..."):
                # Limpar coleção antiga (CUIDADO!)
                docs = db.collection(collection_name).limit(5000).stream()
                for doc in docs:
                    doc.reference.delete()
                
                # Inserir novos dados
                batch = db.batch()
                count = 0
                for index, row in df.iterrows():
                    doc_id = str(row.get(key_column, f"idx_{index}"))
                    if not doc_id:
                        doc_id = f"idx_{index}"
                    doc_ref = db.collection(collection_name).document(doc_id)
                    batch.set(doc_ref, row.to_dict())
                    count += 1
                    # Commits em lotes de 500
                    if count % 500 == 0:
                        batch.commit()
                        batch = db.batch()
                
                batch.commit()
                st.success(f"{count} linhas carregadas para {collection_name}!")
    except Exception as e:
        st.error(f"Erro no upload para {collection_name}: {e}")

# Upload RovemaPay
uploaded_rovema = st.file_uploader("CSV RovemaPay", type="csv", key="rovema")
if uploaded_rovema:
    upload_csv_to_firestore(uploaded_rovema, "raw_rovemapay", "ID Parcela")

# Upload Bionio
uploaded_bionio = st.file_uploader("CSV Bionio", type="csv", key="bionio")
if uploaded_bionio:
    upload_csv_to_firestore(uploaded_bionio, "raw_bionio", "Número do pedido")


# --- 2. Processamento de Dados ---
st.header("2. Processamento de Dados")
st.warning("Este processo é LENTO (pode levar 5-15 min) e pode quebrar o app.")
st.info("Execute após buscar APIs ou fazer upload de CSVs.")

if st.button("EXECUTAR ATUALIZAÇÃO GLOBAL DOS DADOS", type="primary"):
    st.cache_data.clear() # Limpa o cache do dashboard
    
    with st.spinner("Passo 1/4: Buscando dados da API ASTO..."):
        etl_processor.fetch_asto_data(db)
    
    with st.spinner("Passo 2/4: Buscando dados da API ELIQ..."):
        etl_processor.fetch_eliq_data(db)
    
    with st.spinner("Passo 3/4: Lendo dados brutos dos CSVs do Firestore..."):
        dataframes = etl_processor.read_raw_csv_data(db)
    
    with st.spinner("Passo 4/4: Agregando TODOS os KPIs... (Isso é pesado!)"):
        etl_processor.aggregate_and_save_kpis(db, dataframes)
    
    st.success("TUDO PRONTO! Os dashboards foram atualizados.")
    st.balloons()
