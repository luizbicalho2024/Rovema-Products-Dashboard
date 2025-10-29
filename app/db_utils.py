# app/db_utils.py

import streamlit as st
import pandas as pd
from firebase_admin import credentials, firestore, auth
import firebase_admin
import random
import copy # Importação necessária para criar uma cópia mutável
from datetime import datetime

# --- Inicialização Única do Firebase ---

@st.cache_resource
def init_firebase():
    """Inicializa o SDK do Firebase Admin e retorna o cliente Firestore."""
    try:
        if "firebase" not in st.secrets:
            st.error("Erro: Seção 'firebase' ausente no secrets.toml.")
            return None
        
        # 🚨 CORREÇÃO CRÍTICA DE MUTABILIDADE: Cria uma cópia profunda (deepcopy) do dicionário
        cred_dict = copy.deepcopy(st.secrets["firebase"]["credentials"])
        
        # 🚨 CORREÇÃO DA CHAVE PRIVADA (Sanitização)
        if 'private_key' in cred_dict:
            # Substitui o escape de nova linha lido (que pode ser "\\n") pelo caractere "\n".
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
            
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred, name='BI_COMERCIAL_APP')
        
        return firestore.client()
    except Exception as e:
        st.error(f"Erro Crítico ao Inicializar Firebase. Verifique 'secrets.toml'. Detalhes: {e}")
        return None

# Inicializa o cliente Firestore globalmente no módulo
db = init_firebase()


# --- Funções de ETL (Ingestão) ---

def ingest_csv_data(db_client, file_content, collection_name):
    """Lê CSV, normaliza (incluindo correção de SyntaxWarning) e salva no Firestore."""
    if db_client is None:
        st.error("Não foi possível conectar ao banco de dados.")
        return

    try:
        df = pd.read_csv(file_content, delimiter=';')
        
        # Normalização de colunas
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('ã', 'a').str.replace('ç', 'c').str.replace('ó', 'o').str.replace('ê', 'e').str.replace('.', '', regex=False)
        
        # Tratamento de valores (incluindo a correção do SyntaxWarning)
        valor_cols = []
        if 'bionio' in collection_name:
            valor_cols = ['valor_do_beneficio', 'valor_total_do_pedido']
        elif 'rovema_pay' in collection_name:
            valor_cols = ['bruto', 'pagoadquirente', 'liquido', 'antecipado', 'taxa_adquirente', 'taxa_cliente', 'mdr', 'spread']

        for col in valor_cols:
            if col in df.columns:
                # Usa r'(\d+\.?\d*)' (raw string) para evitar SyntaxWarning e sanear
                df[col] = (df[col].astype(str).str.replace('.', '', regex=False)
                                 .str.replace(',', '.', regex=False)
                                 .str.extract(r'(\d+\.?\d*)', expand=False) 
                                 .astype(float))
        
        # Inserção de Novos Dados em lotes (Batch Write)
        docs_to_delete = db_client.collection(collection_name).limit(500).stream() 
        batch_delete = db_client.batch()
        for doc in docs_to_delete:
            batch_delete.delete(doc.reference)
        batch_delete.commit()
        st.success("Limpeza parcial concluída.")
        
        data_to_save = df.to_dict('records')
        batch_size = 400 
        
        for i in range(0, len(data_to_save), batch_size):
            batch_insert = db_client.batch()
            batch_data = data_to_save[i:i + batch_size]
            
            for j, record in enumerate(batch_data):
                doc_ref = db_client.collection(collection_name).document(f'{collection_name}_rec_{i+j}')
                batch_insert.set(doc_ref, record)
            
            batch_insert.commit()

        db_client.collection(collection_name).document('metadata').set({
            'last_updated': firestore.SERVER_TIMESTAMP,
            'record_count': len(data_to_save)
        })

        st.success(f"Dados de **{collection_name.upper()}** ({len(data_to_save)} registros) salvos com sucesso!")
        
    except Exception as e:
        st.error(f"Erro ao processar e salvar dados: {e}")


# --- Funções de Acesso a Dados (BI) ---

@st.cache_data(ttl=600) 
def get_combined_data(db_client):
    """Puxa e combina todos os dados (Bionio, Rovema Pay, API Cache) para o BI."""
    
    if db_client is None:
         return pd.DataFrame()

    # 1. Puxar Bionio Data 
    df_bionio = get_firestore_data('bionio_data')
    if not df_bionio.empty:
        df_bionio['origem'] = 'Bionio'
        df_bionio['receita'] = df_bionio.get('valor_total_do_pedido', 0)
        df_bionio['produto'] = df_bionio.get('nome_do_beneficio', 'Bionio - Pedido')
    
    # 2. Puxar Rovema Pay Data 
    df_rovema = get_firestore_data('rovema_pay_data')
    if not df_rovema.empty:
        df_rovema['origem'] = 'Rovema Pay'
        df_rovema['receita'] = df_rovema.get('liquido', 0)
        df_rovema['produto'] = df_rovema.get('bandeira', 'Rovema Pay - Vendas')

    # 3. Puxar API Cache (ELIQ/ASTO)
    api_cache_doc = db_client.collection('api_cache').document('last_run').get()
    api_data = api_cache_doc.to_dict().get('data_sample', []) if api_cache_doc.exists else []
    df_api = pd.DataFrame(api_data)
    if not df_api.empty:
        df_api['receita'] = df_api.get('valor_bruto', 0)
        df_api['produto'] = df_api.get('produto_api', df_api['origem'])
        
    # 4. Combinação e Normalização
    df_combined = pd.concat([df_bionio, df_rovema, df_api], ignore_index=True)
    df_combined['receita'] = df_combined['receita'].fillna(0)
    
    # 5. MOCK/JOIN de Atribuição de Consultor
    df_carteira = get_firestore_data('carteira_clientes')
    
    if not df_carteira.empty and not df_combined.empty:
         valid_uids = df_carteira['consultor_uid'].unique()
         if valid_uids.size > 0:
            df_combined['consultor_uid'] = df_combined.apply(lambda x: random.choice(valid_uids) if random.random() < 0.6 else None, axis=1)

    return df_combined

def get_consultores_from_db(db_client):
    """Recupera lista de consultores para filtros e gestão."""
    if db_client is None:
        return {}
    consultores_ref = db_client.collection('consultores').stream()
    return {doc.to_dict()['nome']: doc.id for doc in consultores_ref}


def get_firestore_data(collection_name):
    """Busca todos os documentos de uma coleção do Firestore."""
    if db is None:
        return pd.DataFrame()
    
    try:
        # Usa a variável 'db' inicializada no topo do módulo
        docs = db.collection(collection_name).stream()
        data = [doc.to_dict() for doc in docs]
        return pd.DataFrame(data)
        
    except Exception as e:
        st.error(f"Erro ao buscar dados da coleção '{collection_name}': {e}")
        return pd.DataFrame()
