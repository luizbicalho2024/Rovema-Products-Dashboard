# app/db_utils.py

import streamlit as st
import pandas as pd
from firebase_admin import credentials, firestore
import firebase_admin
from datetime import datetime
from firebase_admin import auth # Para funções de autenticação/gestão de usuários
import base64
import random
import io # Para lidar com a leitura de arquivos em memória

# --- Inicialização Única do Firebase ---

@st.cache_resource
def init_firebase():
    """Inicializa o SDK do Firebase Admin e retorna o cliente Firestore.
    
    Aplica a correção para chaves privadas de serviço multilinhas lidas do secrets.toml.
    """
    try:
        if "firebase" not in st.secrets:
            st.error("Erro: Seção 'firebase' ausente no secrets.toml.")
            return None
        
        cred_dict = st.secrets["firebase"]["credentials"]
        
        # 🚨 CORREÇÃO CRÍTICA PARA CHAVE PRIVADA MULTILINHA 🚨
        # A chave privada deve ter os caracteres de nova linha '\n' corretamente
        # formatados para que o SDK do Firebase a reconheça como PEM válido.
        if 'private_key' in cred_dict:
            # Substitui o escape de nova linha lido (que pode ser "\\n") pelo
            # caractere de nova linha literal ("\n").
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
            
        if not firebase_admin._apps:
            # Inicializa a app com o nome 'BI_COMERCIAL_APP' para evitar conflitos
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred, name='BI_COMERCIAL_APP')
        
        # Retorna o cliente Firestore
        return firestore.client()
    except Exception as e:
        st.error(f"Erro Crítico ao Inicializar Firebase. Verifique 'secrets.toml'. Detalhes: {e}")
        # Retorna None para que as funções que dependem do cliente Firestore
        # possam lidar com a falha de inicialização.
        return None

# --- Funções de Utilitário Comuns ---

# Inicializa o cliente Firestore, garantindo que seja feito apenas uma vez
db = init_firebase()

def get_firestore_data(collection_name):
    """
    Busca todos os documentos de uma coleção do Firestore.
    
    Args:
        collection_name (str): O nome da coleção.
        
    Returns:
        pandas.DataFrame: Um DataFrame com os dados, ou um DataFrame vazio em caso de erro.
    """
    if db is None:
        return pd.DataFrame()
    
    try:
        docs = db.collection(collection_name).stream()
        data = [doc.to_dict() for doc in docs]
        
        # Adiciona o ID do documento como uma coluna, se necessário
        # for item in data:
        #     item['id'] = doc.id
            
        return pd.DataFrame(data)
        
    except Exception as e:
        st.error(f"Erro ao buscar dados da coleção '{collection_name}': {e}")
        return pd.DataFrame()

# --- Exemplo de Função de Autenticação (Ajustar conforme o seu 'auth.py') ---

def get_user_by_email(email):
    """
    Busca um usuário no Firebase Auth pelo email.
    
    Returns:
        Um objeto UserRecord se o usuário for encontrado, ou None.
    """
    if not firebase_admin._apps:
        return None
        
    try:
        # Pega a aplicação Firebase inicializada para usar o módulo Auth
        app = firebase_admin.get_app('BI_COMERCIAL_APP')
        user = auth.get_user_by_email(email, app=app)
        return user
    except Exception:
        # Retorna None se o usuário não for encontrado (AUTH_USER_NOT_FOUND)
        return None

# Você pode adicionar outras funções aqui, como:
# def save_data(collection_name, data): ...
# def update_user_profile(uid, profile_data): ...
