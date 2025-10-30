import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore, auth

# Carrega as credenciais do Streamlit Secrets
creds_dict = st.secrets["firebase_service_account"]
firebase_config = st.secrets["firebase_web_config"]

@st.cache_resource
def init_firebase_admin():
    """
    Inicializa o SDK Admin do Firebase para operações de backend 
    (Firestore, criação de usuários).
    """
    try:
        # Verifica se já foi inicializado
        firebase_admin.get_app()
    except ValueError:
        # Inicializa o app se não existir
        cred = credentials.Certificate(creds_dict)
        firebase_admin.initialize_app(cred)
    
    return firestore.client()

def get_db():
    """Retorna a instância do cliente Firestore."""
    return init_firebase_admin()

def get_admin_auth():
    """Retorna o módulo de autenticação do Admin SDK."""
    init_firebase_admin() # Garante que o app admin está inicializado
    return auth
