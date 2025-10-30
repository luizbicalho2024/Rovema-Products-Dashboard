import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore, auth
import json
import os

# Carrega as credenciais do Streamlit Secrets
try:
    creds_input = st.secrets["firebase_service_account"]
    firebase_config = st.secrets["firebase_web_config"]
except KeyError as e:
    st.error(f"ERRO DE CONFIGURAÇÃO: Secret '{e.args[0]}' não encontrado.")
    st.error("Por favor, verifique se o painel de Secrets no Streamlit Cloud está 100% correto.")
    st.stop()

@st.cache_resource
def init_firebase_admin():
    """
    Inicializa o SDK Admin do Firebase de forma robusta,
    corrigindo automaticamente erros de parsing do Streamlit Secrets.
    """
    try:
        # Verifica se o app já foi inicializado
        firebase_admin.get_app()
    except ValueError:
        creds_dict = None
        
        # --- TENTATIVA DE CORREÇÃO AUTOMÁTICA ---
        if isinstance(creds_input, dict):
            # Caso 1 (Ideal): O TOML foi lido corretamente como um dicionário.
            creds_dict = creds_input
        
        elif isinstance(creds_input, str):
            # Caso 2 (O Bug): O TOML foi lido incorretamente como uma string.
            st.warning("Detectado erro de parsing nos Secrets. Tentando correção automática...")
            try:
                # Tenta converter a string (que se parece com um dict/json) para um dict
                creds_dict = json.loads(creds_input)
            except json.JSONDecodeError:
                st.error("ERRO CRÍTICO DE SECRET: O secret [firebase_service_account] é uma string, mas não é um JSON válido.")
                st.error("Isso é 100% um erro de copiar/colar no painel do Streamlit Cloud.")
                st.error("Por favor, apague e cole o bloco TOML da minha resposta anterior (com a 'private_key' em linha única).")
                st.stop()
        
        if not creds_dict:
            st.error("ERRO CRÍTICO DE SECRET: Não foi possível carregar as credenciais do [firebase_service_account].")
            st.stop()

        # --- FIM DA CORREÇÃO AUTOMÁTICA ---
        
        try:
            # Inicializa o app usando o DICIONÁRIO de credenciais (original ou corrigido)
            cred = credentials.Certificate(creds_dict)
            firebase_admin.initialize_app(cred)

        except Exception as e:
            st.error(f"Falha ao inicializar o Firebase Admin. Erro: {e}")
            st.error("Isso quase sempre significa que a 'private_key' está formatada incorretamente nos Secrets.")
            st.stop()

    return firestore.client()

def get_db():
    """Retorna a instância do cliente Firestore."""
    return init_firebase_admin()

def get_admin_auth():
    """Retorna o módulo de autenticação do Admin SDK."""
    init_firebase_admin() # Garante que o app admin está inicializado
    return auth
