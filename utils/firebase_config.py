import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore, auth
import json
import os

# Carrega as credenciais do Streamlit Secrets
try:
    creds_dict = st.secrets["firebase_service_account"]
    firebase_config = st.secrets["firebase_web_config"]
except KeyError as e:
    st.error(f"ERRO DE CONFIGURAÇÃO: Secret '{e.args[0]}' não encontrado.")
    st.error("Por favor, verifique se o painel de Secrets no Streamlit Cloud está 100% correto.")
    st.stop()

@st.cache_resource
def init_firebase_admin():
    """
    Inicializa o SDK Admin do Firebase.
    """
    try:
        # Verifica se o app já foi inicializado
        firebase_admin.get_app()
    except ValueError:
        # ERRO DE CONFIGURAÇÃO MAIS COMUM:
        # Se creds_dict não for um dicionário, o Streamlit leu o TOML errado.
        if not isinstance(creds_dict, dict):
            st.error("ERRO DE CONFIGURAÇÃO CRÍTICO!")
            st.error("Os 'Secrets' do [firebase_service_account] estão formatados como TEXTO, não como um DICIONÁRIO.")
            st.error("SOLUÇÃO: Verifique o PASSO 1 da minha resposta (usar a chave privada em linha única).")
            st.stop()
            
        try:
            # Inicializa o app usando o DICIONÁRIO de credenciais
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
