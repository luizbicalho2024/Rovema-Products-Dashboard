# utils/firebase_config.py
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore, auth
import json
import os

# Define o nome do arquivo temporário
TEMP_SERVICE_ACCOUNT_FILE = "temp_service_account.json"

# Carrega a configuração web (para login)
firebase_config = st.secrets["firebase_web_config"]

@st.cache_resource
def init_firebase_admin():
    """
    Inicializa o SDK Admin do Firebase de forma robusta,
    escrevendo as credenciais em um arquivo JSON temporário
    para contornar bugs de parsing do Streamlit Secrets.
    """
    try:
        # Verifica se o app já foi inicializado
        firebase_admin.get_app()
    except ValueError:
        # Se não foi inicializado, faz a inicialização
        creds_dict = st.secrets["firebase_service_account"]
        
        # ETAPA DE VERIFICAÇÃO CRUCIAL:
        # Verifica se o Streamlit leu os secrets como um dicionário
        if not isinstance(creds_dict, dict):
            st.error("ERRO DE CONFIGURAÇÃO CRÍTICO!")
            st.error("Os 'Secrets' do [firebase_service_account] estão formatados como TEXTO, não como um DICIONÁRIO.")
            st.error("Isso acontece se você colar os secrets incorretamente no painel do Streamlit Cloud.")
            st.error("Por favor, apague seus secrets e cole o bloco TOML da minha resposta anterior novamente.")
            st.stop()
            
        try:
            # Escreve o dicionário de credenciais em um arquivo JSON temporário
            with open(TEMP_SERVICE_ACCOUNT_FILE, "w") as f:
                json.dump(creds_dict, f)

            # Inicializa o Firebase Admin SDK usando o CAMINHO DO ARQUIVO
            cred = credentials.Certificate(TEMP_SERVICE_ACCOUNT_FILE)
            firebase_admin.initialize_app(cred)
            
            # (Opcional) Limpa o arquivo temporário após a inicialização
            # if os.path.exists(TEMP_SERVICE_ACCOUNT_FILE):
            #    os.remove(TEMP_SERVICE_ACCOUNT_FILE)

        except Exception as e:
            st.error(f"Falha ao inicializar o Firebase Admin com o arquivo temporário: {e}")
            st.stop()

    return firestore.client()

def get_db():
    """Retorna a instância do cliente Firestore."""
    return init_firebase_admin()

def get_admin_auth():
    """Retorna o módulo de autenticação do Admin SDK."""
    init_firebase_admin() # Garante que o app admin está inicializado
    return auth
