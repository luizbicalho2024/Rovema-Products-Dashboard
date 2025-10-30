import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore, auth
import json
import os

# Carrega as credenciais do Streamlit Secrets
try:
    # Carrega a string JSON completa do admin
    service_account_json_str = st.secrets["FIREBASE_SERVICE_ACCOUNT_JSON"]
    
    # Carrega a configuração web (para login)
    firebase_config_dict = st.secrets["firebase_config"]
    
except KeyError as e:
    st.error(f"ERRO DE CONFIGURAÇÃO: Secret '{e.args[0]}' não encontrado.")
    st.error("Por favor, verifique se o painel de Secrets no Streamlit Cloud está 100% correto (Ação 1).")
    st.stop()

@st.cache_resource
def init_firebase_admin():
    """
    Inicializa o SDK Admin do Firebase de forma robusta,
    lendo o JSON de serviço como uma string única para
    contornar bugs de parsing do Streamlit Secrets.
    """
    try:
        # Verifica se o app já foi inicializado
        firebase_admin.get_app()
    except ValueError:
        try:
            # Converte a string JSON (lida dos Secrets) em um dicionário Python
            creds_dict = json.loads(service_account_json_str)
            
            # Inicializa o app usando o DICIONÁRIO de credenciais
            cred = credentials.Certificate(creds_dict)
            firebase_admin.initialize_app(cred)

        except json.JSONDecodeError:
            st.error("ERRO CRÍTICO DE SECRET: 'FIREBASE_SERVICE_ACCOUNT_JSON' não é um JSON válido.")
            st.error("Isso é 100% um erro de copiar/colar no painel do Streamlit Cloud.")
            st.stop()
        except Exception as e:
            st.error(f"Falha ao inicializar o Firebase Admin. Erro: {e}")
            st.stop()

    return firestore.client()

@st.cache_resource
def init_pyrebase():
    """
    Inicializa o Pyrebase para operações de cliente (Login).
    """
    # Importa pyrebase aqui para evitar que ele seja importado 
    # se a inicialização do admin falhar.
    import pyrebase
    
    # Adiciona a databaseURL se não estiver presente (Pyrebase precisa disso)
    if "databaseURL" not in firebase_config_dict:
         firebase_config_dict["databaseURL"] = f"https://{firebase_config_dict['projectId']}-default-rtdb.firebaseio.com/"
         
    return pyrebase.initialize_app(firebase_config_dict)

def get_db():
    """Retorna a instância do cliente Firestore."""
    return init_firebase_admin()

def get_admin_auth():
    """Retorna o módulo de autenticação do Admin SDK."""
    init_firebase_admin() # Garante que o app admin está inicializado
    return auth

def get_auth_client():
    """Retorna a instância do cliente de autenticação Pyrebase."""
    return init_pyrebase().auth()
