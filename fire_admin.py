# fire_admin.py
import streamlit as st
import firebase_admin
from firebase_admin import credentials, auth, firestore

def initialize_firebase():
    """
    Inicializa o app Firebase Admin SDK usando os segredos do Streamlit.
    """
    try:
        # Verifica se já foi inicializado
        if not firebase_admin._apps:
            # ---> AQUI ESTÁ A LINHA CRÍTICA <---
            # st.secrets["firebase_credentials"] DEVE ser um objeto tipo dict
            cred = credentials.Certificate(st.secrets["firebase_credentials"]) 
            # ---> FIM DA LINHA CRÍTICA <---

            firebase_admin.initialize_app(cred)
            
    except ValueError as e: # Captura especificamente o erro de certificado inválido
        st.error(f"Erro Crítico ao inicializar o Firebase (Certificado Inválido): {e}")
        st.error("Verifique se a seção [firebase_credentials] no secrets.toml está formatada corretamente como um JSON válido.")
        st.stop()
    except Exception as e:
        st.error(f"Erro Crítico Geral ao inicializar o Firebase: {e}")
        st.stop()
    
    return firestore.client() 

# O resto do arquivo fire_admin.py continua igual...
def get_auth():
    """Retorna o cliente de autenticação do Firebase."""
    if not firebase_admin._apps:
        initialize_firebase()
    return auth

def get_db():
    """Retorna o cliente do Firestore."""
    if not firebase_admin._apps:
        return initialize_firebase()
    return firestore.client()

@st.cache_data(ttl=600) # Cache de 10 minutos
def load_kpis(document_name="kpis_consolidados"):
    # ... (o resto da função load_kpis) ...
    try:
        db = get_db()
        kpis_ref = db.collection("dashboard_agregado").document(document_name).get()
        if kpis_ref.exists:
            return kpis_ref.to_dict()
        else:
            return {} 
    except Exception as e:
        st.error(f"Erro ao carregar KPIs: {e}")
        return {}
