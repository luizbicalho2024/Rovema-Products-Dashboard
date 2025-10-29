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
            cred = credentials.Certificate(st.secrets["firebase_credentials"])
            firebase_admin.initialize_app(cred)
    except Exception as e:
        st.error(f"Erro Crítico ao inicializar o Firebase: {e}")
        st.stop()
    
    return firestore.client()

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
    """
    Carrega um documento de KPIs pré-calculados do Firestore.
    Esta é a função de leitura de dados RÁPIDA para o dashboard.
    """
    try:
        db = get_db()
        kpis_ref = db.collection("dashboard_agregado").document(document_name).get()
        if kpis_ref.exists:
            return kpis_ref.to_dict()
        else:
            # Retorna dict vazio se o doc não existir
            return {} 
    except Exception as e:
        st.error(f"Erro ao carregar KPIs: {e}")
        return {}
