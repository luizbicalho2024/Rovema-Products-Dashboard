import streamlit as st
from utils.firebase_config import get_db
from datetime import datetime
import traceback

def log_audit(action, details: dict = None):
    """
    Registra um evento de auditoria no Firestore.
    """
    try:
        db = get_db()
        
        user_email = st.session_state.get("user_email", "system")
        user_uid = st.session_state.get("user_uid", "N/A")
        
        log_entry = {
            "user_email": user_email,
            "user_uid": user_uid,
            "action": action,
            "timestamp": datetime.now(),
            "details": details or {}
        }
        
        # Adiciona o log a uma coleção 'audit_logs'
        db.collection("audit_logs").add(log_entry)
        
    except Exception as e:
        # Se o log falhar, apenas imprime no console para não quebrar a aplicação
        print(f"Falha ao registrar log de auditoria: {e}")
        print(traceback.format_exc())
