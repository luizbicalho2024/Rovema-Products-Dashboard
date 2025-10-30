import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime

# CORREÇÃO PARA 'KeyError: utils': Adiciona o diretório raiz ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.auth import auth_guard, check_role
from utils.firebase_config import get_db

# --- 1. Proteção da Página ---
auth_guard()
check_role(["admin"])  # Apenas Admins
st.title("📜 Logs de Auditoria do Sistema")

# --- 2. Função de Busca ---
@st.cache_data(ttl=60) # Cache curto (1 min) para logs
def get_audit_logs(limit=100):
    """Busca os logs de auditoria mais recentes."""
    db = get_db()
    logs_ref = db.collection("audit_logs")
    
    # Ordena por timestamp descendente (mais novo primeiro)
    query = logs_ref.order_by("timestamp", direction="DESCENDING").limit(limit)
    
    try:
        docs = query.stream()
        logs = []
        for doc in docs:
            data = doc.to_dict()
            logs.append({
                "timestamp": data.get("timestamp"),
                "user_email": data.get("user_email", "N/A"),
                "action": data.get("action", "N/A"),
                "details": str(data.get("details", {})) # Converte dict para string
            })
        
        if not logs:
            return pd.DataFrame()
            
        return pd.DataFrame(logs)
        
    except Exception as e:
        st.error(f"Erro ao consultar logs: {e}")
        return pd.DataFrame()

# --- 3. Carregamento e Exibição ---
st.subheader("Logs Mais Recentes")

with st.spinner("Carregando logs..."):
    df_logs = get_audit_logs(limit=200)

if df_logs.empty:
    st.info("Nenhum log de auditoria encontrado.")
    st.stop()

# Reordena colunas para melhor visualização
df_logs = df_logs[["timestamp", "user_email", "action", "details"]]

st.dataframe(
    df_logs,
    use_container_width=True,
    column_config={
        "timestamp": st.column_config.DatetimeColumn(
            "Data/Hora",
            format="YYYY-MM-DD HH:mm:ss"
        ),
        "user_email": st.column_config.TextColumn("Usuário"),
        "action": st.column_config.TextColumn("Ação"),
        "details": st.column_config.TextColumn("Detalhes")
    }
)

if st.button("Recarregar Logs"):
    st.cache_data.clear()
    st.rerun()
