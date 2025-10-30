import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime

# CORREÇÃO PARA 'KeyError: utils': Adiciona o diretório raiz ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.auth import auth_guard
from utils.firebase_config import get_db

# --- 1. Proteção da Página ---
auth_guard()
st.title(f"🧑‍💼 Minha Carteira")
st.markdown(f"**Consultor:** {st.session_state.user_name}")

# --- 2. Funções de Busca (com cache) ---
@st.cache_data(ttl=600)
def get_my_clients(consultant_uid):
    """Busca todos os clientes associados a este consultor."""
    db = get_db()
    clients_ref = db.collection("clients").where("consultant_uid", "==", consultant_uid).stream()
    
    clients = []
    for client in clients_ref:
        data = client.to_dict()
        clients.append({
            "cnpj": client.id,
            "name": data.get("client_name", "N/A"),
            "manager_uid": data.get("manager_uid")
        })
    
    if not clients:
        return pd.DataFrame(columns=["cnpj", "name", "manager_uid"])
        
    return pd.DataFrame(clients)

@st.cache_data(ttl=600)
def get_my_sales(consultant_uid, start_date, end_date):
    """Busca as vendas deste consultor no período."""
    db = get_db()
    sales_ref = db.collection("sales_data")
    
    start_ts = datetime.combine(start_date, datetime.min.time())
    end_ts = datetime.combine(end_date, datetime.max.time())
    
    query = sales_ref.where("consultant_uid", "==", consultant_uid) \
                     .where("date", ">=", start_ts) \
                     .where("date", "<=", end_ts)
    
    try:
        docs = query.stream()
        data = [doc.to_dict() for doc in docs]
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df['revenue_net'] = pd.to_numeric(df['revenue_net'])
        return df
    except Exception as e:
        st.error(f"Erro ao consultar vendas: {e}")
        return pd.DataFrame()

# --- 3. Filtros ---
st.sidebar.header("Filtros")
default_start = datetime.now().replace(day=1)
default_end = datetime.now()

filter_start_date = st.sidebar.date_input("Data Inicial", default_start)
filter_end_date = st.sidebar.date_input("Data Final", default_end)

# --- 4. Carregamento de Dados ---
with st.spinner("Carregando seus dados..."):
    df_clients = get_my_clients(st.session_state.user_uid)
    df_sales = get_my_sales(st.session_state.user_uid, filter_start_date, filter_end_date)

if df_clients.empty:
    st.warning("Você ainda não possui clientes cadastrados na sua carteira.")
    st.stop()

# --- 5. KPIs ---
st.subheader(f"Performance do Período ({filter_start_date.strftime('%d/%m/%Y')} a {filter_end_date.strftime('%d/%m/%Y')})")

total_revenue = 0.0
total_sales = 0
clients_activated = 0

if not df_sales.empty:
    total_revenue = df_sales['revenue_net'].sum()
    total_sales = len(df_sales)
    clients_activated = df_sales['client_cnpj'].nunique()

col1, col2, col3 = st.columns(3)
col1.metric("Receita Líquida Gerada", f"R$ {total_revenue:,.2f}")
col2.metric("Total de Vendas", f"{total_sales}")
col3.metric("Clientes Ativados", f"{clients_activated} / {len(df_clients)}")

st.divider()

# --- 6. Tabela de Clientes e Performance ---
st.subheader("Performance por Cliente")

if not df_sales.empty:
    # Agrupa as vendas por cliente
    sales_by_client = df_sales.groupby("client_cnpj")['revenue_net'].sum().reset_index()
    sales_by_client = sales_by_client.rename(columns={"revenue_net": "revenue_periodo"})
    
    # Junta com a lista de clientes para ver quem não comprou
    df_clients_perf = pd.merge(
        df_clients,
        sales_by_client,
        on="client_cnpj",
        how="left"
    )
    # Preenche clientes inativos (NaN) com 0
    df_clients_perf['revenue_periodo'] = df_clients_perf['revenue_periodo'].fillna(0)
    
else:
    # Se não houver vendas, todos têm 0
    df_clients_perf = df_clients.copy()
    df_clients_perf['revenue_periodo'] = 0.0

# Ordena por quem gerou mais receita
df_clients_perf = df_clients_perf.sort_values(by="revenue_periodo", ascending=False)

# Exibe a tabela
st.dataframe(
    df_clients_perf[['name', 'cnpj', 'revenue_periodo']],
    use_container_width=True,
    column_config={
        "name": st.column_config.TextColumn("Cliente"),
        "cnpj": st.column_config.TextColumn("CNPJ"),
        "revenue_periodo": st.column_config.NumberColumn(
            "Receita no Período",
            format="R$ %.2f"
        )
    }
)
