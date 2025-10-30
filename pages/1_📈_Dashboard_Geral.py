import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
from datetime import datetime, timedelta
import calendar

# CORREÇÃO PARA 'KeyError: utils': Adiciona o diretório raiz ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.auth import auth_guard
from utils.firebase_config import get_db

# --- 1. Proteção da Página ---
auth_guard()
st.title(f"📈 Dashboard Geral")
st.markdown(f"Bem-vindo, **{st.session_state.user_name}**!")

# --- 2. Funções de Busca (com cache) ---

@st.cache_data(ttl=600)
def get_supporting_data():
    """Busca dados de usuários (para filtros) e metas."""
    db = get_db()
    users_ref = db.collection("users").stream()
    users = []
    for user in users_ref:
        data = user.to_dict()
        users.append({
            "uid": user.id,
            "name": data.get("name", "N/A"),
            "role": data.get("role", "N/A"),
            "manager_uid": data.get("manager_uid")
        })
    
    # Busca metas do mês atual
    current_month_id = datetime.now().strftime("%Y-%m")
    goals_ref = db.collection("goals").document(current_month_id).get()
    goals = goals_ref.to_dict() if goals_ref.exists else {}

    return pd.DataFrame(users), goals


@st.cache_data(ttl=600) # Cache de 10 minutos
def query_sales_data(start_date, end_date, role, uid, manager_uid_filter=None):
    """
    Busca os dados de vendas no Firestore com base no nível de acesso.
    AGORA BUSCA O PERÍODO ANTERIOR JUNTO PARA COMPARAÇÃO.
    """
    db = get_db()
    sales_ref = db.collection("sales_data")
    
    # 1. Período Atual
    start_ts = datetime.combine(start_date, datetime.min.time())
    end_ts = datetime.combine(end_date, datetime.max.time())
    
    # 2. Período Anterior (mesma duração)
    period_days = (end_date - start_date).days
    prev_end_date = start_date - timedelta(days=1)
    prev_start_date = prev_end_date - timedelta(days=period_days)
    
    prev_start_ts = datetime.combine(prev_start_date, datetime.min.time())
    prev_end_ts = datetime.combine(prev_end_date, datetime.max.time())
    
    # --- Função interna para executar a query ---
    def fetch_data(start, end, role, uid, manager_uid_filter):
        query = sales_ref.where("date", ">=", start).where("date", "<=", end)
        
        # Filtro de Nível de Acesso (CRÍTICO)
        if role == 'consultant':
            query = query.where("consultant_uid", "==", uid)
        elif role == 'manager':
            # Gestor vê o time dele OU pode filtrar por um consultor do time
            if manager_uid_filter:
                query = query.where("consultant_uid", "==", manager_uid_filter)
            else:
                 query = query.where("manager_uid", "==", uid)
        elif role == 'admin':
            # Admin pode filtrar por Gestor ou Consultor
            if manager_uid_filter: # Filtro de gestor
                query = query.where("manager_uid", "==", manager_uid_filter)
            # (Filtro de consultor é aplicado via Pandas depois)

        try:
            docs = query.stream()
            data = [doc.to_dict() for doc in docs]
            if not data:
                return pd.DataFrame()
            return pd.DataFrame(data)
        except Exception as e:
            st.error(f"Erro ao consultar o Firestore: {e}")
            return pd.DataFrame()
    # -------------------------------------------
    
    df_current = fetch_data(start_ts, end_ts, role, uid, manager_uid_filter)
    df_previous = fetch_data(prev_start_ts, prev_end_ts, role, uid, manager_uid_filter)
    
    return df_current, df_previous, (prev_start_date, prev_end_date)


def process_dataframe(df):
    """Processa o DF (tipos de dados) se não estiver vazio."""
    if df.empty:
        return df
    df['date'] = pd.to_datetime(df['date'])
    df['revenue_gross'] = pd.to_numeric(df['revenue_gross'])
    df['revenue_net'] = pd.to_numeric(df['revenue_net'])
    return df

# --- 3. Carrega Dados de Suporte (Filtros, Metas) ---
df_users, user_goals = get_supporting_data()

# --- 4. Filtros na Sidebar ---
st.sidebar.header("Filtros do Dashboard")
my_role = st.session_state.user_role
my_uid = st.session_state.user_uid

# --- MELHORIA DE USABILIDADE: Persistência de Filtros ---
# Inicializa o estado da sessão para os filtros
default_start = datetime.now().replace(day=1)
default_end = datetime.now()

if 'filter_start_date' not in st.session_state:
    st.session_state.filter_start_date = default_start
if 'filter_end_date' not in st.session_state:
    st.session_state.filter_end_date = default_end
if 'filter_source' not in st.session_state:
    st.session_state.filter_source = []
if 'filter_manager' not in st.session_state:
    st.session_state.filter_manager = "all"
if 'filter_consultant' not in st.session_state:
    st.session_state.filter_consultant = "all"

# Usa 'key' para vincular o widget ao st.session_state
st.sidebar.date_input("Data Inicial", key="filter_start_date")
st.sidebar.date_input("Data Final", key="filter_end_date")

st.sidebar.multiselect(
    "Filtrar por Produto",
    options=["Bionio", "Rovema Pay", "ASTO", "ELIQ"],
    key="filter_source",
    placeholder="Todos os Produtos"
)

# Filtros de Acesso
if my_role == 'admin':
    # Admin pode filtrar por Gestor
    manager_list = {u['uid']: u['name'] for _, u in df_users[df_users['role'] == 'manager'].iterrows()}
    manager_list = {"all": "Todos os Gestores"} | manager_list
    
    st.sidebar.selectbox(
        "Filtrar por Gestor",
        options=manager_list.keys(),
        format_func=lambda uid: manager_list[uid],
        key="filter_manager"
    )
    
    # Se selecionou gestor, filtra consultores desse gestor
    if st.session_state.filter_manager != "all":
        consultant_list = {u['uid']: u['name'] for _, u in df_users[
            (df_users['role'] == 'consultant') & (df_users['manager_uid'] == st.session_state.filter_manager)
        ].iterrows()}
    else:
        # Lista todos consultores
        consultant_list = {u['uid']: u['name'] for _, u in df_users[df_users['role'] == 'consultant'].iterrows()}
    
    consultant_list = {"all": "Todos os Consultores"} | consultant_list
    
    st.sidebar.selectbox(
        "Filtrar por Consultor",
        options=consultant_list.keys(),
        format_func=lambda uid: consultant_list.get(uid, "Todos os Consultores"),
        key="filter_consultant"
    )

elif my_role == 'manager':
    # Gestor pode filtrar por seus consultores
    consultant_list = {u['uid']: u['name'] for _, u in df_users[
        (df_users['role'] == 'consultant') & (df_users['manager_uid'] == my_uid)
    ].iterrows()}
    consultant_list = {"all": "Todo o Time"} | consultant_list
    
    st.sidebar.selectbox(
        "Filtrar por Consultor",
        options=consultant_list.keys(),
        format_func=lambda uid: consultant_list[uid],
        key="filter_consultant" # "filter_consultant" será o UID do consultor
    )

# Botão de Carregar
load_button = st.sidebar.button("Aplicar Filtros e Carregar Dados", type="primary", use_container_width=True)

# --- 5. Lógica de Carregamento e Exibição ---

# Pega filtros do st.session_state
filter_start_date = st.session_state.filter_start_date
filter_end_date = st.session_state.filter_end_date
filter_source = st.session_state.filter_source
filter_consultant_id = st.session_state.filter_consultant
filter_manager_id = st.session_state.filter_manager

# Determina o filtro de query
query_filter = None
if my_role == 'admin' and filter_manager_id != 'all':
    query_filter = filter_manager_id # Admin filtrando por gestor
elif my_role == 'manager' and filter_consultant_id != 'all':
    query_filter = filter_consultant_id # Manager filtrando por consultor

# Inicializa estados
if "dashboard_data" not in st.session_state:
    st.session_state.dashboard_data = (pd.DataFrame(), pd.DataFrame(), None)

if load_button:
    with st.spinner("Carregando dados... Por favor, aguarde."):
        df_curr, df_prev, prev_period = query_sales_data(
            filter_start_date, 
            filter_end_date, 
            my_role, 
            my_uid,
            query_filter
        )
        
        df_curr = process_dataframe(df_curr)
        df_prev = process_dataframe(df_prev)
        
        st.session_state.dashboard_data = (df_curr, df_prev, prev_period)
else:
    if st.session_state.dashboard_data[0].empty:
        st.info("Selecione os filtros e clique em 'Carregar Dados' na barra lateral para começar.")
        st.stop()

# --- 6. Exibição do Dashboard (com filtros Pandas) ---

df_data, df_prev_data, prev_period = st.session_state.dashboard_data
df_display = df_data.copy()
df_prev_display = df_prev_data.copy()

# Aplica filtros PANDAS (pós-query)
if filter_source:
    df_display = df_display[df_display['source'].isin(filter_source)]
    df_prev_display = df_prev_display[df_prev_display['source'].isin(filter_source)]

if my_role == 'admin' and filter_consultant_id != 'all':
    df_display = df_display[df_display['consultant_uid'] == filter_consultant_id]
    df_prev_display = df_prev_display[df_prev_display['consultant_uid'] == filter_consultant_id]


if df_display.empty and load_button:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
    st.stop()
elif df_display.empty:
    st.stop()


# --- 7. KPIs Principais (COM COMPARAÇÃO) ---
st.subheader("Visão Geral do Período")

# Período Atual
total_revenue_net = df_display['revenue_net'].sum()
total_revenue_gross = df_display['revenue_gross'].sum()
total_sales = len(df_display)

# Período Anterior
prev_revenue_net = df_prev_display['revenue_net'].sum()
prev_revenue_gross = df_prev_display['revenue_gross'].sum()
prev_sales = len(df_prev_display)

# Funções de Delta
def get_delta(current, previous):
    if previous == 0:
        return None # Evita divisão por zero
    delta = ((current - previous) / previous) * 100
    return f"{delta:.1f}%"

col1, col2, col3 = st.columns(3)
col1.metric("Receita Líquida (Empresa)", 
             f"R$ {total_revenue_net:,.2f}", 
             delta=get_delta(total_revenue_net, prev_revenue_net),
             help=f"Período anterior: R$ {prev_revenue_net:,.2f} ({prev_period[0].strftime('%d/%m')} a {prev_period[1].strftime('%d/%m')})")
col2.metric("Volume Bruto (Clientes)", 
             f"R$ {total_revenue_gross:,.2f}", 
             delta=get_delta(total_revenue_gross, prev_revenue_gross),
             help=f"Período anterior: R$ {prev_revenue_gross:,.2f}")
col3.metric("Total de Vendas", 
             f"{total_sales:,}", 
             delta=get_delta(total_sales, prev_sales),
             help=f"Período anterior: {prev_sales:,}")

st.divider()

# --- 8. Módulo de Metas (Visível para Consultor/Manager) ---
if my_role in ['consultant', 'manager']:
    
    target_uid = my_uid if my_role == 'consultant' else filter_consultant_id # Se manager filtrando consultor
    
    if my_role == 'manager' and filter_consultant_id == 'all': # Manager vendo o time todo
        st.subheader("Meta do Time (Mês Atual)")
        # Lógica para somar metas do time
        my_team_uids = df_users[df_users['manager_uid'] == my_uid]['uid'].tolist()
        team_goal = sum(user_goals.get(uid, 0) for uid in my_team_uids)
        team_revenue_month = df_data[
            (df_data['date'].dt.month == datetime.now().month) &
            (df_data['date'].dt.year == datetime.now().year)
        ]['revenue_net'].sum()
        
        goal_value = team_goal
        current_value = team_revenue_month

    else: # Consultor ou Manager filtrando 1 consultor
        user_name_series = df_users[df_users['uid'] == target_uid]['name']
        user_name = user_name_series.values[0] if not user_name_series.empty else "Consultor"
        st.subheader(f"Meta de {user_name.split(' ')[0]} (Mês Atual)")
        
        goal_value = user_goals.get(target_uid, 0)
        
        # Calcula receita do mês atual para esse usuário
        current_value = df_data[
            (df_data['consultant_uid'] == target_uid) &
            (df_data['date'].dt.month == datetime.now().month) &
            (df_data['date'].dt.year == datetime.now().year)
        ]['revenue_net'].sum()

    if goal_value > 0:
        progress = min(current_value / goal_value, 1.0)
        st.progress(progress)
        st.markdown(f"**R$ {current_value:,.2f}** de **R$ {goal_value:,.2f}** ({progress*100:.1f}%)")
    else:
        st.info("Nenhuma meta definida para este mês.")
    
    st.divider()

# --- 9. Gráficos (Plotly) ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Receita Líquida por Produto")
    df_grouped = df_display.groupby("source")['revenue_net'].sum().reset_index()
    fig = px.pie(
        df_grouped, 
        names="source", 
        values="revenue_net", 
        title="Receita Líquida (R$) por Fonte",
        hole=0.3
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Pontos de Atenção: Menor Receita")
    bottom_products = df_display.groupby('product_name')['revenue_net'].sum().nsmallest(5).reset_index()
    st.dataframe(bottom_products.style.format({'revenue_net': 'R$ {:,.2f}'}), use_container_width=True)

with col2:
    st.subheader("Evolução da Receita Líquida")
    df_time = df_display.set_index('date').resample('D')['revenue_net'].sum().reset_index()
    fig_time = px.area(
        df_time,
        x="date",
        y="revenue_net",
        title="Receita Líquida ao Longo do Tempo"
    )
    st.plotly_chart(fig_time, use_container_width=True)
    
    st.subheader("Estratégia: Maior Receita")
    top_products = df_display.groupby('product_name')['revenue_net'].sum().nlargest(5).reset_index()
    st.dataframe(top_products.style.format({'revenue_net': 'R$ {:,.2f}'}), use_container_width=True)

with st.expander("Ver dados detalhados (Período Atual)"):
    st.dataframe(df_display, use_container_width=True)
