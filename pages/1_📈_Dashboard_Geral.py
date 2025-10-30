import streamlit as st
import pandas as pd
import plotly.express as px
from utils.auth import auth_guard
from utils.firebase_config import get_db
from datetime import datetime, timedelta

# --- 1. Proteção da Página ---
auth_guard()
st.title(f"📈 Dashboard Geral")
st.markdown(f"Bem-vindo, **{st.session_state.user_name}**!")

# --- 2. Função de Busca no Firestore (com cache) ---
@st.cache_data(ttl=600) # Cache de 10 minutos
def query_sales_data(start_date, end_date, role, uid):
    """
    Busca os dados de vendas no Firestore com base no nível de acesso.
    """
    db = get_db()
    sales_ref = db.collection("sales_data")
    
    # Converte datas para Timestamps
    start_ts = datetime.combine(start_date, datetime.min.time())
    end_ts = datetime.combine(end_date, datetime.max.time())
    
    # Monta a query base
    query = sales_ref.where("date", ">=", start_ts).where("date", "<=", end_ts)
    
    # --- FILTRO DE NÍVEL DE ACESSO (CRÍTICO) ---
    if role == 'consultant':
        # Consultor só vê o que é dele
        query = query.where("consultant_uid", "==", uid)
    elif role == 'manager':
        # Gestor só vê o que é do time dele
        query = query.where("manager_uid", "==", uid)
    # Admin (else) não tem filtro, vê tudo.

    # Executa a query
    try:
        docs = query.stream()
        data = [doc.to_dict() for doc in docs]
        if not data:
            return pd.DataFrame() # Retorna DF vazio se não houver dados
        
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Erro ao consultar o Firestore: {e}")
        return pd.DataFrame()


# --- 3. Filtros na Sidebar ---
st.sidebar.header("Filtros do Dashboard")
default_start = datetime.now().replace(day=1)
default_end = datetime.now()

filter_start_date = st.sidebar.date_input("Data Inicial", default_start)
filter_end_date = st.sidebar.date_input("Data Final", default_end)

# CORREÇÃO PARA O BOTÃO:
load_button = st.sidebar.button("Aplicar Filtros e Carregar Dados", type="primary", width='stretch')

# --- 4. Lógica de Carregamento e Exibição ---

# Inicializa o estado da sessão para os dados
if "dashboard_data" not in st.session_state:
    st.session_state.dashboard_data = None

if load_button:
    with st.spinner("Carregando dados... Por favor, aguarde."):
        # Busca os dados
        df = query_sales_data(
            filter_start_date, 
            filter_end_date, 
            st.session_state.user_role, 
            st.session_state.user_uid
        )
        if df.empty:
            st.session_state.dashboard_data = pd.DataFrame() # Armazena DF vazio
        else:
            # Processamento básico
            df['date'] = pd.to_datetime(df['date'])
            df['revenue_gross'] = pd.to_numeric(df['revenue_gross'])
            df['revenue_net'] = pd.to_numeric(df['revenue_net'])
            st.session_state.dashboard_data = df
else:
    # Mantém os dados antigos se o botão não for pressionado
    if st.session_state.dashboard_data is None:
        st.info("Selecione os filtros e clique em 'Carregar Dados' na barra lateral para começar.")
        st.stop()

# --- 5. Exibição do Dashboard (só roda se os dados estiverem carregados) ---

df_data = st.session_state.dashboard_data

if df_data.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
    st.stop()

# --- KPIs Principais ---
st.subheader("Visão Geral do Período")

total_revenue_net = df_data['revenue_net'].sum()
total_revenue_gross = df_data['revenue_gross'].sum()
total_sales = len(df_data)

col1, col2, col3 = st.columns(3)
col1.metric("Receita Líquida (Empresa)", f"R$ {total_revenue_net:,.2f}")
col2.metric("Volume Bruto (Clientes)", f"R$ {total_revenue_gross:,.2f}")
col3.metric("Total de Vendas", f"{total_sales:,}")

st.divider()

# --- Gráficos (Plotly) ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Receita Líquida por Produto")
    df_grouped = df_data.groupby("source")['revenue_net'].sum().reset_index()
    fig = px.pie(
        df_grouped, 
        names="source", 
        values="revenue_net", 
        title="Receita Líquida (R$) por Fonte",
        hole=0.3
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    # Plotly usa use_container_width, o aviso não se aplica aqui.
    st.plotly_chart(fig, use_container_width=True)
    
    # Ponto de Atenção: Produtos com Baixa Receita
    st.subheader("Pontos de Atenção: Menor Receita")
    bottom_products = df_data.groupby('product_name')['revenue_net'].sum().nsmallest(5).reset_index()
    # CORREÇÃO PARA O DATAFRAME:
    st.dataframe(bottom_products.style.format({'revenue_net': 'R$ {:,.2f}'}), width='stretch')

with col2:
    st.subheader("Evolução da Receita Líquida")
    # Agrupa por dia
    df_time = df_data.set_index('date').resample('D')['revenue_net'].sum().reset_index()
    fig_time = px.area(
        df_time,
        x="date",
        y="revenue_net",
        title="Receita Líquida ao Longo do Tempo"
    )
    # Plotly usa use_container_width
    st.plotly_chart(fig_time, use_container_width=True)
    
    # Estratégia: Produtos com Maior Receita
    st.subheader("Estratégia: Maior Receita")
    top_products = df_data.groupby('product_name')['revenue_net'].sum().nlargest(5).reset_index()
    # CORREÇÃO PARA O DATAFRAME:
    st.dataframe(top_products.style.format({'revenue_net': 'R$ {:,.2f}'}), width='stretch')

# Tabela de dados brutos no final
with st.expander("Ver dados detalhados"):
    # CORREÇÃO PARA O DATAFRAME:
    st.dataframe(df_data, width='stretch')
