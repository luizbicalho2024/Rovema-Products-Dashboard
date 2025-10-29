# app/ui_pages.py

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from app.db_utils import ingest_csv_data, get_combined_data, get_consultores_from_db
from app.api_processor import process_api_data
from firebase_admin import firestore, auth

# --- Funções de Páginas ---

def data_ingestion_page(db_client):
    """Página para atualização dos dados (CSV e API)."""
    st.title("🔄 Atualização e Ingestão de Dados")
    
    # --- CSV Upload ---
    st.subheader("Importar Dados Locais (Bionio e Rovema Pay - CSV)")

    col1, col2 = st.columns(2)
    
    with col1:
        uploaded_bionio = st.file_uploader("Upload Bionio.csv (Delimitador ';')", type=['csv'], key='bionio_upload')
        if uploaded_bionio is not None:
            if st.button("Salvar Bionio no Banco de Dados"):
                with st.spinner("Processando e salvando Bionio..."):
                    ingest_csv_data(db_client, uploaded_bionio, 'bionio_data')

    with col2:
        uploaded_rovema = st.file_uploader("Upload RovemaPay.csv (Delimitador ';')", type=['csv'], key='rovema_upload')
        if uploaded_rovema is not None:
            if st.button("Salvar Rovema Pay no Banco de Dados"):
                with st.spinner("Processando e salvando Rovema Pay..."):
                    ingest_csv_data(db_client, uploaded_rovema, 'rovema_pay_data')

    st.markdown("---")
    
    # --- API Consulta ---
    st.subheader("Consultar Dados das APIs (ELIQ e ASTO)")
    
    # Filtro de Data Inteligente
    today = datetime.now().date()
    first_day_of_month = today.replace(day=1) 
    
    col_start, col_end, col_btn = st.columns([1, 1, 0.5])
    
    with col_start:
        data_inicial = st.date_input("Data Inicial", value=first_day_of_month)
    
    with col_end:
        data_final = st.date_input("Data Final", value=today) 
    
    if data_inicial > data_final:
        st.error("A Data Inicial não pode ser maior que a Data Final.")
        return

    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Pesquisar e Processar APIs"):
            with st.spinner("Processando informações das APIs..."):
                process_api_data(db_client, data_inicial, data_final)
                
def management_page(db_client):
    """Página para cadastro de Consultores e atribuição de Carteira."""
    st.title("👥 Gestão de Consultores e Carteiras")
    
    # 4.1. Cadastrar Consultor
    st.subheader("Cadastrar Novo Consultor")
    with st.form("form_consultor"):
        nome = st.text_input("Nome do Consultor")
        email = st.text_input("Email do Consultor (Login)")
        
        submitted = st.form_submit_button("Cadastrar Consultor")
        
        if submitted and nome and email:
            try:
                # Cria o usuário no Firebase Authentication (Senha MOCK)
                user = auth.create_user(
                    email=email,
                    password='logpay123',
                    display_name=nome
                )
                
                # Salva o perfil na coleção 'consultores'
                db_client.collection('consultores').document(user.uid).set({
                    'nome': nome,
                    'email': email,
                    'gestor_id': st.session_state.get('user_uid', 'admin'),
                    'data_cadastro': firestore.SERVER_TIMESTAMP
                })
                st.success(f"Consultor **{nome}** cadastrado e pronto para login (Senha: logpay123).")
            except Exception as e:
                st.error(f"Erro ao cadastrar consultor: {e}")
                
    # 4.2. Atribuir Carteira
    st.markdown("---")
    st.subheader("Atribuir Empresas/Clientes à Carteira")
    
    consultores_map = get_consultores_from_db(db_client)
    if not consultores_map:
        st.warning("Nenhum consultor cadastrado. Cadastre um consultor primeiro.")
        return
        
    consultor_selecionado = st.selectbox("Selecione o Consultor", list(consultores_map.keys()))
    consultor_uid = consultores_map.get(consultor_selecionado)

    with st.form("form_carteira"):
        cnpj_cliente = st.text_input("CNPJ/ID do Cliente a Atribuir")
        nome_cliente = st.text_input("Nome da Empresa/Cliente")

        submitted_carteira = st.form_submit_button(f"Atribuir Cliente a {consultor_selecionado}")
        
        if submitted_carteira and cnpj_cliente and nome_cliente:
            doc_id = f"{consultor_uid}_{cnpj_cliente}"
            db_client.collection('carteira_clientes').document(doc_id).set({
                'consultor_uid': consultor_uid,
                'nome_consultor': consultor_selecionado,
                'cliente_id': cnpj_cliente,
                'nome_cliente': nome_cliente,
                'data_atribuicao': firestore.SERVER_TIMESTAMP
            })
            st.success(f"Cliente **{nome_cliente}** atribuído a **{consultor_selecionado}**.")

def bi_dashboard_page(db_client):
    """Dashboard de BI e Estratégia Comercial."""
    st.title("📊 Dashboard de Inteligência Comercial")
    
    with st.spinner("Carregando e combinando dados..."):
        df_raw = get_combined_data(db_client)
        
    if df_raw.empty:
        st.warning("Sem dados para análise. Por favor, ingira dados na página de Atualização.")
        return

    # --- 1. Filtros de Vendedor e Carteira ---
    consultores_map = get_consultores_from_db(db_client)
    
    vendedor_filtro_nome = st.sidebar.selectbox(
        "Filtrar por Vendedor (Consultor)", 
        ['Todos'] + list(consultores_map.keys())
    )
    selected_uid = consultores_map.get(vendedor_filtro_nome) if vendedor_filtro_nome != 'Todos' else None
    
    # Aplica o filtro
    if selected_uid:
        df_data = df_raw[df_raw['consultor_uid'] == selected_uid].copy()
        if df_data.empty:
             st.warning(f"O consultor **{vendedor_filtro_nome}** não possui dados atribuídos para este período. Exibindo dados gerais.")
             df_data = df_raw.copy() 
    else:
        df_data = df_raw.copy()
        
    # Métrica principal
    receita_total = df_data['receita'].sum()
    st.metric("Receita Total (Filtro Aplicado)", f"R$ {receita_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    # --- 2. Receita por Produto ---
    st.subheader("Distribuição de Receita e Estratégia")
    col_graph, col_metrics = st.columns([2, 1])

    df_receita_produto = df_data.groupby('produto')['receita'].sum().reset_index().sort_values(by='receita', ascending=False)
    
    with col_graph:
        fig_receita = px.bar(
            df_receita_produto, 
            x='produto', 
            y='receita', 
            title='Receita por Produto/Origem',
            labels={'receita': 'Receita (R$)', 'produto': 'Produto/Origem'},
            color='produto'
        )
        st.plotly_chart(fig_receita, use_container_width=True)

    with col_metrics:
        st.info("🏆 **Top Produtos (Maior Receita):**")
        st.dataframe(df_receita_produto.head(5).style.format({'receita': "R$ {:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")}), hide_index=True, use_container_width=True)
        
        st.warning("🔻 **Produtos de Atenção (Menor Receita):**")
        st.dataframe(df_receita_produto.tail(5).style.format({'receita': "R$ {:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")}), hide_index=True, use_container_width=True)
        st.markdown("*Estratégia:* Focar em clientes que usam Top Produtos para introduzir os de Menor Receita (Cross-Sell).")


    # --- 3. Pontos de Estratégia de Retenção ---
    st.markdown("---")
    st.subheader("🛡️ Estratégia de Retenção (Alerta de Churn e Pagamento)")

    # Alerta de Retenção Bionio (Aguardando Pagamento)
    if 'status_do_pedido' in df_data.columns:
        df_alerta = df_data[df_data['status_do_pedido'] == 'Aguardando pagamento']
        
        if not df_alerta.empty:
            st.error(f"**ALERTA!** {len(df_alerta)} Pedidos Bionio Aguardando Pagamento!")
            st.markdown("##### Pedidos Bionio Suspensos (Risco de Churn)")
            st.dataframe(df_alerta[['numero_do_pedido', 'nome_fantasia', 'data_da_criacao_do_pedido', 'valor_total_do_pedido']].head(10), use_container_width=True)
            st.markdown("*Ação Sugerida:* Consultores devem contatar estes clientes imediatamente para converter a venda ou reativar o pedido.")
        else:
            st.success("Nenhum pedido Bionio em status 'Aguardando Pagamento' encontrado no filtro.")
