import streamlit as st
import pandas as pd
import requests
import json
import plotly.express as px
from datetime import datetime, timedelta
import base64
import random # Usado para MOCK de dados API

# --- Configurações da Página Streamlit ---
st.set_page_config(layout="wide", page_title="BI Estratégia Comercial")


# --- 1. Configuração e Inicialização do Firebase ---
try:
    import firebase_admin
    from firebase_admin import credentials, auth, firestore

    # 1.1. Carregar Credenciais do Streamlit Secrets
    cred_dict = st.secrets["firebase"]["credentials"]
    
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, name='BI_COMERCIAL_APP')
    
    db = firestore.client()

except Exception as e:
    st.error(f"Erro Crítico ao Inicializar Firebase. Verifique 'secrets.toml'.")
    # st.stop() # Comentado para permitir visualização do código mesmo sem secrets válidos

    
# --- 2. Funções de Autenticação e Sessão ---

def login_user(email, password):
    """
    Função de login simplificada (POC).
    Simula autenticação verificando a existência do e-mail e uma senha MOCK.
    """
    try:
        # Busca o usuário pelo e-mail
        user = auth.get_user_by_email(email)
        
        # MOCK de Senha. Em PROD, você usaria Firebase Auth REST API.
        if password == "logpay123": 
            st.session_state['logged_in'] = True
            st.session_state['user_email'] = email
            st.session_state['user_uid'] = user.uid
            st.session_state['user_name'] = user.display_name or "Usuário"
            st.success(f"Login realizado com sucesso! Bem-vindo(a), {st.session_state['user_name']}.")
            st.rerun()
        else:
            st.error("Falha no Login: Senha inválida (MOCK: use 'logpay123').")

    except firebase_admin.exceptions.FirebaseError as e:
        if 'auth/user-not-found' in str(e):
            st.error("Falha no Login: Usuário não encontrado. Peça para o Gestor cadastrá-lo.")
        else:
            st.error(f"Erro Firebase: {e}")
    except Exception as e:
        st.error(f"Erro desconhecido durante o login: {e}")

def logout():
    """Função de Logout."""
    keys_to_delete = ['logged_in', 'user_email', 'user_uid', 'user_name']
    for key in keys_to_delete:
        if key in st.session_state:
            del st.session_state[key]
    st.info("Sessão encerrada.")
    st.rerun()

def login_page():
    """Página de Login na tela inicial."""
    st.title("🔒 Login - Sistema de BI e Estratégia Comercial")
    
    email = st.text_input("Email")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if email and password:
            login_user(email, password)
        else:
            st.warning("Preencha e-mail e senha.")

# --- 3. Módulo de Ingestão de Dados (ETL) ---

@st.cache_resource
def get_db_ref():
    return firestore.client()

def ingest_csv_data(db_client, file_content, collection_name):
    """Lê CSV, normaliza e salva no Firestore."""
    
    # 3.1. Leitura e Normalização
    df = pd.read_csv(file_content, delimiter=';')
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('ã', 'a').str.replace('ç', 'c').str.replace('ó', 'o').str.replace('ê', 'e')
    
    # Colunas de valor para tratamento
    valor_cols = []
    if 'bionio' in collection_name:
        valor_cols = ['valor_do_beneficio', 'valor_total_do_pedido']
    elif 'rovema_pay' in collection_name:
        # Colunas com percentuais também são tratadas como float
        valor_cols = ['bruto', 'pagoadquirente', 'liquido', 'antecipado', 'taxa_adquirente', 'taxa_cliente', 'mdr', 'spread']

    for col in valor_cols:
        if col in df.columns:
            # Remove ponto (milhar), troca vírgula (decimal) por ponto, e converte para float
            df[col] = (df[col].astype(str).str.replace('.', '', regex=False)
                             .str.replace(',', '.', regex=False)
                             .str.extract('(\d+\.?\d*)', expand=False)
                             .astype(float))
    
    # 3.2. Salvar no Firestore (Deleção e Inserção para limpar a coleção)
    
    try:
        # Limpar coleção existente (Atenção: Operação de alto custo e risco no Free Tier!)
        st.info(f"Limpando dados antigos de '{collection_name}'...")
        docs = db_client.collection(collection_name).limit(500).stream() 
        batch_delete = db_client.batch()
        for doc in docs:
            batch_delete.delete(doc.reference)
        batch_delete.commit()
        st.success("Limpeza parcial concluída.")
        
        # Inserção de Novos Dados em lotes (batch)
        data_to_save = df.to_dict('records')
        batch_size = 400 
        
        for i in range(0, len(data_to_save), batch_size):
            batch_insert = db_client.batch()
            batch_data = data_to_save[i:i + batch_size]
            
            for j, record in enumerate(batch_data):
                # Usa um ID determinístico para sobrescrever com segurança
                doc_ref = db_client.collection(collection_name).document(f'{collection_name}_rec_{i+j}')
                batch_insert.set(doc_ref, record)
            
            batch_insert.commit()

        # Documento de Metadata
        db_client.collection(collection_name).document('metadata').set({
            'last_updated': firestore.SERVER_TIMESTAMP,
            'record_count': len(data_to_save)
        })

        st.success(f"Dados de **{collection_name.upper()}** ({len(data_to_save)} registros) salvos com sucesso!")
        
    except Exception as e:
        st.error(f"Erro ao salvar dados no Firebase Firestore: {e}")

# --- Funções de API ---

def get_api_data(api_name, url, username=None, password=None, token=None, start_date=None, end_date=None):
    """
    Função MOCK para simular a chamada de API. 
    Implementa a Basic Auth para ASTO.
    """
    
    st.info(f"Chamando API {api_name} de {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}...")
    
    headers = {}
    
    # 3.3. Lógica de Basic Authorization para ASTO
    if api_name == 'ASTO' and username and password:
        credentials = f"{username}:{password}"
        encoded_creds = base64.b64encode(credentials.encode()).decode()
        headers = {"Authorization": f"Basic {encoded_creds}"}
        st.caption(f"ASTO Headers: Authorization: Basic {encoded_creds[:10]}...") # Exibe apenas o prefixo
    elif token:
         headers = {"Authorization": f"Bearer {token}"}
         st.caption(f"{api_name} Headers: Authorization: Bearer {token[:10]}...")

    # --- MOCK: Retorna um DataFrame simulado ---
    try:
        num_records = random.randint(150, 300) 
        data = {
            'data_transacao': [start_date + timedelta(days=random.randint(0, (end_date - start_date).days)) for _ in range(num_records)],
            'valor_bruto': [round(random.uniform(50, 500), 2) for _ in range(num_records)],
            'produto_api': [f'Produto_{api_name}_{i % 5}' for i in range(num_records)],
            'cnpj_cliente': [f'222352020001{i % 100}' for i in range(num_records)],
            'vendedor_mock_id': [f'mock_v{i % 3}' for i in range(num_records)],
            'origem': [api_name] * num_records
        }
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Falha no MOCK de dados API {api_name}: {e}")
        return pd.DataFrame()


def process_api_data(db_client, data_inicial, data_final):
    """Orquestra a busca de dados de todas as APIs (ELIQ e ASTO)."""
    
    api_secrets = st.secrets["api_credentials"]
    
    # 1. Busca ELIQ
    df_eliq = get_api_data(
        api_name='ELIQ',
        url=api_secrets["eliq_url"], 
        token=api_secrets["eliq_token"], 
        start_date=data_inicial, 
        end_date=data_final
    )
    
    # 2. Busca ASTO (Basic Auth)
    df_asto = get_api_data(
        api_name='ASTO',
        url=api_secrets["asto_url"],
        username=api_secrets["asto_username"],
        password=api_secrets["asto_password"],
        start_date=data_inicial, 
        end_date=data_final
    )

    # 3. Combinação
    df_combined = pd.concat([df_eliq, df_asto], ignore_index=True)
    
    if not df_combined.empty:
        # 4. Salvar no Firestore (Cache)
        data_list = df_combined.to_dict('records')
        
        # Limita o tamanho para evitar estourar o limite de 1MB por documento do Firestore
        limit = 500 
        
        db_client.collection('api_cache').document('last_run').set({
            'start_date': data_inicial.strftime('%Y-%m-%d'),
            'end_date': data_final.strftime('%Y-%m-%d'),
            'record_count': len(data_list),
            'data_sample': data_list[:limit] 
        })
        st.success(f"✅ APIs consultadas com sucesso! {len(df_combined)} registros combinados (amostra de {min(len(data_list), limit)} salva no cache).")
    else:
        st.warning("⚠️ Nenhuma informação de API encontrada para o período selecionado.")


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
                
                
# --- 4. Módulo de Gestão de Equipe e Carteira ---

@st.cache_data
def get_consultores_from_db(db_client):
    consultores_ref = db_client.collection('consultores').stream()
    return {doc.to_dict()['nome']: doc.id for doc in consultores_ref}

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
                # 1. Cria o usuário no Firebase Authentication (Senha MOCK)
                user = auth.create_user(
                    email=email,
                    password='logpay123',
                    display_name=nome
                )
                
                # 2. Salva o perfil na coleção 'consultores'
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


# --- 5. Módulo de Inteligência Comercial (BI) ---

@st.cache_data(ttl=600) 
def get_combined_data(db_client):
    """Puxa e combina todos os dados para o BI."""
    
    # 1. Puxar Bionio Data (Produtos/Pedidos)
    df_bionio = pd.DataFrame([doc.to_dict() for doc in db_client.collection('bionio_data').stream()])
    if not df_bionio.empty:
        df_bionio['origem'] = 'Bionio'
        df_bionio['receita'] = df_bionio.get('valor_total_do_pedido', 0)
        df_bionio['produto'] = df_bionio.get('nome_do_beneficio', 'Bionio - Pedido')
    
    # 2. Puxar Rovema Pay Data (Vendas/Transações)
    df_rovema = pd.DataFrame([doc.to_dict() for doc in db_client.collection('rovema_pay_data').stream()])
    if not df_rovema.empty:
        df_rovema['origem'] = 'Rovema Pay'
        df_rovema['receita'] = df_rovema.get('liquido', 0)
        df_rovema['produto'] = df_rovema.get('bandeira', 'Rovema Pay - Vendas')

    # 3. Puxar API Cache (ELIQ/ASTO)
    api_cache_doc = db_client.collection('api_cache').document('last_run').get()
    api_data = api_cache_doc.to_dict().get('data_sample', []) if api_cache_doc.exists else []
    df_api = pd.DataFrame(api_data)
    if not df_api.empty:
        df_api['receita'] = df_api.get('valor_bruto', 0)
        df_api['produto'] = df_api.get('produto_api', df_api['origem'])
        
    # 4. Combinação
    df_combined = pd.concat([df_bionio, df_rovema, df_api], ignore_index=True)
    df_combined['receita'] = df_combined['receita'].fillna(0)
    
    # 5. MOCK/JOIN de Atribuição de Consultor
    carteira_docs = db_client.collection('carteira_clientes').stream()
    df_carteira = pd.DataFrame([doc.to_dict() for doc in carteira_docs])
    
    if not df_carteira.empty and not df_combined.empty:
         # MOCK: atribui aleatoriamente para simular o filtro do BI.
         valid_uids = df_carteira['consultor_uid'].unique()
         if valid_uids.size > 0:
            df_combined['consultor_uid'] = df_combined.apply(lambda x: random.choice(valid_uids) if random.random() < 0.6 else None, axis=1)

    return df_combined

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
             st.warning(f"O consultor **{vendedor_filtro_nome}** não possui dados atribuídos para este período.")
             df_data = df_raw.copy() # Mostra o geral se não houver dados do vendedor.
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
            st.markdown("*Ação Sugerida:* Consultores devem contatar estes clientes para converter a venda. Filtre por vendedor no menu lateral para focar.")
        else:
            st.success("Nenhum pedido Bionio em status 'Aguardando Pagamento' encontrado no filtro.")

# --- 6. Roteamento da Aplicação Principal ---

if 'logged_in' not in st.session_state or not st.session_state['logged_in']:
    login_page()
else:
    # Obter a referência do Firestore (já inicializado)
    db_client = firestore.client() 

    # Menu Lateral para Navegação
    with st.sidebar:
        st.write(f"Usuário: **{st.session_state['user_email']}**")
        page = st.radio("Navegação", ["Dashboard (BI)", "Atualização de Dados", "Gestão de Equipe"])
        st.markdown("---")
        st.button("Sair", on_click=logout)

    # Conteúdo da Página
    if page == "Dashboard (BI)":
        bi_dashboard_page(db_client)
    elif page == "Atualização de Dados":
        data_ingestion_page(db_client)
    elif page == "Gestão de Equipe":
        management_page(db_client)
