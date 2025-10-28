import pandas as pd
import numpy as np
import requests
import streamlit as st
from datetime import datetime, date, time
import altair as alt

# Importa módulos Firebase
try:
    from fire_admin import log_event, db, server_timestamp, save_data_to_firestore
except ImportError:
    def log_event(action: str, details: str = ""): pass
    def save_data_to_firestore(product_name, df_data, source_type): return False, "DB Error"
    db = None


# --- MOCKS de APIs (Retorna RAW data para cache) ---

def fetch_asto_data(start_date: date, end_date: date):
    """Simula a coleta de dados RAW de Fatura Pagamento Fechada do Asto (API)."""
    
    data = [
        {"dataFimApuracao": "2025-10-07T00:00:00", "valorBruto": 2159.81, "valorLiquido": 2084.21},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 15986.99, "valorLiquido": 15427.44},
        {"dataFimApuracao": "2025-10-21T00:00:00", "valorBruto": 9970.72, "valorLiquido": 9621.74},
        {"dataFimApuracao": "2025-07-15T00:00:00", "valorBruto": 5000.00, "valorLiquido": 4800.00},
    ]
    df = pd.DataFrame(data)
    # Garante que os dados mocados sejam convertidos para datetime para salvar corretamente
    df['dataFimApuracao'] = pd.to_datetime(df['dataFimApuracao']).dt.normalize()
    df['Receita'] = df['valorBruto'] - df['valorLiquido']
    return df

def fetch_eliq_data(start_date: date, end_date: date):
    """Simula a coleta de dados RAW de Transações do Eliq (API)."""
    
    data = [
        {"data_cadastro": "2025-09-01 10:00:00", "valor_total": 500.00, "consumo_medio": 7.5, "status": "confirmada"},
        {"data_cadastro": "2025-09-08 15:30:00", "valor_total": 1200.50, "consumo_medio": 6.8, "status": "confirmada"},
        {"data_cadastro": "2025-10-01 14:00:00", "valor_total": 900.00, "consumo_medio": 7.3, "status": "confirmada"},
        {"data_cadastro": "2025-07-20 10:00:00", "valor_total": 300.00, "consumo_medio": 7.0, "status": "confirmada"},
    ]
    df = pd.DataFrame(data)
    # Garante que os dados mocados sejam convertidos para datetime para salvar corretamente
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro']).dt.normalize()
    return df

@st.cache_data(ttl=3600, show_spinner="Buscando dados da API e armazenando...")
def fetch_api_and_save(product_name: str, start_date: date, end_date: date):
    """
    (Movido de fire_admin.py)
    Busca os dados via API Mock, salva no Firestore e retorna o DataFrame.
    """
    
    if product_name == 'Asto':
        df_raw = fetch_asto_data(start_date, end_date)
    elif product_name == 'Eliq':
        df_raw = fetch_eliq_data(start_date, end_date)
    else:
        return pd.DataFrame()
        
    if df_raw.empty:
        log_event("API_FETCH_EMPTY", f"API de {product_name} retornou dados vazios.")
        return pd.DataFrame()
        
    # Salva o resultado no Firestore
    # (Em um cenário real, isso teria lógica para evitar duplicatas)
    success, message = save_data_to_firestore(product_name, df_raw, 'API')

    return df_raw


# --- Funções de Processamento de Arquivos ---

def process_uploaded_file(uploaded_file, product_name):
    """Processa o arquivo, faz a limpeza, padroniza nomes de coluna e retorna o DataFrame RAW."""
    try:
        is_csv = uploaded_file.name.lower().endswith('.csv')
        uploaded_file.seek(0)

        if is_csv:
            try:
                df = pd.read_csv(uploaded_file, decimal=',', sep=';', thousands='.', header=0, encoding='latin-1')
            except UnicodeDecodeError:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, decimal=',', sep=';', thousands='.', header=0, encoding='utf-8')
            except Exception:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, header=0)
        else: # XLSX
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, header=0) 
        
        df.columns = [col.strip().lower().replace(' ', '_').replace('.', '').replace('%', '') if isinstance(col, str) else str(col).lower() for col in df.columns]
        
        df = df.dropna(how='all', axis=0)
        df = df.dropna(how='all', axis=1)

        if product_name == 'Bionio':
            df_processed = process_bionio_data(df.copy())
        elif product_name == 'RovemaPay':
            df_processed = process_rovemapay_data(df.copy())
        else:
            df_processed = df
            
        return True, "Processamento bem-sucedido.", df_processed
        
    except Exception as e:
        log_event("FILE_PROCESSING_FAIL", f"Erro no processamento de {product_name}: {e}")
        return False, f"Erro no processamento do arquivo: {e}", None 

def process_bionio_data(df):
    """Limpa e formata os dados do Bionio. Sem agregação."""
    
    VALOR_COL = 'valor_total_do_pedido'
    DATA_COL = 'data_da_criação_do_pedido'
    REQUIRED_COLS = [VALOR_COL, DATA_COL]
    
    if not all(col in df.columns for col in REQUIRED_COLS):
        missing = [col for col in REQUIRED_COLS if col not in df.columns]
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}. Colunas disponíveis: {df.columns.tolist()}")

    df[VALOR_COL] = df[VALOR_COL].astype(str).str.replace(r'[\sR\$]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    df[VALOR_COL] = pd.to_numeric(df[VALOR_COL], errors='coerce')
    
    # Converte para datetime e normaliza (remove horas)
    df[DATA_COL] = pd.to_datetime(df[DATA_COL], format='%d/%m/%Y', errors='coerce').dt.normalize()
    
    df = df.dropna(subset=[VALOR_COL, DATA_COL])
    return df

def process_rovemapay_data(df):
    """Limpa e formata os dados do Rovema Pay. Sem agregação."""
    LIQUIDO_COL = 'liquido'
    BRUTO_COL = 'bruto'
    VENDA_COL = 'venda'
    STATUS_COL = 'status'
    REQUIRED_COLS = [LIQUIDO_COL, BRUTO_COL, VENDA_COL, STATUS_COL]

    if not all(col in df.columns for col in REQUIRED_COLS):
        missing = [col for col in REQUIRED_COLS if col not in df.columns]
        raise ValueError(f"Colunas obrigatóbrias ausentes: {missing}. Colunas disponíveis: {df.columns.tolist()}")

    def clean_value(series_key):
        series = df.get(series_key)
        if series is None: return pd.Series([np.nan] * len(df))
            
        if series.dtype == 'object':
            cleaned = series.fillna('').astype(str).str.replace(r'[\sR\$\%]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            return cleaned
        return series
        
    df[LIQUIDO_COL] = pd.to_numeric(clean_value(LIQUIDO_COL), errors='coerce')
    df[BRUTO_COL] = pd.to_numeric(clean_value(BRUTO_COL), errors='coerce')
    
    df['taxa_cliente'] = pd.to_numeric(clean_value('taxa_cliente'), errors='coerce')
    df['taxa_adquirente'] = pd.to_numeric(clean_value('taxa_adquirente'), errors='coerce')
    
    # Converte para datetime e normaliza (remove horas)
    df[VENDA_COL] = pd.to_datetime(df.get(VENDA_COL), errors='coerce').dt.normalize()
    
    df = df.dropna(subset=[BRUTO_COL, LIQUIDO_COL, VENDA_COL, STATUS_COL])
    
    df['receita'] = df[BRUTO_COL] - df[LIQUIDO_COL]
    df['custo_total_perc'] = np.where(df[BRUTO_COL] != 0, (df[LIQUIDO_COL] / df[BRUTO_COL]) * 100, 0)
    
    return df

# --- Funções de Busca e Agregação de Dados ---

@st.cache_data(ttl=600, show_spinner=False)
def get_raw_data_from_firestore(product_name: str, start_date: date, end_date: date):
    """
    **[NOVA FUNÇÃO ROBUSTA]**
    Busca dados RAW do Firestore, FILTRANDO por data no lado do servidor.
    """
    if st.session_state.get('db') is None: 
        return pd.DataFrame()

    # 1. Define o nome da coleção e a coluna de data relevante
    collection_name = f"data_{product_name.lower().replace(' ', '_')}"
    DATA_COL = None
    if product_name == 'Bionio':
        DATA_COL = 'data_da_criação_do_pedido'
    elif product_name == 'Rovema Pay':
        DATA_COL = 'venda'
    elif product_name == 'Asto':
        DATA_COL = 'dataFimApuracao'
    elif product_name == 'Eliq':
        DATA_COL = 'data_cadastro'
    
    if not DATA_COL:
        log_event("FIRESTORE_FETCH_FAIL", f"Produto '{product_name}' desconhecido para query.")
        return pd.DataFrame()

    # 2. Converte datas para datetime para a query do Firestore
    start_datetime = datetime.combine(start_date, time.min)
    end_datetime = datetime.combine(end_date, time.max)

    # 3. Executa a query filtrada no Firestore
    try:
        query_ref = st.session_state['db'].collection(collection_name)
        
        # FILTRA NO LADO DO BANCO DE DADOS
        query_ref = query_ref.where(DATA_COL, '>=', start_datetime)
        query_ref = query_ref.where(DATA_COL, '<=', end_datetime)
        
        # Limite de segurança para evitar custos excessivos
        docs = query_ref.limit(20000).stream() 
        
        data_list = [doc.to_dict() for doc in docs]

        if not data_list: 
            return pd.DataFrame()

        df = pd.DataFrame(data_list)
        
        # 4. Converte colunas de data (que vêm como Timestamps) para datetime
        if DATA_COL in df.columns:
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce').dt.normalize()
            df = df.dropna(subset=[DATA_COL]) # Garante que datas nulas sejam removidas
        else:
            return pd.DataFrame() # Coluna de data não encontrada nos dados retornados

        return df

    except Exception as e:
        log_event("FIRESTORE_FETCH_FAIL", f"Falha ao buscar dados de {product_name} no Firestore: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def get_latest_aggregated_data(product_name: str, start_date: date, end_date: date):
    """
    **[FUNÇÃO REFATORADA]**
    Busca os dados RAW (já filtrados) e os AGREGA para os gráficos.
    """
    
    # 1. Se for Asto ou Eliq, dispara o salvamento/cache da API
    # (Em produção, isso seria um job agendado, mas aqui simula a atualização)
    if product_name in ['Asto', 'Eliq']:
        fetch_api_and_save(product_name, start_date, end_date)
    
    # 2. Busca os dados RAW já filtrados por data no Firestore
    df = get_raw_data_from_firestore(product_name, start_date, end_date)
    
    if df.empty:
        return pd.DataFrame()

    # 3. AGREGAÇÃO
    try:
        if product_name == 'Bionio':
            DATA_COL = 'data_da_criação_do_pedido'
            if DATA_COL not in df.columns or 'valor_total_do_pedido' not in df.columns:
                return pd.DataFrame()

            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))['valor_total_do_pedido'].sum().reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg.rename(columns={'valor_total_do_pedido': 'Valor Total Pedidos'}).drop(columns=[DATA_COL])
            
        elif product_name == 'Rovema Pay':
            DATA_COL = 'venda'
            REQUIRED = ['venda', 'receita', 'liquido', 'custo_total_perc', 'status']
            if not all(col in df.columns for col in REQUIRED): 
                return pd.DataFrame()
            
            df['status'] = df['status'].astype(str)

            df_agg = df.groupby([df[DATA_COL].dt.to_period('M'), 'status']).agg(
                Liquido=('liquido', 'sum'),
                Receita=('receita', 'sum'),
                Taxa_Media=('custo_total_perc', 'mean')
            ).reset_index()
            
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            
            df_agg['Receita'] = pd.to_numeric(df_agg['Receita'], errors='coerce')
            df_agg['Taxa_Media'] = pd.to_numeric(df_agg['Taxa_Media'], errors='coerce')

            return df_agg.drop(columns=[DATA_COL]).dropna()
        
        elif product_name == 'Asto':
            DATA_COL = 'dataFimApuracao'
            if DATA_COL not in df.columns or 'valorBruto' not in df.columns or 'Receita' not in df.columns:
                return pd.DataFrame()
            
            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))[['valorBruto', 'Receita']].sum().reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg

        elif product_name == 'Eliq':
            DATA_COL = 'data_cadastro'
            if DATA_COL not in df.columns or 'valor_total' not in df.columns or 'consumo_medio' not in df.columns:
                return pd.DataFrame()
                
            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))[['valor_total', 'consumo_medio']].agg({'valor_total': 'sum', 'consumo_medio': 'mean'}).reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg

        return pd.DataFrame()

    except Exception as e:
        log_event("DATA_AGGREGATION_FAIL", f"Falha ao agregar dados de {product_name}: {e}")
        return pd.DataFrame()

# Adicione esta função ao final do seu arquivo utils/data_processing.py

@st.cache_data(ttl=3600, show_spinner="Buscando lista de clientes...")
def get_unique_clients_from_raw_data():
    """
    [NOVO] Busca todos os clientes (empresas) únicos nos dados brutos
    para vincular aos consultores.
    """
    # Define um período de tempo longo para buscar todos os clientes
    # Em um app real, isso poderia ser uma coleção 'empresas' separada.
    start_date = date(2020, 1, 1)
    end_date = date(2099, 12, 31)
    
    clients = set()

    # 1. Busca clientes do Rovema Pay
    df_rovemapay_raw = get_raw_data_from_firestore('Rovema Pay', start_date, end_date)
    if not df_rovemapay_raw.empty:
        # Tenta usar 'cnpj' primeiro, se não, 'cliente'
        if 'cnpj' in df_rovemapay_raw.columns:
            clients.update(df_rovemapay_raw['cnpj'].dropna().unique())
        elif 'cliente' in df_rovemapay_raw.columns:
            clients.update(df_rovemapay_raw['cliente'].dropna().unique())
            
    # 2. Busca clientes do Bionio
    df_bionio_raw = get_raw_data_from_firestore('Bionio', start_date, end_date)
    if not df_bionio_raw.empty:
        # (Assumindo que Bionio também tenha uma coluna 'cliente' ou 'cnpj')
        if 'cnpj' in df_bionio_raw.columns:
            clients.update(df_bionio_raw['cnpj'].dropna().unique())
        elif 'cliente' in df_bionio_raw.columns:
             clients.update(df_bionio_raw['cliente'].dropna().unique())
             
    # Adicione aqui lógicas para 'Asto' e 'Eliq' se eles também tiverem clientes
    
    return sorted(list(clients))
