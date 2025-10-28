import pandas as pd
import numpy as np
import requests
import streamlit as st
from datetime import datetime, date
import altair as alt

# Importa módulos Firebase
try:
    from fire_admin import log_event, db, server_timestamp
    from google.cloud.firestore import SERVER_TIMESTAMP as server_timestamp
except ImportError:
    def log_event(action: str, details: str = ""): pass


# --- MOCKS de APIs (Asto e Eliq) ---

def fetch_asto_data(start_date: date, end_date: date):
    """Simula a coleta de dados de Fatura Pagamento Fechada do Asto (API)."""
    log_event("API_CALL_ASTO", f"Simulação de chamada Asto para {start_date} a {end_date}")
    
    data = [
        {"dataFimApuracao": "2025-10-07T00:00:00", "valorBruto": 2159.81, "valorLiquido": 2084.21},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 15986.99, "valorLiquido": 15427.44},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 683.80, "valorLiquido": 659.86},
        {"dataFimApuracao": "2025-10-21T00:00:00", "valorBruto": 9970.72, "valorLiquido": 9621.74},
        {"dataFimApuracao": "2025-10-28T00:00:00", "valorBruto": 3582.86, "valorLiquido": 3457.45},
        {"dataFimApuracao": "2025-07-15T00:00:00", "valorBruto": 5000.00, "valorLiquido": 4800.00},
    ]
    df = pd.DataFrame(data)
    df['dataFimApuracao'] = pd.to_datetime(df['dataFimApuracao']).dt.normalize()
    df['Receita'] = df['valorBruto'] - df['valorLiquido']

    # FILTRO DE DATA
    df = df[
        (df['dataFimApuracao'].dt.date >= start_date) & 
        (df['dataFimApuracao'].dt.date <= end_date)
    ].copy()
    
    return df.groupby('dataFimApuracao')[['valorBruto', 'Receita']].sum().reset_index()

def fetch_eliq_data(start_date: date, end_date: date):
    """Simula a coleta de dados de Transações do Eliq (API)."""
    log_event("API_CALL_ELIQ", f"Simulação de chamada Eliq para {start_date} a {end_date}")
    
    data = [
        {"data_cadastro": "2025-09-01 10:00:00", "valor_total": 500.00, "consumo_medio": 7.5, "status": "confirmada"},
        {"data_cadastro": "2025-09-08 15:30:00", "valor_total": 1200.50, "consumo_medio": 6.8, "status": "confirmada"},
        {"data_cadastro": "2025-09-15 11:20:00", "valor_total": 850.25, "consumo_medio": 7.2, "status": "confirmada"},
        {"data_cadastro": "2025-09-22 09:00:00", "valor_total": 1500.00, "consumo_medio": 7.0, "status": "confirmada"},
        {"data_cadastro": "2025-10-01 14:00:00", "valor_total": 900.00, "consumo_medio": 7.3, "status": "confirmada"},
        {"data_cadastro": "2025-07-20 10:00:00", "valor_total": 300.00, "consumo_medio": 7.0, "status": "confirmada"},
    ]
    df = pd.DataFrame(data)
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro']).dt.normalize()

    # FILTRO DE DATA
    df = df[
        (df['data_cadastro'].dt.date >= start_date) & 
        (df['data_cadastro'].dt.date <= end_date)
    ].copy()

    return df.groupby('data_cadastro')[['valor_total', 'consumo_medio']].agg({'valor_total': 'sum', 'consumo_medio': 'mean'}).reset_index()


# [Mantenha process_uploaded_file e as funções de processamento RAW IGUAIS]

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
            df_processed = df.head(5)
            
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
    
    df[DATA_COL] = pd.to_datetime(df[DATA_COL], format='%d/%m/%Y', errors='coerce')
    
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
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}. Colunas disponíveis: {df.columns.tolist()}")

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
    
    df[VENDA_COL] = pd.to_datetime(df.get(VENDA_COL), errors='coerce')
    
    df = df.dropna(subset=[BRUTO_COL, LIQUIDO_COL, VENDA_COL, STATUS_COL])
    
    df['receita'] = df[BRUTO_COL] - df[LIQUIDO_COL]
    df['custo_total_perc'] = np.where(df[BRUTO_COL] != 0, (df[LIQUIDO_COL] / df[BRUTO_COL]) * 100, 0)
    
    return df


# --- FUNÇÃO: BUSCAR DADOS DO FIRESTORE E AGREGAR (CORRIGIDA) ---

# NOTE: A função precisa aceitar start_date e end_date para que o cache funcione corretamente
@st.cache_data(ttl=600, show_spinner=False)
def get_latest_uploaded_data(product_name, start_date: date, end_date: date):
    """
    Busca todos os dados RAW da coleção do Firestore, FILTRA por data e AGGREGA.
    """
    if st.session_state.get('db') is None: return pd.DataFrame()
    collection_name = f"data_{product_name.lower()}"
    
    try:
        # 1. BUSCA RAW DATA DO FIRESTORE (Busca sem filtro de data no Firebase)
        docs = st.session_state['db'].collection(collection_name).limit(1000).stream()
        data_list = [doc.to_dict() for doc in docs]

        if not data_list: return pd.DataFrame()

        df = pd.DataFrame(data_list)
        
        # 2. FILTRAGEM E AGREGAÇÃO (Aplica o filtro de data no Pandas)
        
        if product_name == 'Bionio':
            DATA_COL = 'data_da_criação_do_pedido'
            if DATA_COL not in df.columns: return pd.DataFrame()
                
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce').dt.normalize()
            df = df.dropna(subset=[DATA_COL, 'valor_total_do_pedido'])
            
            # FILTRO DE DATA RAW
            df = df[
                (df[DATA_COL].dt.date >= start_date) & 
                (df[DATA_COL].dt.date <= end_date)
            ].copy()
            
            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))['valor_total_do_pedido'].sum().reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg.rename(columns={'valor_total_do_pedido': 'Valor Total Pedidos'}).drop(columns=[DATA_COL])
            
        elif product_name == 'RovemaPay':
            DATA_COL = 'venda'
            REQUIRED = ['venda', 'receita', 'liquido', 'custo_total_perc', 'status']
            if not all(col in df.columns for col in REQUIRED): return pd.DataFrame()
                
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce').dt.normalize()
            df = df.dropna(subset=[DATA_COL])
            
            # FILTRO DE DATA RAW
            df = df[
                (df[DATA_COL].dt.date >= start_date) & 
                (df[DATA_COL].dt.date <= end_date)
            ].copy()
            
            df_agg = df.groupby([df[DATA_COL].dt.to_period('M'), 'status']).agg(
                Liquido=('liquido', 'sum'),
                Receita=('receita', 'sum'),
                Taxa_Media=('custo_total_perc', 'mean')
            ).reset_index()
            
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            
            df_agg['Receita'] = pd.to_numeric(df_agg['Receita'], errors='coerce')
            df_agg['Taxa_Media'] = pd.to_numeric(df_agg['Taxa_Media'], errors='coerce')

            return df_agg.drop(columns=[DATA_COL]).dropna()

        return pd.DataFrame()

    except Exception as e:
        log_event("FIRESTORE_FETCH_FAIL", f"Falha ao buscar dados de {product_name} no Firestore:
