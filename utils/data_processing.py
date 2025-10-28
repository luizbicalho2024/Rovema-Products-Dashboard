import pandas as pd
import numpy as np
import requests
import streamlit as st
from datetime import datetime
import altair as alt

# Importa módulos Firebase
try:
    from fire_admin import log_event, db, server_timestamp
    from google.cloud.firestore import SERVER_TIMESTAMP as server_timestamp
except ImportError:
    # Mocks para ambiente de teste sem Firebase (Se necessário)
    def log_event(action: str, details: str = ""): pass


# --- MOCKS de APIs (Asto e Eliq) ---

def fetch_asto_data(start_date: str, end_date: str):
    """Simula a coleta de dados de Fatura Pagamento Fechada do Asto (API)."""
    log_event("API_CALL_ASTO", f"Simulação de chamada Asto para {start_date} a {end_date}")
    
    data = [
        {"dataFimApuracao": "2025-10-07T00:00:00", "valorBruto": 2159.81, "valorLiquido": 2084.21},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 15986.99, "valorLiquido": 15427.44},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 683.80, "valorLiquido": 659.86},
        {"dataFimApuracao": "2025-10-21T00:00:00", "valorBruto": 9970.72, "valorLiquido": 9621.74},
        {"dataFimApuracao": "2025-10-28T00:00:00", "valorBruto": 3582.86, "valorLiquido": 3457.45},
    ]
    df = pd.DataFrame(data)
    df['dataFimApuracao'] = pd.to_datetime(df['dataFimApuracao']).dt.normalize()
    df['Receita'] = df['valorBruto'] - df['valorLiquido']
    return df.groupby('dataFimApuracao')[['valorBruto', 'Receita']].sum().reset_index()

def fetch_eliq_data(start_date: str, end_date: str):
    """Simula a coleta de dados de Transações do Eliq (API)."""
    log_event("API_CALL_ELIQ", f"Simulação de chamada Eliq para {start_date} a {end_date}")
    
    data = [
        {"data_cadastro": "2025-09-01 10:00:00", "valor_total": 500.00, "consumo_medio": 7.5, "status": "confirmada"},
        {"data_cadastro": "2025-09-08 15:30:00", "valor_total": 1200.50, "consumo_medio": 6.8, "status": "confirmada"},
        {"data_cadastro": "2025-09-15 11:20:00", "valor_total": 850.25, "consumo_medio": 7.2, "status": "confirmada"},
        {"data_cadastro": "2025-09-22 09:00:00", "valor_total": 1500.00, "consumo_medio": 7.0, "status": "confirmada"},
        {"data_cadastro": "2025-09-29 14:00:00", "valor_total": 900.00, "consumo_medio": 7.3, "status": "confirmada"},
    ]
    df = pd.DataFrame(data)
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro']).dt.normalize()
    return df.groupby('data_cadastro')[['valor_total', 'consumo_medio']].agg({'valor_total': 'sum', 'consumo_medio': 'mean'}).reset_index()


# --- Funções de Processamento de Arquivos ---

def process_uploaded_file(uploaded_file, product_name):
    """
    Processa o arquivo, faz a limpeza, padroniza nomes de coluna e retorna
    o DataFrame RAW (sem agregação) para salvamento completo no Firestore.
    """
    try:
        is_csv = uploaded_file.name.lower().endswith('.csv')
        uploaded_file.seek(0)

        if is_csv:
            # Tenta leituras robustas para CSV
            try:
                df = pd.read_csv(uploaded_file, decimal=',', sep=';', thousands='.')
            except Exception:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file)
        else: # XLSX
            # Lê o Excel, assumindo header na primeira linha
            df = pd.read_excel(uploaded_file, header=0) 
        
        # CRITICAL FIX: Limpa, padroniza (lowercase e underscore) os nomes das colunas
        df.columns = [col.strip().lower().replace(' ', '_') if isinstance(col, str) else str(col).lower() for col in df.columns]
        
        if product_name == 'Bionio':
            df_processed = process_bionio_data(df.copy())
        elif product_name == 'RovemaPay':
            df_processed = process_rovemapay_data(df.copy())
        else:
            df_processed = df.head(5)
            
        return True, "Processamento bem-sucedido.", df_processed
        
    except Exception as e:
        log_event("FILE_PROCESSING_FAIL", f"Erro no processamento de {product_name}: {e}")
        # Retorna o erro real para o usuário
        return False, f"Erro no processamento do arquivo: {e}", None 

def process_bionio_data(df):
    """Limpa e formata os dados do Bionio. Sem agregação."""
    
    # Mapeamento de colunas padronizadas
    VALOR_COL = 'valor_total_do_pedido'
    DATA_COL = 'data_da_criação_do_pedido'
    
    REQUIRED_COLS = [VALOR_COL, DATA_COL]
    
    # 1. Validação de colunas
    if not all(col in df.columns for col in REQUIRED_COLS):
        missing = [col for col in REQUIRED_COLS if col not in df.columns]
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}. Colunas disponíveis: {df.columns.tolist()}")

    # 2. Limpeza e conversão de Valor
    df[VALOR_COL] = df[VALOR_COL].astype(str).str.replace(r'[\sR\$]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    df[VALOR_COL] = pd.to_numeric(df[VALOR_COL], errors='coerce')
    
    # 3. Conversão de Data
    df[DATA_COL] = pd.to_datetime(df[DATA_COL], format='%d/%m/%Y', errors='coerce')
    
    # 4. Limpeza final
    df = df.dropna(subset=[VALOR_COL, DATA_COL])
    
    # Retorna o DataFrame RAW limpo (todas as linhas).
    return df

def process_rovemapay_data(df):
    """Limpa e formata os dados do Rovema Pay. Sem agregação."""
    
    # Mapeamento de colunas padronizadas
    LIQUIDO_COL = 'liquido'
    BRUTO_COL = 'bruto'
    VENDA_COL = 'venda'
    STATUS_COL = 'status'
    
    REQUIRED_COLS = [LIQUIDO_COL, BRUTO_COL, VENDA_COL, STATUS_COL]

    # 1. Validação de colunas
    if not all(col in df.columns for col in REQUIRED_COLS):
        missing = [col for col in REQUIRED_COLS if col not in df.columns]
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}. Colunas disponíveis: {df.columns.tolist()}")

    # Função auxiliar para limpar e converter valores de string (mais defensiva)
    def clean_value(series_key):
        series = df.get(series_key)
        if series is None: return pd.Series([np.nan] * len(df))
            
        if series.dtype == 'object':
            cleaned = series.fillna('').astype(str).str.replace(r'[\sR\$\%]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            return cleaned
        return cleaned
        
    # 2. Conversão de colunas de valor
    df[LIQUIDO_COL] = pd.to_numeric(clean_value(LIQUIDO_COL), errors='coerce')
    df[BRUTO_COL] = pd.to_numeric(clean_value(BRUTO_COL), errors='coerce')
    df['taxa_cliente'] = pd.to_numeric(clean_value('taxa_cliente'), errors='coerce')
    df['taxa_adquirente'] = pd.to_numeric(clean_value('taxa_adquirente'), errors='coerce')
    
    # 3. Conversão de Data
    df[VENDA_COL] = pd.to_datetime(df.get(VENDA_COL), errors='coerce')
    
    # 4. Limpeza: Remove linhas com valores nulos nas colunas críticas
    df = df.dropna(subset=[BRUTO_COL, LIQUIDO_COL, VENDA_COL])
    
    # 5. Cálculo das colunas de Receita e Custo_Total_Perc (para salvar com o dado)
    df['receita'] = df[BRUTO_COL] - df[LIQUIDO_COL]
    df['custo_total_perc'] = np.where(df[BRUTO_COL] != 0, (df['receita'] / df[BRUTO_COL]) * 100, 0)
    
    # Retorna o DataFrame RAW limpo (todas as linhas).
    return df


# --- FUNÇÃO: BUSCAR DADOS DO FIRESTORE E AGREGAR ---

@st.cache_data(ttl=600) 
def get_latest_uploaded_data(product_name):
    """
    Busca todos os dados RAW da coleção do Firestore e AGGREGA para o Dashboard.
    """
    if st.session_state.get('db') is None: return pd.DataFrame()
    collection_name = f"data_{product_name.lower()}"
    
    try:
        # Busca até 1000 documentos para evitar timeout (idealmente, use filtros)
        docs = st.session_state['db'].collection(collection_name).limit(1000).stream()
        data_list = []
        for doc in docs:
            data = doc.to_dict()
            data_list.append(data)

        if not data_list: return pd.DataFrame()

        df = pd.DataFrame(data_list)
        
        # --- NOVO BLOCO: AGREGAÇÃO FEITA AQUI, NO CONSUMIDOR ---
        if product_name == 'Bionio':
            # 1. Converte a coluna de data
            if 'data_da_criação_do_pedido' not in df.columns: return pd.DataFrame()
                
            df['data_da_criação_do_pedido'] = pd.to_datetime(df['data_da_criação_do_pedido'], errors='coerce')
            df = df.dropna(subset=['data_da_criação_do_pedido', 'valor_total_do_pedido'])
            
            # 2. Agregação
            df_agg = df.groupby(df['data_da_criação_do_pedido'].dt.to_period('M'))['valor_total_do_pedido'].sum().reset_index()
            df_agg['Mês'] = df_agg['data_da_criação_do_pedido'].astype(str)
            return df_agg.rename(columns={'valor_total_do_pedido': 'Valor Total Pedidos'}).drop(columns=['data_da_criação_do_pedido'])
            
        elif product_name == 'RovemaPay':
            # Validação das colunas salvas
            REQUIRED = ['venda', 'receita', 'liquido', 'custo_total_perc', 'status']
            if not all(col in df.columns for col in REQUIRED): return pd.DataFrame()
                
            # 1. Converte a coluna de data
            df['venda'] = pd.to_datetime(df['venda'], errors='coerce')
            df = df.dropna(subset=['venda'])
            
            # 2. Agregação
            df_agg = df.groupby([df['venda'].dt.to_period('M'), 'status']).agg(
                Liquido=('liquido', 'sum'),
                Receita=('receita', 'sum'),
                Taxa_Media=('custo_total_perc', 'mean')
            ).reset_index()
            
            df_agg['Mês'] = df_agg['venda'].astype(str)
            return df_agg.drop(columns=['venda']).dropna()

        return pd.DataFrame()

    except Exception as e:
        log_event("FIRESTORE_FETCH_FAIL", f"Falha ao buscar dados de {product_name} no Firestore: {e}")
        return pd.DataFrame()
