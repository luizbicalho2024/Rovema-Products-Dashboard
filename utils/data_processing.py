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
        {"dataFimApuracao": "2025-10-07T00:00:00", "valorBruto": 2159.81, "valorLiquido": 2084.21, "cnpj": "11.111.111/0001-11", "cliente": "Cliente Asto 1"},
        {"dataFimApuracao": "2025-10-14T00:00:00", "valorBruto": 15986.99, "valorLiquido": 15427.44, "cnpj": "22.222.222/0001-22", "cliente": "Cliente Asto 2"},
    ]
    df = pd.DataFrame(data)
    df['dataFimApuracao'] = pd.to_datetime(df['dataFimApuracao']).dt.normalize()
    df['Receita'] = df['valorBruto'] - df['valorLiquido']
    return df

def fetch_eliq_data(start_date: date, end_date: date):
    """Simula a coleta de dados RAW de Transações do Eliq (API)."""
    
    data = [
        {"data_cadastro": "2025-09-01 10:00:00", "valor_total": 500.00, "consumo_medio": 7.5, "status": "confirmada", "cnpj_posto": "33.333.333/0001-33"},
        {"data_cadastro": "2025-10-01 14:00:00", "valor_total": 900.00, "consumo_medio": 7.3, "status": "confirmada", "cnpj_posto": "44.444.444/0001-44"},
    ]
    df = pd.DataFrame(data)
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
    CLIENTE_COL = 'cnpj' # Assumindo que Bionio tem uma coluna 'cnpj' ou 'cliente'
    
    REQUIRED_COLS = [VALOR_COL, DATA_COL] # CLIENTE_COL é opcional mas necessário para gestão
    
    if not all(col in df.columns for col in REQUIRED_COLS):
        missing = [col for col in REQUIRED_COLS if col not in df.columns]
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}. Colunas disponíveis: {df.columns.tolist()}")

    # Verifica se a coluna de cliente existe, se não, não pode ser usada na gestão
    if CLIENTE_COL not in df.columns and 'cliente' not in df.columns:
        raise ValueError(f"Coluna de cliente ('{CLIENTE_COL}' ou 'cliente') não encontrada. Não é possível processar para gestão de carteira.")

    df[VALOR_COL] = df[VALOR_COL].astype(str).str.replace(r'[\sR\$]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    df[VALOR_COL] = pd.to_numeric(df[VALOR_COL], errors='coerce')
    df[DATA_COL] = pd.to_datetime(df[DATA_COL], format='%d/%m/%Y', errors='coerce').dt.normalize()
    
    df = df.dropna(subset=[VALOR_COL, DATA_COL])
    return df

def process_rovemapay_data(df):
    """Limpa e formata os dados do Rovema Pay. Sem agregação."""
    LIQUIDO_COL = 'liquido'
    BRUTO_COL = 'bruto'
    VENDA_COL = 'venda'
    STATUS_COL = 'status'
    CLIENTE_COL = 'cnpj' # Assumindo que RovemaPay tem 'cnpj' ou 'cliente'
    
    REQUIRED_COLS = [LIQUIDO_COL, BRUTO_COL, VENDA_COL, STATUS_COL] # CLIENTE_COL é opcional mas necessário

    if not all(col in df.columns for col in REQUIRED_COLS):
        missing = [col for col in REQUIRED_COLS if col not in df.columns]
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}. Colunas disponíveis: {df.columns.tolist()}")

    # Verifica se a coluna de cliente existe
    if CLIENTE_COL not in df.columns and 'cliente' not in df.columns:
        raise ValueError(f"Coluna de cliente ('{CLIENTE_COL}' ou 'cliente') não encontrada. Não é possível processar para gestão de carteira.")

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
    df[VENDA_COL] = pd.to_datetime(df.get(VENDA_COL), errors='coerce').dt.normalize()
    
    df = df.dropna(subset=[BRUTO_COL, LIQUIDO_COL, VENDA_COL, STATUS_COL])
    
    df['receita'] = df[BRUTO_COL] - df[LIQUIDO_COL]
    df['custo_total_perc'] = np.where(df[BRUTO_COL] != 0, (df[LIQUIDO_COL] / df[BRUTO_COL]) * 100, 0)
    
    return df


# --- Funções de Busca e Agregação de Dados ---

@st.cache_data(ttl=600, show_spinner=False)
def get_raw_data_from_firestore(product_name: str, start_date: date, end_date: date):
    """
    Busca dados RAW do Firestore, FILTRANDO por data no lado do servidor.
    """
    if st.session_state.get('db') is None: 
        return pd.DataFrame()

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

    start_datetime = datetime.combine(start_date, time.min)
    end_datetime = datetime.combine(end_date, time.max)

    try:
        query_ref = st.session_state['db'].collection(collection_name)
        query_ref = query_ref.where(DATA_COL, '>=', start_datetime)
        query_ref = query_ref.where(DATA_COL, '<=', end_datetime)
        docs = query_ref.limit(20000).stream() 
        data_list = [doc.to_dict() for doc in docs]

        if not data_list: 
            return pd.DataFrame()

        df = pd.DataFrame(data_list)
        
        if DATA_COL in df.columns:
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce').dt.normalize()
            df = df.dropna(subset=[DATA_COL])
        else:
            return pd.DataFrame()

        return df

    except Exception as e:
        log_event("FIRESTORE_FETCH_FAIL", f"Falha ao buscar dados de {product_name} no Firestore: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def get_latest_aggregated_data(product_name: str, start_date: date, end_date: date):
    """
    Busca os dados RAW (já filtrados) e os AGREGA para os gráficos.
    """
    
    if product_name in ['Asto', 'Eliq']:
        # Dispara o cache da API
        fetch_api_and_save(product_name, start_date, end_date)
    
    df = get_raw_data_from_firestore(product_name, start_date, end_date)
    
    if df.empty:
        return pd.DataFrame()

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

# --- [FUNÇÃO CORRIGIDA] ---

@st.cache_data(ttl=600, show_spinner="Buscando lista de clientes...")
def get_all_clients_with_products():
    """
    [CORRIGIDO] Busca todos os clientes (empresas) únicos nos dados brutos
    e retorna um DataFrame com o cliente e o produto de origem.
    Garante que os dados de API (Asto, Eliq) sejam carregados.
    """
    start_date = date(2020, 1, 1)
    end_date = date(2099, 12, 31)
    
    all_clients_dfs = []

    product_client_column_map = {
        'Rovema Pay': 'cnpj', 
        'Bionio': 'cnpj',     
        'Asto': 'cnpj',       
        'Eliq': 'cnpj_posto'
    }

    for product, client_col in product_client_column_map.items():
        
        # --- INÍCIO DA CORREÇÃO ---
        # Garante que os dados da API (Asto, Eliq) sejam carregados
        # no cache do Firestore antes de tentar lê-los.
        if product in ['Asto', 'Eliq']:
            try:
                # Dispara a função que busca da API e salva no Firestore
                fetch_api_and_save(product, start_date, end_date)
            except Exception as e:
                log_event("CLIENT_FETCH_API_FAIL", f"Falha ao salvar API {product} para Gestão: {e}")
                continue # Pula para o próximo produto se a API falhar
        # --- FIM DA CORREÇÃO ---

        # Agora, lê os dados (sejam de upload ou da API recém-salva)
        df_raw = get_raw_data_from_firestore(product, start_date, end_date)
        
        if not df_raw.empty:
            
            df_product_clients = pd.DataFrame()
            
            # Tenta a coluna principal
            if client_col in df_raw.columns:
                df_product_clients = df_raw[[client_col]].copy()
                df_product_clients = df_product_clients.rename(columns={client_col: 'client_id'})
            
            # Fallback para 'cnpj' ou 'cliente' genérico
            elif 'cnpj' in df_raw.columns:
                df_product_clients = df_raw[['cnpj']].copy()
                df_product_clients = df_product_clients.rename(columns={'cnpj': 'client_id'})
            elif 'cliente' in df_raw.columns:
                df_product_clients = df_raw[['cliente']].copy()
                df_product_clients = df_product_clients.rename(columns={'cliente': 'client_id'})
            else:
                log_event("CLIENT_FETCH_WARN", f"Produto {product} não possui coluna de cliente identificável.")
                continue

            df_product_clients['product'] = product
            df_product_clients = df_product_clients.dropna(subset=['client_id'])
            all_clients_dfs.append(df_product_clients)

    if not all_clients_dfs:
        return pd.DataFrame(columns=['client_id', 'product'])

    final_df = pd.concat(all_clients_dfs).drop_duplicates()
    return final_df
