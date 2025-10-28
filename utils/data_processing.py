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

    # Retorna o dataframe mesmo se o save falhar, para que os dados sejam usados na sessão atual
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
        
        # Padroniza nomes de colunas
        df.columns = [col.strip().lower().replace(' ', '_').replace('.', '').replace('%', '') if isinstance(col, str) else str(col).lower() for col in df.columns]
        
        df = df.dropna(how='all', axis=0)
        df = df.dropna(how='all', axis=1)

        if product_name == 'Bionio':
            df_processed = process_bionio_data(df.copy())
        elif product_name == 'RovemaPay':
            df_processed = process_rovemapay_data(df.copy())
        else:
            df_processed = df # Retorna o DF original se não for Bionio ou RovemaPay
            
        return True, "Processamento bem-sucedido.", df_processed
        
    except Exception as e:
        log_event("FILE_PROCESSING_FAIL", f"Erro no processamento de {product_name}: {e}")
        print(f"ERRO Processamento Arquivo ({product_name}): {e}") 
        return False, f"Erro no processamento do arquivo: {e}", None

def process_bionio_data(df):
    """Limpa e formata os dados do Bionio, mantendo outras colunas."""
    VALOR_COL = 'valor_total_do_pedido'
    DATA_COL = 'data_da_criação_do_pedido'
    REQUIRED_COLS = [VALOR_COL, DATA_COL]
    
    if not all(col in df.columns for col in REQUIRED_COLS):
        missing = [col for col in REQUIRED_COLS if col not in df.columns]
        raise ValueError(f"[BIONIO] Colunas obrigatórias ausentes: {missing}. Colunas disponíveis: {df.columns.tolist()}")

    # Limpa colunas conhecidas
    if VALOR_COL in df.columns:
        df[VALOR_COL] = df[VALOR_COL].astype(str).str.replace(r'[\sR\$]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df[VALOR_COL] = pd.to_numeric(df[VALOR_COL], errors='coerce')
    if DATA_COL in df.columns:
        df[DATA_COL] = pd.to_datetime(df[DATA_COL], format='%d/%m/%Y', errors='coerce').dt.normalize()
    
    # Remove linhas onde as colunas *requeridas* são nulas após a conversão
    df = df.dropna(subset=REQUIRED_COLS)
    
    return df

def process_rovemapay_data(df):
    """Limpa e formata os dados do Rovema Pay, mantendo outras colunas."""
    LIQUIDO_COL = 'liquido'
    BRUTO_COL = 'bruto'
    VENDA_COL = 'venda'
    STATUS_COL = 'status'
    REQUIRED_COLS = [LIQUIDO_COL, BRUTO_COL, VENDA_COL, STATUS_COL]

    if not all(col in df.columns for col in REQUIRED_COLS):
        missing = [col for col in REQUIRED_COLS if col not in df.columns]
        raise ValueError(f"[ROVEMAPAY] Colunas obrigatórias ausentes: {missing}. Colunas disponíveis: {df.columns.tolist()}")

    def clean_value(series_key):
        # Função interna para limpar valores monetários/numéricos
        if series_key not in df.columns:
             return pd.Series([np.nan] * len(df)) # Retorna NaN se a coluna não existir
        series = df[series_key]
        if series.dtype == 'object':
            cleaned = series.fillna('').astype(str).str.replace(r'[\sR\$\%]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            return cleaned
        return series
    
    # Limpa colunas conhecidas
    df[LIQUIDO_COL] = pd.to_numeric(clean_value(LIQUIDO_COL), errors='coerce')
    df[BRUTO_COL] = pd.to_numeric(clean_value(BRUTO_COL), errors='coerce')
    df[VENDA_COL] = pd.to_datetime(df.get(VENDA_COL), errors='coerce').dt.normalize()
    
    # Limpa colunas opcionais se existirem
    if 'taxa_cliente' in df.columns:
        df['taxa_cliente'] = pd.to_numeric(clean_value('taxa_cliente'), errors='coerce')
    if 'taxa_adquirente' in df.columns:
        df['taxa_adquirente'] = pd.to_numeric(clean_value('taxa_adquirente'), errors='coerce')
    
    # Remove linhas onde as colunas *requeridas* são nulas após a conversão
    df = df.dropna(subset=REQUIRED_COLS)
    
    # Calcula colunas derivadas (apenas se as colunas base existirem e forem válidas)
    if BRUTO_COL in df.columns and LIQUIDO_COL in df.columns:
        df['receita'] = df[BRUTO_COL] - df[LIQUIDO_COL]
        df['custo_total_perc'] = np.where(df[BRUTO_COL].notna() & (df[BRUTO_COL] != 0) & df[LIQUIDO_COL].notna(), 
                                         (df[LIQUIDO_COL] / df[BRUTO_COL]) * 100, 
                                         0)
    
    return df


# --- Funções de Busca e Agregação de Dados ---

@st.cache_data(ttl=600, show_spinner=False)
def get_raw_data_from_firestore(product_name: str, start_date: date, end_date: date):
    """
    Busca dados RAW do Firestore, FILTRANDO por data no lado do servidor.
    """
    db_conn = st.session_state.get('db')
    if db_conn is None: 
        log_event("FIRESTORE_FETCH_FAIL", f"Conexão DB nula ao buscar dados para {product_name}.")
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
        query_ref = db_conn.collection(collection_name)
        # Aplica o filtro de data - **É AQUI QUE O ÍNDICE É NECESSÁRIO**
        query_ref = query_ref.where(DATA_COL, '>=', start_datetime)
        query_ref = query_ref.where(DATA_COL, '<=', end_datetime)
        
        docs = query_ref.limit(50000).stream() # Limite alto, mas a performance depende do índice
        data_list = [doc.to_dict() for doc in docs]

        if not data_list: 
            # Isso é normal se não houver dados no período
            # log_event("FIRESTORE_FETCH_INFO", f"Nenhum dado encontrado para {product_name} no período {start_date} a {end_date}.")
            return pd.DataFrame()

        df = pd.DataFrame(data_list)
        
        # Converte a coluna de data para datetime APÓS carregar os dados
        if DATA_COL in df.columns:
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce').dt.normalize()
            # Mantém linhas mesmo que a data seja inválida por enquanto, filtra depois se necessário
            # df = df.dropna(subset=[DATA_COL]) 
        else:
             log_event("FIRESTORE_FETCH_WARN", f"Coluna de data '{DATA_COL}' não encontrada nos dados retornados de {product_name}.")
             # Retorna o DF mesmo sem a coluna de data, outras colunas podem ser úteis
             # return pd.DataFrame() 

        return df

    except Exception as e:
        # Este erro provavelmente indicará a falta de índice
        log_event("FIRESTORE_FETCH_FAIL", f"Falha ao buscar dados de {product_name} no Firestore: {e}")
        print(f"ERRO Firestore ({product_name}): {e}") # <<-- PROCURE O LINK AQUI NO CONSOLE
        st.error(f"Erro ao buscar dados de {product_name}. Verifique os logs e se os índices do Firestore foram criados.")
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def get_latest_aggregated_data(product_name: str, start_date: date, end_date: date):
    """
    Busca os dados RAW (já filtrados) e os AGREGA para os gráficos.
    """
    
    if product_name in ['Asto', 'Eliq']:
        # Dispara o cache da API para garantir que os dados existam no Firestore
        fetch_api_and_save(product_name, start_date, end_date)
    
    # Tenta buscar os dados filtrados (depende de índices)
    df = get_raw_data_from_firestore(product_name, start_date, end_date)
    
    if df.empty:
        # Se get_raw_data falhou (ex: por falta de índice), df será vazio
        return pd.DataFrame()

    # --- AGREGAÇÃO --- 
    # (O código aqui permanece o mesmo, mas só executa se df não for vazio)
    try:
        if product_name == 'Bionio':
            # ... (código de agregação Bionio) ...
            DATA_COL = 'data_da_criação_do_pedido'
            if DATA_COL not in df.columns or 'valor_total_do_pedido' not in df.columns: return pd.DataFrame()
            # Garante que a coluna de data seja datetime antes de agrupar
            df = df.dropna(subset=[DATA_COL])
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce')
            df = df.dropna(subset=[DATA_COL])
            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))['valor_total_do_pedido'].sum().reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg.rename(columns={'valor_total_do_pedido': 'Valor Total Pedidos'}).drop(columns=[DATA_COL])

            
        elif product_name == 'Rovema Pay':
            # ... (código de agregação Rovema Pay) ...
            DATA_COL = 'venda'
            REQUIRED = ['venda', 'receita', 'liquido', 'custo_total_perc', 'status']
            if not all(col in df.columns for col in REQUIRED): return pd.DataFrame()
            # Garante que a coluna de data seja datetime
            df = df.dropna(subset=[DATA_COL])
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce')
            df = df.dropna(subset=[DATA_COL])
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
            # ... (código de agregação Asto) ...
            DATA_COL = 'dataFimApuracao'
            if DATA_COL not in df.columns or 'valorBruto' not in df.columns or 'Receita' not in df.columns: return pd.DataFrame()
            # Garante que a coluna de data seja datetime
            df = df.dropna(subset=[DATA_COL])
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce')
            df = df.dropna(subset=[DATA_COL])
            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))[['valorBruto', 'Receita']].sum().reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg

        elif product_name == 'Eliq':
            # ... (código de agregação Eliq) ...
            DATA_COL = 'data_cadastro'
            if DATA_COL not in df.columns or 'valor_total' not in df.columns or 'consumo_medio' not in df.columns: return pd.DataFrame()
            # Garante que a coluna de data seja datetime
            df = df.dropna(subset=[DATA_COL])
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce')
            df = df.dropna(subset=[DATA_COL])
            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))[['valor_total', 'consumo_medio']].agg({'valor_total': 'sum', 'consumo_medio': 'mean'}).reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg

        return pd.DataFrame() # Caso o produto não seja nenhum dos acima

    except Exception as e:
        log_event("DATA_AGGREGATION_FAIL", f"Falha ao agregar dados de {product_name}: {e}")
        print(f"ERRO Agregação ({product_name}): {e}") 
        return pd.DataFrame()


# --- [FUNÇÃO RESTAURADA - USA get_raw_data_from_firestore] ---
# REMOVIDA A LINHA DE CACHE
def get_all_clients_with_products():
    """
    [RESTAURADO] Busca todos os clientes únicos usando get_raw_data_from_firestore.
    Depende dos índices do Firestore para funcionar corretamente.
    """
    # Usa um período longo para tentar pegar todos os clientes via filtro
    # A performance aqui dependerá CRITICAMENTE dos índices de data no Firestore
    start_date = date(2020, 1, 1) 
    end_date = date(2099, 12, 31) 
    
    all_clients_dfs = [] # Lista para DataFrames parciais

    db_conn = st.session_state.get('db')
    if db_conn is None:
        log_event("CLIENT_FETCH_ERROR", "Conexão DB nula ao buscar lista de clientes.")
        return pd.DataFrame(columns=['client_id', 'product'])

    product_client_column_map = {
        'Rovema Pay': ['cnpj', 'ec'], 
        'Bionio': ['cnpj_da_organização', 'razão_social'],
        'Asto': ['cnpj', 'cliente'],
        'Eliq': ['cnpj_posto']
    }

    for product, client_columns in product_client_column_map.items():
        
        # Garante que dados da API existam no Firestore (se aplicável)
        if product in ['Asto', 'Eliq']:
            try:
                fetch_api_and_save(product, start_date, end_date)
            except Exception as e:
                log_event("CLIENT_FETCH_API_FAIL", f"Falha ao salvar API {product} para Gestão: {e}")
                print(f"ERRO API Save ({product}): {e}") 
                continue # Pula este produto se a API falhar
        
        # Tenta buscar os dados filtrados por data (REQUER ÍNDICE DE DATA)
        df_raw = get_raw_data_from_firestore(product, start_date, end_date)
        
        if not df_raw.empty:
            df_product_clients = pd.DataFrame()
            found_col = None
            for col in client_columns:
                if col in df_raw.columns:
                    found_col = col
                    break 
            
            if found_col:
                # Extrai apenas a coluna de cliente e adiciona a coluna de produto
                df_product_clients = df_raw[[found_col]].copy()
                df_product_clients = df_product_clients.rename(columns={found_col: 'client_id'})
                df_product_clients['product'] = product
                df_product_clients = df_product_clients.dropna(subset=['client_id'])
                # Garante que o ID seja string
                df_product_clients['client_id'] = df_product_clients['client_id'].astype(str) 
                all_clients_dfs.append(df_product_clients)
            else:
                log_event("CLIENT_FETCH_WARN", f"Produto {product} não possui nenhuma coluna de cliente identificável nos dados retornados (ex: {', '.join(client_columns)}).")
                # Continua mesmo assim, pode haver clientes de outros produtos
        # Se df_raw for vazio (devido a erro de índice ou falta de dados), simplesmente continua para o próximo produto

    if not all_clients_dfs:
        log_event("CLIENT_FETCH_ERROR", "Nenhum cliente encontrado em nenhum produto. Verifique os logs do Firestore para erros de índice.")
        # Retorna DF vazio para a página de Gestão mostrar a mensagem correta
        return pd.DataFrame(columns=['client_id', 'product'])

    # Concatena todos os dataframes encontrados e remove duplicatas
    final_df = pd.concat(all_clients_dfs).drop_duplicates(subset=['client_id', 'product']).reset_index(drop=True)
    return final_df
