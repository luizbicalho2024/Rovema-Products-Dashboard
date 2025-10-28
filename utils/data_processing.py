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
        print(f"ERRO Processamento Arquivo ({product_name}): {e}") # Adicionado
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
        # Garante que sejam numéricos antes de subtrair
        df[BRUTO_COL] = pd.to_numeric(df[BRUTO_COL], errors='coerce')
        df[LIQUIDO_COL] = pd.to_numeric(df[LIQUIDO_COL], errors='coerce')
        df['receita'] = df[BRUTO_COL] - df[LIQUIDO_COL]

        # Garante que não haja divisão por zero ou NaN
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
        print(f"ERRO Conexão DB nula para {product_name}") # Log adicionado
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
        print(f"ERRO Produto desconhecido para query: {product_name}") # Log adicionado
        return pd.DataFrame()

    start_datetime = datetime.combine(start_date, time.min)
    end_datetime = datetime.combine(end_date, time.max)

    print(f"INFO: Consultando {collection_name} por {DATA_COL} entre {start_datetime} e {end_datetime}") # Log de início de consulta

    try:
        query_ref = db_conn.collection(collection_name)
        # Aplica o filtro de data - **É AQUI QUE O ÍNDICE É NECESSÁRIO**
        query_ref = query_ref.where(DATA_COL, '>=', start_datetime)
        query_ref = query_ref.where(DATA_COL, '<=', end_datetime)

        docs = query_ref.limit(50000).stream() # Limite alto, mas a performance depende do índice
        print(f"INFO: Iniciando stream para {collection_name}...") # Log antes do stream
        # Tenta ler o stream de forma segura
        data_list = []
        for doc in docs:
            data_list.append(doc.to_dict())
        # data_list = list(docs) # Força a leitura do stream (pode travar se houver erro no stream)
        print(f"INFO: Stream concluído para {collection_name}. Documentos lidos: {len(data_list)}") # Log após o stream

        if not data_list:
            print(f"INFO: Nenhum dado encontrado para {product_name} no período.")
            return pd.DataFrame()

        df = pd.DataFrame(data_list)

        # Converte a coluna de data para datetime APÓS carregar os dados
        if DATA_COL in df.columns:
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce').dt.normalize()
            # print(f"INFO: Coluna {DATA_COL} convertida para datetime.")
        else:
             log_event("FIRESTORE_FETCH_WARN", f"Coluna de data '{DATA_COL}' não encontrada nos dados retornados de {product_name}.")
             print(f"WARN: Coluna de data '{DATA_COL}' não encontrada em {product_name}.")
             # Retorna o DF mesmo sem a coluna de data

        return df

    except Exception as e:
        # Este erro provavelmente indicará a falta de índice, se ainda ocorrer
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
        # Se get_raw_data falhou (ex: por falta de índice ou dados), df será vazio
        return pd.DataFrame()

    # --- AGREGAÇÃO ---
    try:
        if product_name == 'Bionio':
            DATA_COL = 'data_da_criação_do_pedido'
            VALUE_COL = 'valor_total_do_pedido'
            if DATA_COL not in df.columns or VALUE_COL not in df.columns: return pd.DataFrame()
            df = df.dropna(subset=[DATA_COL, VALUE_COL])
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce')
            df[VALUE_COL] = pd.to_numeric(df[VALUE_COL], errors='coerce') # Garante que é numérico
            df = df.dropna(subset=[DATA_COL, VALUE_COL])
            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))[VALUE_COL].sum().reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg.rename(columns={VALUE_COL: 'Valor Total Pedidos'}).drop(columns=[DATA_COL])


        elif product_name == 'Rovema Pay':
            DATA_COL = 'venda'
            # Verifica se as colunas necessárias para agregação existem
            REQUIRED_AGG = [DATA_COL, 'receita', 'liquido', 'custo_total_perc', 'status']
            if not all(col in df.columns for col in REQUIRED_AGG):
                 print(f"WARN: Colunas faltando para agregação RovemaPay: {[c for c in REQUIRED_AGG if c not in df.columns]}")
                 return pd.DataFrame()
            df = df.dropna(subset=[DATA_COL])
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce')
            # Garante que colunas numéricas sejam numéricas
            df['liquido'] = pd.to_numeric(df['liquido'], errors='coerce')
            df['receita'] = pd.to_numeric(df['receita'], errors='coerce')
            df['custo_total_perc'] = pd.to_numeric(df['custo_total_perc'], errors='coerce')
            df = df.dropna(subset=[DATA_COL, 'liquido', 'receita', 'custo_total_perc', 'status'])
            df['status'] = df['status'].astype(str)
            df_agg = df.groupby([df[DATA_COL].dt.to_period('M'), 'status']).agg(
                Liquido=('liquido', 'sum'),
                Receita=('receita', 'sum'),
                Taxa_Media=('custo_total_perc', 'mean')
            ).reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg.drop(columns=[DATA_COL]) # Remove dropna() daqui


        elif product_name == 'Asto':
            DATA_COL = 'dataFimApuracao'
            if DATA_COL not in df.columns or 'valorBruto' not in df.columns or 'Receita' not in df.columns: return pd.DataFrame()
            df = df.dropna(subset=[DATA_COL])
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce')
            # Garante numéricos
            df['valorBruto'] = pd.to_numeric(df['valorBruto'], errors='coerce')
            df['Receita'] = pd.to_numeric(df['Receita'], errors='coerce')
            df = df.dropna(subset=[DATA_COL, 'valorBruto', 'Receita'])
            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))[['valorBruto', 'Receita']].sum().reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg

        elif product_name == 'Eliq':
            DATA_COL = 'data_cadastro'
            if DATA_COL not in df.columns or 'valor_total' not in df.columns or 'consumo_medio' not in df.columns: return pd.DataFrame()
            df = df.dropna(subset=[DATA_COL])
            df[DATA_COL] = pd.to_datetime(df[DATA_COL], errors='coerce')
             # Garante numéricos
            df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce')
            df['consumo_medio'] = pd.to_numeric(df['consumo_medio'], errors='coerce')
            df = df.dropna(subset=[DATA_COL, 'valor_total', 'consumo_medio'])
            df_agg = df.groupby(df[DATA_COL].dt.to_period('M'))[['valor_total', 'consumo_medio']].agg({'valor_total': 'sum', 'consumo_medio': 'mean'}).reset_index()
            df_agg['Mês'] = df_agg[DATA_COL].astype(str)
            return df_agg

        return pd.DataFrame() # Caso o produto não seja nenhum dos acima

    except Exception as e:
        log_event("DATA_AGGREGATION_FAIL", f"Falha ao agregar dados de {product_name}: {e}")
        print(f"ERRO Agregação ({product_name}): {e}")
        return pd.DataFrame()


# --- [FUNÇÃO RESTAURADA - USA get_raw_data_from_firestore SEM CACHE] ---
def get_all_clients_with_products():
    """
    [RESTAURADO] Busca todos os clientes únicos usando get_raw_data_from_firestore.
    Depende dos índices do Firestore para funcionar corretamente.
    """
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

    print("INFO: Iniciando busca de clientes para Gestão de Consultores...") # Log geral
    for product, client_columns in product_client_column_map.items():

        # Garante que dados da API existam no Firestore (se aplicável)
        if product in ['Asto', 'Eliq']:
            try:
                print(f"INFO: Disparando fetch_api_and_save para {product}...")
                fetch_api_and_save(product, start_date, end_date)
                print(f"INFO: fetch_api_and_save para {product} concluído.")
            except Exception as e:
                log_event("CLIENT_FETCH_API_FAIL", f"Falha ao salvar API {product} para Gestão: {e}")
                print(f"ERRO API Save ({product}): {e}")
                continue # Pula este produto se a API falhar

        # Tenta buscar os dados filtrados por data (REQUER ÍNDICE DE DATA)
        print(f"INFO: Buscando dados brutos para {product}...")
        df_raw = get_raw_data_from_firestore(product, start_date, end_date)
        print(f"INFO: Busca de dados brutos para {product} concluída. Linhas: {len(df_raw)}")


        if not df_raw.empty:
            df_product_clients = pd.DataFrame()
            found_col = None
            for col in client_columns:
                if col in df_raw.columns:
                    found_col = col
                    break

            if found_col:
                print(f"INFO: Coluna de cliente encontrada para {product}: {found_col}")
                # Extrai apenas a coluna de cliente e adiciona a coluna de produto
                df_product_clients = df_raw[[found_col]].copy()
                df_product_clients = df_product_clients.rename(columns={found_col: 'client_id'})
                df_product_clients['product'] = product
                df_product_clients = df_product_clients.dropna(subset=['client_id'])
                # Garante que o ID seja string
                df_product_clients['client_id'] = df_product_clients['client_id'].astype(str)
                all_clients_dfs.append(df_product_clients)
                print(f"INFO: Clientes adicionados de {product}. Total parcial DFs: {len(all_clients_dfs)}")
            else:
                log_event("CLIENT_FETCH_WARN", f"Produto {product} não possui nenhuma coluna de cliente identificável nos dados retornados (ex: {', '.join(client_columns)}).")
                print(f"WARN: Nenhuma coluna de cliente encontrada para {product} nas colunas: {df_raw.columns.tolist()}")
        else:
             print(f"INFO: Nenhum dado bruto retornado por get_raw_data_from_firestore para {product}.")
        # Se df_raw for vazio (devido a erro de índice ou falta de dados), simplesmente continua para o próximo produto

    if not all_clients_dfs:
        log_event("CLIENT_FETCH_ERROR", "Nenhum cliente encontrado em nenhum produto. Verifique os logs do Firestore para erros de índice.")
        print("ERROR: Nenhum DataFrame de cliente foi gerado.")
        # Retorna DF vazio para a página de Gestão mostrar a mensagem correta
        return pd.DataFrame(columns=['client_id', 'product'])

    # Concatena todos os dataframes encontrados e remove duplicatas
    try:
        final_df = pd.concat(all_clients_dfs).drop_duplicates(subset=['client_id', 'product']).reset_index(drop=True)
        print(f"INFO: Concatenação final concluída. Total de clientes únicos por produto: {len(final_df)}")
        return final_df
    except Exception as e:
         log_event("CLIENT_FETCH_CONCAT_ERROR", f"Erro ao concatenar DFs de clientes: {e}")
         print(f"ERRO ao concatenar DFs de clientes: {e}")
         return pd.DataFrame(columns=['client_id', 'product'])
