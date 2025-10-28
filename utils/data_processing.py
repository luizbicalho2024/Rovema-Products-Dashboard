import pandas as pd
import numpy as np
import requests
import streamlit as st
from datetime import datetime
import altair as alt

# Importa o db e server_timestamp do fire_admin (necessário para logs e referências)
try:
    from fire_admin import log_event, db, server_timestamp
except ImportError:
    st.warning("fire_admin não pode ser importado, logs/DB podem estar indisponíveis.")
    class MockDB:
        def collection(self, name): return self
        def add(self, data): pass
        def limit(self, num): return self
        def stream(self): return []
    db = MockDB()
    server_timestamp = datetime.now()
    def log_event(action: str, details: str = ""): pass


# --- MOCKS de APIs (Asto e Eliq) ---

def fetch_asto_data(start_date: str, end_date: str):
    """
    Simula a coleta de dados de Fatura Pagamento Fechada do Asto (API).
    Em um projeto real, 'requests.get' seria usado aqui, com autenticação.
    """
    log_event("API_CALL_ASTO", f"Simulação de chamada Asto para {start_date} a {end_date}")
    
    # Mock de dados com base no JSON que você forneceu
    data = [
        # Dados de exemplo do JSON do Asto
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
    """
    Simula a coleta de dados de Transações do Eliq (API).
    Em um projeto real, 'requests.get' seria usado aqui, com autenticação.
    """
    log_event("API_CALL_ELIQ", f"Simulação de chamada Eliq para {start_date} a {end_date}")
    
    # Mock de dados de transação com foco em volume e consumo
    data = [
        # Dados de exemplo do JSON do Eliq
        {"data_cadastro": "2025-09-01 10:00:00", "valor_total": 500.00, "consumo_medio": 7.5, "status": "confirmada"},
        {"data_cadastro": "2025-09-08 15:30:00", "valor_total": 1200.50, "consumo_medio": 6.8, "status": "confirmada"},
        {"data_cadastro": "2025-09-15 11:20:00", "valor_total": 850.25, "consumo_medio": 7.2, "status": "confirmada"},
        {"data_cadastro": "2025-09-22 09:00:00", "valor_total": 1500.00, "consumo_medio": 7.0, "status": "confirmada"},
        {"data_cadastro": "2025-09-29 14:00:00", "valor_total": 900.00, "consumo_medio": 7.3, "status": "confirmada"},
    ]
    df = pd.DataFrame(data)
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro']).dt.normalize()
    return df.groupby('data_cadastro')[['valor_total', 'consumo_medio']].agg({'valor_total': 'sum', 'consumo_medio': 'mean'}).reset_index()

# --- Funções de Processamento de Arquivos Locais (Bionio e Rovema Pay) ---

def process_uploaded_file(uploaded_file, product_name):
    """
    Processa o arquivo CSV/Excel e o prepara para visualização de preview.
    """
    try:
        # Tenta ler o arquivo como CSV e depois como Excel
        is_csv = uploaded_file.name.lower().endswith('.csv')
        
        if is_csv:
            # Tenta a leitura com os parâmetros do Bionio/Rovema Pay (separador ',', decimal '.')
            try:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, decimal=',', sep=',', thousands='.')
            except Exception:
                 # Tentativa de fallback para CSV padrão (caso os separadores não estejam conforme o esperado)
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file)
        else: # XLSX
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file)
        
        if product_name == 'Bionio':
            df_processed = process_bionio_data(df.copy())
        elif product_name == 'RovemaPay':
            df_processed = process_rovemapay_data(df.copy())
        else:
            df_processed = df.head(5) # Retorna só o cabeçalho para outros tipos
            
        return True, "Processamento bem-sucedido.", df_processed
        
    except Exception as e:
        log_event("FILE_PROCESSING_FAIL", f"Erro no processamento de {product_name}: {e}")
        return False, f"Erro no processamento do arquivo: {e}", None

def process_bionio_data(df):
    """Processamento específico para dados do Bionio."""
    df['Valor total do pedido'] = df['Valor total do pedido'].astype(str).str.replace(r'[\sR\$]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    
    # Tentativa de conversão para float, forçando erros para NaN
    df['Valor total do pedido'] = pd.to_numeric(df['Valor total do pedido'], errors='coerce')
    
    # Conversão de Data, tratando o formato 'dd/mm/yyyy'
    df['Data da criação do pedido'] = pd.to_datetime(df['Data da criação do pedido'], format='%d/%m/%Y', errors='coerce')
    
    # Limpeza: Remove linhas com valores inválidos
    df = df.dropna(subset=['Valor total do pedido', 'Data da criação do pedido'])
    
    # Agrupamento para o dashboard
    df_agg = df.groupby(df['Data da criação do pedido'].dt.to_period('M'))['Valor total do pedido'].sum().reset_index()
    df_agg['Mês'] = df_agg['Data da criação do pedido'].astype(str)
    return df_agg.rename(columns={'Valor total do pedido': 'Valor Total Pedidos'}).drop(columns=['Data da criação do pedido'])

def process_rovemapay_data(df):
    """Processamento específico para dados do Rovema Pay."""
    
    # Limpeza e conversão de valores (removendo '%' e ',' decimal)
    def clean_value(series):
        if series.dtype == 'object':
            return series.astype(str).str.replace(r'[\sR\$\%]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        return series
        
    df['Liquido'] = pd.to_numeric(clean_value(df['Liquido']), errors='coerce')
    df['Bruto'] = pd.to_numeric(clean_value(df['Bruto']), errors='coerce')
    df['Taxa Cliente'] = pd.to_numeric(clean_value(df['Taxa Cliente']), errors='coerce')
    df['Taxa Adquirente'] = pd.to_numeric(clean_value(df['Taxa Adquirente']), errors='coerce')
    
    # Conversão de Data
    df['Venda'] = pd.to_datetime(df['Venda'], errors='coerce')
    
    # Limpeza: Remove linhas com valores inválidos
    df = df.dropna(subset=['Bruto', 'Liquido', 'Venda'])
    
    # Cálculo da Receita (MDR + Spread)
    df['Receita'] = df['Bruto'] - df['Liquido']
    df['Custo_Total_Perc'] = (df['Receita'] / df['Bruto']) * 100
    
    # Agrupamento para o dashboard
    df_agg = df.groupby([df['Venda'].dt.to_period('M'), 'Status']).agg(
        Liquido=('Liquido', 'sum'),
        Receita=('Receita', 'sum'),
        Taxa_Media=('Custo_Total_Perc', 'mean')
    ).reset_index()
    
    df_agg['Mês'] = df_agg['Venda'].astype(str)
    return df_agg.drop(columns=['Venda'])

def get_processed_data_from_last_upload(product_name):
    """
    Simula a busca dos dados processados do último arquivo enviado para um produto.
    Em um ambiente real, você faria o download do Storage e processaria.
    Aqui, retornamos um mock simples para o Dashboard funcionar.
    """
    if 'db' not in st.session_state:
        return pd.DataFrame()
        
    try:
        # Busca a referência do último upload (na produção, você usaria essa ref para baixar o arquivo)
        last_upload_ref = st.session_state['db'].collection('file_uploads').where('product', '==', product_name).order_by('timestamp', direction='DESCENDING').limit(1).stream()
        
        last_upload = next(last_upload_ref, None)
        
        if last_upload:
            st.info(f"Usando dados simulados com base no último upload de {product_name}: {last_upload.to_dict()['filename']}")
            # Mock dos dados processados
            if product_name == 'Bionio':
                 return pd.DataFrame({
                    'Mês': ['2025-08', '2025-09', '2025-10'],
                    'Valor Total Pedidos': [120000.00, 155000.00, 190000.00]
                })
            elif product_name == 'RovemaPay':
                return pd.DataFrame({
                    'Mês': ['2025-08', '2025-09', '2025-10'],
                    'Status': ['Pago', 'Antecipado', 'Pago'],
                    'Liquido': [85000.00, 32000.00, 95000.00],
                    'Receita': [2500.00, 1500.00, 2700.00],
                    'Taxa_Media': [2.8, 4.5, 2.7]
                })
        else:
            return pd.DataFrame() # Retorna vazio se não houver upload
            
    except Exception as e:
        st.warning(f"Erro ao buscar referência de upload no Firestore: {e}")
        return pd.DataFrame()
