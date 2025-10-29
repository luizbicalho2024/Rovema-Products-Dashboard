# app/api_processor.py

import streamlit as st
import pandas as pd
import requests
import base64
from datetime import datetime, timedelta
import random # Para MOCK

# --- Funções de Chamada de API ---

def get_api_data(api_name, url, username=None, password=None, token=None, start_date=None, end_date=None):
    """
    Simula a chamada de API. Implementa a Basic Auth para ASTO.
    Retorna um DataFrame simulado.
    """
    
    st.info(f"Chamando API {api_name} de {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}...")
    
    headers = {}
    
    # Lógica de Basic Authorization para ASTO
    if api_name == 'ASTO' and username and password:
        credentials = f"{username}:{password}"
        encoded_creds = base64.b64encode(credentials.encode()).decode()
        headers = {"Authorization": f"Basic {encoded_creds}"}
        st.caption(f"ASTO Headers: Authorization: Basic {encoded_creds[:10]}...") 
    elif token:
         headers = {"Authorization": f"Bearer {token}"}
         st.caption(f"{api_name} Headers: Authorization: Bearer {token[:10]}...")

    # --- MOCK: Retorna um DataFrame simulado ---
    try:
        # Aqui, você faria a chamada real: 
        # response = requests.get(url, headers=headers, params={...})
        
        num_records = random.randint(150, 300) 
        data = {
            'data_transacao': [start_date + timedelta(days=random.randint(0, (end_date - start_date).days)) for _ in range(num_records)],
            'valor_bruto': [round(random.uniform(50, 500), 2) for _ in range(num_records)],
            'produto_api': [f'Produto_{api_name}_{i % 5}' for i in range(num_records)],
            'cnpj_cliente': [f'222352020001{i % 100}' for i in range(num_records)],
            'vendedor_mock_id': [f'mock_v{i % 3}' for i in range(num_records)],
            'origem': [api_name] * num_records
        }
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Falha na consulta API {api_name}: {e}")
        return pd.DataFrame()


def process_api_data(db_client, data_inicial, data_final):
    """Busca dados das APIs ELIQ e ASTO, unifica e salva em api_cache."""
    
    api_secrets = st.secrets["api_credentials"]
    
    # 1. Busca ELIQ
    df_eliq = get_api_data(
        api_name='ELIQ',
        url=api_secrets.get("eliq_url"), 
        token=api_secrets.get("eliq_token"), 
        start_date=data_inicial, 
        end_date=data_final
    )
    
    # 2. Busca ASTO (Basic Auth)
    df_asto = get_api_data(
        api_name='ASTO',
        url=api_secrets.get("asto_url"),
        username=api_secrets.get("asto_username"),
        password=api_secrets.get("asto_password"),
        start_date=data_inicial, 
        end_date=data_final
    )

    # 3. Combinação
    df_combined = pd.concat([df_eliq, df_asto], ignore_index=True)
    
    if not df_combined.empty:
        # 4. Salvar no Firestore (Cache)
        data_list = df_combined.to_dict('records')
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
