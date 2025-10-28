import streamlit as st
import pandas as pd
from fire_admin import db, log_event, server_timestamp # Importa server_timestamp

def logs_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    # Verificação de Autorização (Admin)
    if st.session_state.get('user_role') != 'Admin':
        st.error("Permissão negada. Apenas usuários 'Admin' podem acessar esta página.")
        return

    st.title("📋 Logs de Auditoria e Sistema")
    log_event("VIEW_LOGS", "Acessando a página de logs.")

    @st.cache_data(ttl=60) # Cache por 60 segundos
    def fetch_logs():
        """Busca os 500 logs mais recentes da coleção 'logs' no Firestore."""
        if 'db' not in st.session_state:
            return pd.DataFrame()
        
        try:
            # Usando a referência de timestamp correta para ordenação
            logs_ref = st.session_state['db'].collection('logs').order_by('timestamp', direction='DESCENDING').limit(500)
            docs = logs_ref.stream()
            log_list = []
            for doc in docs:
                data = doc.to_dict()
                # Converte o timestamp do servidor para string legível
                if 'timestamp' in data and hasattr(data['timestamp'], 'isoformat'):
                    data['timestamp'] = data['timestamp'].isoformat()
                elif 'timestamp' in data: # Trata casos onde pode ser um objeto diferente (ex: server_timestamp ainda não resolvido)
                    data['timestamp'] = str(data['timestamp'])
                    
                log_list.append(data)
            return pd.DataFrame(log_list)
        except Exception as e:
            st.error(f"Erro ao buscar logs: {e}. Verifique as permissões de leitura no Firestore Rules.")
            return pd.DataFrame()

    df_logs = fetch_logs()
    
    if not df_logs.empty:
        st.subheader(f"Últimos {len(df_logs)} Registros")
        
        # Reordena e renomeia colunas para visualização
        df_logs = df_logs.rename(columns={'timestamp': 'Data/Hora', 'user_email': 'Usuário', 'action': 'Ação', 'details': 'Detalhes'})
        df_logs = df_logs[['Data/Hora', 'Usuário', 'Ação', 'Detalhes']]
        
        st.dataframe(df_logs, use_container_width=True)
        
        # Opção de download
        csv = df_logs.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Logs (CSV)",
            data=csv,
            file_name='logs_auditoria.csv',
            mime='text/csv',
        )
    else:
        st.info("Nenhum log encontrado. O sistema está iniciando a auditoria.")

# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    logs_page()
else:
    pass
