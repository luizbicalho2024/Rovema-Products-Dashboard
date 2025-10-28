import streamlit as st
import pandas as pd
# Removido db e server_timestamp daqui, serão pegos do state
from fire_admin import log_event

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
        # Pega a conexão do DB do session_state
        db_conn = st.session_state.get('db')
        if db_conn is None:
            st.error("Conexão com Firestore não disponível.")
            print("ERRO Logs: Conexão DB nula.") # Adicionado
            return pd.DataFrame()

        try:
            # Importa Query aqui para evitar dependência global
            from google.cloud.firestore import Query

            # Usando a referência de timestamp correta para ordenação
            logs_ref = db_conn.collection('logs').order_by('timestamp', direction=Query.DESCENDING).limit(500)
            docs = logs_ref.stream()
            log_list = []
            for doc in docs:
                data = doc.to_dict()
                # Converte o timestamp do servidor para string legível
                if 'timestamp' in data and data['timestamp'] is not None and hasattr(data['timestamp'], 'strftime'):
                    # Formata para incluir data e hora, tratando fuso se existir
                    try:
                        # Tenta formatar com fuso horário se disponível
                         dt_obj = data['timestamp']
                         # Formato ISO 8601 com fuso horário (se tiver)
                         # data['timestamp'] = dt_obj.isoformat()
                         # Ou formato mais legível:
                         data['timestamp'] = dt_obj.strftime('%Y-%m-%d %H:%M:%S %Z%z')

                    except Exception: # Fallback para string simples se a formatação falhar
                         data['timestamp'] = str(data['timestamp'])

                elif 'timestamp' in data: # Trata casos onde pode ser um objeto diferente ou nulo
                    data['timestamp'] = str(data['timestamp']) if data['timestamp'] is not None else "N/A"

                log_list.append(data)
            return pd.DataFrame(log_list)
        except Exception as e:
            st.error(f"Erro ao buscar logs: {e}. Verifique as permissões de leitura no Firestore Rules.")
            print(f"ERRO Firestore (Logs): {e}") # Adicionado para visibilidade no console
            return pd.DataFrame()

    df_logs = fetch_logs()

    if not df_logs.empty:
        st.subheader(f"Últimos {len(df_logs)} Registros")

        # Reordena e renomeia colunas para visualização
        df_logs = df_logs.rename(columns={'timestamp': 'Data/Hora', 'user_email': 'Usuário', 'action': 'Ação', 'details': 'Detalhes'})
        # Garante que as colunas existam antes de tentar reordenar
        cols_to_show = ['Data/Hora', 'Usuário', 'Ação', 'Detalhes']
        available_cols = [col for col in cols_to_show if col in df_logs.columns]
        df_display = df_logs[available_cols]

        st.dataframe(df_display, use_container_width=True)

        # Opção de download
        @st.cache_data
        def convert_df_to_csv(df):
           # Salva CSV com codificação que o Excel entende bem
           return df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig')

        csv = convert_df_to_csv(df_display)
        st.download_button(
            label="Download Logs (CSV)",
            data=csv,
            file_name='logs_auditoria.csv',
            mime='text/csv',
        )
    else:
        st.info("Nenhum log encontrado.") # Mensagem ajustada

# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    logs_page()
else:
    pass
