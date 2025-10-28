import streamlit as st
import pandas as pd
from fire_admin import get_all_users, update_user_details, log_event
# Precisamos importar a nova função de utils
try:
    from utils.data_processing import get_unique_clients_from_raw_data
except ImportError:
    st.error("Erro ao importar 'utils.data_processing'.")
    # Função mock para evitar quebra total
    def get_unique_clients_from_raw_data():
        return ["Erro: Não foi possível carregar clientes"]

def consultant_management_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    # Verificação de Autorização (Admin)
    if st.session_state.get('user_role') != 'Admin':
        st.error("Permissão negada. Apenas usuários 'Admin' podem acessar esta página.")
        return

    st.title("💼 Gestão de Consultores e Carteiras")
    log_event("VIEW_CONSULTANT_MANAGEMENT", "Acessando a página de gestão de consultores.")

    # --- 1. Carregar Dados ---
    
    @st.cache_data(ttl=60)
    def fetch_consultants():
        # Busca apenas usuários com a role 'Usuário'
        return get_all_users(role_filter='Usuário')

    consultants = fetch_consultants()
    
    # Busca a lista de todas as empresas/clientes dos dados brutos
    all_clients_list = get_unique_clients_from_raw_data()
    
    if not consultants:
        st.warning("Nenhum consultor (usuário com role 'Usuário') encontrado. Crie um na página 'Gerenciamento de Acessos'.")
        return

    consultant_names = {c['nome']: c['uid'] for c in consultants if c.get('nome')}
    consultant_details = {c['uid']: c for c in consultants}

    st.subheader("Editar Consultor")
    
    # --- 2. Seleção do Consultor ---
    
    selected_name = st.selectbox(
        "Selecione o Consultor para Gerenciar",
        options=consultant_names.keys(),
        index=None,
        placeholder="Escolha um consultor..."
    )

    if selected_name:
        selected_uid = consultant_names[selected_name]
        user_data = consultant_details[selected_uid]

        st.info(f"Editando: **{user_data['nome']}** (`{user_data['email']}`)")

        # --- 3. Formulário de Edição ---
        
        with st.form("edit_consultant_form"):
            
            # Campo 1: Nome (Editar)
            new_name = st.text_input("Nome Completo", value=user_data.get('nome', ''))
            
            # Campo 2: Status (Desabilitar/Ativar)
            current_status = user_data.get('status', 'ativo')
            status_options = ['ativo', 'inativo']
            status_index = status_options.index(current_status)
            new_status = st.selectbox(
                "Status da Conta", 
                options=status_options, 
                index=status_index,
                help="Se 'inativo', o usuário não poderá fazer login."
            )
            
            # Campo 3: Carteira (Vincular Empresas)
            current_carteira = user_data.get('carteira_cnpjs', [])
            
            # Filtra a carteira atual para garantir que apenas clientes existentes sejam pré-selecionados
            valid_current_carteira = [client for client in current_carteira if client in all_clients_list]
            
            new_carteira = st.multiselect(
                "Carteira de Clientes (Empresas Vinculadas)",
                options=all_clients_list,
                default=valid_current_carteira,
                help="Selecione os clientes (CNPJs ou Nomes) pelos quais este consultor é responsável."
            )
            
            if st.form_submit_button("Salvar Alterações"):
                # Monta o dicionário de dados para atualização
                data_to_update = {
                    'nome': new_name,
                    'status': new_status,
                    'carteira_cnpjs': new_carteira
                }
                
                with st.spinner("Atualizando dados do consultor..."):
                    success, message = update_user_details(selected_uid, data_to_update)
                
                if success:
                    st.success(message)
                    log_event("CONSULTANT_UPDATE_SUCCESS", f"Consultor {selected_uid} atualizado.")
                    # Limpa o cache para recarregar os dados
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(message)
                    log_event("CONSULTANT_UPDATE_FAIL", f"Falha ao atualizar {selected_uid}. Erro: {message}")

# --- INÍCIO DA CORREÇÃO (CÓDIGO FALTANTE) ---

# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    consultant_management_page()
else:
    pass
# --- FIM DA CORREÇÃO ---
