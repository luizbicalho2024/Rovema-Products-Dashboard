import streamlit as st
import pandas as pd
from fire_admin import get_all_users, update_user_details, log_event
try:
    # Importa a nova função
    from utils.data_processing import get_all_clients_with_products
except ImportError:
    st.error("Erro ao importar 'utils/data_processing'.")
    def get_all_clients_with_products():
        return pd.DataFrame(columns=['client_id', 'product'])

# --- Funções de Carregamento de Dados ---

@st.cache_data(ttl=60)
def fetch_consultants_and_map():
    """
    Busca todos os consultores e cria um mapa de quem está vinculado a qual cliente.
    Retorna:
    - Lista de consultores (dicts)
    - Mapa de atribuição (dict): {'cnpj_123': {'uid': 'uid_A', 'nome': 'Nome A'}}
    """
    consultants = get_all_users(role_filter='Usuário')
    assignment_map = {}
    
    if not consultants:
        return [], {}
        
    for c in consultants:
        consultant_name = c.get('nome', c['email'])
        for client_cnpj in c.get('carteira_cnpjs', []):
            assignment_map[client_cnpj] = {
                'uid': c['uid'],
                'nome': consultant_name
            }
            
    return consultants, assignment_map

@st.cache_data(ttl=600)
def load_all_clients_df():
    """Carrega o DataFrame de todos os clientes e seus produtos."""
    return get_all_clients_with_products()

# --- Página Principal ---

def consultant_management_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    if st.session_state.get('user_role') != 'Admin':
        st.error("Permissão negada. Apenas usuários 'Admin' podem acessar esta página.")
        return

    st.title("💼 Gestão de Consultores e Carteiras")
    log_event("VIEW_CONSULTANT_MANAGEMENT", "Acessando a página de gestão de consultores.")

    # --- 1. Carregar Dados ---
    
    consultants, assignment_map = fetch_consultants_and_map()
    all_clients_df = load_all_clients_df()
    
    if not consultants:
        st.warning("Nenhum consultor (usuário com role 'Usuário') encontrado. Crie um na página 'Gerenciamento de Acessos'.")
        return

    if all_clients_df.empty:
        st.warning("Nenhum cliente (empresa/cnpj) encontrado nos dados de produtos. Faça upload de dados primeiro.")
        return

    consultant_names = {c['nome']: c['uid'] for c in consultants if c.get('nome')}
    consultant_details = {c['uid']: c for c in consultants}

    st.subheader("1. Selecione o Consultor")
    
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

        # --- 3. Formulário de Edição (Dados Básicos) ---
        
        with st.form("edit_consultant_form"):
            st.subheader("2. Dados Básicos do Consultor")
            
            new_name = st.text_input("Nome Completo", value=user_data.get('nome', ''))
            
            current_status = user_data.get('status', 'ativo')
            status_options = ['ativo', 'inativo']
            status_index = status_options.index(current_status)
            new_status = st.selectbox(
                "Status da Conta", 
                options=status_options, 
                index=status_index,
                help="Se 'inativo', o usuário não poderá fazer login."
            )
            
            st.subheader("3. Gerenciar Carteira de Clientes")
            
            # --- Filtro de Produtos ---
            product_list = sorted(all_clients_df['product'].unique())
            selected_products = st.multiselect(
                "Filtrar clientes por Produtos",
                options=product_list,
                default=product_list,
                help="Selecione os produtos para ver os clientes disponíveis."
            )
            
            # --- Lógica de Filtro de Clientes (Regra de Negócio) ---
            
            # 1. Filtra clientes pelos produtos selecionados
            if selected_products:
                clients_from_products = all_clients_df[all_clients_df['product'].isin(selected_products)]['client_id'].unique()
            else:
                clients_from_products = all_clients_df['client_id'].unique()
            
            # 2. Gera a lista de opções, aplicando a regra de exclusividade
            available_options = []
            for client_id in clients_from_products:
                assignment = assignment_map.get(client_id)
                
                # Se o cliente não está no mapa (livre) OU está no mapa mas pertence ao consultor selecionado
                if assignment is None or assignment['uid'] == selected_uid:
                    available_options.append(client_id)
                # Se estiver atribuído a outra pessoa, ele não é adicionado à lista.
            
            # 3. Pega a carteira atual do consultor
            current_carteira = user_data.get('carteira_cnpjs', [])
            
            # 4. O multiselect de carteira
            new_carteira = st.multiselect(
                "Vincular Clientes ao Consultor",
                options=sorted(available_options),
                default=current_carteira,
                help="Apenas clientes 'Disponíveis' ou 'Já vinculados' a este consultor são mostrados."
            )
            
            if st.form_submit_button("Salvar Alterações"):
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
                    st.cache_data.clear() # Limpa todo o cache
                    st.rerun()
                else:
                    st.error(message)
                    log_event("CONSULTANT_UPDATE_FAIL", f"Falha ao atualizar {selected_uid}. Erro: {message}")

        # --- 4. Exibição da Carteira Atual e Conflitos ---
        with st.expander("Verificar Clientes Atribuídos a Outros Consultores"):
            st.warning("Os clientes abaixo já estão atribuídos a outros consultores e não podem ser vinculados.")
            
            conflicts = []
            for client_id, assignment in assignment_map.items():
                if assignment['uid'] != selected_uid:
                    conflicts.append({
                        "Cliente (CNPJ/ID)": client_id,
                        "Consultor Atual": assignment['nome']
                    })
            
            if conflicts:
                st.dataframe(pd.DataFrame(conflicts), use_container_width=True)
            else:
                st.info("Nenhum conflito encontrado.")


# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    consultant_management_page()
else:
    pass
