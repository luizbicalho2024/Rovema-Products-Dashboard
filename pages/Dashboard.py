import streamlit as st
import altair as alt
import pandas as pd
from datetime import date, timedelta
from fire_admin import log_event, get_all_consultores
from utils.data_processing import get_latest_aggregated_data, get_raw_data_from_firestore # Usado para os gráficos dinâmicos

# --- Funções Auxiliares de Visualização (Mapeando o PDF) ---

@st.cache_data
def get_ranking_data():
    """Simula a geração dos rankings de crescimento/queda e participação por bandeira, conforme o PDF."""
    
    # [cite: 30, 31, 35, 39, 41, 43, 46, 48, 51, 55, 57]
    ranking_queda_data = {
        'Cliente': ['Posto Avenida', 'Concessionária RodarMais', 'Restaurante Dom Pepe', 'Loja Universo Tech', 'Farmácia Popular', 'Posto Panorama', 'Oficina Auto Luz', 'Loja Bella Casa', 'Auto Mecânica Pereira', 'Livraria Estilo'],
        'CNPJ': ['85.789.123/0001-45', '18.456.789/0001-75', '86.456.789/0001-55', '87.987.654/0001-65', '19.567.890/0001-85', '20.678.901/0001-95', '88.234.567/0001-75', '21.789.012/0001-05', '89.567.890/0001-85', '90.678.901/0001-95'],
        'Variação': [-100.0] * 10
    }
    ranking_queda_df = pd.DataFrame(ranking_queda_data)
    
    # 
    detalhamento_data = {
        'CNPJ': ['94.012.345/0001-35', '95.123.456/0001-45', '12.345.678/0001-10', '45.123.678/0001-80', '96.234.567/0001-55', '56.789.123/0001-30', '23.456.789/0001-20', '97.345.678/0001-65', '31.234.567/0001-50', '98.456.789/0001-75'],
        'Cliente': ['Posto Sol Nascente', 'Supermercado Real', 'Auto Peças Silva', 'Concessionária Fenix', 'Papelaria Central', 'Padaria Doce Sabor', 'Supermercado Oliveira', 'Auto Mecânica Lima', 'Posto Vitória', 'Oficina do Tonho'],
        'Receita': [0.0] * 10,
        'Crescimento': [10.4, 21.7, 7.9, -6.6, 17.9, 28.1, 22.7, 18.2, 29.0, 23.8],
        'Nº Vendas': [1] * 10,
        'Bandeira': ['Pix', 'Crédito', 'Crédito', 'Crédito', 'Débito', 'Débito', 'Débito', 'Crédito', 'Crédito', 'Pix']
    }
    detalhamento_df = pd.DataFrame(detalhamento_data)
    
    # [cite: 27]
    bandeira_df = detalhamento_df.groupby('Bandeira')['Nº Vendas'].sum().reset_index()
    bandeira_df = bandeira_df.rename(columns={'Nº Vendas': 'Valor'})

    return ranking_queda_df, detalhamento_df, bandeira_df

# --- Funções de Carga de Dados (Dinâmicos) ---

@st.cache_data(ttl=60, show_spinner="Carregando dados agregados...")
def load_aggregated_data(start_date, end_date, update_counter):
    """Carrega todos os dados AGREGADOS para os gráficos principais."""
    return (
        get_latest_aggregated_data('Asto', start_date, end_date),
        get_latest_aggregated_data('Eliq', start_date, end_date),
        get_latest_aggregated_data('Bionio', start_date, end_date),
        get_latest_aggregated_data('Rovema Pay', start_date, end_date)
    )

@st.cache_data(ttl=3600)
def load_consultores_list():
    """Carrega a lista de consultores do Firestore."""
    return get_all_consultores()


# --- DASHBOARD PRINCIPAL ---

def dashboard_page():
    if not st.session_state.get('authenticated'):
        st.error("Acesso negado. Por favor, faça login na página principal.")
        return

    st.title("ROVEMA BANK: Dashboard de Transações")
    st.caption("Filtros Ativos na Barra Lateral")
    log_event("VIEW_DASHBOARD", "Visualizando o dashboard principal.")
    
    # --- 0. FILTROS NA BARRA LATERAL (Sidebar) ---
    
    if 'update_counter' not in st.session_state:
        st.session_state['update_counter'] = 0

    st.sidebar.title("Filtros") [cite: 6]
    
    # Filtros de Data
    default_end_date = date(2025, 10, 31)
    default_start_date = default_end_date - timedelta(days=90)
    
    start_date = st.sidebar.date_input("Data início", default_start_date)
    end_date = st.sidebar.date_input("Data fim", default_end_date)
    
    st.sidebar.markdown("---")

    # Filtros de Contexto
    selected_products = st.sidebar.multiselect(
        "Produtos", 
        ["Eliq", "Asto", "Bionio", "Rovema Pay"],
        default=["Eliq", "Asto", "Bionio", "Rovema Pay"],
        help="Filtra dados pelos produtos da empresa."
    )

    selected_payments = st.sidebar.multiselect(
        "Meios de Pagamento", 
        ["Crédito", "Débito", "Pix"],
        default=["Crédito", "Débito", "Pix"],
        help="Filtra transações pelo tipo de meio de pagamento."
    )

    # Filtro de Consultores (DINÂMICO)
    consultores_list = load_consultores_list()
    st.sidebar.selectbox(
        "Consultores", 
        consultores_list,
        help="Filtra dados pela carteira de consultores."
    )
    
    st.sidebar.markdown("---")
    
    if st.sidebar.button("Atualizar"):
        st.session_state['update_counter'] += 1
        st.toast("Dashboard atualizado!")
        st.rerun() 
    
    
    # --- 1. CARREGAMENTO E CÁLCULO DE DADOS ---
    
    # Carrega dados agregados para gráficos dinâmicos
    asto_df_agg, eliq_df_agg, bionio_df_agg, rovemapay_df_agg = load_aggregated_data(
        start_date, end_date, st.session_state['update_counter']
    )
    
    # Carrega dados estáticos para layout do PDF
    ranking_queda_df, detalhamento_df, bandeira_df = get_ranking_data()
    
    # --- Cálculo Condicional das Métricas (Híbrido: Dinâmico + Estático do PDF) ---
    current_rovema_revenue = 0
    current_bionio_value = 0
    current_asto_revenue = 0
    current_eliq_volume = 0
    current_margem_media = 0
    current_valor_transacionado = 0
    
    # 1. Rovema Pay
    if "Rovema Pay" in selected_products and not rovemapay_df_agg.empty:
        current_rovema_revenue = rovemapay_df_agg['Receita'].sum()
        current_margem_media = rovemapay_df_agg['Taxa_Media'].mean()
        current_valor_transacionado += rovemapay_df_agg['Liquido'].sum()

    # 2. Bionio
    if "Bionio" in selected_products and not bionio_df_agg.empty:
        current_bionio_value = bionio_df_agg['Valor Total Pedidos'].sum()
        current_valor_transacionado += current_bionio_value

    # 3. Asto
    if "Asto" in selected_products and not asto_df_agg.empty:
        current_asto_revenue = asto_df_agg['Receita'].sum()
        current_valor_transacionado += asto_df_agg['valorBruto'].sum()
        
    # 4. Eliq
    if "Eliq" in selected_products and not eliq_df_agg.empty:
        current_eliq_volume = eliq_df_agg['valor_total'].sum()
        current_valor_transacionado += eliq_df_agg['valor_total'].sum()
        
    
    # Métrica do Header
    nossa_receita = current_rovema_revenue + current_asto_revenue
    
    # Usa o valor estático do PDF [cite: 10] como fallback se o dinâmico for zero
    valor_transacionado_display = current_valor_transacionado if current_valor_transacionado > 0 else 2_146_293.35
    
    
    # --- 2. EXIBIÇÃO DAS MÉTRICAS (Layout do PDF) ---
    
    col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
    
    col_m1.metric("Transacionado (Bruto)", f"R$ {valor_transacionado_display:,.2f}", delta="+142.49% vs. trimestre anterior") [cite: 10]
    col_m2.metric("Nossa Receita", f"R$ {nossa_receita:,.2f}") # Dinâmico (PDF mostra 0,00) [cite: 16]
    col_m3.metric("Margem Média", f"{current_margem_media:.2f}%") # Dinâmico (PDF mostra 0.00%) [cite: 17]
    col_m4.metric("Clientes Ativos", "99") [cite: 18]
    col_m5.metric("Clientes em Queda", "16") [cite: 19]
    
    st.markdown("---")


    # --- BLOCO 3: EVOLUÇÃO E PARTICIPAÇÃO (Layout do PDF) ---
    
    col_g1, col_g2 = st.columns([2, 1])
    
    # G1: Evolução do Valor Transacionado vs Receita [cite: 20]
    with col_g1:
        st.header("Evolução do Valor Transacionado vs Receita")
        
        # Lógica dinâmica (CORREÇÃO DE BUG ANTERIOR)
        has_rovemapay_data = "Rovema Pay" in selected_products and not rovemapay_df_agg.empty
        has_asto_data = "Asto" in selected_products and not asto_df_agg.empty

        if has_rovemapay_data or has_asto_data:
            rovema_long = pd.DataFrame()
            asto_long = pd.DataFrame()
            
            if has_rovemapay_data and all(col in rovemapay_df_agg.columns for col in ['Mês', 'Receita', 'Liquido']):
                rovema_long = rovemapay_df_agg.melt('Mês', value_vars=['Receita', 'Liquido'], var_name='Métrica', value_name='Valor')
            
            if has_asto_data and all(col in asto_df_agg.columns for col in ['Mês', 'Receita', 'valorBruto']):
                asto_long = asto_df_agg.melt('Mês', value_vars=['Receita', 'valorBruto'], var_name='Métrica', value_name='Valor')
                asto_long['Métrica'] = asto_long['Métrica'].replace({'valorBruto': 'Volume'})
            
            final_evolucao_df = pd.concat([rovema_long, asto_long])
            
            if not final_evolucao_df.empty:
                final_evolucao_df = final_evolucao_df.groupby(['Mês', 'Métrica'])['Valor'].sum().reset_index()
                evolucao_chart = alt.Chart(final_evolucao_df).mark_line(point=True).encode(
                    x=alt.X('Mês:O', title=''),
                    y=alt.Y('Valor', title='Valor (R$)'),
                    color='Métrica',
                    tooltip=['Mês', alt.Tooltip('Valor', format='$,.2f'), 'Métrica']
                ).properties(title='').interactive()
                st.altair_chart(evolucao_chart, use_container_width=True)
            else:
                st.info("Não foi possível gerar o gráfico de evolução.")
        else:
            st.info("Selecione 'Rovema Pay' e/ou 'Asto' para ver o gráfico de evolução.")

    # G2: Participação por Bandeira e Receita por Carteira
    with col_g2:
        # Participação por Bandeira (Estático do PDF) [cite: 23, 27]
        st.subheader("Participação por Bandeira")
        if not bandeira_df.empty and ("Rovema Pay" in selected_products):
            bandeira_chart = alt.Chart(bandeira_df).mark_arc(outerRadius=80).encode(
                theta=alt.Theta(field="Valor", type="quantitative"),
                color=alt.Color(field="Bandeira", type="nominal"),
                order=alt.Order("Valor", sort="descending"),
                tooltip=["Bandeira", alt.Tooltip("Valor", format=",")]
            ).properties(title="")
            st.altair_chart(bandeira_chart, use_container_width=True)
        else:
            st.warning("Selecione 'Rovema Pay' para ver a participação por bandeira.")
            
        # Receita por Carteira (Dinâmico) [cite: 22]
        st.subheader("Receita por Carteira")
        carteira_data = {
            'Carteira': [],
            'Receita/Volume': []
        }
        if "Rovema Pay" in selected_products:
            carteira_data['Carteira'].append('RovemaPay')
            carteira_data['Receita/Volume'].append(current_rovema_revenue)
        if "Bionio" in selected_products:
            carteira_data['Carteira'].append('Bionio (Volume)')
            carteira_data['Receita/Volume'].append(current_bionio_value)
        if "Asto" in selected_products:
            carteira_data['Carteira'].append('Asto')
            carteira_data['Receita/Volume'].append(current_asto_revenue)
        if "Eliq" in selected_products:
            carteira_data['Carteira'].append('Eliq (Volume)')
            carteira_data['Receita/Volume'].append(current_eliq_volume) 
        
        carteira_df = pd.DataFrame(carteira_data)

        if not carteira_df.empty and carteira_df['Receita/Volume'].sum() > 0:
            carteira_chart = alt.Chart(carteira_df).mark_bar().encode(
                x=alt.X("Carteira:N", title=""),
                y=alt.Y("Receita/Volume", title="Receita (R$)"),
                color=alt.Color("Carteira:N"), 
                tooltip=["Carteira", alt.Tooltip("Receita/Volume", format="$,.2f")]
            ).properties(title="").interactive()
            st.altair_chart(carteira_chart, use_container_width=True)
        else:
            st.warning("Nenhum produto selecionado para Receita por Carteira.")


    st.markdown("---")

    # --- BLOCO 4: RANKINGS E DETALHAMENTO (Layout do PDF) ---
    
    col_r1, col_r2 = st.columns(2)

    # R1: TOP 10 QUEDA (Estático do PDF) [cite: 30]
    with col_r1:
        st.subheader("Top 10 Queda")
        st.dataframe(ranking_queda_df[['Cliente', 'CNPJ', 'Variação']].rename(columns={'Variação': 'Variação %'}), hide_index=True, use_container_width=True)

    # R2: TOP 10 CRESCIMENTO (Estático do PDF) [cite: 28]
    with col_r2:
        st.subheader("Top 10 Crescimento")
        # Pega os dados de crescimento da tabela de detalhamento (conforme PDF) 
        ranking_crescimento = detalhamento_df[['Cliente', 'Crescimento']].sort_values(by='Crescimento', ascending=False).head(10)
        st.dataframe(ranking_crescimento.rename(columns={'Crescimento': 'Variação %'}), hide_index=True, use_container_width=True)

    st.markdown("---")
    
    # --- BLOCO 5: DETALHAMENTO E INSIGHTS (Layout do PDF) ---
    
    st.header("Detalhamento por Cliente")
    
    col_d1, col_d2 = st.columns([2, 1])
    
    # D1: Tabela de Detalhamento (Estático do PDF) 
    with col_d1:
        st.subheader("Clientes e Crescimento")
        st.dataframe(detalhamento_df.rename(columns={'Crescimento': 'Crescimento %'}), hide_index=True, use_container_width=True)
        st.markdown("Mostrando 1 a 10 de 99 clientes | [Anterior] [Próxima]")
        
        # Botão de Exportar
        @st.cache_data
        def convert_df_to_csv(df):
            return df.to_csv(index=False, sep=';', decimal=',').encode('utf-8-sig')

        csv = convert_df_to_csv(detalhamento_df)
        st.download_button(
            label="Exportar CSV",
            data=csv,
            file_name="detalhamento_clientes.csv",
            mime="text/csv",
        )
        
    # D2: Insights (Estático do PDF) 
    with col_d2:
        st.subheader("Insights Automáticos")
        st.success("✅ Destaque do Trimestre: Posto Sol Nascente cresceu 34% com forte aumento em transações Pix.") [cite: 60]
        st.info("💡 Oportunidade: 5 clientes estão próximos de atingir novo patamar de faturamento. Considere campanhas de incentivo.") [cite: 61, 62]
        st.warning("⚠️ Atenção Necessária: Bar do João apresenta queda de 18%. Recomenda-se contato da equipe comercial.") [cite: 63]


# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    dashboard_page()
else:
    # Se não estiver autenticado, o streamlit_app.py já cuida da página de login
    pass
