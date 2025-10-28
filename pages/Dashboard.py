import streamlit as st
import altair as alt
import pandas as pd
from datetime import date, timedelta
from fire_admin import log_event
from utils.data_processing import fetch_asto_data, fetch_eliq_data, get_latest_uploaded_data

# --- Funções Auxiliares de Visualização (Mapeando o PDF) ---

def get_ranking_data(rovemapay_df):
    """Simula a geração dos rankings de crescimento/queda e participação por bandeira."""
        
    # --- Top 10 Queda (Hardcoded para replicar o layout do PDF) ---
    ranking_queda_data = {
        'Cliente': ['Posto Avenida', 'Concessionária RodarMais', 'Restaurante Dom Pepe', 'Loja Universo Tech', 'Farmácia Popular', 'Posto Panorama', 'Oficina Auto Luz', 'Loja Bella Casa', 'Auto Mecânica Pereira', 'Livraria Estilo'],
        'CNPJ': ['85.789.123/0001-45', '18.456.789/0001-75', '86.456.789/0001-55', '87.987.654/0001-65', '19.567.890/0001-85', '20.678.901/0001-95', '88.234.567/0001-75', '21.789.012/0001-05', '89.567.890/0001-85', '90.678.901/0001-95'],
        'Variação': [-100.0] * 10
    }
    ranking_queda_df = pd.DataFrame(ranking_queda_data)
    
    # --- Detalhamento por Cliente (Hardcoded para replicar o layout do PDF) ---
    detalhamento_data = {
        'CNPJ': ['94.012.345/0001-35', '95.123.456/0001-45', '12.345.678/0001-10', '45.123.678/0001-80', '96.234.567/0001-55', '56.789.123/0001-30', '23.456.789/0001-20', '97.345.678/0001-65', '31.234.567/0001-50', '98.456.789/0001-75'],
        'Cliente': ['Posto Sol Nascente', 'Supermercado Real', 'Auto Peças Silva', 'Concessionária Fenix', 'Papelaria Central', 'Padaria Doce Sabor', 'Supermercado Oliveira', 'Auto Mecânica Lima', 'Posto Vitória', 'Oficina do Tonho'],
        'Receita': [0.0] * 10,
        'Crescimento': [10.4, 21.7, 7.9, -6.6, 17.9, 28.1, 22.7, 18.2, 29.0, 23.8],
        'Nº Vendas': [1] * 10,
        'Bandeira': ['Pix', 'Crédito', 'Crédito', 'Crédito', 'Débito', 'Débito', 'Débito', 'Crédito', 'Crédito', 'Pix']
    }
    detalhamento_df = pd.DataFrame(detalhamento_data)
    
    # --- Participação por Bandeira (Mapeando do Detalhamento) ---
    bandeira_df = detalhamento_df.groupby('Bandeira')['Nº Vendas'].sum().reset_index()
    bandeira_df = bandeira_df.rename(columns={'Nº Vendas': 'Valor'})

    return ranking_queda_df, detalhamento_df, bandeira_df


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

    st.sidebar.title("Filtros")
    
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

    st.sidebar.selectbox(
        "Consultores", 
        ["Todos", "Leandro", "Fernanda", "Yure", "Lorrana"],
        help="Filtra dados pela carteira de consultores."
    )
    
    st.sidebar.markdown("---")
    
    if st.sidebar.button("Atualizar"):
        st.session_state['update_counter'] += 1
        st.toast("Dashboard atualizado!")
        st.rerun() 
    
    
    # Carregamento de Dados (usa o update_counter para invalidar o cache)
    @st.cache_data(ttl=60, show_spinner=False)
    def load_data(start_date, end_date, update_counter):
        """Carrega todos os dados, garantindo que o cache seja invalidado pelo botão Atualizar."""
        return (
            fetch_asto_data(start_date.isoformat(), end_date.isoformat()),
            fetch_eliq_data(start_date.isoformat(), end_date.isoformat()),
            get_latest_uploaded_data('Bionio'),
            get_latest_uploaded_data('RovemaPay')
        )

    asto_df, eliq_df, bionio_df_db, rovemapay_df_db = load_data(start_date, end_date, st.session_state['update_counter'])
    
    
    # --- 1. CÁLCULO CONDICIONAL DAS MÉTRICAS ---

    current_rovema_revenue = 0
    current_bionio_value = 0
    current_asto_revenue = 0
    current_eliq_volume = 0
    current_margem_media = 0
    current_valor_transacionado = 0
    
    # 1. Rovema Pay (Liquido/Receita/Margem)
    if "Rovema Pay" in selected_products and not rovemapay_df_db.empty:
        current_rovema_revenue = rovemapay_df_db['Receita'].sum()
        current_margem_media = rovemapay_df_db['Taxa_Media'].mean()
        current_valor_transacionado += rovemapay_df_db['Liquido'].sum()

    # 2. Bionio (Valor Total Pedidos)
    if "Bionio" in selected_products and not bionio_df_db.empty:
        current_bionio_value = bionio_df_db['Valor Total Pedidos'].sum()
        current_valor_transacionado += current_bionio_value

    # 3. Asto (Receita/Volume)
    if "Asto" in selected_products and not asto_df.empty:
        current_asto_revenue = asto_df['Receita'].sum()
        current_valor_transacionado += asto_df['valorBruto'].sum()

    # 4. Eliq (Volume)
    if "Eliq" in selected_products and not eliq_df.empty:
        current_eliq_volume = eliq_df['valor_total'].sum()
        current_valor_transacionado += current_eliq_volume
        
    
    # Métrica do Header
    nossa_receita = current_rovema_revenue + current_asto_revenue
    
    # Valor transacionado deve ser o valor total somado (usamos o mock total do PDF se a soma for 0)
    valor_transacionado_display = current_valor_transacionado if current_valor_transacionado > 0 else 2_146_293.35 
    
    
    # --- 2. EXIBIÇÃO DAS MÉTRICAS ---
    
    col_m1, col_m2, col_m3, col_m4, col_m5, col_m6 = st.columns(6)
    
    col_m1.metric("Transacionado (Bruto)", f"R$ {valor_transacionado_display:,.2f}", delta="+142.49% vs. trimestre anterior")
    col_m2.metric("Nossa Receita", f"R$ {nossa_receita:,.2f}")
    col_m3.metric("Margem Média", f"{current_margem_media:.2f}%")
    col_m4.metric("Clientes Ativos", "99")
    col_m5.metric("Clientes em Queda", "16")
    
    st.markdown("---")


    # --- BLOCO 3: EVOLUÇÃO E PARTICIPAÇÃO (Gráficos Condicionais) ---
    
    col_g1, col_g2 = st.columns([2, 1])
    
    # G1: Evolução do Valor Transacionado vs Receita
    with col_g1:
        st.header("Evolução do Valor Transacionado vs Receita")
        
        # O gráfico de evolução deve ser baseado nos produtos selecionados. Usamos o Rovema Pay como base principal.
        if "Rovema Pay" in selected_products and not rovemapay_df_db.empty:
            rovema_long = rovemapay_df_db.melt('Mês', value_vars=['Receita', 'Liquido'], var_name='Métrica', value_name='Valor')
            
            evolucao_chart = alt.Chart(rovema_long).mark_line(point=True).encode(
                x=alt.X('Mês:O', title=''),
                y=alt.Y('Valor', title='Valor (R$)'),
                color='Métrica',
                tooltip=['Mês', alt.Tooltip('Valor', format='$,.2f'), 'Métrica']
            ).properties(title='Evolução da Receita e Volume Rovema Pay').interactive()
            
            st.altair_chart(evolucao_chart, use_container_width=True)
        else:
            st.info("Selecione 'Rovema Pay' ou 'Asto' para ver o gráfico de evolução.")

    # G2: Participação por Bandeira e Receita por Carteira
    with col_g2:
        ranking_queda_df, detalhamento_df, bandeira_df = get_ranking_data(rovemapay_df_db)
        
        st.subheader("Participação por Bandeira")
        if not bandeira_df.empty and bandeira_df['Valor'].sum() > 0:
            bandeira_chart = alt.Chart(bandeira_df).mark_arc(outerRadius=80).encode(
                theta=alt.Theta(field="Valor", type="quantitative"),
                color=alt.Color(field="Bandeira", type="nominal"),
                order=alt.Order("Valor", sort="descending"),
                tooltip=["Bandeira", alt.Tooltip("Valor", format=",")]
            ).properties(title="")
            st.altair_chart(bandeira_chart, use_container_width=True)
        else:
            st.warning("Dados de Bandeira insuficientes ou produto desmarcado.")
            
        st.subheader("Receita por Carteira")
        
        # Filtra o DataFrame de Carteiras (Produtos)
        carteira_data = {
            'Carteira': [],
            'Receita Total': []
        }
        
        if "Rovema Pay" in selected_products:
            carteira_data['Carteira'].append('RovemaPay')
            carteira_data['Receita Total'].append(current_rovema_revenue)
        if "Bionio" in selected_products:
            carteira_data['Carteira'].append('Bionio')
            carteira_data['Receita Total'].append(current_bionio_value)
        if "Asto" in selected_products:
            carteira_data['Carteira'].append('Asto')
            carteira_data['Receita Total'].append(current_asto_revenue)
        if "Eliq" in selected_products:
            carteira_data['Carteira'].append('Eliq')
            # Eliq usa volume, mas é mapeado como "Receita Total" no gráfico
            carteira_data['Receita Total'].append(current_eliq_volume) 
        
        carteira_df = pd.DataFrame(carteira_data)

        if not carteira_df.empty and carteira_df['Receita Total'].sum() > 0:
            carteira_chart = alt.Chart(carteira_df).mark_bar().encode(
                x=alt.X("Carteira:N", title=""),
                y=alt.Y("Receita Total", title="Receita (R$)"),
                tooltip=["Carteira", alt.Tooltip("Receita Total", format="$,.2f")]
            ).properties(title="").interactive()
            st.altair_chart(carteira_chart, use_container_width=True)
        else:
            st.warning("Nenhum produto selecionado para Receita por Carteira.")


    st.markdown("---")

    # --- BLOCO 4: RANKINGS E DETALHAMENTO ---
    
    col_r1, col_r2 = st.columns(2)

    # R1: TOP 10 QUEDA (Estático)
    with col_r1:
        st.subheader("Top 10 Queda")
        st.dataframe(ranking_queda_df[['Cliente', 'CNPJ', 'Variação']].rename(columns={'Variação': 'Variação %'}), hide_index=True, use_container_width=True)

    # R2: TOP 10 CRESCIMENTO (Estático)
    with col_r2:
        st.subheader("Top 10 Crescimento")
        ranking_crescimento_data = {
            'Cliente': ['Posto Sol Nascente', 'Supermercado Real', 'Auto Peças Silva', 'Concessionária Fenix'],
            'Variação %': [10.4, 21.7, 7.9, -6.6]
        }
        ranking_crescimento = pd.DataFrame(ranking_crescimento_data)
        st.dataframe(ranking_crescimento, hide_index=True, use_container_width=True)

    st.markdown("---")
    
    # --- BLOCO 5: DETALHAMENTO E INSIGHTS ---
    
    st.header("Detalhamento por Cliente")
    
    col_d1, col_d2 = st.columns([2, 1])
    
    # D1: Tabela de Detalhamento
    with col_d1:
        st.subheader("Clientes e Crescimento")
        st.dataframe(detalhamento_df.rename(columns={'Crescimento': 'Crescimento %'}), hide_index=True, use_container_width=True)
        st.markdown("Mostrando 1 a 10 de 99 clientes | [Anterior] [Próxima]")
        st.button("Exportar CSV") 
        
    # D2: Insights
    with col_d2:
        st.subheader("Insights Automáticos")
        st.success("✅ Destaque do Trimestre: Posto Sol Nascente cresceu 34% com forte aumento em transações Pix.")
        st.info("💡 Oportunidade: 5 clientes estão próximos de atingir novo patamar de faturamento. Considere campanhas de incentivo.")
        st.warning("⚠️ Atenção Necessária: Bar do João apresenta queda de 18%. Recomenda-se contato da equipe comercial.")


# Garante que a função da página é chamada
if st.session_state.get('authenticated'):
    dashboard_page()
else:
    pass
