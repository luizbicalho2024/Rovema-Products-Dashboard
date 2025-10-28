# Rovema-Products-Dashboard (Rovema Bank Pulse)

Este projeto é um dashboard analítico multi-produto construído em Streamlit, projetado para monitorar métricas de performance de diferentes produtos do Rovema Bank (como Rovema Pay, Bionio, Asto e Eliq).

O dashboard é alimentado por dados do Firestore, permitindo o upload de relatórios (CSV/XLSX) e a conexão (simulada) com APIs.

## Funcionalidades Principais

* **Autenticação Segura:** Login baseado em e-mail/senha com controle de acesso por papéis (Admin, Usuário) via Firebase Authentication.
* **Dashboard Dinâmico:** Visualização de métricas (Valor Transacionado, Receita) e gráficos de evolução e participação, com filtros por data, produto e consultor.
* **Upload de Dados:** Página protegida para upload de relatórios (Bionio, Rovema Pay) que são processados, limpos e salvos no Firestore.
* **Gestão de Acessos (Admin):** Interface para criar novos usuários e definir seus níveis de acesso.
* **Logs de Auditoria (Admin):** Página para visualizar todos os eventos do sistema (logins, uploads, visualizações de página).

## Configuração do Projeto

Para executar este projeto, você precisará de credenciais do Google Cloud (Firebase).

### 1. Dependências

Instale as bibliotecas Python necessárias:

```bash
pip install -r requirements.txt
