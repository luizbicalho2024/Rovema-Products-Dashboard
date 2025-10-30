import streamlit as st
import sys
import os

# CORREÇÃO PARA 'KeyError: utils': Adiciona o diretório raiz ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.firebase_config import get_admin_auth, get_db

st.set_page_config(page_title="Admin Bootstrap", layout="centered")
st.title("🔧 Criador de Usuário Admin (Temporário)")
st.warning("PERIGO: Esta página deve ser excluída após o uso!")

try:
    # Tenta inicializar o Firebase
    admin_auth = get_admin_auth()
    db = get_db()
except Exception as e:
    st.error(f"Erro ao conectar ao Firebase: {e}")
    st.error("Verifique seus Secrets no Streamlit Cloud antes de continuar.")
    st.stop()

st.info("Esta página cria um usuário no Firebase Authentication E define sua permissão como 'admin' no Firestore.")

with st.form("admin_bootstrap_form"):
    name = st.text_input("Nome do Admin")
    email = st.text_input("Email do Admin")
    password = st.text_input("Senha do Admin", type="password")
    
    submit_button = st.form_submit_button("Criar Admin Agora", width='stretch')

if submit_button:
    if not name or not email or not password:
        st.error("Por favor, preencha todos os campos.")
    else:
        try:
            with st.spinner("Criando usuário..."):
                # 1. Cria o usuário no Firebase Authentication
                user_record = admin_auth.create_user(
                    email=email,
                    password=password,
                    display_name=name
                )
                
                # 2. Salva os dados (role: admin) no Firestore
                user_data = {
                    "name": name,
                    "email": email,
                    "role": "admin", # A permissão principal
                    "manager_uid": None # Admin não tem gestor
                }
                db.collection("users").document(user_record.uid).set(user_data)
                
                st.success(f"Usuário Admin '{name}' criado com sucesso!")
                st.info(f"UID: {user_record.uid}")
                st.balloons()

        except Exception as e:
            if "EMAIL_EXISTS" in str(e):
                st.error(f"Erro: O email '{email}' já existe no Firebase Authentication.")
            else:
                st.error(f"Erro ao criar usuário: {e}")
