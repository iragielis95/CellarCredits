import streamlit as st
import streamlit_authenticator as stauth

st.title("TEST LOGIN PAGE")

credentials = {
    "usernames": {
        "testuser": {
            "email": "test@example.com",
            "name": "Test User",
            "password": stauth.Hasher().hash("test123"),
        }
    }
}

authenticator = stauth.Authenticate(
    credentials,
    cookie_name="test_cookie",
    cookie_key="abcdef123456",
    cookie_expiry_days=1,
)

fields = {
    "Form name": "Login",
    "Username": "Username",
    "Password": "Password",
}

# EXACT signature for your version:
login_result = authenticator.login(
    "main",     # location
    None,
    None,
    fields,     # REQUIRED for 0.4.2
)

if login_result is None:
    st.write("login_result is None (login form could not render)")
    st.stop()

name, auth_status, username = login_result

if not auth_status:
    if auth_status is False:
        st.error("Incorrect username or password")
    else:
        st.warning("Please enter your username and password")
    st.stop()

st.success(f"Welcome, {name}!")