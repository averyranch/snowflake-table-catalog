import streamlit as st
import requests
from requests.auth import HTTPBasicAuth

# Base URL of the Starburst BIAC API
BASE_URL = "https://your-starburst-api.example.com/api/v1"

headers = {
    'Content-Type': 'application/json'
}

# Streamlit UI
st.title("Starburst BIAC Role Management")

# --- User Authentication ---
st.sidebar.header("User Authentication")
USER = st.sidebar.text_input("Username")
PASSWORD = st.sidebar.text_input("Password", type="password")

auth = HTTPBasicAuth(USER, PASSWORD)

# --- List Existing Roles ---
st.header("Existing Roles")

if st.button("Refresh Roles"):
    response = requests.get(f"{BASE_URL}/roles", auth=auth, headers=headers)
    if response.ok:
        roles = response.json()
        st.json(roles)
    else:
        st.error("Failed to fetch roles.")

# --- Add a New Role ---
st.header("Add a New Role")
new_role_name = st.text_input("Role Name", key='new_role')

if st.button("Create Role"):
    if new_role_name:
        payload = {"role_name": new_role_name}
        response = requests.post(f"{BASE_URL}/roles", json=payload, auth=auth, headers=headers)

        if response.status_code == 201:
            st.success(f"Role '{new_role_name}' created successfully.")
        else:
            st.error(f"Failed to create role: {response.text}")
    else:
        st.error("Please enter a role name.")

# --- Delete a Role ---
st.header("Delete Role")
delete_role_name = st.text_input("Role Name to Delete", key='delete_role')

if st.button("Delete Role"):
    if delete_role_name:
        response = requests.delete(f"{BASE_URL}/roles/{delete_role_name}", auth=auth, headers=headers)

        if response.status_code == 200:
            st.success(f"Role '{delete_role_name}' deleted successfully.")
        elif response.status_code == 404:
            st.error("Role not found.")
        else:
            st.error(f"Failed to delete role. Status code: {response.status_code}")
    else:
        st.error("Please enter the role name to delete.")
