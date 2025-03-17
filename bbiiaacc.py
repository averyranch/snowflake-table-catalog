import streamlit as st
import requests
from requests.auth import HTTPBasicAuth

BASE_URL = "https://your-starburst-api.example.com/api/v1"

st.set_page_config(page_title="Starburst Role Manager", layout="centered")
st.title("🌟 Starburst BIAC Role Management")

# Sidebar for User Authentication
st.sidebar.header("🔐 Authentication")
username = st.sidebar.text_input("Username")
password = st.sidebar.text_input("Password", type="password")

# Authentication object
auth = HTTPBasicAuth(username, password)

# Fetch and display roles
def get_roles():
    res = requests.get(f"{BASE_URL}/roles", auth=auth, verify=False)
    return res.json() if res.ok else []

# Add new role
def add_role(role_name):
    res = requests.post(f"{BASE_URL}/roles", json={"name": role_name}, auth=auth, verify=False)
    return res.status_code == 201, res.text

# Delete role
def delete_role(role_name):
    res = requests.delete(f"{BASE_URL}/roles/{role_name}", auth=auth, verify=False)
    return res.status_code

# UI for listing roles
st.subheader("📋 Roles")
if st.button("🔄 Refresh"):
    roles = get_roles()
    if roles:
        for role in roles:
            st.markdown(f"- **{role['name']}**")
    else:
        st.info("No roles found or unable to fetch roles.")

# UI for adding roles
st.subheader("➕ Add Role")
new_role = st.text_input("Enter new role")
if st.button("✅ Add"):
    if new_role:
        success, msg = add_role(new_role)
        st.success(f"Role '{new_role}' created successfully.") if success else st.error(f"Failed: {msg}")
    else:
        st.error("Please enter a role name.")

# UI for deleting roles
st.subheader("🗑️ Delete Role")
role_to_delete = st.text_input("Role to delete")
if st.button("❌ Delete"):
    if role_to_delete:
        status = delete_role(role_to_delete)
        if status == 204:
            st.success(f"Role '{role_to_delete}' deleted successfully.")
        elif status == 404:
            st.error("Role not found.")
        else:
            st.error(f"Failed with status code: {status}")
    else:
        st.error("Please specify a role to delete.")
