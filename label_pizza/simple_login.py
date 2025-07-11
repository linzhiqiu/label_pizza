# Zhiqiu: This is a failed attempt to implement a simple auto-login system.
import streamlit as st
import streamlit.components.v1 as components
import time

# Must be first
st.set_page_config(
    page_title="Login App", 
    page_icon="🔐", 
    layout="centered"
)

# Hardcoded credentials
VALID_USERNAME = "Zhiqiu Lin"
VALID_PASSWORD = "123"

def check_credentials(username, password):
    """Check if the provided credentials are valid"""
    return username == VALID_USERNAME and password == VALID_PASSWORD

def get_localStorage_credentials():
    """Get credentials from localStorage without page refresh"""
    
    # Use a unique key based on current time to avoid caching
    component_key = f"cred_check_{int(time.time() * 1000)}"
    
    html_code = """
    <div id="login-checker" style="padding: 15px; text-align: center; border: 1px solid #ddd; border-radius: 5px; background: #f9f9f9;">
        <div id="status">🔍 Checking for saved login credentials...</div>
        <div style="margin-top: 10px;">
            <div style="display: inline-block; width: 20px; height: 20px; border: 2px solid #f3f3f3; border-top: 2px solid #3498db; border-radius: 50%; animation: spin 1s linear infinite;"></div>
        </div>
    </div>
    
    <style>
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    </style>
    
    <script>
    function checkCredentials() {
        const statusDiv = document.getElementById('status');
        
        try {
            const savedUsername = localStorage.getItem('streamlit_username');
            const savedPassword = localStorage.getItem('streamlit_password');
            
            if (savedUsername && savedPassword) {
                statusDiv.innerHTML = '✅ Found saved credentials for: <strong>' + savedUsername + '</strong>';
                statusDiv.style.color = 'green';
                
                // Send credentials to Streamlit
                window.parent.postMessage({
                    type: 'streamlit:setComponentValue',
                    value: JSON.stringify({
                        found: true,
                        username: savedUsername,
                        password: savedPassword
                    })
                }, '*');
                
            } else {
                statusDiv.innerHTML = '❌ No saved credentials found';
                statusDiv.style.color = 'orange';
                
                // Send "not found" to Streamlit
                window.parent.postMessage({
                    type: 'streamlit:setComponentValue',
                    value: JSON.stringify({
                        found: false
                    })
                }, '*');
            }
        } catch (error) {
            statusDiv.innerHTML = '⚠️ Error: ' + error.message;
            statusDiv.style.color = 'red';
            
            window.parent.postMessage({
                type: 'streamlit:setComponentValue',
                value: JSON.stringify({
                    found: false,
                    error: error.message
                })
            }, '*');
        }
    }
    
    // Check credentials after a short delay
    setTimeout(checkCredentials, 300);
    </script>
    """
    
    return components.html(html_code, height=120)

def save_to_localStorage(username, password):
    """Save credentials to localStorage"""
    html_code = f"""
    <div style="padding: 10px; text-align: center; border: 1px solid #ddd; border-radius: 5px; background: #f0f8ff;">
        <div id="save-status">💾 Saving credentials...</div>
    </div>
    
    <script>
    try {{
        localStorage.setItem('streamlit_username', '{username}');
        localStorage.setItem('streamlit_password', '{password}');
        
        document.getElementById('save-status').innerHTML = '✅ Credentials saved for future auto-login!';
        document.getElementById('save-status').style.color = 'green';
    }} catch (error) {{
        document.getElementById('save-status').innerHTML = '❌ Error saving: ' + error.message;
        document.getElementById('save-status').style.color = 'red';
    }}
    </script>
    """
    
    components.html(html_code, height=60)

def clear_localStorage():
    """Clear credentials from localStorage"""
    html_code = """
    <div style="padding: 10px; text-align: center; border: 1px solid #ddd; border-radius: 5px; background: #ffe6e6;">
        <div id="clear-status">🗑️ Clearing credentials...</div>
    </div>
    
    <script>
    try {
        localStorage.removeItem('streamlit_username');
        localStorage.removeItem('streamlit_password');
        
        document.getElementById('clear-status').innerHTML = '✅ All credentials cleared!';
        document.getElementById('clear-status').style.color = 'green';
    } catch (error) {
        document.getElementById('clear-status').innerHTML = '❌ Error: ' + error.message;
        document.getElementById('clear-status').style.color = 'red';
    }
    </script>
    """
    
    components.html(html_code, height=60)

def auto_login_page():
    """Check for saved credentials and auto-login if found"""
    st.title("🔐 Login System")
    
    # Initialize checking state
    if 'check_attempt' not in st.session_state:
        st.session_state.check_attempt = 0
    
    # Increment attempt counter
    st.session_state.check_attempt += 1
    
    # Get saved credentials
    result = get_localStorage_credentials()
    
    # Check if result is a string (JavaScript returned data)
    if isinstance(result, str) and result:
        try:
            import json
            data = json.loads(result)
            
            if data.get('found') and data.get('username') and data.get('password'):
                username = data['username']
                password = data['password']
                
                # Validate credentials
                if check_credentials(username, password):
                    # Success! Auto-login
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.session_state.login_time = time.time()
                    st.session_state.auto_login = True
                    # Clean up checking state
                    if 'check_attempt' in st.session_state:
                        del st.session_state.check_attempt
                    
                    st.success("🚀 Auto-login successful! Redirecting...")
                    time.sleep(1)
                    st.rerun()
                else:
                    # Invalid credentials
                    st.error("Saved credentials are invalid. Clearing them...")
                    clear_localStorage()
                    st.session_state.credentials_checked = True
                    if 'check_attempt' in st.session_state:
                        del st.session_state.check_attempt
                    time.sleep(2)
                    st.rerun()
            else:
                # No credentials found
                st.session_state.credentials_checked = True
                if 'check_attempt' in st.session_state:
                    del st.session_state.check_attempt
                time.sleep(1)
                st.rerun()
                
        except (json.JSONDecodeError, TypeError) as e:
            # Error parsing result - retry a few times
            if st.session_state.check_attempt < 3:
                st.info(f"Parsing credentials... (attempt {st.session_state.check_attempt})")
                time.sleep(0.5)
                st.rerun()
            else:
                # Give up after 3 attempts
                st.warning("Could not read saved credentials. Proceeding to manual login.")
                st.session_state.credentials_checked = True
                if 'check_attempt' in st.session_state:
                    del st.session_state.check_attempt
                time.sleep(1)
                st.rerun()
    else:
        # No result yet or result is not a string (component still loading)
        if st.session_state.check_attempt < 8:
            # Still waiting for component result
            st.info(f"Loading credentials... (attempt {st.session_state.check_attempt})")
            time.sleep(0.5)
            st.rerun()
        else:
            # Give up after 8 attempts
            st.warning("Credential check timed out. Proceeding to manual login.")
            st.session_state.credentials_checked = True
            if 'check_attempt' in st.session_state:
                del st.session_state.check_attempt
            time.sleep(1)
            st.rerun()
    
    # Show skip button while waiting
    st.markdown("---")
    if st.button("Skip to manual login", use_container_width=True):
        st.session_state.credentials_checked = True
        if 'check_attempt' in st.session_state:
            del st.session_state.check_attempt
        st.rerun()

def login_page():
    """Display the login page"""
    st.title("🔐 Login")
    st.markdown("---")
    
    # Create a form for login
    with st.form("login_form"):
        username = st.text_input("Username", placeholder="Enter your username", value="")
        password = st.text_input("Password", type="password", placeholder="Enter your password", value="")
        remember_me = st.checkbox("Remember me for future logins", value=True)
        
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            submit_button = st.form_submit_button("Login", use_container_width=True)
        
        if submit_button:
            if check_credentials(username, password):
                # Login successful
                st.session_state.logged_in = True
                st.session_state.username = username
                st.session_state.login_time = time.time()
                
                # Save credentials if remember me is checked
                if remember_me:
                    save_to_localStorage(username, password)
                    st.success("✅ Login successful! Credentials saved for next time.")
                else:
                    st.success("✅ Login successful!")
                
                time.sleep(2)
                st.rerun()
            else:
                st.error("❌ Invalid username or password. Please try again.")
                
    # Display hint for demo purposes
    with st.expander("📝 Demo Credentials", expanded=False):
        st.info(f"**Username:** {VALID_USERNAME}")
        st.info("**Password:** 123")

def main_app():
    """Display the main application after successful login"""
    st.title(f"Welcome, {st.session_state.username}! 👋")
    st.markdown("---")
    
    # Display auto-login message
    if st.session_state.get('auto_login', False):
        st.success("🚀 Welcome back! You were automatically logged in from saved credentials.")
        st.session_state.auto_login = False  # Show only once
    else:
        st.success("✅ You are successfully logged in!")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.info(f"**Logged in as:** {st.session_state.username}")
        
    with col2:
        login_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(st.session_state.login_time))
        st.info(f"**Login time:** {login_time}")
    
    st.markdown("### 📊 Dashboard")
    st.write("This is where your main application content would go.")
    
    # Sample content
    tab1, tab2, tab3 = st.tabs(["📈 Overview", "📊 Data", "⚙️ Settings"])
    
    with tab1:
        st.write("Welcome to your dashboard overview!")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Users", "1", "0")
        with col2:
            st.metric("Active Sessions", "1", "1")
        with col3:
            st.metric("Uptime", "24h", "2h")
        
    with tab2:
        st.write("Here you could display data tables, charts, etc.")
        import pandas as pd
        df = pd.DataFrame({
            'Date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'Value': [10, 20, 15]
        })
        st.dataframe(df, use_container_width=True)
        
    with tab3:
        st.write("Application settings would go here.")
        st.toggle("Enable notifications")
        st.selectbox("Theme", ["Light", "Dark", "Auto"])
    
    # Logout buttons
    st.markdown("---")
    st.subheader("Logout Options")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔓 Logout (Keep saved credentials)", use_container_width=True):
            # Clear Streamlit session but keep localStorage
            keys_to_clear = ['logged_in', 'username', 'login_time', 'auto_login', 'credentials_checked']
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            
            st.success("✅ Logged out! Your credentials remain saved for next time.")
            time.sleep(1)
            st.rerun()
            
    with col2:
        if st.button("🗑️ Logout (Clear everything)", use_container_width=True):
            # Clear localStorage
            clear_localStorage()
            time.sleep(1)
            
            # Clear Streamlit session
            keys_to_clear = ['logged_in', 'username', 'login_time', 'auto_login', 'credentials_checked']
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            
            st.success("✅ Logged out! All credentials cleared.")
            time.sleep(1)
            st.rerun()
            
    with col3:
        if st.button("🔧 Clear saved credentials only", use_container_width=True):
            clear_localStorage()
            st.success("✅ Saved credentials cleared!")

def main():
    """Main application logic"""
    # Initialize session state
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'credentials_checked' not in st.session_state:
        st.session_state.credentials_checked = False
    
    # Determine which page to show
    if st.session_state.logged_in:
        # User is logged in - show main app
        main_app()
    elif st.session_state.credentials_checked:
        # Already checked for credentials - show login page
        login_page()
    else:
        # First time - check for auto-login
        auto_login_page()

if __name__ == "__main__":
    main()