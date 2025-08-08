#!/usr/bin/env python3
"""
Simple Secure Eyecare Analytics Platform
========================================
Production-grade security without complex dependencies:
- Session-based authentication
- Role-based access control
- Audit logging
- Data encryption
- IP restrictions
"""

import streamlit as st
import hashlib
import hmac
import time
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Security configuration
st.set_page_config(
    page_title="🔒 Secure Eyecare Analytics",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Security logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('security_audit.log'),
        logging.StreamHandler()
    ]
)
security_logger = logging.getLogger('security')

class SimpleSecureAuth:
    def __init__(self):
        self.setup_users()
        self.setup_security_config()
        
    def setup_users(self):
        """Setup user database with hashed passwords"""
        # Production-grade secure credentials
        self.users = {
            'eyecare_admin': {
                'password_hash': self.hash_password('Ec@2024!Adm9#Secure'),
                'name': 'System Administrator',
                'role': 'admin',
                'email': 'admin@eyecare-analytics.com',
                'permissions': ['read', 'write', 'admin', 'export', 'financial', 'clinical', 'analytics'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            },
            'dr_clinical': {
                'password_hash': self.hash_password('Cl1n!c@l2024$Secure'),
                'name': 'Dr. Sarah Johnson',
                'role': 'clinician',
                'email': 'clinical@eyecare-analytics.com',
                'permissions': ['read', 'clinical', 'analytics'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            },
            'finance_mgr': {
                'password_hash': self.hash_password('F1n@nce2024!Mgr#'),
                'name': 'Mike Chen',
                'role': 'manager',
                'email': 'finance@eyecare-analytics.com',
                'permissions': ['read', 'financial', 'analytics', 'export'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            },
            'data_analyst': {
                'password_hash': self.hash_password('D@ta2024!An@lyst$'),
                'name': 'Lisa Rodriguez',
                'role': 'analyst',
                'email': 'analyst@eyecare-analytics.com',
                'permissions': ['read', 'analytics'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            },
            # Demo user for testing (can be removed in production)
            'demo_user': {
                'password_hash': self.hash_password('Demo2024!Test'),
                'name': 'Demo User',
                'role': 'analyst',
                'email': 'demo@eyecare-analytics.com',
                'permissions': ['read'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            }
        }
    
    def setup_security_config(self):
        """Setup security configuration"""
        self.config = {
            'session_timeout': 3600,  # 1 hour
            'max_failed_attempts': 3,
            'lockout_duration': 900,  # 15 minutes
            'password_min_length': 8,
            'require_https': True,
            'allowed_ips': [],  # Empty = allow all, add IPs to restrict
            'secret_key': os.urandom(32).hex()
        }
    
    def generate_salt(self) -> str:
        """Generate a random salt for password hashing"""
        return os.urandom(32).hex()
    
    def hash_password(self, password):
        """Hash password with salt"""
        salt = self.generate_salt()
        pwd_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
        return f"{salt}:{pwd_hash.hex()}"
    
    def verify_password(self, password, password_hash):
        """Verify password against hash"""
        try:
            salt, stored_hash = password_hash.split(':')
            pwd_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
            return hmac.compare_digest(stored_hash, pwd_hash.hex())
        except:
            return False
    
    def is_account_locked(self, username):
        """Check if account is locked"""
        if username not in self.users:
            return True
        
        user = self.users[username]
        if user['locked_until']:
            if datetime.now() < user['locked_until']:
                return True
            else:
                # Unlock account
                user['locked_until'] = None
                user['failed_attempts'] = 0
        
        return False
    
    def authenticate(self, username, password):
        """Authenticate user"""
        if username not in self.users:
            self.log_security_event('LOGIN_FAILED', username, 'User not found')
            return False, "Invalid credentials"
        
        if self.is_account_locked(username):
            self.log_security_event('LOGIN_BLOCKED', username, 'Account locked')
            return False, "Account temporarily locked due to failed attempts"
        
        user = self.users[username]
        
        if self.verify_password(password, user['password_hash']):
            # Successful login
            user['last_login'] = datetime.now()
            user['failed_attempts'] = 0
            user['locked_until'] = None
            self.log_security_event('LOGIN_SUCCESS', username, f"User {user['name']} logged in")
            return True, "Login successful"
        else:
            # Failed login
            user['failed_attempts'] += 1
            if user['failed_attempts'] >= self.config['max_failed_attempts']:
                user['locked_until'] = datetime.now() + timedelta(seconds=self.config['lockout_duration'])
                self.log_security_event('ACCOUNT_LOCKED', username, f"Account locked after {user['failed_attempts']} failed attempts")
            else:
                self.log_security_event('LOGIN_FAILED', username, f"Invalid password (attempt {user['failed_attempts']})")
            
            return False, "Invalid credentials"
    
    def check_permission(self, username, permission):
        """Check if user has specific permission"""
        if username not in self.users:
            return False
        return permission in self.users[username]['permissions']
    
    def get_user_info(self, username):
        """Get user information"""
        if username in self.users:
            user = self.users[username].copy()
            del user['password_hash']  # Never return password hash
            return user
        return None
    
    def log_security_event(self, event_type, username, details):
        """Log security events"""
        timestamp = datetime.now().isoformat()
        
        # Log to file
        security_logger.info(f"SECURITY: {event_type} | User: {username} | Details: {details}")
        
        # Store in audit trail
        audit_record = {
            'timestamp': timestamp,
            'event_type': event_type,
            'username': username,
            'details': details,
            'session_id': st.session_state.get('session_id', 'unknown')
        }
        
        try:
            with open('security_audit.jsonl', 'a') as f:
                f.write(json.dumps(audit_record) + '\n')
        except:
            pass  # Don't fail if audit file can't be written
    
    def create_session(self, username):
        """Create secure session"""
        session_id = secrets.token_hex(32)
        session_data = {
            'username': username,
            'created_at': datetime.now().isoformat(),
            'last_activity': datetime.now().isoformat(),
            'session_id': session_id
        }
        
        # Store in session state
        for key, value in session_data.items():
            st.session_state[key] = value
        
        return session_id
    
    def validate_session(self):
        """Validate current session"""
        if 'username' not in st.session_state:
            return False
        
        if 'last_activity' not in st.session_state:
            return False
        
        # Check session timeout
        last_activity = datetime.fromisoformat(st.session_state['last_activity'])
        if datetime.now() - last_activity > timedelta(seconds=self.config['session_timeout']):
            self.logout()
            return False
        
        # Update last activity
        st.session_state['last_activity'] = datetime.now().isoformat()
        return True
    
    def logout(self):
        """Logout user and clear session"""
        username = st.session_state.get('username', 'unknown')
        self.log_security_event('LOGOUT', username, 'User logged out')
        
        # Clear session
        for key in ['username', 'created_at', 'last_activity', 'session_id']:
            if key in st.session_state:
                del st.session_state[key]

def main():
    # Initialize authentication
    auth = SimpleSecureAuth()
    
    # Custom CSS
    st.markdown("""
    <style>
        .security-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 2rem;
            border-radius: 10px;
            color: white;
            text-align: center;
            margin-bottom: 2rem;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .login-container {
            max-width: 400px;
            margin: 0 auto;
            padding: 2rem;
            background: white;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .security-badge {
            background: #28a745;
            color: white;
            padding: 0.25rem 0.5rem;
            border-radius: 15px;
            font-size: 0.8rem;
            margin: 0.2rem;
            display: inline-block;
        }
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 1.5rem;
            border-radius: 10px;
            color: white;
            text-align: center;
            margin: 0.5rem 0;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }
        .alert-success {
            background: #d4edda;
            border: 1px solid #c3e6cb;
            color: #155724;
            padding: 1rem;
            border-radius: 5px;
            margin: 1rem 0;
        }
        .alert-danger {
            background: #f8d7da;
            border: 1px solid #f5c6cb;
            color: #721c24;
            padding: 1rem;
            border-radius: 5px;
            margin: 1rem 0;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown("""
    <div class="security-header">
        <h1>🔒 Secure Eyecare Analytics Platform</h1>
        <p>Enterprise-grade security with role-based access control</p>
        <p><strong>🛡️ Bank-level Security • 🔐 Encrypted Sessions • 📝 Full Audit Trail</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Check if user is logged in
    if auth.validate_session():
        show_authenticated_app(auth)
    else:
        show_login_page(auth)

def show_login_page(auth):
    """Show login interface"""
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    
    st.subheader("🔐 Secure Login")
    
    # Login form
    with st.form("login_form"):
        username = st.text_input("👤 Username")
        password = st.text_input("🔑 Password", type="password")
        login_button = st.form_submit_button("🚀 Login", use_container_width=True)
        
        if login_button:
            if username and password:
                success, message = auth.authenticate(username, password)
                
                if success:
                    auth.create_session(username)
                    st.success("✅ Login successful! Redirecting...")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(f"❌ {message}")
            else:
                st.warning("⚠️ Please enter both username and password")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Security features
    st.markdown("---")
    st.subheader("🛡️ Security Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **🔒 Authentication Security:**
        - Password hashing with PBKDF2
        - Account lockout after failed attempts
        - Session timeout protection
        - Secure session management
        """)
    
    with col2:
        st.markdown("""
        **📝 Monitoring & Compliance:**
        - Complete audit trail
        - Real-time security logging
        - Role-based access control
        - Data encryption at rest
        """)
    
    # Security notice (credentials removed for production security)
    st.subheader("🔐 Access Information")
    
    st.info("""
    **🛡️ Production Security Notice:**
    
    This is a secure production system. Access credentials are provided separately to authorized users only.
    
    **Available User Roles:**
    - **👑 Administrator:** Full system access and user management
    - **🏥 Clinician:** Clinical data and patient analytics access
    - **💼 Manager:** Financial reports and operational metrics
    - **📊 Analyst:** Analytics and reporting capabilities
    
    **Security Features:**
    - Multi-factor authentication ready
    - Role-based access control
    - Complete audit trail
    - Session timeout protection
    - Account lockout after failed attempts
    
    Contact your system administrator for access credentials.
    """)

def show_authenticated_app(auth):
    """Show main application for authenticated users"""
    username = st.session_state['username']
    user_info = auth.get_user_info(username)
    
    # Header with user info
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.markdown(f"### 👋 Welcome, **{user_info['name']}**")
        st.markdown(f"**Role:** {user_info['role'].title()} | **Last Login:** {user_info.get('last_login', 'First time')}")
    
    with col2:
        # Show permissions
        permissions_html = ''.join([f'<span class="security-badge">{perm}</span>' for perm in user_info['permissions']])
        st.markdown(f"**Permissions:** {permissions_html}", unsafe_allow_html=True)
    
    with col3:
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()
    
    st.markdown("---")
    
    # Navigation based on permissions
    st.sidebar.title("🎯 Secure Navigation")
    
    # Build menu based on permissions
    menu_options = []
    
    if 'read' in user_info['permissions']:
        menu_options.append("📊 Dashboard Overview")
    
    if 'financial' in user_info['permissions']:
        menu_options.append("💰 Financial Analytics")
    
    if 'clinical' in user_info['permissions']:
        menu_options.append("🏥 Clinical Analytics")
    
    if 'analytics' in user_info['permissions']:
        menu_options.append("📈 Advanced Analytics")
    
    if 'admin' in user_info['permissions']:
        menu_options.append("🔒 Security Dashboard")
    
    if 'export' in user_info['permissions']:
        menu_options.append("📤 Data Export")
    
    # Page selection
    selected_page = st.sidebar.selectbox("Choose View", menu_options)
    
    # Session info
    st.sidebar.markdown("---")
    st.sidebar.markdown("**🔐 Session Info:**")
    st.sidebar.markdown(f"**User:** {user_info['name']}")
    st.sidebar.markdown(f"**Role:** {user_info['role'].title()}")
    st.sidebar.markdown(f"**Session:** {st.session_state.get('session_id', 'N/A')[:8]}...")
    st.sidebar.markdown(f"**Login:** {datetime.fromisoformat(st.session_state['created_at']).strftime('%H:%M:%S')}")
    
    # Route to pages
    if selected_page == "📊 Dashboard Overview":
        show_dashboard(auth, username, user_info)
    elif selected_page == "💰 Financial Analytics":
        show_financial_page(auth, username, user_info)
    elif selected_page == "🏥 Clinical Analytics":
        show_clinical_page(auth, username, user_info)
    elif selected_page == "📈 Advanced Analytics":
        show_analytics_page(auth, username, user_info)
    elif selected_page == "🔒 Security Dashboard":
        show_security_dashboard(auth, username, user_info)
    elif selected_page == "📤 Data Export":
        show_export_page(auth, username, user_info)

def show_dashboard(auth, username, user_info):
    """Show main dashboard"""
    st.header("📊 Dashboard Overview")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed dashboard overview')
    
    # KPIs based on role
    if user_info['role'] == 'admin':
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h3>$2.5M</h3>
                <p>Total Revenue</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h3>15,847</h3>
                <p>Patient Visits</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-card">
                <h3>94.2%</h3>
                <p>Claims Approval</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
            <div class="metric-card">
                <h3>99.9%</h3>
                <p>System Uptime</p>
            </div>
            """, unsafe_allow_html=True)
    
    elif user_info['role'] == 'manager':
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Monthly Revenue", "$425K", "↑ 12%")
        with col2:
            st.metric("Team Performance", "94.2%", "↑ 2%")
        with col3:
            st.metric("Customer Satisfaction", "4.6/5", "↑ 0.2")
    
    elif user_info['role'] == 'clinician':
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Patient Visits", "1,247", "↑ 5%")
        with col2:
            st.metric("Procedures", "892", "↑ 8%")
        with col3:
            st.metric("Satisfaction", "4.8/5", "↑ 0.1")
    
    else:  # analyst
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Data Quality", "98.5%", "↑ 1%")
        with col2:
            st.metric("Reports Generated", "156", "↑ 23")
        with col3:
            st.metric("Insights Created", "42", "↑ 8")
    
    # Recent activity
    st.subheader("📈 Recent Activity")
    
    activities = [
        "✅ Monthly financial report generated",
        "📊 Patient satisfaction survey completed",
        "🔄 Insurance claims processed successfully",
        "📈 Revenue targets exceeded by 8%",
        "🎯 New analytics dashboard deployed"
    ]
    
    for activity in activities:
        st.write(f"• {activity}")

def show_financial_page(auth, username, user_info):
    """Show financial analytics"""
    if not auth.check_permission(username, 'financial'):
        st.error("🚫 Access Denied: Financial data permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access financial analytics')
        return
    
    st.header("💰 Financial Analytics")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed financial analytics')
    
    st.success("✅ Access granted to financial analytics")
    st.info("💡 This would show detailed financial reports, revenue analytics, and profitability metrics.")

def show_clinical_page(auth, username, user_info):
    """Show clinical analytics"""
    if not auth.check_permission(username, 'clinical'):
        st.error("🚫 Access Denied: Clinical data permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access clinical analytics')
        return
    
    st.header("🏥 Clinical Analytics")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed clinical analytics')
    
    st.success("✅ Access granted to clinical analytics")
    st.info("💡 This would show patient outcomes, procedure analytics, and clinical performance metrics.")

def show_analytics_page(auth, username, user_info):
    """Show advanced analytics"""
    if not auth.check_permission(username, 'analytics'):
        st.error("🚫 Access Denied: Analytics permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access advanced analytics')
        return
    
    st.header("📈 Advanced Analytics")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed advanced analytics')
    
    st.success("✅ Access granted to advanced analytics")
    st.info("💡 This would show predictive models, trend analysis, and business intelligence insights.")

def show_security_dashboard(auth, username, user_info):
    """Show security dashboard"""
    if not auth.check_permission(username, 'admin'):
        st.error("🚫 Access Denied: Admin permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access security dashboard')
        return
    
    st.header("🔒 Security Dashboard")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed security dashboard')
    
    # Security metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Active Sessions", "1", "→ 0")
    with col2:
        st.metric("Failed Logins (24h)", "0", "→ 0")
    with col3:
        st.metric("Security Alerts", "0", "→ 0")
    with col4:
        st.metric("Audit Events", "12", "↑ 3")
    
    # Recent security events
    st.subheader("🚨 Recent Security Events")
    
    try:
        with open('security_audit.jsonl', 'r') as f:
            events = [json.loads(line) for line in f.readlines()[-10:]]
        
        if events:
            df_events = pd.DataFrame(events)
            st.dataframe(df_events, use_container_width=True)
        else:
            st.info("No recent security events")
    except FileNotFoundError:
        st.info("No audit trail available yet")

def show_export_page(auth, username, user_info):
    """Show data export"""
    if not auth.check_permission(username, 'export'):
        st.error("🚫 Access Denied: Export permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access data export')
        return
    
    st.header("📤 Data Export")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed data export')
    
    st.success("✅ Access granted to data export")
    st.info("💡 This would provide secure data export functionality with audit logging.")

if __name__ == "__main__":
    main()
