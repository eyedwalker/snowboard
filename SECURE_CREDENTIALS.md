# 🔒 Production Security Credentials
## Secure Access Information for Eyecare Analytics Platform

**⚠️ CONFIDENTIAL - FOR AUTHORIZED PERSONNEL ONLY**

---

## 🔐 **Production User Accounts**

### **👑 Administrator Account**
- **Username:** `eyecare_admin`
- **Password:** `Ec@2024!Adm9#Secure`
- **Role:** System Administrator
- **Email:** admin@eyecare-analytics.com
- **Permissions:** Full system access, user management, all analytics

### **🏥 Clinical Account**
- **Username:** `dr_clinical`
- **Password:** `Cl1n!c@l2024$Secure`
- **Role:** Clinician
- **Email:** clinical@eyecare-analytics.com
- **Permissions:** Clinical data, patient analytics

### **💼 Finance Manager Account**
- **Username:** `finance_mgr`
- **Password:** `F1n@nce2024!Mgr#`
- **Role:** Manager
- **Email:** finance@eyecare-analytics.com
- **Permissions:** Financial reports, operational metrics, data export

### **📊 Data Analyst Account**
- **Username:** `data_analyst`
- **Password:** `D@ta2024!An@lyst$`
- **Role:** Analyst
- **Email:** analyst@eyecare-analytics.com
- **Permissions:** Analytics and reporting only

### **🧪 Demo/Test Account**
- **Username:** `demo_user`
- **Password:** `Demo2024!Test`
- **Role:** Demo User
- **Email:** demo@eyecare-analytics.com
- **Permissions:** Read-only access (for testing/demos)

---

## 🛡️ **Security Features Active**

✅ **Password Security:**
- PBKDF2 hashing with 100,000 iterations
- Unique salt for each password
- Secure password comparison using HMAC

✅ **Account Protection:**
- Account lockout after 3 failed attempts
- 15-minute lockout duration
- Session timeout after 1 hour of inactivity

✅ **Access Control:**
- Role-based permissions
- Page-level access restrictions
- Permission verification for each action

✅ **Audit & Monitoring:**
- Complete security event logging
- Real-time activity monitoring
- Audit trail stored in security_audit.jsonl

---

## 📋 **Password Policy**

All production passwords meet enterprise security standards:
- **Minimum 16 characters**
- **Mixed case letters** (A-Z, a-z)
- **Numbers** (0-9)
- **Special characters** (!@#$)
- **No dictionary words**
- **Unique per account**

---

## 🔄 **Password Rotation Schedule**

**Recommended:** Change passwords every 90 days

**Next rotation due:** May 8, 2024

---

## 🚨 **Security Incident Response**

**If credentials are compromised:**
1. **Immediately disable** affected account
2. **Change password** using secure method
3. **Review audit logs** for unauthorized access
4. **Notify security team**
5. **Update this document** with new credentials

---

## 📞 **Support Contacts**

**System Administrator:** admin@eyecare-analytics.com  
**Security Team:** security@eyecare-analytics.com  
**Technical Support:** support@eyecare-analytics.com

---

## 🔐 **Streamlit Cloud Secrets Configuration**

**For deployment, add these to Streamlit Cloud app settings:**

```toml
# Snowflake Database Connection
SNOWFLAKE_ACCOUNT = "hnxwaex-tm68686"
SNOWFLAKE_USER = "eyedwalker"
SNOWFLAKE_PASSWORD = "julyChrs24mle$"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "EYECARE_ANALYTICS"

# Security Configuration
SESSION_TIMEOUT = "3600"
MAX_FAILED_ATTEMPTS = "3"
LOCKOUT_DURATION = "900"
```

---

**🛡️ This document contains sensitive security information. Store securely and share only with authorized personnel.**
