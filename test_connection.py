#!/usr/bin/env python3
"""
Simple Snowflake connection test to diagnose connection issues
"""

import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_snowflake_connection():
    """Test Snowflake connection with detailed error reporting"""
    
    print("🔍 Testing Snowflake Connection...")
    print("=" * 50)
    
    # Get connection parameters
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER') 
    password = os.getenv('SNOWFLAKE_PASSWORD')
    role = os.getenv('SNOWFLAKE_ROLE')
    
    print(f"Account: {account}")
    print(f"User: {user}")
    print(f"Role: {role}")
    print(f"Password: {'*' * len(password) if password else 'NOT SET'}")
    print("=" * 50)
    
    # Test different account formats
    account_formats = [
        account,  # Current format
        account.replace('.', '-'),  # Try with dash
        f"{account}.snowflakecomputing.com",  # Try with full domain
    ]
    
    for i, test_account in enumerate(account_formats, 1):
        print(f"\n🧪 Test {i}: Account format '{test_account}'")
        
        try:
            conn = snowflake.connector.connect(
                account=test_account,
                user=user,
                password=password,
                role=role,
                client_session_keep_alive=True
            )
            
            print(f"✅ SUCCESS! Connected with account format: {test_account}")
            
            # Test a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            print(f"✅ Snowflake Version: {version}")
            
            cursor.close()
            conn.close()
            
            return test_account  # Return working format
            
        except Exception as e:
            print(f"❌ Failed with account format '{test_account}': {str(e)}")
            continue
    
    print("\n❌ All connection attempts failed!")
    return None

if __name__ == "__main__":
    working_format = test_snowflake_connection()
    
    if working_format:
        print(f"\n🎉 Use this account format: {working_format}")
    else:
        print("\n🔧 Please check your Snowflake account details and try again.")
