#!/usr/bin/env python3
"""
Database Connection Manager for Eyecare Analytics Platform
Handles connections to source databases and Snowflake
"""

import os
import logging
from typing import Dict, List, Optional, Any, Union
from contextlib import contextmanager
from dataclasses import dataclass
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

@dataclass
class ConnectionConfig:
    """Database connection configuration"""
    db_type: str
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: Optional[str] = None
    additional_params: Optional[Dict[str, Any]] = None

class DatabaseConnector:
    """Manages database connections for source systems and Snowflake"""
    
    def __init__(self):
        """Initialize connection manager"""
        self.source_engine: Optional[Engine] = None
        self.snowflake_conn: Optional[snowflake.connector.SnowflakeConnection] = None
        self._setup_logging()
        
    def _setup_logging(self):
        """Setup logging configuration"""
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def create_source_engine(self, config: Optional[ConnectionConfig] = None) -> Engine:
        """Create SQLAlchemy engine for source database"""
        if config is None:
            config = self._get_source_config_from_env()
        
        try:
            connection_string = self._build_connection_string(config)
            
            engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=int(os.getenv('SOURCE_DB_POOL_SIZE', 5)),
                max_overflow=int(os.getenv('SOURCE_DB_MAX_OVERFLOW', 10)),
                pool_timeout=int(os.getenv('SOURCE_DB_POOL_TIMEOUT', 30)),
                pool_pre_ping=True,  # Validate connections before use
                echo=os.getenv('ENABLE_DEBUG_MODE', 'false').lower() == 'true'
            )
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.source_engine = engine
            logger.info(f"✅ Connected to {config.db_type} database at {config.host}")
            return engine
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to source database: {e}")
            raise
    
    def _get_source_config_from_env(self) -> ConnectionConfig:
        """Get source database configuration from environment variables"""
        return ConnectionConfig(
            db_type=os.getenv('SOURCE_DB_TYPE', 'postgresql'),
            host=os.getenv('SOURCE_DB_HOST', 'localhost'),
            port=int(os.getenv('SOURCE_DB_PORT', 5432)),
            database=os.getenv('SOURCE_DB_NAME', 'eyecare_db'),
            username=os.getenv('SOURCE_DB_USER'),
            password=os.getenv('SOURCE_DB_PASSWORD'),
            schema=os.getenv('SOURCE_DB_SCHEMA', 'public')
        )
    
    def _build_connection_string(self, config: ConnectionConfig) -> str:
        """Build database connection string based on database type"""
        base_params = f"{config.username}:{config.password}@{config.host}:{config.port}/{config.database}"
        
        connection_strings = {
            'postgresql': f"postgresql+psycopg2://{base_params}",
            'mysql': f"mysql+pymysql://{base_params}",
            'sqlserver': f"mssql+pymssql://{base_params}?charset=utf8",  # Use pymssql for jTDS compatibility
            'oracle': f"oracle+cx_oracle://{base_params}",
            'sqlite': f"sqlite:///{config.database}"
        }
        
        if config.db_type not in connection_strings:
            raise ValueError(f"Unsupported database type: {config.db_type}")
        
        return connection_strings[config.db_type]
    
    def create_snowflake_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Create Snowflake connection"""
        try:
            connection_params = {
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': os.getenv('SNOWFLAKE_PASSWORD'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': os.getenv('SNOWFLAKE_DATABASE'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
                'client_session_keep_alive': True,
                'client_session_keep_alive_heartbeat_frequency': 60,
                'numpy': True
            }
            
            # Remove None values
            connection_params = {k: v for k, v in connection_params.items() if v is not None}
            
            conn = snowflake.connector.connect(**connection_params)
            
            # Test connection
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()
            
            self.snowflake_conn = conn
            logger.info(f"✅ Connected to Snowflake version: {version}")
            return conn
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    @contextmanager
    def source_connection(self):
        """Context manager for source database connections"""
        if self.source_engine is None:
            self.create_source_engine()
        
        conn = self.source_engine.connect()
        try:
            yield conn
        finally:
            conn.close()
    
    @contextmanager
    def snowflake_connection(self):
        """Context manager for Snowflake connections"""
        if self.snowflake_conn is None:
            self.create_snowflake_connection()
        
        try:
            yield self.snowflake_conn
        finally:
            # Don't close the connection, let it be reused
            pass
    
    def get_source_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of tables from source database"""
        try:
            with self.source_connection() as conn:
                metadata = MetaData()
                schema_name = schema or os.getenv('SOURCE_DB_SCHEMA', 'public')
                metadata.reflect(bind=conn, schema=schema_name)
                
                tables = list(metadata.tables.keys())
                logger.info(f"Found {len(tables)} tables in schema '{schema_name}'")
                return tables
                
        except Exception as e:
            logger.error(f"❌ Failed to get source tables: {e}")
            raise
    
    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed information about a table"""
        try:
            with self.source_connection() as conn:
                schema_name = schema or os.getenv('SOURCE_DB_SCHEMA', 'public')
                
                # Get row count
                count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
                row_count = pd.read_sql(count_query, conn).iloc[0, 0]
                
                # Get column information
                metadata = MetaData()
                metadata.reflect(bind=conn, schema=schema_name)
                table = metadata.tables[f"{schema_name}.{table_name}"]
                
                columns = []
                for column in table.columns:
                    columns.append({
                        'name': column.name,
                        'type': str(column.type),
                        'nullable': column.nullable,
                        'primary_key': column.primary_key
                    })
                
                return {
                    'table_name': table_name,
                    'schema': schema_name,
                    'row_count': row_count,
                    'column_count': len(columns),
                    'columns': columns
                }
                
        except Exception as e:
            logger.error(f"❌ Failed to get table info for {table_name}: {e}")
            raise
    
    def extract_data(self, 
                    table_name: str, 
                    schema: Optional[str] = None,
                    limit: Optional[int] = None,
                    where_clause: Optional[str] = None,
                    columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Extract data from source table"""
        try:
            with self.source_connection() as conn:
                schema_name = schema or os.getenv('SOURCE_DB_SCHEMA', 'public')
                
                # Build query
                column_list = ', '.join(columns) if columns else '*'
                query = f"SELECT {column_list} FROM {schema_name}.{table_name}"
                
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                if limit:
                    # Add limit based on database type
                    db_type = os.getenv('SOURCE_DB_TYPE', 'postgresql')
                    if db_type in ['postgresql', 'mysql', 'sqlite']:
                        query += f" LIMIT {limit}"
                    elif db_type == 'sqlserver':
                        query = query.replace('SELECT', f'SELECT TOP {limit}')
                    elif db_type == 'oracle':
                        query += f" AND ROWNUM <= {limit}"
                
                logger.info(f"Extracting from {table_name} with query: {query}")
                
                start_time = time.time()
                df = pd.read_sql(query, conn)
                execution_time = time.time() - start_time
                
                logger.info(f"✅ Extracted {len(df)} rows from {table_name} in {execution_time:.2f}s")
                return df
                
        except Exception as e:
            logger.error(f"❌ Failed to extract data from {table_name}: {e}")
            raise
    
    def load_to_snowflake(self, 
                         df: pd.DataFrame, 
                         table_name: str,
                         schema: str = 'RAW',
                         if_exists: str = 'replace',
                         chunk_size: int = 10000) -> bool:
        """Load DataFrame to Snowflake table"""
        try:
            with self.snowflake_connection() as conn:
                database = os.getenv('SNOWFLAKE_DATABASE')
                full_table_name = f"{database}.{schema}.{table_name}"
                
                logger.info(f"Loading {len(df)} rows to {full_table_name}")
                start_time = time.time()
                
                # Use Snowflake's pandas integration
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=table_name,
                    database=database,
                    schema=schema,
                    chunk_size=chunk_size,
                    compression='gzip',
                    on_error='continue',
                    overwrite=(if_exists == 'replace'),
                    auto_create_table=True
                )
                
                execution_time = time.time() - start_time
                
                if success:
                    logger.info(f"✅ Loaded {nrows} rows to {full_table_name} in {execution_time:.2f}s")
                    return True
                else:
                    logger.error(f"❌ Failed to load data to {full_table_name}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Failed to load data to Snowflake: {e}")
            raise
    
    def execute_snowflake_query(self, query: str) -> pd.DataFrame:
        """Execute query on Snowflake and return results"""
        try:
            with self.snowflake_connection() as conn:
                cursor = conn.cursor()
                start_time = time.time()
                
                cursor.execute(query)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                execution_time = time.time() - start_time
                
                df = pd.DataFrame(results, columns=columns)
                logger.info(f"✅ Executed query, returned {len(df)} rows in {execution_time:.2f}s")
                
                return df
                
        except Exception as e:
            logger.error(f"❌ Failed to execute Snowflake query: {e}")
            raise
    
    def analyze_source_database(self) -> Dict[str, Any]:
        """Analyze source database and provide recommendations"""
        logger.info("🔍 Analyzing source database...")
        
        try:
            tables = self.get_source_tables()
            
            analysis = {
                'total_tables': len(tables),
                'tables': {},
                'estimated_storage_gb': 0,
                'total_rows': 0,
                'recommendations': []
            }
            
            for table in tables[:20]:  # Analyze first 20 tables
                try:
                    table_info = self.get_table_info(table)
                    analysis['tables'][table] = table_info
                    analysis['total_rows'] += table_info['row_count']
                    
                    # Estimate storage (rough calculation)
                    estimated_mb = table_info['row_count'] * table_info['column_count'] * 0.1  # Very rough estimate
                    analysis['estimated_storage_gb'] += estimated_mb / 1024
                    
                except Exception as e:
                    logger.warning(f"⚠️ Could not analyze table {table}: {e}")
            
            # Generate recommendations
            if analysis['total_rows'] < 1000000:
                analysis['recommendations'].append("Small dataset - X-Small Snowflake warehouse recommended")
                analysis['estimated_monthly_cost'] = "$100-300"
            elif analysis['total_rows'] < 10000000:
                analysis['recommendations'].append("Medium dataset - Small Snowflake warehouse recommended")
                analysis['estimated_monthly_cost'] = "$300-800"
            else:
                analysis['recommendations'].append("Large dataset - Medium or Large Snowflake warehouse recommended")
                analysis['estimated_monthly_cost'] = "$800-2000"
            
            if analysis['estimated_storage_gb'] > 100:
                analysis['recommendations'].append("Consider data archiving strategy for older records")
            
            logger.info(f"✅ Analysis complete: {analysis['total_rows']} total rows across {len(tables)} tables")
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Failed to analyze source database: {e}")
            raise
    
    def close_connections(self):
        """Close all database connections"""
        if self.source_engine:
            self.source_engine.dispose()
            logger.info("✅ Source database connection closed")
        
        if self.snowflake_conn:
            self.snowflake_conn.close()
            logger.info("✅ Snowflake connection closed")

# Utility functions
def test_connections():
    """Test all database connections"""
    connector = DatabaseConnector()
    
    try:
        # Test source connection
        connector.create_source_engine()
        print("✅ Source database connection successful")
        
        # Test Snowflake connection
        connector.create_snowflake_connection()
        print("✅ Snowflake connection successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False
    finally:
        connector.close_connections()

if __name__ == "__main__":
    # Run connection tests
    test_connections()
