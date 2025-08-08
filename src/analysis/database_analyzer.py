#!/usr/bin/env python3
"""
Database Analyzer for Snowflake Cost Estimation
Analyzes SQL Server blink_dev1 database and provides precise Snowflake sizing recommendations
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
import json
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.ingestion.database_connector import DatabaseConnector

console = Console()
logger = logging.getLogger(__name__)

class DatabaseAnalyzer:
    """Analyze SQL Server database for Snowflake migration and cost estimation"""
    
    def __init__(self):
        """Initialize analyzer"""
        self.connector = DatabaseConnector()
        self.analysis_results = {}
        
    def analyze_database_structure(self) -> Dict[str, Any]:
        """Analyze complete database structure"""
        console.print("[bold blue]🔍 Analyzing SQL Server Database Structure...[/bold blue]")
        
        try:
            # Connect to source database
            self.connector.create_source_engine()
            
            # Get all tables
            tables = self.connector.get_source_tables()
            console.print(f"[green]✅ Found {len(tables)} tables in blink_dev1[/green]")
            
            structure_analysis = {
                'database_name': 'blink_dev1',
                'total_tables': len(tables),
                'tables': {},
                'summary_stats': {
                    'total_rows': 0,
                    'total_columns': 0,
                    'estimated_size_mb': 0,
                    'largest_table': None,
                    'most_columns': None
                },
                'eyecare_specific_tables': [],
                'business_domains': {
                    'patient_data': [],
                    'appointments': [],
                    'billing': [],
                    'inventory': [],
                    'staff': [],
                    'clinical': [],
                    'other': []
                }
            }
            
            # Analyze each table
            for i, table in enumerate(tables):
                try:
                    console.print(f"[yellow]Analyzing table {i+1}/{len(tables)}: {table}[/yellow]")
                    
                    table_info = self.connector.get_table_info(table)
                    structure_analysis['tables'][table] = table_info
                    
                    # Update summary stats
                    structure_analysis['summary_stats']['total_rows'] += table_info['row_count']
                    structure_analysis['summary_stats']['total_columns'] += table_info['column_count']
                    
                    # Estimate size (rough calculation: rows * columns * 10 bytes average)
                    estimated_mb = (table_info['row_count'] * table_info['column_count'] * 10) / (1024 * 1024)
                    structure_analysis['summary_stats']['estimated_size_mb'] += estimated_mb
                    
                    # Track largest table
                    if (structure_analysis['summary_stats']['largest_table'] is None or 
                        table_info['row_count'] > structure_analysis['tables'][structure_analysis['summary_stats']['largest_table']]['row_count']):
                        structure_analysis['summary_stats']['largest_table'] = table
                    
                    # Track table with most columns
                    if (structure_analysis['summary_stats']['most_columns'] is None or
                        table_info['column_count'] > structure_analysis['tables'][structure_analysis['summary_stats']['most_columns']]['column_count']):
                        structure_analysis['summary_stats']['most_columns'] = table
                    
                    # Categorize tables by business domain
                    self._categorize_table(table, structure_analysis['business_domains'])
                    
                    # Identify eyecare-specific tables
                    if self._is_eyecare_table(table):
                        structure_analysis['eyecare_specific_tables'].append(table)
                        
                except Exception as e:
                    console.print(f"[red]⚠️ Could not analyze table {table}: {e}[/red]")
                    continue
            
            self.analysis_results['structure'] = structure_analysis
            return structure_analysis
            
        except Exception as e:
            console.print(f"[red]❌ Database analysis failed: {e}[/red]")
            raise
    
    def _categorize_table(self, table_name: str, domains: Dict[str, List[str]]):
        """Categorize table by business domain"""
        table_lower = table_name.lower()
        
        if any(keyword in table_lower for keyword in ['patient', 'client', 'customer', 'person']):
            domains['patient_data'].append(table_name)
        elif any(keyword in table_lower for keyword in ['appointment', 'schedule', 'visit', 'exam']):
            domains['appointments'].append(table_name)
        elif any(keyword in table_lower for keyword in ['bill', 'payment', 'invoice', 'transaction', 'charge']):
            domains['billing'].append(table_name)
        elif any(keyword in table_lower for keyword in ['inventory', 'product', 'frame', 'lens', 'stock']):
            domains['inventory'].append(table_name)
        elif any(keyword in table_lower for keyword in ['employee', 'staff', 'user', 'doctor', 'provider']):
            domains['staff'].append(table_name)
        elif any(keyword in table_lower for keyword in ['exam', 'diagnosis', 'prescription', 'clinical', 'medical']):
            domains['clinical'].append(table_name)
        else:
            domains['other'].append(table_name)
    
    def _is_eyecare_table(self, table_name: str) -> bool:
        """Check if table is eyecare-specific"""
        eyecare_keywords = [
            'patient', 'exam', 'prescription', 'frame', 'lens', 'optometry', 
            'vision', 'eye', 'optical', 'refraction', 'diagnosis', 'appointment'
        ]
        return any(keyword in table_name.lower() for keyword in eyecare_keywords)
    
    def estimate_snowflake_costs(self, structure_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate Snowflake costs based on data analysis"""
        console.print("[bold blue]💰 Estimating Snowflake Costs...[/bold blue]")
        
        total_rows = structure_analysis['summary_stats']['total_rows']
        estimated_size_gb = structure_analysis['summary_stats']['estimated_size_mb'] / 1024
        
        # Snowflake cost estimation
        cost_analysis = {
            'data_metrics': {
                'total_rows': total_rows,
                'estimated_size_gb': round(estimated_size_gb, 2),
                'compressed_size_gb': round(estimated_size_gb * 0.3, 2),  # Snowflake compression ~70%
                'tables_count': structure_analysis['total_tables']
            },
            'storage_costs': {},
            'compute_costs': {},
            'total_monthly_estimate': {},
            'recommendations': []
        }
        
        # Storage costs ($40 per TB per month, but Snowflake has compression)
        compressed_tb = cost_analysis['data_metrics']['compressed_size_gb'] / 1024
        monthly_storage_cost = compressed_tb * 40
        cost_analysis['storage_costs'] = {
            'raw_size_tb': round(estimated_size_gb / 1024, 3),
            'compressed_size_tb': round(compressed_tb, 3),
            'monthly_cost_usd': round(monthly_storage_cost, 2)
        }
        
        # Compute costs based on data volume and usage patterns
        if total_rows < 100000:  # Small dev database
            warehouse_size = 'X-SMALL'
            hours_per_day = 4  # Development usage
            credits_per_hour = 1
            monthly_credits = hours_per_day * 30 * credits_per_hour
            cost_analysis['recommendations'].extend([
                "Perfect for development and testing",
                "X-Small warehouse is sufficient for this data volume",
                "Consider auto-suspend after 5 minutes"
            ])
        elif total_rows < 1000000:
            warehouse_size = 'SMALL'
            hours_per_day = 6
            credits_per_hour = 2
            monthly_credits = hours_per_day * 30 * credits_per_hour
            cost_analysis['recommendations'].extend([
                "Small warehouse recommended for analytics workloads",
                "Good for small practice analytics"
            ])
        elif total_rows < 10000000:
            warehouse_size = 'MEDIUM'
            hours_per_day = 8
            credits_per_hour = 4
            monthly_credits = hours_per_day * 30 * credits_per_hour
            cost_analysis['recommendations'].extend([
                "Medium warehouse for production analytics",
                "Suitable for medium-sized practice chains"
            ])
        else:
            warehouse_size = 'LARGE'
            hours_per_day = 12
            credits_per_hour = 8
            monthly_credits = hours_per_day * 30 * credits_per_hour
            cost_analysis['recommendations'].extend([
                "Large warehouse for high-volume analytics",
                "Enterprise-level processing capability"
            ])
        
        # Snowflake credit pricing (approximately $2-4 per credit depending on edition)
        credit_cost = 3  # Average cost per credit
        monthly_compute_cost = monthly_credits * credit_cost
        
        cost_analysis['compute_costs'] = {
            'recommended_warehouse': warehouse_size,
            'estimated_hours_per_day': hours_per_day,
            'credits_per_hour': credits_per_hour,
            'monthly_credits': monthly_credits,
            'cost_per_credit_usd': credit_cost,
            'monthly_cost_usd': round(monthly_compute_cost, 2)
        }
        
        # Total monthly estimate
        total_monthly = monthly_storage_cost + monthly_compute_cost
        cost_analysis['total_monthly_estimate'] = {
            'storage_usd': round(monthly_storage_cost, 2),
            'compute_usd': round(monthly_compute_cost, 2),
            'total_usd': round(total_monthly, 2)
        }
        
        # Additional recommendations
        if total_rows < 50000:
            cost_analysis['recommendations'].append("Consider using Snowflake's free trial credits for initial setup")
        
        if estimated_size_gb > 100:
            cost_analysis['recommendations'].append("Implement data retention policies to manage storage costs")
        
        cost_analysis['recommendations'].extend([
            "Enable auto-suspend to minimize compute costs",
            "Use result caching to reduce query costs",
            "Consider time-travel retention settings based on compliance needs"
        ])
        
        self.analysis_results['costs'] = cost_analysis
        return cost_analysis
    
    def generate_migration_plan(self) -> Dict[str, Any]:
        """Generate step-by-step migration plan"""
        console.print("[bold blue]📋 Generating Migration Plan...[/bold blue]")
        
        structure = self.analysis_results.get('structure', {})
        
        migration_plan = {
            'phases': [
                {
                    'phase': 1,
                    'name': 'Infrastructure Setup',
                    'duration_days': 1,
                    'tasks': [
                        'Initialize Snowflake environment',
                        'Create databases, schemas, and roles',
                        'Set up compute warehouses',
                        'Configure security and permissions'
                    ]
                },
                {
                    'phase': 2,
                    'name': 'Data Ingestion - Core Tables',
                    'duration_days': 2,
                    'tasks': [
                        'Migrate patient and customer data',
                        'Transfer appointment and scheduling data',
                        'Load billing and transaction data',
                        'Validate data quality and integrity'
                    ]
                },
                {
                    'phase': 3,
                    'name': 'Data Modeling & Analytics',
                    'duration_days': 3,
                    'tasks': [
                        'Create dimensional models',
                        'Build business logic views',
                        'Implement KPI calculations',
                        'Set up data quality monitoring'
                    ]
                },
                {
                    'phase': 4,
                    'name': 'Analytics Platform',
                    'duration_days': 2,
                    'tasks': [
                        'Deploy Streamlit dashboards',
                        'Integrate Snowflake Cortex AI',
                        'Create automated reports',
                        'User training and documentation'
                    ]
                }
            ],
            'total_duration_days': 8,
            'priority_tables': structure.get('eyecare_specific_tables', [])[:10],
            'estimated_effort': 'Small development project suitable for rapid deployment'
        }
        
        self.analysis_results['migration_plan'] = migration_plan
        return migration_plan
    
    def display_analysis_results(self):
        """Display comprehensive analysis results"""
        console.print("\n")
        
        if 'structure' in self.analysis_results:
            self._display_structure_analysis()
        
        if 'costs' in self.analysis_results:
            self._display_cost_analysis()
        
        if 'migration_plan' in self.analysis_results:
            self._display_migration_plan()
    
    def _display_structure_analysis(self):
        """Display database structure analysis"""
        structure = self.analysis_results['structure']
        stats = structure['summary_stats']
        
        # Database overview table
        table = Table(title="🗄️ Database Structure Analysis")
        table.add_column("Metric", style="bold blue")
        table.add_column("Value", style="bold green")
        
        table.add_row("Database Name", structure['database_name'])
        table.add_row("Total Tables", str(structure['total_tables']))
        table.add_row("Total Rows", f"{stats['total_rows']:,}")
        table.add_row("Total Columns", f"{stats['total_columns']:,}")
        table.add_row("Estimated Size", f"{stats['estimated_size_mb']:.1f} MB")
        table.add_row("Largest Table", stats['largest_table'] or 'N/A')
        table.add_row("Eyecare Tables", str(len(structure['eyecare_specific_tables'])))
        
        console.print(table)
        
        # Business domains table
        domains_table = Table(title="📊 Business Domain Analysis")
        domains_table.add_column("Domain", style="bold blue")
        domains_table.add_column("Tables Count", style="bold green")
        domains_table.add_column("Examples")
        
        for domain, tables in structure['business_domains'].items():
            if tables:
                examples = ', '.join(tables[:3])
                if len(tables) > 3:
                    examples += f" (and {len(tables)-3} more)"
                domains_table.add_row(
                    domain.replace('_', ' ').title(),
                    str(len(tables)),
                    examples
                )
        
        console.print(domains_table)
    
    def _display_cost_analysis(self):
        """Display cost analysis"""
        costs = self.analysis_results['costs']
        
        # Cost breakdown table
        cost_table = Table(title="💰 Snowflake Cost Estimation")
        cost_table.add_column("Component", style="bold blue")
        cost_table.add_column("Details", style="bold yellow")
        cost_table.add_column("Monthly Cost", style="bold green")
        
        cost_table.add_row(
            "Storage",
            f"{costs['storage_costs']['compressed_size_tb']:.3f} TB (compressed)",
            f"${costs['storage_costs']['monthly_cost_usd']:.2f}"
        )
        cost_table.add_row(
            "Compute",
            f"{costs['compute_costs']['recommended_warehouse']} warehouse",
            f"${costs['compute_costs']['monthly_cost_usd']:.2f}"
        )
        cost_table.add_row(
            "TOTAL MONTHLY",
            "Storage + Compute",
            f"${costs['total_monthly_estimate']['total_usd']:.2f}"
        )
        
        console.print(cost_table)
        
        # Recommendations panel
        recommendations_text = "\n".join([f"• {rec}" for rec in costs['recommendations']])
        recommendations_panel = Panel(
            recommendations_text,
            title="💡 Cost Optimization Recommendations",
            border_style="green"
        )
        console.print(recommendations_panel)
    
    def _display_migration_plan(self):
        """Display migration plan"""
        plan = self.analysis_results['migration_plan']
        
        # Migration phases table
        phases_table = Table(title="🚀 Migration Plan")
        phases_table.add_column("Phase", style="bold blue")
        phases_table.add_column("Name", style="bold yellow")
        phases_table.add_column("Duration", style="bold green")
        phases_table.add_column("Key Tasks")
        
        for phase in plan['phases']:
            tasks = '\n'.join([f"• {task}" for task in phase['tasks'][:3]])
            phases_table.add_row(
                str(phase['phase']),
                phase['name'],
                f"{phase['duration_days']} days",
                tasks
            )
        
        console.print(phases_table)
        
        # Summary panel
        summary_panel = Panel(
            f"[bold]Total Duration:[/bold] {plan['total_duration_days']} days\n"
            f"[bold]Effort Level:[/bold] {plan['estimated_effort']}\n"
            f"[bold]Priority Tables:[/bold] {len(plan['priority_tables'])} eyecare-specific tables identified",
            title="📋 Project Summary",
            border_style="blue"
        )
        console.print(summary_panel)
    
    def save_analysis_report(self, filename: Optional[str] = None):
        """Save analysis results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"database_analysis_{timestamp}.json"
        
        report_path = project_root / "docs" / "analysis" / filename
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w') as f:
            json.dump(self.analysis_results, f, indent=2, default=str)
        
        console.print(f"[green]✅ Analysis report saved to: {report_path}[/green]")
    
    def run_complete_analysis(self) -> Dict[str, Any]:
        """Run complete database analysis"""
        console.print(Panel(
            "[bold blue]🏥 Blink Dev Database Analysis for Snowflake Migration[/bold blue]",
            border_style="blue"
        ))
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            
            # Step 1: Analyze structure
            task1 = progress.add_task("Analyzing database structure...", total=None)
            structure_analysis = self.analyze_database_structure()
            progress.update(task1, description="✅ Database structure analyzed")
            
            # Step 2: Estimate costs
            task2 = progress.add_task("Estimating Snowflake costs...", total=None)
            cost_analysis = self.estimate_snowflake_costs(structure_analysis)
            progress.update(task2, description="✅ Cost estimation complete")
            
            # Step 3: Generate migration plan
            task3 = progress.add_task("Generating migration plan...", total=None)
            migration_plan = self.generate_migration_plan()
            progress.update(task3, description="✅ Migration plan generated")
        
        # Display results
        self.display_analysis_results()
        
        # Save report
        self.save_analysis_report()
        
        return self.analysis_results

def main():
    """Main execution function"""
    try:
        analyzer = DatabaseAnalyzer()
        results = analyzer.run_complete_analysis()
        
        console.print("\n[bold green]🎉 Database analysis complete![/bold green]")
        console.print(f"[yellow]💰 Estimated monthly Snowflake cost: ${results['costs']['total_monthly_estimate']['total_usd']:.2f}[/yellow]")
        
    except Exception as e:
        console.print(f"\n[red]❌ Analysis failed: {e}[/red]")
        return False
    
    return True

if __name__ == "__main__":
    main()
