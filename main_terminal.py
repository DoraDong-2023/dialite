# terminal_app.py
import os
import sys
import shutil
import time
import random
import pandas as pd
from main import (  # Import required functions from main module
    query_santos,
    integrate_alite,
    new_outer_join_integration_algorithm,
    ConvertTextToTable,
    QueryGPT3,
    find_string_cols
)
from main import app  # Reuse Flask app configuration

# Reuse path configurations from Flask app
QUERY_TABLE_FOLDER = app.config['query_table_folder']
INTEGRATION_SET_FOLDER = app.config['integration_set_folder']
RESULT_FOLDER = os.path.join(app.root_path, 'data', 'integration-result')
DATALAKE_FOLDER = os.path.join(app.root_path, 'data', 'dialite_datalake')

def clear_screen():
    """Clear terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header(title):
    """Print formatted header with title"""
    clear_screen()
    print("=" * 60)
    print(f"{title:^60}")
    print("=" * 60)
    print()

def list_query_tables():
    """List available query tables in the system"""
    return [f for f in os.listdir(QUERY_TABLE_FOLDER) 
           if f.endswith('.csv') and os.path.isfile(os.path.join(QUERY_TABLE_FOLDER, f))]

def generate_query_table():
    """Generate new query table using GPT"""
    print_header("Generate New Query Table with GPT")
    
    try:
        # Get user inputs
        prompt = input("Enter your table generation prompt: ")
        filename = input("Enter output filename (without extension): ") + ".csv"
        api_key = input("Enter your OpenAI API key: ")
        
        output_path = os.path.join(QUERY_TABLE_FOLDER, filename)
        
        # Check for existing files
        if os.path.exists(output_path):
            raise FileExistsError("File already exists!")
        
        # Reuse GPT query and conversion functions from main module
        table_text = QueryGPT3(prompt, api_key)
        df = ConvertTextToTable(table_text)
        
        if df.empty:
            raise ValueError("Failed to generate valid table")
        
        # Save generated table
        df.to_csv(output_path, index=False)
        print(f"\nSuccessfully generated table with {len(df)} rows")
        print(df.head())
        
    except Exception as e:
        print(f"\nError: {str(e)}")
    
    input("\nPress Enter to continue...")

def upload_query_table():
    """Handle CSV file uploads"""
    print_header("Upload Query Table")
    
    try:
        file_path = input("Enter full path to CSV file: ").strip()
        if not os.path.isfile(file_path):
            raise FileNotFoundError("File not found")
        
        # Validate and copy file
        filename = os.path.basename(file_path)
        dest_path = os.path.join(QUERY_TABLE_FOLDER, filename)
        
        if os.path.exists(dest_path):
            raise FileExistsError("File already exists in query folder")
        
        # Verify CSV validity
        pd.read_csv(file_path, nrows=1)
        shutil.copy(file_path, dest_path)
        print(f"\nSuccessfully uploaded {filename}")
        
    except Exception as e:
        print(f"\nError: {str(e)}")
    
    input("\nPress Enter to continue...")

def discover_tables_workflow():
    """Complete workflow for discovering integration tables"""
    print_header("Discover Integration Tables")
    
    try:
        # Show available query tables
        query_files = list_query_tables()
        if not query_files:
            raise ValueError("No query tables found")
        
        print("Available query tables:")
        for i, f in enumerate(query_files, 1):
            print(f"{i}. {f}")
        
        # Select query table
        choice = int(input("\nSelect query table (number): "))
        if choice < 1 or choice > len(query_files):
            raise ValueError("Invalid selection")
        
        selected_file = query_files[choice-1]
        query_path = os.path.join(QUERY_TABLE_FOLDER, selected_file)
        
        # Show data preview
        df = pd.read_csv(query_path, nrows=5)
        print("\nFirst 5 rows:")
        print(df)
        print("\nColumns:", df.columns.tolist())
        
        # Get discovery parameters
        intent_col = int(input("Enter index of intent column (0-based): "))
        k = int(input("Enter number of tables to discover: "))
        
        # Execute discovery process
        full_df = pd.read_csv(query_path)
        discovered_tables = query_santos(full_df, intent_col, k)
        
        # Create integration set
        integration_set_name = os.path.splitext(selected_file)[0]
        integration_path = os.path.join(INTEGRATION_SET_FOLDER, integration_set_name)
        os.makedirs(integration_path, exist_ok=True)
        
        # Copy discovered tables
        print("\nDiscovered tables:")
        for table in discovered_tables:
            src = os.path.join(DATALAKE_FOLDER, table[0])
            dest = os.path.join(integration_path, table[0])
            if os.path.exists(src):
                shutil.copy(src, dest)
                print(f"- {table[0]} (score: {table[1]:.2f})")
            else:
                print(f"- {table[0]} (not found in datalake)")
                
    except Exception as e:
        print(f"\nError: {str(e)}")
    
    input("\nPress Enter to continue...")

def integrate_tables_workflow():
    """Complete workflow for table integration"""
    print_header("Integrate Tables")
    
    try:
        # List available integration sets
        integration_sets = [d for d in os.listdir(INTEGRATION_SET_FOLDER) 
                          if os.path.isdir(os.path.join(INTEGRATION_SET_FOLDER, d))]
        
        if not integration_sets:
            raise ValueError("No integration sets found")
        
        print("Available integration sets:")
        for i, d in enumerate(integration_sets, 1):
            print(f"{i}. {d}")
        
        # Select integration set
        choice = int(input("\nSelect integration set (number): "))
        if choice < 1 or choice > len(integration_sets):
            raise ValueError("Invalid selection")
        
        selected_set = integration_sets[choice-1]
        set_path = os.path.join(INTEGRATION_SET_FOLDER, selected_set)
        
        # Show tables in set
        tables = [f for f in os.listdir(set_path) if f.endswith('.csv')]
        print("\nTables in set:")
        for t in tables:
            print(f"- {t}")
        
        # Select integration method
        print("\nIntegration methods:")
        print("1. ALITE (FD-based)")
        print("2. Outer Join")
        method = input("Select integration method (1/2): ")
        
        # Execute integration
        table_paths = [os.path.join(set_path, t) for t in tables]
        
        if method == '1':
            result = integrate_alite(table_paths)
        elif method == '2':
            result = new_outer_join_integration_algorithm(table_paths)
        else:
            raise ValueError("Invalid method selected")
        
        # Save integration result
        timestamp = int(time.time())
        output_file = f"result_{selected_set}_{timestamp}.csv"
        output_path = os.path.join(RESULT_FOLDER, output_file)
        result.to_csv(output_path, index=False)
        
        print(f"\nIntegration successful! Saved to: {output_path}")
        print("\nFirst 5 rows:")
        print(result.head())
        
    except Exception as e:
        print(f"\nError: {str(e)}")
    
    input("\nPress Enter to continue...")

def main_menu():
    """Main application menu"""
    while True:
        print_header("Data Integration Workbench")
        print("1. Generate new query table with GPT")
        print("2. Upload query table")
        print("3. Discover integration tables")
        print("4. Integrate tables")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ")
        
        try:
            if choice == '1':
                generate_query_table()
            elif choice == '2':
                upload_query_table()
            elif choice == '3':
                discover_tables_workflow()
            elif choice == '4':
                integrate_tables_workflow()
            elif choice == '5':
                print("\nExiting program...")
                sys.exit(0)
            else:
                print("Invalid choice! Please enter 1-5")
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
            time.sleep(1)

if __name__ == "__main__":
    # Ensure result directory exists
    os.makedirs(RESULT_FOLDER, exist_ok=True)
    main_menu()

