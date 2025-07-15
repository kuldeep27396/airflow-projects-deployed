#!/usr/bin/env python3
"""
Test script to verify all DAGs can be imported without errors.
"""

import os
import sys
import importlib.util

def test_dag_imports():
    """Test importing all DAG files."""
    dags_dir = '/Users/kuldeep/astro-project/dags'
    
    if not os.path.exists(dags_dir):
        print(f"❌ DAGs directory not found: {dags_dir}")
        return False
    
    # Add dags directory to Python path
    sys.path.insert(0, dags_dir)
    
    success_count = 0
    error_count = 0
    
    print("Testing DAG imports...\n")
    
    # Get all Python files in dags directory
    for filename in sorted(os.listdir(dags_dir)):
        if filename.endswith('.py') and not filename.startswith('__'):
            module_name = filename[:-3]  # Remove .py extension
            file_path = os.path.join(dags_dir, filename)
            
            try:
                # Import the module
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                print(f"✅ {filename} - Import successful")
                success_count += 1
                
            except Exception as e:
                print(f"❌ {filename} - Import failed: {str(e)}")
                error_count += 1
    
    print(f"\n=== Results ===")
    print(f"✅ Successful imports: {success_count}")
    print(f"❌ Failed imports: {error_count}")
    print(f"Total DAGs tested: {success_count + error_count}")
    
    if error_count == 0:
        print("\n🎉 All DAGs imported successfully!")
        return True
    else:
        print(f"\n⚠️  {error_count} DAGs have import errors")
        return False

if __name__ == "__main__":
    success = test_dag_imports()
    sys.exit(0 if success else 1)