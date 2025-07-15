#!/usr/bin/env python3
"""
Test script to verify the specific fixed DAGs can be imported.
"""

import sys
import os
sys.path.insert(0, '/Users/kuldeep/astro-project/dags')

def test_fixed_dags():
    """Test the specific DAGs that were fixed."""
    fixed_dags = [
        'taskgroup_dag',
        'data_pipeline_etl_dag', 
        'production_patterns_dag',
        'kubernetes_docker_dag'
    ]
    
    print("Testing fixed DAGs...\n")
    
    all_success = True
    
    for dag_name in fixed_dags:
        try:
            module = __import__(dag_name)
            print(f"✅ {dag_name}.py - Import successful")
        except Exception as e:
            print(f"❌ {dag_name}.py - Import failed: {str(e)}")
            all_success = False
    
    print(f"\n{'='*50}")
    if all_success:
        print("🎉 All fixed DAGs imported successfully!")
    else:
        print("⚠️  Some DAGs still have issues")
    
    return all_success

if __name__ == "__main__":
    success = test_fixed_dags()
    sys.exit(0 if success else 1)