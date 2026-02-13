"""
Analytics Pipeline Runner

This script executes the complete analytics pipeline:
1. Runs performance tests
2. Generates static dashboards
3. Provides instructions for the Streamlit dashboard
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, description):
    """Run a command and display its output"""
    print(f"\n{description}")
    print("-" * 50)
    
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            cwd=os.getcwd()
        )
        
        if result.returncode == 0:
            print("SUCCESS")
            if result.stdout.strip():
                print(result.stdout)
        else:
            print("FAILED")
            print("Error:", result.stderr)
            return False
            
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False
    
    return True

def main():
    print("Starting E-commerce Analytics Pipeline")
    print("=" * 60)
    
    # Ensure reports directory exists
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    # Run performance tests
    success1 = run_command(
        "python analytics/performance_test.py",
        "1. Running Performance Tests"
    )
    
    # Generate dashboards
    success2 = run_command(
        "python analytics/generate_dashboards.py", 
        "2. Generating Static Dashboards"
    )
    
    print("\nDashboard Generation Complete!")
    print("\nGenerated files in 'reports/' directory:")
    print("- sales_dashboard.html")
    print("- customer_dashboard.html")
    print("- cohort_dashboard.html")
    print("- performance_report.txt")
    
    print("\nTo run the interactive Streamlit dashboard:")
    print("   streamlit run analytics/streamlit_dashboard.py")
    
    print("\nTo view static dashboards, open the HTML files in your browser")
    
    if success1 and success2:
        print("\nAll analytics pipeline steps completed successfully!")
    else:
        print("\nSome steps failed. Check the output above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()