# verify_setup.py
import importlib
import sys

def check_dependencies():
    required_packages = [
        "google.cloud.aiplatform",
        "google.cloud.storage",
        "flask",
        "PyPDF2",
        "pydantic",
        "prometheus_client",
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            importlib.import_module(package.split(".")[0])
            print(f"✅ {package} successfully imported")
        except ImportError as e:
            missing_packages.append(package)
            print(f"❌ Failed to import {package}: {str(e)}")
    
    if missing_packages:
        print("\n❌ Some required packages are missing. Please install them using:")
        print("pip install -r requirements.txt")
        sys.exit(1)
    else:
        print("\n✅ All required packages are installed!")

if __name__ == "__main__":
    check_dependencies()