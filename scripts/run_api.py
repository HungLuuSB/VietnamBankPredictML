import sys
import uvicorn
from pathlib import Path
from loguru import logger
file_path = Path(__file__).resolve()
project_root = file_path.parent.parent
sys.path.append(str(project_root))

from src.utils.config_loader import config

def main():
    logger.info(f"[scripts/run_api] Starting API server...")
    uvicorn.run(
        "src.api_services.main:app",
        host="0.0.0.0",
        port=8080,
        reload=True
    )

if __name__ == "__main__":
    main()
