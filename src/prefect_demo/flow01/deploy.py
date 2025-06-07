"""
ANA001 - 台股選股相關的 Prefect Flows
包含各種技術指標和籌碼面的選股流程
"""

from datetime import datetime
from pathlib import Path
from prefect import flow, task
from prefect.logging import get_run_logger
import json
import sys

# 設定目錄路徑
CURRENT_DIR = Path(__file__).parent
PROJECT_ROOT = CURRENT_DIR.parent

# 添加專案根目錄到 Python 路徑
sys.path.append(str(PROJECT_ROOT))


@flow(name="do_flow01", log_prints=True)
def do_flow01():
    """ do_flow01流程 """

    logger = get_run_logger()
    logger.info("🚀 開始執行 flow01 流程")
    
    try:
        logger.info("🎉 Do flow01 流程完成！")
        return {"status": "success"}
        
    except Exception as e:
        logger.error(f"❌ flow01流程發生錯誤: {str(e)}")
        return {"status": "failed", "error": str(e)}

