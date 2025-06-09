# -*- coding: utf-8 -*-
"""
ANA001 - 台股選股相關的 Prefect Flows
包含各種技術指標和籌碼面的選股流程
"""

import os
from datetime import datetime
from pathlib import Path
import json
import sys
from dotenv import load_dotenv

# 導入自建公用模組 - 載入時會自動初始化環境設定
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from prefect_demo.proj_util_pkg.settings import settings

# 導入 Prefect 相關模組
from prefect import flow, task
from prefect.logging import get_run_logger


@flow(name="do_flow01", log_prints=True)
def do_flow01():
    """ do_flow01流程 """
    logger = get_run_logger()
    logger.info("🚀 開始執行 flow01 流程")
    logger.info(f"[Flow執行]PROJECT_ROOT: {os.environ.get('PROJECT_ROOT')}")
    
    try:
        logger.info("🎉 Do flow01 流程完成！")
        return {"status": "success"}
        
    except Exception as e:
        logger.error(f"❌ flow01流程發生錯誤: {str(e)}")
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    # 直接執行此檔案時，執行 flow01 流程
    result = do_flow01()
    print(f"Flow execution result: {result}")
    