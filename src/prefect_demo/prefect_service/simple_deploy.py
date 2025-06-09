#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
簡化的 Prefect 部署腳本
直接執行必要的部署命令
"""

import os
import sys
import subprocess
from pathlib import Path

# 引用自建公用模組 - 載入時會自動初始化環境設定
import sys
from pathlib import Path

# 將 src 目錄添加到 Python 路徑
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from prefect_demo.proj_util_pkg.settings import settings


def run_command(command: list, description: str) -> bool:
    """執行命令"""
    
    try:
        print(f"🔄 {description}...")
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',  # 明確指定 UTF-8 編碼
            errors='replace',  # 遇到無法解碼的字符時替換為 ? 而不是拋出異常
            cwd=os.getcwd()
        )
        print(f"✅ {description}成功")
        if result.stdout.strip():
            print(f"📄 輸出:\n{result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description}失敗")
        if e.stderr:
            print(f"📄 錯誤: {e.stderr.strip()}")
        if e.stdout:
            print(f"📄 輸出: {e.stdout.strip()}")
        return False


def main():
    """主函數"""

    print("=" * 60)
    print("🎯 Prefect Demo - 簡化部署器")
    print("=" * 60)
    
    # 設定編碼環境變數，解決 Windows 中文編碼問題
    os.environ["PYTHONIOENCODING"] = "utf-8"
    os.environ["PYTHONUTF8"] = "1"
    
    # 設定環境變數
    api_url = os.environ.get("PREFECT_API_URL", "http://127.0.0.1:4200/api")
    
    # 從 API URL 中解析 host 和 port 用於顯示
    import urllib.parse
    parsed_url = urllib.parse.urlparse(api_url)
    api_host = parsed_url.hostname or "127.0.0.1"
    api_port = str(parsed_url.port) if parsed_url.port else "4200"
    
    # 設定 Prefect API URL
    os.environ["PREFECT_API_URL"] = api_url
    print(f"🌐 API URL: {api_url}")
    print(f"📁 PROJECT_ROOT: {os.environ['PROJECT_ROOT']}")

    # 切換到 src 目錄（這是 prefect.yaml 中設定的工作目錄）
    src_dir = os.environ['PROJECT_ROOT']
    prefect_yaml_path = os.path.join(src_dir, "prefect_service", "prefect.yaml")

    original_cwd = os.getcwd()
    
    try:
        os.chdir(src_dir)
        print(f"📁 工作目錄: {src_dir}")
        print(f"📄 Prefect YAML: {prefect_yaml_path}")
        
        # 1. 建立 work pool
        print("\n📝 步驟 1: 建立 Work Pool")
        print("-" * 40)
        run_command(
            [sys.executable, "-m", "prefect", "work-pool", "create", "default", "--type", "process"],
            "建立 work pool 'default'"
        )
        
        # 2. 部署 flows
        print("\n📝 步驟 2: 部署 Flows")
        print("-" * 40)
        success = run_command(
            [sys.executable, "-m", "prefect", "deploy", "--all", "--prefect-file", str(prefect_yaml_path)],
            "部署所有 flows"
        )
        
        if success:
            # 3. 列出部署
            print("\n📝 步驟 3: 檢查部署結果")
            print("-" * 40)
            run_command(
                [sys.executable, "-m", "prefect", "deployment", "ls"],
                "列出所有部署"
            )
            
            # 4. 啟動 worker (背景)
            print("\n📝 步驟 4: 啟動 Worker")
            print("-" * 40)
            try:
                log_file = Path("worker.log")
                with open(log_file, "w") as f:
                    subprocess.Popen(
                        [sys.executable, "-m", "prefect", "worker", "start", "--pool", "default"],
                        stdout=f,
                        stderr=subprocess.STDOUT,
                        start_new_session=True
                    )
                print(f"✅ Worker 已在背景啟動")
                print(f"📄 日誌文件: {log_file.absolute()}")
            except Exception as e:
                print(f"❌ 啟動 worker 失敗: {e}")
            
            print("\n" + "=" * 60)
            print("🎉 部署完成！")
            print("=" * 60)
            print("📊 接下來您可以:")
            print(f"   - 訪問 Web UI: http://{api_host}:{api_port}")
            print("   - 手動觸發 flow 執行")
            print("   - 監控排程執行情況")
            print("=" * 60)
        else:
            print("❌ 部署失敗")
            return False
            
    finally:
        os.chdir(original_cwd)
    
    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1) 