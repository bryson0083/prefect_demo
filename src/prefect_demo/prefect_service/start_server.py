#!/usr/bin/env python3
"""
Prefect 伺服器啟動腳本
提供基本驗證功能和Web UI
"""

import os
import asyncio
import subprocess
import sys
from pathlib import Path
from prefect.settings import PREFECT_HOME, PREFECT_SERVER_API_HOST, PREFECT_SERVER_API_PORT

# 引用自建公用模組 - 載入時會自動初始化環境設定
import sys
from pathlib import Path

# 將 src 目錄添加到 Python 路徑
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from prefect_demo.proj_util_pkg.settings import settings


def setup_prefect_environment():
    """設置Prefect環境變量"""
    # 環境設定已在import時載入完成
    
    # 設定Prefect home目錄
    prefect_home = Path(os.getcwd()) / "data"
    prefect_home.mkdir(exist_ok=True)
    
    # 設定環境變數
    os.environ["PREFECT_HOME"] = str(prefect_home)
    
    # 從 PREFECT_API_URL 中解析 host 和 port
    api_url = os.environ.get("PREFECT_API_URL", "http://127.0.0.1:4200/api")
    import urllib.parse
    parsed_url = urllib.parse.urlparse(api_url)
    api_host = parsed_url.hostname or "127.0.0.1"
    api_port = str(parsed_url.port) if parsed_url.port else "4200"
    
    # 設定環境變數以供 Prefect 伺服器使用
    os.environ["PREFECT_SERVER_API_HOST"] = api_host
    os.environ["PREFECT_SERVER_API_PORT"] = api_port
    
    print(f"✅ Prefect環境設定完成")
    print(f"📁 Prefect Home: {prefect_home}")
    print(f"🌐 API URL: {api_url}")
    print(f"🌐 API Host: {api_host}:{api_port}")

def start_prefect_server():
    """啟動Prefect伺服器"""
    try:
        print("🚀 正在啟動Prefect伺服器...")
        
        # 從環境變數獲取 host 和 port
        api_host = os.environ.get("PREFECT_SERVER_API_HOST", "127.0.0.1")
        api_port = os.environ.get("PREFECT_SERVER_API_PORT", "4200")
        
        print(f"📊 Web UI將在 http://{api_host}:{api_port} 提供服務")
        print("⚡ 按 Ctrl+C 停止伺服器")
        print("-" * 50)
        
        # 啟動Prefect伺服器
        subprocess.run([
            sys.executable, "-m", "prefect", "server", "start",
            "--host", api_host,
            "--port", api_port
        ], check=True)
        
    except KeyboardInterrupt:
        print("\n⏹️  正在停止Prefect伺服器...")
    except subprocess.CalledProcessError as e:
        print(f"❌ 啟動Prefect伺服器時發生錯誤: {e}")
        return False
    
    return True

def main():
    """主函數"""
    print("=" * 60)
    print("🎯 Prefect Demo - Prefect 服務啟動器")
    print("=" * 60)
    
    # 設置環境
    setup_prefect_environment()
    
    # 啟動伺服器
    success = start_prefect_server()
    
    if success:
        print("✅ Prefect伺服器已成功啟動")
    else:
        print("❌ Prefect伺服器啟動失敗")
        sys.exit(1)

if __name__ == "__main__":
    main() 