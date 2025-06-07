#!/usr/bin/env python3
"""
Prefect 部署管理腳本
自動建立 work pool、worker 並部署所有 flows
"""

import os
import sys
import subprocess
import asyncio
import yaml
import time
from pathlib import Path
from prefect.client.orchestration import PrefectClient
from prefect.exceptions import ObjectNotFound

# 引用自建公用模組 - 載入時會自動初始化環境設定
import sys
from pathlib import Path

# 將 src 目錄添加到 Python 路徑
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from prefect_demo.proj_util_pkg.settings import settings


class PrefectDeployManager:
    """Prefect 部署管理器"""
    
    def __init__(self):
        self.work_pool_name = "default"
        self.work_pool_type = "process"
        self.prefect_yaml_path = Path(__file__).parent / "prefect.yaml"
        self.api_host = os.environ.get("PREFECT_SERVER_API_HOST", "127.0.0.1")
        self.api_port = os.environ.get("PREFECT_SERVER_API_PORT", "4200")
        self.api_url = f"http://{self.api_host}:{self.api_port}/api"
        
    def run_prefect_command(self, command: list, description: str) -> bool:
        """執行 Prefect CLI 命令"""
        try:
            print(f"🔄 {description}...")
            result = subprocess.run(
                [sys.executable, "-m", "prefect"] + command,
                check=True,
                capture_output=True,
                text=True
            )
            print(f"✅ {description}成功")
            if result.stdout.strip():
                print(f"📄 輸出: {result.stdout.strip()}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ {description}失敗")
            if e.stderr:
                print(f"📄 錯誤詳情: {e.stderr.strip()}")
            return False
    
    async def check_server_status(self) -> bool:
        """檢查 Prefect 服務器是否運行"""
        try:
            # 設定 Prefect API URL
            os.environ["PREFECT_API_URL"] = self.api_url
            
            async with PrefectClient() as client:
                # 嘗試讀取工作池列表來測試連接
                await client.read_work_pools()
                return True
        except Exception as e:
            print(f"⚠️ 無法連接到 Prefect 服務器，請確認服務器已啟動")
            return False
    
    async def check_work_pool_exists(self) -> bool:
        """檢查 work pool 是否已存在"""
        try:
            # 設定 Prefect API URL
            os.environ["PREFECT_API_URL"] = self.api_url
            
            async with PrefectClient() as client:
                work_pools = await client.read_work_pools()
                return any(wp.name == self.work_pool_name for wp in work_pools)
        except Exception as e:
            print(f"⚠️ 檢查 work pool 時發生錯誤: {e}")
            return False
    
    def create_work_pool(self) -> bool:
        """建立 work pool"""
        return self.run_prefect_command(
            ["work-pool", "create", self.work_pool_name, "--type", self.work_pool_type],
            f"建立 work pool '{self.work_pool_name}'"
        )
    
    def start_worker_background(self) -> bool:
        """在背景啟動 worker"""
        try:
            print(f"🔄 在背景啟動 worker...")
            
            # 使用 nohup 在背景執行 worker
            cmd = [
                "nohup", 
                sys.executable, "-m", "prefect", "worker", "start", 
                "--pool", self.work_pool_name, 
                "--name", f"{self.work_pool_name}-worker"
            ]
            
            # 將輸出重定向到日誌文件
            log_file = Path("worker.log")
            with open(log_file, "w") as f:
                subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    start_new_session=True
                )
            
            print(f"✅ Worker 已在背景啟動")
            print(f"📄 日誌文件: {log_file.absolute()}")
            return True
        except Exception as e:
            print(f"❌ 啟動 worker 失敗: {e}")
            return False
    
    def deploy_flows(self) -> bool:
        """部署所有 flows"""
        if not self.prefect_yaml_path.exists():
            print(f"❌ 找不到 prefect.yaml 文件: {self.prefect_yaml_path}")
            return False
        
        # 切換到 prefect.yaml 所在目錄
        original_cwd = os.getcwd()
        try:
            os.chdir(self.prefect_yaml_path.parent)
            
            return self.run_prefect_command(
                ["deploy", "--all"],
                "部署所有 flows"
            )
        finally:
            os.chdir(original_cwd)
    
    def list_deployments(self) -> bool:
        """列出所有部署"""
        return self.run_prefect_command(
            ["deployment", "ls"],
            "列出所有部署"
        )
    
    def show_help_message(self):
        """顯示幫助訊息"""
        print("\n" + "=" * 60)
        print("📋 部署前置條件檢查失敗")
        print("=" * 60)
        print("請先執行以下步驟：")
        print()
        print("1️⃣ 啟動 Prefect 服務器：")
        print(f"   python src/prefect_demo/prefect_service/start_server.py")
        print()
        print("2️⃣ 等待服務器完全啟動後，再執行部署：")
        print(f"   python run_deploy.py")
        print()
        print("3️⃣ 或者在瀏覽器中訪問 Prefect UI：")
        print(f"   http://{self.api_host}:{self.api_port}")
        print("=" * 60)
    
    async def setup_and_deploy(self):
        """完整的設定和部署流程"""
        print("=" * 60)
        print("🎯 Prefect Demo - 部署管理器")
        print("=" * 60)
        
        # 0. 檢查服務器狀態
        print("\n📝 步驟 0: 檢查 Prefect 服務器狀態")
        print("-" * 40)
        
        if not await self.check_server_status():
            print("❌ Prefect 服務器未運行")
            self.show_help_message()
            return False
        
        print("✅ Prefect 服務器運行正常")
        
        # 1. 檢查並建立 work pool
        print("\n📝 步驟 1: 檢查並建立 Work Pool")
        print("-" * 40)
        
        pool_exists = await self.check_work_pool_exists()
        if pool_exists:
            print(f"✅ Work pool '{self.work_pool_name}' 已存在")
        else:
            if not self.create_work_pool():
                print("❌ 建立 work pool 失敗，中止部署")
                return False
        
        # 2. 啟動 worker
        print("\n📝 步驟 2: 啟動 Worker")
        print("-" * 40)
        
        if not self.start_worker_background():
            print("❌ 啟動 worker 失敗，但繼續部署流程")
        
        # 3. 部署 flows
        print("\n📝 步驟 3: 部署 Flows")
        print("-" * 40)
        
        if not self.deploy_flows():
            print("❌ 部署 flows 失敗")
            return False
        
        # 4. 列出部署結果
        print("\n📝 步驟 4: 檢查部署結果")
        print("-" * 40)
        
        self.list_deployments()
        
        print("\n" + "=" * 60)
        print("🎉 部署流程完成！")
        print("=" * 60)
        print("📊 接下來您可以:")
        print("   - 在 Prefect UI 中查看部署狀態")
        print("   - 手動觸發 flow 執行")
        print("   - 監控排程執行情況")
        print(f"   - 訪問 Web UI: http://{self.api_host}:{self.api_port}")
        print("=" * 60)
        
        return True


def main():
    """主函數"""
    manager = PrefectDeployManager()
    
    # 執行異步設定和部署
    success = asyncio.run(manager.setup_and_deploy())
    
    if not success:
        print("❌ 部署流程失敗")
        sys.exit(1)


if __name__ == "__main__":
    main() 