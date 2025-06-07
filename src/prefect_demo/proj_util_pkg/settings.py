# -*- coding: utf-8 -*-
"""
初始環境參數設定

"""
import os
from dotenv import load_dotenv

class ProjEnvSettings:

    def __init__(self):
        self.proj_env_settings_init()

    def proj_env_settings_init(self):
        """ Load .env file """
        # Get the path to the directory this file is in (proj_util_pkg directory)
        base_dir = os.path.abspath(os.path.dirname(__file__))

        # Setting app project head path to system environment variable
        os.environ["PROJECT_ROOT"] = base_dir

        # Load .env file from the config subdirectory
        env_file_path = os.path.join(base_dir, "config", ".env")
        
        if os.path.exists(env_file_path):
            load_dotenv(env_file_path)
            print(f"✅ 成功載入環境變數檔案: {env_file_path}")
        else:
            print(f"⚠️  找不到環境變數檔案: {env_file_path}")

settings = ProjEnvSettings()

print(f"[環境變數]PROJECT_ROOT: {os.environ.get('PROJECT_ROOT')}")
print(f"[環境變數]PREFECT_SERVER_API_HOST: {os.environ.get('PREFECT_SERVER_API_HOST')}")
print(f"[環境變數]PREFECT_SERVER_API_PORT: {os.environ.get('PREFECT_SERVER_API_PORT')}")
