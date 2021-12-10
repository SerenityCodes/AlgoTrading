import assets_setup
import os

getter = assets_setup.AssetGetter(os.getcwd() + "/datasets", "config.json")
getter.download_algoseek_data()