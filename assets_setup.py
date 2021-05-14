import os
import numpy as np
import pandas as pd
import pandas_datareader.data as web
import requests
from urllib.request import urlopen
from zipfile import ZipFile
import json


class AssetGetter:
    def __init__(self, data_path, geckodriver_path, config_path):
        self.data_path = data_path
        self.geckodriver_path = geckodriver_path
        self.config_path = config_path

    def set_base_path(self, data_path):
        self.data_path = data_path

    def get_base_path(self):
        return self.data_path

    def get_login_information(self):
        with open(self.config_path) as file:
            return json.load(file)["login"]

    def get_wiki_prices_url(self):
        with open(self.config_path) as file:
            return json.load(file)["wiki_url"]

    def get_wiki_stocks_url(self):
        with open(self.config_path) as file:
            return json.load(file)["wiki_stocks_url"]

    def download_wiki_prices_file(self):
        if not os.path.exists(os.path.join(self.data_path, "wiki_prices.csv")):
            url = self.get_wiki_prices_url()

            zipresp = urlopen(url)
            print("URL Opened")
            with open(os.path.join(self.data_path, "wiki_prices_zipped.zip"), "wb") as f:
                print("Beginning to Write")
                f.write(zipresp.read())
            print("Finished Writing to File")

            unzipped_file = os.path.join(self.data_path, f"wiki_prices_unzipped")
            if not os.path.exists(os.path.join(self.data_path, "wiki_prices_unzipped")):
                os.mkdir(unzipped_file)
            zf = ZipFile(os.path.join(self.data_path, "wiki_prices_zipped.zip"))
            zf.extractall(unzipped_file)
            zf.close()
            print("Finished Unzipping File")

            file_to_rename = os.listdir(os.path.join(self.data_path, "wiki_prices_unzipped"))[0]
            os.rename(os.path.join(self.data_path, os.path.join("wiki_prices_unzipped", file_to_rename)),
                      os.path.join(self.data_path, "wiki_prices.csv"))
            os.rmdir(os.path.join(self.data_path, "wiki_prices_unzipped"))
            os.remove(os.path.join(self.data_path, "wiki_prices_zipped.zip"))

            print("Successfully Unzipped and Downloaded Wiki Prices.csv")
        else:
            print("Wiki Prices File Already Exists")

    def download_wiki_stocks_file(self):
        if not os.path.exists(os.path.join(self.data_path, "wiki_stocks.csv")):
            url = self.get_wiki_stocks_url()

            r = requests.get(url)
            with open(os.path.join(self.data_path, "wiki_stocks.csv"), "wb") as f:
                f.write(r.content)
            print("Stocks File Downloaded")
        else:
            print("Wiki stocks file is already downloaded")

    def download_fred_sp500_data(self):
        df = web.DataReader(name="SP500", data_source="fred", start=2009).squeeze().to_frame("close")
        print(df.info())

        with pd.HDFStore(os.path.join(self.data_path, "assets.h5")) as store:
            store.put("sp500/fred", df)
        print("Successfully downloaded Fred data into assets file")

    def download_sp500_constituents(self):
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        df = pd.read_html(url, header=0)[0]
        df.columns = ["ticker", "name", "sec_filings", "gics_sector", "gics_sub_industry", "location", "first_added",
                      "cik", "founded"]
        df = df.drop("sec_filings", axis=1).set_index("ticker")

        with pd.HDFStore(os.path.join(self.data_path, "assets.h5")) as store:
            store.put("sp500/stocks", df)
        print("Downloaded SP500 Constituents")

    def download_assets_file(self):
        assets_file = os.path.join(self.data_path, "assets.h5")

        if not os.path.exists(assets_file):
            self.download_wiki_prices_file()
            self.download_wiki_stocks_file()
            self.download_fred_sp500_data()
            self.download_sp500_constituents()

            prices = pd.read_csv(os.path.join(self.data_path, "wiki_prices.csv"), parse_dates=["date"],
                                 index_col=["date", "ticker"], infer_datetime_format=True).sort_index()
            stocks = pd.read_csv(os.path.join(self.data_path, "wiki_stocks.csv"))

            with pd.HDFStore(assets_file) as store:
                store.put("quandl/wiki/prices", prices)
                store.put("quandl/wiki/stocks", stocks)
