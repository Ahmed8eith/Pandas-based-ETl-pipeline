import pandas as pd
import json

class Pipeline:
    def __init__(self, config_path):
        # Loading Config
        with open(config_path)as f:
            config=json.load(f)
        
        self.source=config["source"]
        self.destination=config["destination"]

    #EXTRACTING
    def extract_data(self):
        print(f"Extracting data from {self.source}")
        df=pd.read_csv(self.source)
        return df
    
    def transform_data(self, df):
        print("Transforming data")

        #Replace empty strings or "NULL" with pandas NA
        df=df.replace(["", "NULL"], pd.NA)

        #Drop rows where "amount" is missing
        df=df.dropna(subset=["amount"])

        #Convert amount into float
        df["amount"] = df["amount"].astype(float)

        return df
    
    #Load data 
    def load_data(self,df):
        print (f"Loading {len(df)} rows into {self.destination}")

        if df.empty:
            print("No data to Load")
            return
        
        df.to_csv(self.destination, index = False)
        print("Load complete")

    #Orchestration
    def run(self):
        df=self.extract_data()
        df=self.transform_data(df)
        self.load_data(df)


test=Pipeline("config.json")
test.run()