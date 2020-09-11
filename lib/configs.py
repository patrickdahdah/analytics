import json
import os
import argparse
import pandas as pd

priceTableN = "price_history2"
circulatingSupplyN = "circulatingsupply_history2"
indicatorsTableNA = "indicatorsAll2"
indicatorsTableN = "indicators2"

def getSettings():
    global settings
    try:
        settings
    except NameError:
        
        # Initialize parser 
        parser = argparse.ArgumentParser() 
        # Adding optional argument 
        parser.add_argument("-s", "--Settings", help = "Add settings path. example: $ python3 -s /home/blabla/settings.json") 
        # Read arguments from command line 
        args = parser.parse_args() 

        

        if args.Settings: 
            print("Path as: % s" % args.Settings)
            with open(args.Settings) as f:
                settings = json.load(f)

        elif 'ANALYTICS_SETTINGS' in os.environ:
            ruta=os.getenv('ANALYTICS_SETTINGS')
            try: 
                with open(ruta) as f:
                    settings = json.load(f)
            except Exception as e:
                print(str(e) + "\n\Environment settings variable name exists but no such file or something went wrong!\n")

        else:
            basepath = os.path.dirname(__file__)
            filepath = os.path.abspath(os.path.join(basepath, "..", "settingsT.json"))
            with open(filepath) as f:
                settings = json.load(f)
          
getSettings()

dfAddresses = pd.read_csv("Addresses.csv", index_col=False ) #list of inc/foundation addresses
addrList = dfAddresses['Address'].tolist()


dfE = pd.read_csv("exchanges.csv", index_col=False ) #list of exchange's addresses 
exchanges = dfE['Address'].tolist()

