from lib.configs import settings, circulatingSupplyN, priceTableN, indicatorsTableN, indicatorsTableNA, addrList
from database import sql
from network import interface
import time
import requests
import signal
import pandas as pd
BLOCK0 = 2100000010  #total sum of the addresses created in the genesis block that havent moved 
REWARD_ADDRESS = "737777777777777777777777777777777777777777777777777UFEJ2CI"

URL=settings["baseUrl"]  #Base api-url from configs.settings
CSVPRICE=pd.read_csv('price.csv') 
CSVPRICE['timestamp'] = CSVPRICE['timestamp'].apply(interface.dateformatcsv) #price dataframe with more dates than what the circulating supply_sql table can provide (price api frandlabs). (data from Coinmarketcap.com)

def receiveSignal(signalNumber, frame): #changes the infinite while loop to FALSE if CTRL + C is invoked to terminate the program
    global shouldNotQuit
    shouldNotQuit = False
    return
signal.signal(signal.SIGINT, receiveSignal)
signal.signal(signal.SIGILL, receiveSignal)

def initializeDataframeTransactions(): #this function initialize the transactions table from a local csv. this optimizes the quering time since the data is pre-loaded
    global transactionsDF
    global maxDateCSV

    transactionsDF = pd.read_csv("transactions.csv")
    transactionsDF["timestamp"] = pd.to_datetime(transactionsDF['timestamp'], format = "%Y-%m-%d")
    maxDateCSV = transactionsDF["timestamp"].max() #global variable used in insertIndiccators function

    maxDateSQL = pd.to_datetime(sql.getLastTimestamp(indicatorsTableN), format = "%Y-%m-%d")
    
    if maxDateSQL >= maxDateCSV:
        print(transactionsDF.shape)
        print("append")
        sinceIndex = transactionsDF["txIndex"].max() #get lastindex of the last row of the csv local data
        transactionsMissing = sql.getTransactions(sinceIndex, maxDateSQL) #request day by day: since using transactions index and until using date.
        print("\n {sinceIndex}  {maxDateSQL} \n".format(sinceIndex=sinceIndex,maxDateSQL=maxDateSQL))

        interface.checkForAddresses(transactionsMissing) #check if we have to add new addresses to the list of inc/foundation adresses

        transactionsMissing["timestamp"]= pd.to_datetime(transactionsMissing['timestamp'], format = "%Y-%m-%d")
        transactionsDF = transactionsDF.append(transactionsMissing) #if we are requesting the sql means that the global transaction dataframe must be updated
        print(transactionsMissing.shape)
        print(transactionsDF.shape)
    
def insertCirculatingSupply(timestampCS):
    
    dateNow= int(time.time())

    if timestampCS+86400>dateNow: #if a day has passed: check for new entries
        return {"status":200,"type":"updated"}
    else: 
        try:
            endTime=timestampCS+86400
            #get table
            response=interface.fetchGet("{url}/stats/circ_supply/since/{since}/until/{until}?samples={samples}".format(url=URL,since=endTime-1,until=endTime,samples=1)) #returns 1 row =  1 day
            jsonCS=response.json()
            dfCS = pd.json_normalize(jsonCS)
            dfCS=dfCS.drop(columns=["realTimestamp"]) #drop realtimestamp we dont need it

            responsePrice=interface.fetchGet("{url}/stats/algoprice/since/{since}/until/{until}?samples={samples}".format(url=URL,since=dfCS['timestamp'][0],until=dfCS['timestamp'][0]+1,samples=1))#get price to calculate the Market Cap
            jsonPrice=responsePrice.json()

            dfCS['price'] = jsonPrice[0]["price"]
            dfCS['marketCap'] = dfCS["circulatingSupply"] * dfCS['price'] #marketCap= price * circulating supply

            dfCS['timestamp'] = dfCS['timestamp'].apply(interface.timestampToString) #convert timestamp to string to be able to insert it to MYSQL
            dfCS=dfCS.set_index("timestamp") #setting index as timestamp 

            sql.insert_df(dfCS, circulatingSupplyN) #inserting row/day into the sql table
            return {"status":200,"type":"keepUpdating"}

        #error descriptions
        except requests.exceptions.ConnectionError as errc:
            return {"status":500,"type":"connectionErr"}
            print ("Error Connecting:",errc)
        except requests.exceptions.Timeout as errt:
            return {"status":500,"type":"timeout"}
            print ("Timeout Error:",errt)  
        except requests.exceptions.RequestException as err: 
            print ("Ops: Something Else",err) 
            return{"status":status_code,"type":err} 


def insertHistoricalprice(timestampPrice):

    dateNow= int(time.time())

    if timestampPrice+60>dateNow:
        return {"status":200,"type":"updated"}
    else: 
        try:
            endTime=timestampPrice+6000
            if endTime > dateNow:
                endTime=dateNow
                samples=int((dateNow-timestampPrice)/60)
            else: 
                samples=100
            #print(interface.timestampToString(timestampPrice))
            #print(interface.timestampToString(endTime))

            responsePrice = interface.fetchGet("{url}/stats/algoprice/since/{since}/until/{until}?samples={samples}".format(url=URL,since=timestampPrice,until=endTime,samples=samples))
            jsonPrice=responsePrice.json()
            dfPrice = pd.json_normalize(jsonPrice)


            dfPrice['timestamp'] = dfPrice['timestamp'].apply(interface.timestampToString) #convert timestamp to string to be able to insert it to MYSQL
            dfPrice['realTimestamp'] = dfPrice['realTimestamp'].apply(interface.timestampToString) #convert timestamp to string to be able to insert it to MYSQL
            dfPrice=dfPrice.set_index("timestamp") #setting index as timestamp

            sql.insert_df(dfPrice,priceTableN)

            return {"status":200,"type":"keepUpdating"}
        
        except requests.exceptions.ConnectionError as errc:
            return {"status":500,"type":"connectionErr"}
            print ("Error Connecting:",errc)
        except requests.exceptions.Timeout as errt:
            return {"status":500,"type":"timeout"}
            print ("Timeout Error:",errt)  
        except requests.exceptions.RequestException as err:
            print ("Ops: Something Else",err)
            return{"status":status_code,"type":err}


def insertIndicators(timestampCS):
    
    global transactionsDF
    
    untilDate = pd.to_datetime(sql.getLastTimestamp(indicatorsTableN), format = "%Y-%m-%d") + pd.DateOffset(1)
    if untilDate > pd.to_datetime(timestampCS, format = "%Y-%m-%d"):  #carefull timestamp is int # waits for circulating table to update #we dont check current time because its already checked with the last timestamp of the ciruclating supply table
        return {"status":200,"type":"updated"}
    else:
        try:
            tableSQLCirculatingSupply = sql.getCirculatingSupply() #gets the circulating supply table because we need the daily price for the indicators
            cs = CSVPRICE.append(tableSQLCirculatingSupply[["timestamp","price"]]) #append to the csv price list that have more past dates than the sql 
            cs["timestamp"]=pd.to_datetime(cs['timestamp'], format = "%Y-%m-%d") #convert timestamp string to pandas datetime

            if untilDate > maxDateCSV: #if we have extrapolated all the transactions from the local csv file, its time to request more transactions to the sql
                sinceIndex = sql.getLastIndex(indicatorsTableN) #get lastindex of the last row of the indicators table in the SQL
                transactionsDFDaytemp = sql.getTransactions(sinceIndex,untilDate) #request day by day: since using transactions index and until using date.

                interface.checkForAddresses(transactionsDFDaytemp) #check if we have to add new addresses to the list of inc/foundation adresses

                transactionsDFDaytemp["timestamp"]= pd.to_datetime(transactionsDFDaytemp['timestamp'], format = "%Y-%m-%d")
                transactionsDF = transactionsDF.append(transactionsDFDaytemp) #if we are requesting the sql means that the global transaction dataframe must be updated
                transactionsDFDay = transactionsDF
            else: #we have the data in the local csv no need to request the databse
                transactionsDFDay = transactionsDF.loc[transactionsDF['timestamp'] < untilDate]
                interface.checkForAddresses(transactionsDF.loc[(transactionsDF['timestamp'] < untilDate) & (transactionsDF['timestamp'] > untilDate - pd.DateOffset(1))])
                

            addrDF=interface.transactionsToAdrrs(transactionsDFDay) #transactions to list of unique addresses Columns: address|balance|adressIndex
            addrDF.loc[addrDF['addr'] == REWARD_ADDRESS, 'balance'] = interface.getBalance(REWARD_ADDRESS) #the reward address balance is not showned correctly in the transactions data therefore we have to change it here
            
            addrDF = interface.addRealizedCapColumn(addrDF,cs) #Columns added: price and realized cap 

            #-----indicators Table------

            circulatingSupply, marketCap, realizedCap, realizedPrice, MVRV_Ratio, MVRV_Zscore = interface.calculations(addrDF, indicatorsTableNA) #multiple indicators calculated (check the function for details)
            MVRV_Ratio30o , MVRV_Ratio30y , MVRV_Ratio60o , MVRV_Ratio60y = interface.calculationsMVRVdays(untilDate,transactionsDFDay,cs)  #calculating 30 and 60 days #null returns for the first 30 or 60 days
            circulatingSupply += BLOCK0 #in the Genesis block there are rich addresses that hasnt move since therefore they do not appear in the transactions data. We add them here with a constant (Those addreses are included in the addrList)

            lastIndex = transactionsDFDay["txIndex"].max() #last transaction takling into consideration on each day/row

            newRow= {'timestamp' : [untilDate],'circulatingSupply' : [circulatingSupply], 'marketCap' : [marketCap] ,'realizedPrice' : [realizedPrice], 'realizedCap' : [realizedCap], 'numberAddr': [len(addrDF['addr'])],
            'averageBalance': [addrDF['balance'].mean()], 'medianBalance': [addrDF['balance'].median()],
            'MVRV_Ratio' : [MVRV_Ratio], "MVRV_Ratio30o" :[ MVRV_Ratio30o], "MVRV_Ratio30y": [MVRV_Ratio30y] , "MVRV_Ratio60o": [MVRV_Ratio60o] , "MVRV_Ratio60y": [MVRV_Ratio60y] , "MVRV_Zscore": [MVRV_Zscore],
            'addrOver1k' : [len(addrDF[addrDF["balance"] > 1000]) ],'addrOver10k' :[len(addrDF[addrDF.balance > 10000]) ],'addrOver100k' : [len(addrDF[addrDF.balance > 100000])],'addrOver1m' : [ len(addrDF[addrDF.balance > 1000000])],'addrOver10m' :  [len(addrDF[addrDF.balance > 10000000])] ,'addrOver100m' : [ len(addrDF[addrDF.balance > 100000000])],
            'lastTxIndex' : [lastIndex] } #dictionary to convert it to a pandas dataframe
            dfindicatorsAllRow = pd.DataFrame(newRow)
            dfindicatorsAllRow=dfindicatorsAllRow.set_index("timestamp") #setting index as timestamp 

            #-----indicators Table WITHOUT inc/foundation addresses------
            #addrDF.drop(addrList, inplace= True, errors= 'ignore', columns='addr')

            addrDF = addrDF[~addrDF['addr'].isin(addrList)]


            circulatingSupply, marketCap, realizedCap, realizedPrice, MVRV_Ratio, MVRV_Zscore = interface.calculations(addrDF,indicatorsTableN) #multiple indicators calculated (check the function for details)
            MVRV_Ratio30o , MVRV_Ratio30y , MVRV_Ratio60o , MVRV_Ratio60y = interface.calculationsMVRVdays(untilDate,transactionsDFDay,cs)  #calculating 30 and 60 days #null returns for the first 30 or 60 day
            

            newRow = {'timestamp' : [untilDate],'circulatingSupply' : [circulatingSupply], 'marketCap' : [marketCap] ,'realizedPrice' : [realizedPrice], 'realizedCap' : [realizedCap], 'numberAddr': [len(addrDF['addr'])],
            'averageBalance': [addrDF['balance'].mean()], 'medianBalance': [addrDF['balance'].median()],
            'MVRV_Ratio' : [MVRV_Ratio], "MVRV_Ratio30o" :[ MVRV_Ratio30o], "MVRV_Ratio30y": [MVRV_Ratio30y] , "MVRV_Ratio60o": [MVRV_Ratio60o] , "MVRV_Ratio60y": [MVRV_Ratio60y] , "MVRV_Zscore": [MVRV_Zscore],
            'addrOver1k' : [len(addrDF[addrDF["balance"] > 1000]) ],'addrOver10k' :[len(addrDF[addrDF.balance > 10000]) ],'addrOver100k' : [len(addrDF[addrDF.balance > 100000])],'addrOver1m' : [ len(addrDF[addrDF.balance > 1000000])],'addrOver10m' :  [len(addrDF[addrDF.balance > 10000000])] ,'addrOver100m' : [ len(addrDF[addrDF.balance > 100000000])],
            'lastTxIndex' : [lastIndex] } #dictionary to convert it to a pandas dataframe
            dfindicatorsRow = pd.DataFrame(newRow)
            dfindicatorsRow=dfindicatorsRow.set_index("timestamp") #setting index as timestamp

            #insert both rows in their crresponding table
            sql.insert_df(dfindicatorsAllRow,indicatorsTableNA) 
            sql.insert_df(dfindicatorsRow,indicatorsTableN)   
            
            return {"status":200,"type":"keepUpdating"}

        except requests.exceptions.ConnectionError as errc:
            return {"status":500,"type":"connectionErr"}
            print ("Error Connecting:",errc)
        except requests.exceptions.Timeout as errt:
            return {"status":500,"type":"timeout"}
            print ("Timeout Error:",errt)  
        except requests.exceptions.RequestException as err: 
            print ("Ops: Something Else",err) 
            return{"status":status_code,"type":err} 

