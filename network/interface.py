from lib.configs import settings, circulatingSupplyN, priceTableN, indicatorsTableN, indicatorsTableNA, dfAddresses, addrList
from database import sql
import time
import requests
import calendar
import datetime 
import pandas as pd

URL=settings["baseUrl"]

def fetchGet(url: str):
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception('The request generates an unexpected response  \
            Status code= %d' %(response.status_code) )
    return response


def fetchPost(url: str, body: dict): #function never used
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(body))
    if response.status_code != 200:
        raise Exception('The request generates an unexpected response\n \
            Status code= %d' %(response.status_code) )
    return response

def getBalance(address: str): 
    response= fetchGet("{url}/account/{addr}".format(url=URL, addr=address))
    info=response.json()
    return info["balance"]/1000000 #returns in algos not microalgos

def getStatus(address: str):
    response= fetchGet("{url}/account/{addr}".format(url=URL, addr=address))
    info=response.json()
    return info["status"]

def timestampToString(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t))

def timestampToStringYMD(t): #same as above but in Y m d format
    return time.strftime("%Y-%m-%d", time.gmtime(t))

def stringToTimestamp(t):
    return int(calendar.timegm(time.strptime(str(t), "%Y-%m-%d %H:%M:%S")))

def dateformatcsv(t): #when importing csv, tha date format changes so this functions corrects it
    return datetime.datetime.strptime(t, "%m/%d/%Y").strftime("%Y-%m-%d")

def checkForAddresses(transactionsDataFrame): #checks for new addresses that most be excluded from the calculations for indicators tables
    for i in transactionsDataFrame.index: 
        if (transactionsDataFrame['from'][i] in addrList) and (pd.isnull(transactionsDataFrame['close'][i]) is not True) and  (transactionsDataFrame['close'][i] not in addrList): #check if a new address must be added by checking the close to
            addrList.append(transactionsDataFrame['close'][i])

def checkForBigTransactions(transactionsDataFrame): 
    a = []
    for i in transactionsDataFrame.index: 
        if (transactionsDataFrame['from'][i] in addrList) and (transactionsDataFrame['amount'][i] > 15000000) and  (transactionsDataFrame['to'][i] not in addrList):
            inf = {transactionsDataFrame["txid"][i] : transactionsDataFrame['to'][i]}
            a.append(inf)
    return a

def AddCSVinfo(AddrDF): #fills the Address.csv columns status|balance|Aindex|txid  
    global dfAddresses
    originalList = dfAddresses['Address'].tolist()
    listWithMore = AddrDF["addr"].tolist()

    for i in addrList: 
        # print(i)
        if i in listWithMore:
            if i in originalList: #normal address
                dfAddresses.loc[dfAddresses['Address'] == i, 'balance'] = AddrDF.loc[AddrDF['addr'] == i, 'balance'].tolist()[0]
                dfAddresses.loc[dfAddresses['Address'] == i, 'Aindex'] = AddrDF.loc[AddrDF['addr'] == i, 'Aindex'].tolist()[0]
                dfAddresses.loc[dfAddresses['Address'] == i, 'txid'] = AddrDF.loc[AddrDF['addr'] == i, 'txid'].tolist()[0]
                dfAddresses.loc[dfAddresses['Address'] == i, 'status'] = getStatus(i)
            else: #address founded by checkForAddresses() function
                # print(i)
                row = {'Address': i, 
                        'Owner': "tracked", 
                        'status': getStatus(i),
                        'balance': AddrDF.loc[AddrDF['addr'] == i, 'balance'].tolist()[0],
                        'Aindex' : AddrDF.loc[AddrDF['addr'] == i, 'Aindex'].tolist()[0],
                        'txid' : AddrDF.loc[AddrDF['addr'] == i, 'txid'].tolist()[0]
                        }
                dfAddresses = dfAddresses.append(row, ignore_index = True)

            
        else: #genesis/block 0   no transactions made 
            dfAddresses.loc[dfAddresses['Address'] == i, 'balance'] = getBalance(i)
            dfAddresses.loc[dfAddresses['Address'] == i, 'Aindex'] = 0
            dfAddresses.loc[dfAddresses['Address'] == i, 'status'] = getStatus(i)
    return dfAddresses

def transactionsToAdrrs(transactionsDF): #transactions to unique addresses list with their corresponding last balance and Aindex
    fromTransactions= transactionsDF[['txIndex',  'timestamp', 'txid', 'from', 'from_balance', 'from_index' ]].rename(columns = {'from' : 'addr', 'from_balance' : 'balance','from_index':'Aindex'})#split the `from` addrs columns

    toTransactions=transactionsDF[['txIndex',  'timestamp', 'txid', 'to', 'to_balance', 'to_index' ]].rename(columns = {'to' : 'addr', 'to_balance' : 'balance','to_index':'Aindex'})#split the `to` addrs columns

    closeTransactions=transactionsDF[['txIndex',  'timestamp', 'txid', 'close', 'close_balance', 'close_index' ]].rename(columns = {'close' : 'addr', 'close_balance' : 'balance','close_index':'Aindex'})#split the `close` addrs columns
    closeTransactions.dropna(inplace = True)
    closeTransactions['Aindex'] = closeTransactions['Aindex'].astype(int)

    addrDF=fromTransactions.append([toTransactions,closeTransactions])

    addrDF.sort_values("txIndex", inplace = True) #sort transaction index by ASC
    addrDF.drop_duplicates(subset="addr" , inplace = True, keep="last") #keep last txinfo (last balance info)

    return addrDF

def addRealizedCapColumn(addrsDF, cs): #merger cs table to add price column and calculate realized price column
    addrsDF=pd.merge(addrsDF,cs, on="timestamp")
    addrsDF['realizedCapAddr']= addrsDF["balance"]*addrsDF["price"]
    addrsDF.set_index("timestamp", inplace=True)
    addrsDF.sort_index(inplace=True)

    return addrsDF

def calculations(addrsDF,tableName):
    circulatingSupply = addrsDF['balance'].sum()
    marketCap = round(circulatingSupply * addrsDF["price"].iloc[-1], 4) # * last price
    realizedCap = addrsDF['realizedCapAddr'].sum()
    realizedPrice = round(realizedCap / circulatingSupply, 6)
    MVRV_Ratio = round(marketCap / realizedCap, 6)
    STDDevMarketCap, count = sql.getSTDDevMarketCap(tableName)

    if count: #If results is not None or marketcap # of rows grater than 3 include MRV_Zscore
        MVRV_Zscore = (marketCap - realizedCap) / STDDevMarketCap
    else:  #else MRVZscore null|none
        MVRV_Zscore = None

    return circulatingSupply, marketCap, realizedCap, realizedPrice, MVRV_Ratio, MVRV_Zscore


def calculationsMVRV(addrsDF): #calculationsMVRVdays() uses this function, dont delete
    circulatingSupply = addrsDF['balance'].sum()
    marketCap = circulatingSupply *  addrsDF["price"].iloc[-1]
    realizedCap = addrsDF['realizedCapAddr'].sum()
    MVRV_Ratio = round(marketCap / realizedCap, 6)
    
    return MVRV_Ratio


def calculationsMVRVdays(untilDate, transactionsDF, cs):
    if (untilDate > pd.to_datetime("2019-11-28", format = "%Y-%m-%d")):
        split60 = untilDate - pd.DateOffset(60, 'D')
        addrDF60o = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] <= split60])
        addrDF60y = transactionsToAdrrs(transactionsDF.loc[transactionsDF['timestamp'] > split60])
        addrDF60o = addRealizedCapColumn(addrDF60o,cs)
        addrDF60y = addRealizedCapColumn(addrDF60y,cs)
        MVRV_Ratio60o = calculationsMVRV(addrDF60o)
        MVRV_Ratio60y = calculationsMVRV(addrDF60y)

        split30= untilDate - pd.DateOffset(30, 'D')
        addrDF30o = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] <= split30])
        addrDF30y = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] > split30])
        addrDF30o = addRealizedCapColumn(addrDF30o,cs)
        addrDF30y = addRealizedCapColumn(addrDF30y,cs)
        MVRV_Ratio30o = calculationsMVRV(addrDF30o)
        MVRV_Ratio30y = calculationsMVRV(addrDF30y)


    elif (untilDate > pd.to_datetime("2019-09-29", format = "%Y-%m-%d")):
        split30= untilDate - pd.DateOffset(30, 'D')
        addrDF30o = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] <= split30])
        addrDF30y = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] > split30])
        addrDF30o = addRealizedCapColumn(addrDF30o,cs)
        addrDF30y = addRealizedCapColumn(addrDF30y,cs)
        MVRV_Ratio30o = calculationsMVRV(addrDF30o)
        MVRV_Ratio30y = calculationsMVRV(addrDF30y)

        MVRV_Ratio60o = None
        MVRV_Ratio60y = None
        
    else:
        MVRV_Ratio30o = None
        MVRV_Ratio30y = None
        MVRV_Ratio60o = None
        MVRV_Ratio60y = None

    return MVRV_Ratio30o , MVRV_Ratio30y , MVRV_Ratio60o , MVRV_Ratio60y
