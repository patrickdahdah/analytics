from lib.configs import settings, circulatingSupplyN, priceTableN, indicatorsTableN, indicatorsTableNA
from sqlalchemy import create_engine
from lib.configs import settings
from network import interface
import pandas as pd


def startSQL():
    global engine
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/"
                        .format(user=settings["database"]["user"],
                                pw=settings["database"]["password"],
                                ))
    connection = engine.connect()
    connection.execute("CREATE DATABASE IF NOT EXISTS {db}".format(db=settings["database"]["databaseName"]))  #create database if not exists
    connection.close()
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                    .format(user=settings["database"]["user"],
                            pw=settings["database"]["password"],
                            db=settings["database"]["databaseName"],
                            host=settings["database"]["host"]
                            ))
    
    createTablePrice(engine, priceTableN) #create all tables
    createTableCirculatingSupply(engine, circulatingSupplyN)
    
    
    createTableIndicators(engine, indicatorsTableN)
    createTableIndicators(engine, indicatorsTableNA)
    
    return engine


def createTablePrice(e, tableName): #CREATE PRICE HISTORY TABLE

        connection = e.connect()
        connection.execute("""CREATE TABLE IF NOT EXISTS `{db}`.`{tn}` (
                    timestamp TIMESTAMP,
                    low FLOAT,
                    high FLOAT,
                    open FLOAT,
                    close FLOAT,
                    volume DOUBLE,
                    realTimestamp TIMESTAMP,
                    price FLOAT
                    )""".format(tn=tableName,db=settings["database"]["databaseName"]))
        connection.close()


def createTableCirculatingSupply(e, tableName): #CREATE CIRCULATING SUPPLY TABLE
        connection = e.connect() #changed  circulatingSupply FLOAT
        connection.execute("""CREATE TABLE IF NOT EXISTS `{db}`.`{tn}` (
                    timestamp TIMESTAMP,
                    circulatingSupply DOUBLE,  
                    marketCap DOUBLE,
                    price FLOAT
                    )""".format(tn = tableName,db=settings["database"]["databaseName"]))
        connection.close()


def createTableIndicators(e, tableName): #CREATE INDICATORS TABLE
        connection = e.connect()
        connection.execute("""CREATE TABLE IF NOT EXISTS `{db}`.`{tn}` (
                                        timestamp TIMESTAMP,
                                        circulatingSupply DOUBLE,
                                        marketCap DOUBLE, 
                                        realizedCap DOUBLE,
                                        realizedPrice DOUBLE,
                                        numberAddr INT, 
                                        averageBalance DOUBLE, 
                                        medianBalance FLOAT,
                                        MVRV_Ratio DOUBLE, 
                                        MVRV_Ratio30o DOUBLE, 
                                        MVRV_Ratio30y DOUBLE, 
                                        MVRV_Ratio60o DOUBLE, 
                                        MVRV_Ratio60y DOUBLE,
                                        MVRV_Zscore DOUBLE,
                                        addrOver1k INT, 
                                        addrOver10k INT, 
                                        addrOver100k INT,
                                        addrOver1m INT, 
                                        addrOver10m INT, 
                                        addrOver100m INT,
                                        lastTxIndex INT
                                )""".format(tn=tableName,db=settings["database"]["databaseName"]))
        connection.close()


def getLastTimestamp(tableName):
        connection = engine.connect()
        result = connection.execute("""SELECT MAX(timestamp) FROM  `{db}`.`{tn}`""".format(tn=tableName,db=settings["database"]["databaseName"])) #get max(last) timestamp of existing tables
        timestamp= result.first()[0]
        
        connection.close()

        if timestamp == None: #if tables dosent have data/rows return start date point
                if  tableName==priceTableN:
                        print("none price table")
                        return 1567296120  #  Sunday, September 1, 2019 12:02:00 AM #first data availvable in the randlabs api for pirce history
                elif tableName==circulatingSupplyN:
                        print("none cs")
                        return 1568678400 #Tuesday, September 17, 2019 12:00:00 AM #first data availvable in the randlabs api for circulating supply
                else:
                        print("none indicator") 
                        return '2019-07-31' #'2020-07-05' #last timestamp for the pre initialized transaction table  insert.initializeDataframeTransactions()
        elif tableName == indicatorsTableN:
                return timestamp
        else:
                return interface.stringToTimestamp(timestamp)
def getLastIndex(tableName):
        connection = engine.connect()
        result = connection.execute("""SELECT MAX(lastTxIndex) FROM  `{db}`.`{tn}`""".format(tn=tableName,db=settings["database"]["databaseName"])) #get max(last) timestamp of existing tables
        lastIndex= result.first()[0]
        connection.close()

        if lastIndex == None: #if tables dosent have data/rows return start date point
                raise Exception("lastIndex none. something is worng because the indicator table had to have existed to get to this point")
        else:
                return lastIndex


def insert_df(dataFrame, tableName): 
    dataFrame.to_sql(tableName, con = engine, if_exists = 'append', chunksize = 1000) #INSERT pandas dataframe. if it exists append the rows to the existing table

                                                
def getTransactions(sinceIndex, untilDate): #get transaction table: since (use txIndex) | until (use day time Y-m-d)| 
        connection = engine.connect()       #discard ammounts == 0 and only algo transactions (No ASA transactions) 
        #print(connection)
        result = pd.read_sql_query("""SELECT `index` AS `txIndex`, DATE_FORMAT(timestamp, "%%Y-%%m-%%d") AS `timestamp`,
                                        `txid`, 
                                        `from`, `from_balance`/1000000 AS `from_balance`, `from_index`,
                                            `to`, `to_balance`/1000000 AS `to_balance`, `to_index`,
                                            `amount`/1000000 AS `amount`,
                                             `close`,`close_balance`/1000000 as `close_balance`, `close_index`, `close_amount`/1000000 AS `close_amount`
                                        FROM `{db}`.`transactions`
                                        WHERE `index` > {since} AND `timestamp` < '{date}' AND `asset_id`= 0 AND (`amount`!=0 OR ( `amount`= 0 AND `close_amount` > 0))
                                        ORDER BY `index` ASC 
                                        """.format(since = sinceIndex, date=str(untilDate), db=settings["database"]["databaseName"] ) , connection )
        connection.close()
        return result

def getTransactions2(untilDate): #get transaction table: since (use txIndex) | until (use day time Y-m-d)| 
        connection = engine.connect()       #discard ammounts == 0 and only algo transactions (No ASA transactions) 
        #print(connection)
        result = pd.read_sql_query("""SELECT `index` AS `txIndex`, timestamp AS `timestamp`,
                                        `txid`, 
                                        `from`, `from_balance`/1000000 AS `from_balance`, `from_index`,
                                            `to`, `to_balance`/1000000 AS `to_balance`, `to_index`,
                                            `amount`/1000000 AS `amount`,
                                             `close`,`close_balance`/1000000 as `close_balance`, `close_index`, `close_amount`/1000000 AS `close_amount`
                                        FROM `{db}`.`transactions`
                                        WHERE `timestamp` >= '{date}' AND `timestamp` < '{date2}' AND `asset_id`= 0 AND (`amount`!=0 OR ( `amount`= 0 AND `close_amount` > 0))
                                        ORDER BY `index` ASC 
                                        """.format(date=str(untilDate), date2 = str('2021-08-30'), db=settings["database"]["databaseName"] ) , connection)
        connection.close()
        return result


def getCirculatingSupply(): #get all circulating supply table timestamp format Y-m-d
        connection = engine.connect()
        result = pd.read_sql_query("""SELECT DATE_FORMAT(timestamp, "%%Y-%%m-%%d") AS `timestamp`,circulatingSupply,marketCap,ROUND(marketCap/circulatingSupply, 4) AS price
                                        FROM `{db}`.`{tn}`
                                        order by timestamp asc;""".format(tn=circulatingSupplyN,db=settings["database"]["databaseName"]) , connection)
        connection.close()
        return result


def getSTDDevMarketCap(tableName): #get standar deviation of market cap for the MRVR ZERO Calculation 
        connection = engine.connect()
        result = connection.execute("""SELECT  stddev(`marketCap`), count(`marketcap`) FROM `{db}`.`{tn}`""".format(tn=tableName, db=settings["database"]["databaseName"]))
        row = result.first()        
        connection.close()
        if not row or (row[1] < 3) or (row[0] == 0.0): #if result == None or marketCap days less than 4 rows or std == 0: return None 
                return None, None
        return row[0], row[1]


