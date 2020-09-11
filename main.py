from lib.configs import settings, circulatingSupplyN, priceTableN, indicatorsTableN, indicatorsTableNA, addrList  #Superglobal variables. Settings is a dictionary with the sql info
from database import insert
from sqlalchemy import create_engine
import signal #library to be able to quit the program with CTRL + C
import time
import pandas as pd  

shouldNotQuit = True

def receiveSignal(signalNumber, frame): #changes the infinite while loop to FALSE if CTRL + C is invoked to terminate the program
    global shouldNotQuit
    shouldNotQuit = False
    return
signal.signal(signal.SIGINT, receiveSignal)
signal.signal(signal.SIGILL, receiveSignal)

'''

try:  #Drop tables that were running with past scripts.
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                    .format(user=settings["database"]["user"],
                            pw=settings["database"]["password"],
                            db=settings["database"]["databaseName"]
                            ))
    connection = engine.connect()
    connection.execute("DROP TABLE IF EXISTS `{tableName1}`, `{tableName2}` , `{tableName3}` , `{tableName4}`;".format( tableName1 =  indicatorsTableN , tableName2 = indicatorsTableNA, tableName3= circulatingSupplyN, tableName4 = priceTableN))
    connection.close()

except Exception as e:
    print("Something happened trying to drop the tables.")
    print(e)
'''
while shouldNotQuit: #True unless CTRL + C is invoked.
    try:
        insert.sql.startSQL() #creates engine variable | creates databse if it dosent exist | creates the tables if they dont exists.
        s = time.time()
        insert.initializeDataframeTransactions() #loads all transactions from index 0 to 2019-07-31 for the indicator table to have a start point #CUIDADO ESTA MAL PPORQUE QUE PASA SI UN ERROR OCURRE Y SALE DEL WHILE
        e = time.time()
        print('\n\n')
        print("TIME init transactions: " + str(e-s))
        while shouldNotQuit:

            try:
                print("\n******** New loop *******")
                                #-------------Query price---------- 
                lastTimestampPrice=insert.sql.getLastTimestamp(priceTableN) #gets last timestamp existing in the historic price table.

                result = insert.insertHistoricalprice(lastTimestampPrice) #request price rows from api and insert them to the sql price table

                if result["status"]==200:
                    if result["type"]=="keepUpdating":
                        sleepPrice=False #table NOT updated | succesfull insert 
                    elif result["type"]=="updated":
                        print("Price history updated")
                        sleepPrice=True #table updated
                else:
                    print(result)
                    raise result

                lastTimestampCirculatingSupply=insert.sql.getLastTimestamp(circulatingSupplyN) #gets last timestamp existing in the circulating supply table

                                #----------Query Circulating  --------------------
                result = insert.insertCirculatingSupply(lastTimestampCirculatingSupply) #request price rows from api and insert them to the sql circulating supply table
                
                if result["status"]==200:
                    if result["type"]=="keepUpdating":
                        sleepCS=False #table NOT updated | succesfull insert 
                    elif result["type"]=="updated":
                        print("Circulating supply updated")
                        sleepCS=True #table updated
                else:
                    print(result)
                    raise result
                
                                #----------Query Indicators  --------------------
                s = time.time()
                lastTimestampCirculatingSupply=insert.sql.getLastTimestamp(circulatingSupplyN) #gets last timestamp existing in the circulating supply table
                
                result = insert.insertIndicators(insert.interface.timestampToStringYMD(lastTimestampCirculatingSupply)) #indicators table depends on circulating supply data
                
                if result["status"]==200:
                    if result["type"]=="keepUpdating":
                        sleepIndicators=False #table NOT updated | succesfull insert 
                        e = time.time()
                        print("TIME indicators: " + str(e-s))
                    elif result["type"]=="updated":
                        print("Indicators updated")
                        sleepIndicators=True #table updated
                else:
                    print(result)
                    raise result
                
                #----------data updated sleep-----------
                if sleepCS and sleepPrice and sleepIndicators: #if the three tables are updated sleep for 60seconds
                    print("60s sleep ALL TABLES ARE UP TO DATE")
                    time.sleep(60)  
                
                
            except Exception as e:
                print(e)
                time.sleep(20) 
                #sleep for 20 seconds maybe a network problem
                raise e     
           
    except TimeoutError as err:
        print("TimeoutEroor")
        print(err) 
        time.sleep(20) 
    except Exception as e: #Major error retry in 25 seconds
        print("\nERROR:")
        print(e)
        print("\n Retrying in 25 seconds")
        time.sleep(25)
        #raise e        
 
