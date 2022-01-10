import json
from web3 import Web3
from web3.logs import STRICT, IGNORE, DISCARD, WARN
from typing import List
from math import *
import pandas as pd
import requests
from ast import literal_eval
import time
import decimal
import schedule
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import urllib.parse 

start_time = time.time()
#Connect Ethereum node 
avarpc = "https://api.avax.network/ext/bc/C/rpc"
web3 = Web3(Web3.HTTPProvider(avarpc))  
# web3 = Web3(Web3.WebsocketProvider(bscrpc))
 
print(web3.isConnected())
print(web3.eth.blockNumber)
latestBlk = web3.eth.blockNumber

# Load BAVAABI data
bavaJson = open('Bava.json')
bavaAbi = json.load(bavaJson)

# Load LpTokenABI data
lpJson = open('LpToken.json')
lpAbi = json.load(lpJson)

# Load bavaMasterFarmAbi data
bavaMasterFarmJson = open('BavaMasterFarm.json')
bavaMasterFarmAbi = json.load(bavaMasterFarmJson)

# Load Pool data
farmJson = open('farm.json')
farm = json.load(farmJson)

bavaAddress = '0xe19A1684873faB5Fb694CfD06607100A632fF21c'
tokenContract = web3.eth.contract(address=bavaAddress, abi=bavaAbi["abi"])
bavaMasterFarm = "0xb5a054312A73581A3c0FeD148b736911C02f4539"
bavaMasterFarmContract = web3.eth.contract(address=bavaMasterFarm, abi=bavaMasterFarmAbi["abi"])

totalSupply = tokenContract.functions.totalSupply().call(block_identifier= 'latest')
print(web3.fromWei(totalSupply, 'ether'))
print(tokenContract.functions.name().call())
print("......")

load_dotenv()
infuraKey = os.getenv("INFURA_KEY")
mongoDBUser = os.getenv("MONGODB_USERNAME")
mongoDBPW = os.getenv("MONGODB_PASSWORD")

# ##########################################################################################################
# Query ERC20 transfer event
# ##########################################################################################################
def queryData():
# receipt = web3.eth.get_transaction_receipt("0x59c4f19ea4a6af4876f617419b812248bae8c5d915db5b6cc67ded5ede7ff593")   # or use tx_hash deifined on above command line
# event = proxyContract.events.Transfer().processReceipt(receipt, errors= DISCARD)
    response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=joe%2Cwrapped-avax%2Cpangolin%2Cweth%2Cusd-coin%2Ctether%2Cbenqi&vs_currencies=usd")
    responseJson = response.json()
    BAVAPrice = 0.10
    AVAXPrice = responseJson["wrapped-avax"]["usd"]
    PNGPrice = responseJson["pangolin"]["usd"]
    WETHPrice = responseJson["weth"]["usd"]
    USDTPrice = responseJson["tether"]["usd"]
    USDCPrice = responseJson["usd-coin"]["usd"]
    JOEPrice = responseJson["joe"]["usd"]
    QIPrice = responseJson["benqi"]["usd"]
    tvlArray=[]
    aprArray=[]
    apyArray=[]

    rewardPerBlock = bavaMasterFarmContract.functions.REWARD_PER_BLOCK().call()
    totalAllocPoint = bavaMasterFarmContract.functions.totalAllocPoint().call()

    for event in farm["farm"]:
        lpContract = web3.eth.contract(address=event["lpAddresses"]["43114"], abi=lpAbi["abi"])
        lpTokenA = web3.eth.contract(address=event["token"]["MAINNET"]["address"], abi=lpAbi["abi"])
        lpTokenB = web3.eth.contract(address=event["quoteToken"]["MAINNET"]["address"], abi=lpAbi["abi"])

        lpTokenInContract = bavaMasterFarmContract.functions.poolInfo(event["pid"]).call()
        lpTokenInContract = lpTokenInContract[4]

        lpTokenTSupply = lpContract.functions.totalSupply().call()
        lpTokenABalanceContract = lpTokenA.functions.balanceOf(event["lpAddresses"]["43114"]).call()
        lpTokenBBalanceContract = lpTokenB.functions.balanceOf(event["lpAddresses"]["43114"]).call()

        if event["token"]["MAINNET"]["symbol"] == "BAVA" :
            tokenAPrice = BAVAPrice
        elif event["token"]["MAINNET"]["symbol"] == "AVAX" :
            tokenAPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "PNG" :
            tokenAPrice = PNGPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDT.e") :
            tokenAPrice = USDTPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "WETH.e") :
            tokenAPrice = WETHPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDC.e") :
            tokenAPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "JOE") :
            tokenAPrice = JOEPrice
        elif (event["token"]["MAINNET"]["symbol"] == "QI") :
            tokenAPrice = QIPrice    

        if event["quoteToken"]["MAINNET"]["symbol"] == "BAVA" :
            tokenBPrice = BAVAPrice
        if event["quoteToken"]["MAINNET"]["symbol"] == "AVAX" :
            tokenBPrice = AVAXPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "PNG" :
            tokenBPrice = PNGPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDT.e" :
            tokenBPrice = USDTPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "WETH.e" :
            tokenBPrice = WETHPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDC.e" :
            tokenBPrice = USDCPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "JOE" :
            tokenBPrice = JOEPrice
        elif (event["quoteToken"]["MAINNET"]["symbol"] == "QI") :
            tokenBPrice = QIPrice 

        lpTokenValue = ((lpTokenABalanceContract * tokenAPrice) + (lpTokenBBalanceContract * tokenBPrice)) / lpTokenTSupply
        if event["lpTokenPairsymbol"] == "XJOE" or event["lpTokenPairsymbol"] == "PNG" :
            tvl = web3.fromWei(tokenAPrice * lpTokenInContract, 'ether')
        else:
            tvl = web3.fromWei(lpTokenValue * lpTokenInContract, 'ether')
        apr = ((28000 * 365 * 315 * event["allocPoint"] * web3.fromWei(rewardPerBlock, 'ether') * decimal.Decimal(BAVAPrice) ) / (tvl * totalAllocPoint)) * 100
        apyDaily = ((1 + apr/36500*8/10)**365 -1) * 100
        apyWeekly = ((1 + apr/5200*8/10)**52 -1) * 100
        apyMonthly = ((1 + apr/1200*8/10)**12 -1) * 100

        tvl = {"tvl":str(tvl)}
        apr = {"apr":str(apr)}
        apyDaily = {"apyDaily":str(apyDaily)}

        tvlArray.append(tvl)
        aprArray.append(apr)
        apyArray.append(apyDaily)

        print(tvlArray)


    with open("TVL.json", 'w') as tvl_file:
        tvlFile = {"tvl":tvlArray}
        json.dump(tvlFile, tvl_file, indent=4)
    
    with open("APR.json", 'w') as apr_file:
        aprFile = {"apr":aprArray}
        json.dump((aprFile), apr_file, indent=4)

    with open("APYDaily.json", 'w') as apy_file:
        apyFile = {"apyDaily":apyArray}
        json.dump((apyFile), apy_file, indent=4)



##############################################################################################################
# Update and Retreive BDL Total and Past 30 Days Amount from MongoDB
##############################################################################################################

def connectDB():
    # CONNECTION_STRING = "mongodb+srv://"+mongoDBUser+":"+mongoDBPW+"@pundix.ruhha.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    CONNECTION_STRING = "mongodb+srv://"+mongoDBUser+":"+urllib.parse.quote(mongoDBPW)+"@cluster0.adqfx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING)
    return client['TVLAmount']

def updateDB():
    dbName = connectDB()
    collectionName1 = dbName["TVL"]
    collectionName2 = dbName["APR"]
    collectionName3 = dbName["APYDaily"]

    with open('TVL.json') as tvl:
        data1 = json.load(tvl)
        print(data1)
        collectionName1.delete_many({})
        if isinstance(data1, list):
            collectionName1.insert_many(data1)  
        else:
            collectionName1.insert_one(data1)
    
    with open('APR.json') as apr:
        data2 = json.load(apr)
        print(data2)
        collectionName2.delete_many({})
        if isinstance(data2, list):
            collectionName2.insert_many(data2)  
        else:
            collectionName2.insert_one(data2)

    with open('APYDaily.json') as apyDaily:
        data3 = json.load(apyDaily)
        print(data3)
        collectionName3.delete_many({})
        if isinstance(data3, list):
            collectionName3.insert_many(data3)  
        else:
            collectionName3.insert_one(data3)
    

def getDB():
    dbName = connectDB() 
    collectionName1 = dbName["TVL"]
    collectionName2 = dbName["APR"]
    collectionName3 = dbName["APYDaily"]
    print("done")

    cursor1 = collectionName1.find({})
    for data1 in cursor1:
        totalTransferAmount = data1["tvl"]
        print(totalTransferAmount)

    cursor2 = collectionName2.find({})
    for data2 in cursor2:
        totalTransferAmount30Days = data2["apr"]
        print(totalTransferAmount30Days)
        
    cursor3 = collectionName3.find({})
    for data3 in cursor3:
        totalBurnAmount = data3["apyDaily"]
        print(totalBurnAmount)



# #############################################################################################################
# Main code
# #############################################################################################################
def main():
    #11732360 (Oct-13-2021 09:01:59 AM +UTC) Purse contract created time
    
    # queryData()
    connectDB()
    updateDB()
    # getDB()

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":     # __name__ is a built-in variable in Python which evaluates to the name of the current module.
    main()

schedule.every().hour.do(queryData)
schedule.every().hour.do(updateDB)

while True:
    schedule.run_pending()
    time.sleep(1)