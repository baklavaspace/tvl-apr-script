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

full_path = os.getcwd()
# Load BAVAABI data
bavaJson = open(full_path+'/abi/'+'Bava.json')
bavaAbi = json.load(bavaJson)

# Load LpTokenABI data
lpJson = open(full_path+'/abi/'+'LpToken.json')
lpAbi = json.load(lpJson)

# Load bavaMasterFarmAbi data
bavaMasterFarmJson = open(full_path+'/abi/'+'BavaMasterFarm.json')
bavaMasterFarmAbi = json.load(bavaMasterFarmJson)
bavaMasterFarmV1Json = open(full_path+'/abi/'+'BavaMasterFarmV1.json')
bavaMasterFarmV1Abi = json.load(bavaMasterFarmV1Json)
bavaMasterFarmV2_2Json = open(full_path+'/abi/'+'BavaMasterFarmV2_2.json')
bavaMasterFarmV2_2Abi = json.load(bavaMasterFarmV2_2Json)

# Load Pool data
farmJson = open(full_path+'/farm/'+'farm.json')
farm = json.load(farmJson)
farmV1Json = open(full_path+'/farm/'+'farmV1.json')
farmV1 = json.load(farmV1Json)
farmV2_2Json = open(full_path+'/farm/'+'farmV2_2.json')
farmV2_2 = json.load(farmV2_2Json)

bavaPGL = '0xeB69651B7146F4A42EBC32B03785C3eEddE58Ee7'
bavaAddress = '0xe19A1684873faB5Fb694CfD06607100A632fF21c'
avaxAddress = '0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7'
bavaContract = web3.eth.contract(address=bavaAddress, abi=bavaAbi["abi"])
avaxContract = web3.eth.contract(address=avaxAddress, abi=bavaAbi["abi"])

bavaMasterFarm = "0xb5a054312A73581A3c0FeD148b736911C02f4539"
bavaMasterFarmContract = web3.eth.contract(address=bavaMasterFarm, abi=bavaMasterFarmAbi["abi"])
bavaMasterFarmV1 = "0x221C6774CF60277b36D21a57A31154EC96299d50"
bavaMasterFarmContractV1 = web3.eth.contract(address=bavaMasterFarmV1, abi=bavaMasterFarmV1Abi["abi"])
bavaMasterFarmV2_2 = "0xfD6b09A76f81c83F7E7D792070Ab2A05550F887C"
bavaMasterFarmContractV2_2 = web3.eth.contract(address=bavaMasterFarmV2_2, abi=bavaMasterFarmV2_2Abi["abi"])

totalSupply = bavaContract.functions.totalSupply().call(block_identifier= 'latest')
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
    response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=joe%2Cwrapped-avax%2Cpangolin%2Cweth%2Cbaklava%2Cusd-coin%2Ctether%2Cbenqi&vs_currencies=usd")
    responseJson = response.json()

    AVAXPrice = responseJson["wrapped-avax"]["usd"]
    BAVAPrice = responseJson["baklava"]["usd"]
    PNGPrice = responseJson["pangolin"]["usd"]
    WETHPrice = responseJson["weth"]["usd"]
    USDTPrice = responseJson["tether"]["usd"]
    USDCPrice = responseJson["usd-coin"]["usd"]
    JOEPrice = responseJson["joe"]["usd"]
    QIPrice = responseJson["benqi"]["usd"]
    tvlArray=[]
    aprArray=[]
    apyArray=[]
    returnRatioArray=[]
    tvlArrayV2_2=[]
    aprArrayV2_2=[]
    apyArrayV2_2=[]
    returnRatioArrayV2_2=[]
    bavatvlArray=[]
    bavaaprArray=[]
    bavaapyArray=[]

    rewardPerBlock = bavaMasterFarmContract.functions.REWARD_PER_BLOCK().call()
    rewardPerBlockV1 = bavaMasterFarmContractV1.functions.REWARD_PER_BLOCK().call()
    rewardPerBlockV2_2 = bavaMasterFarmContractV2_2.functions.REWARD_PER_BLOCK().call()
    totalAllocPoint = bavaMasterFarmContract.functions.totalAllocPoint().call()
    totalAllocPointV1 = bavaMasterFarmContractV1.functions.totalAllocPoint().call()
    totalAllocPointV2_2 = bavaMasterFarmContractV2_2.functions.totalAllocPoint().call()
    

    for event in farm["farm"]:
        lpContract = web3.eth.contract(address=event["lpAddresses"]["43114"], abi=lpAbi["abi"])
        lpTokenA = web3.eth.contract(address=event["token"]["MAINNET"]["address"], abi=lpAbi["abi"])
        lpTokenB = web3.eth.contract(address=event["quoteToken"]["MAINNET"]["address"], abi=lpAbi["abi"])

        lpTokenInContract = bavaMasterFarmContract.functions.poolInfo(event["pid"]).call()
        lpReceiptInContract = lpTokenInContract[5]
        lpTokenInContract = lpTokenInContract[4]
        returnRatio = lpTokenInContract/lpReceiptInContract

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

        apr = ((28000 * 365 * 682 * event["allocPoint"] * web3.fromWei(rewardPerBlock, 'ether') * decimal.Decimal(BAVAPrice) ) / (tvl * totalAllocPoint)) * 100
        apyDaily = ((1 + apr/36500)**365 -1) * 100
        apyWeekly = ((1 + apr/5200)**52 -1) * 100
        apyMonthly = ((1 + apr/1200)**12 -1) * 100

        tvl = {"tvl":str(tvl)}
        apr = {"apr":str(apr)}
        apyDaily = {"apyDaily":str(apyDaily)}
        returnRatio={"returnRatio":str(returnRatio)}

        tvlArray.append(tvl)
        aprArray.append(apr)
        apyArray.append(apyDaily)
        returnRatioArray.append(returnRatio)

    for event in farmV1["farm"]:
        lpContract = web3.eth.contract(address=event["lpAddresses"]["43114"], abi=lpAbi["abi"])
        lpTokenA = web3.eth.contract(address=event["token"]["MAINNET"]["address"], abi=lpAbi["abi"])
        lpTokenB = web3.eth.contract(address=event["quoteToken"]["MAINNET"]["address"], abi=lpAbi["abi"])

        lpTokenInContract = bavaMasterFarmContractV1.functions.poolInfo(event["pid"]).call()
        lpTokenInContract = lpTokenInContract[4]

        lpTokenTSupply = lpContract.functions.totalSupply().call()
        lpTokenABalanceContract = lpTokenA.functions.balanceOf(event["lpAddresses"]["43114"]).call()
        lpTokenBBalanceContract = lpTokenB.functions.balanceOf(event["lpAddresses"]["43114"]).call()

        if event["token"]["MAINNET"]["symbol"] == "BAVA" :
            tokenAPrice = BAVAPrice
        elif event["token"]["MAINNET"]["symbol"] == "AVAX" :
            tokenAPrice = AVAXPrice 

        if event["quoteToken"]["MAINNET"]["symbol"] == "BAVA" :
            tokenBPrice = BAVAPrice
        if event["quoteToken"]["MAINNET"]["symbol"] == "AVAX" :
            tokenBPrice = AVAXPrice

        lpTokenValue = ((lpTokenABalanceContract * tokenAPrice) + (lpTokenBBalanceContract * tokenBPrice)) / lpTokenTSupply
        if event["lpTokenPairsymbol"] == "XJOE" or event["lpTokenPairsymbol"] == "PNG" :
            tvl = web3.fromWei(tokenAPrice * lpTokenInContract, 'ether')
        else:
            tvl = web3.fromWei(lpTokenValue * lpTokenInContract, 'ether')
        
        if tvl == 0 :
            apr = ""
            apyDaily = ""
            apyMonthly = ""
        else:
            apr = ((28000 * 365 * 682 * event["allocPoint"] * web3.fromWei(rewardPerBlockV1, 'ether') * decimal.Decimal(BAVAPrice) ) / (tvl * totalAllocPointV1)) * 100
            apyDaily = ((1 + apr/36500)**365 -1) * 100
            apyWeekly = ((1 + apr/5200)**52 -1) * 100
            apyMonthly = ((1 + apr/1200)**12 -1) * 100

        bavatvl = {"tvl":str(tvl)}
        bavaapr = {"apr":str(apr)}
        bavaapyDaily = {"apyDaily":str(apyDaily)}

        bavatvlArray.append(bavatvl)
        bavaaprArray.append(bavaapr)
        bavaapyArray.append(bavaapyDaily)


    for event in farmV2_2["farm"]:
        lpContract = web3.eth.contract(address=event["lpAddresses"]["43114"], abi=lpAbi["abi"])
        lpTokenA = web3.eth.contract(address=event["token"]["MAINNET"]["address"], abi=lpAbi["abi"])
        lpTokenB = web3.eth.contract(address=event["quoteToken"]["MAINNET"]["address"], abi=lpAbi["abi"])

        lpTokenInContract = bavaMasterFarmContractV2_2.functions.poolInfo(event["pid"]).call()
        lpReceiptInContract = lpTokenInContract[5]
        lpTokenInContract = lpTokenInContract[4]
        if lpReceiptInContract == 0 :
            returnRatio = 1
        else: 
            returnRatio = lpTokenInContract/lpReceiptInContract

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

        if tvl == 0 :
            apr = ""
            apyDaily = ""
            apyMonthly = ""
        else:
            apr = ((28000 * 365 * 682 * event["allocPoint"] * web3.fromWei(rewardPerBlockV2_2, 'ether') * decimal.Decimal(BAVAPrice) ) / (tvl * totalAllocPointV2_2)) * 100
            apyDaily = ((1 + apr/36500)**365 -1) * 100
            apyWeekly = ((1 + apr/5200)**52 -1) * 100
            apyMonthly = ((1 + apr/1200)**12 -1) * 100

        tvlV2_2 = {"tvl":str(tvl)}
        aprV2_2 = {"apr":str(apr)}
        apyDailyV2_2 = {"apyDaily":str(apyDaily)}
        returnRatioV2_2 = {"returnRatio":str(returnRatio)}

        tvlArrayV2_2.append(tvlV2_2)
        aprArrayV2_2.append(aprV2_2)
        apyArrayV2_2.append(apyDailyV2_2)
        returnRatioArrayV2_2.append(returnRatioV2_2)

# **************************************** Update data ******************************************************

    with open("TVL.json", 'w') as tvl_file:
        tvlFile = {"tvl":tvlArray}
        json.dump(tvlFile, tvl_file, indent=4)
    
    with open("APR.json", 'w') as apr_file:
        aprFile = {"apr":aprArray}
        json.dump((aprFile), apr_file, indent=4)

    with open("APYDaily.json", 'w') as apy_file:
        apyFile = {"apyDaily":apyArray}
        json.dump((apyFile), apy_file, indent=4)

    with open("ReturnRatio.json", 'w') as return_file:
        returnFile = {"returnRatio":returnRatioArray}
        json.dump(returnFile, return_file, indent=4) 

    with open("TVLV2_2.json", 'w') as tvl_file:
        tvlFile = {"tvl":tvlArrayV2_2}
        json.dump(tvlFile, tvl_file, indent=4)
    
    with open("APRV2_2.json", 'w') as apr_file:
        aprFile = {"apr":aprArrayV2_2}
        json.dump((aprFile), apr_file, indent=4)

    with open("APYDailyV2_2.json", 'w') as apy_file:
        apyFile = {"apyDaily":apyArrayV2_2}
        json.dump((apyFile), apy_file, indent=4)

    with open("ReturnRatioV2_2.json", 'w') as return_file:
        returnFile = {"returnRatio":returnRatioArrayV2_2}
        json.dump(returnFile, return_file, indent=4) 

    with open("BAVATVL.json", 'w') as bavatvl_file:
        bavatvlFile = {"tvl":bavatvlArray}
        json.dump(bavatvlFile, bavatvl_file, indent=4)
    
    with open("BAVAAPR.json", 'w') as bavaapr_file:
        bavaaprFile = {"apr":bavaaprArray}
        json.dump(bavaaprFile, bavaapr_file, indent=4)

    with open("BAVAAPYDaily.json", 'w') as bavaapy_file:
        bavaapyFile = {"apyDaily":bavaapyArray}
        json.dump(bavaapyFile, bavaapy_file, indent=4)

    with open("BAVAPrice.json", 'w') as bava_file:
        bavaFile = {"bavaPrice":BAVAPrice}
        json.dump(bavaFile, bava_file, indent=4)

     



##############################################################################################################
# Update and Retreive BDL Total and Past 30 Days Amount from MongoDB
##############################################################################################################

def connectDB():
    # CONNECTION_STRING = "mongodb+srv://"+mongoDBUser+":"+mongoDBPW+"@pundix.ruhha.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    CONNECTION_STRING = "mongodb+srv://"+mongoDBUser+":"+urllib.parse.quote(mongoDBPW)+"@cluster0.adqfx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    # s = MongoClient("mongodb+srv://"+mongoDBUser+":"+urllib.parse.quote(mongoDBPW)+"@cluster0.adqfx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", tlsCAFile=certifi.where())
    client = MongoClient(CONNECTION_STRING,tls=True, tlsAllowInvalidCertificates=True)
    return client['TVLAmount']

def updateDB():
    dbName = connectDB()
    collectionName1 = dbName["TVL"]
    collectionName2 = dbName["APR"]
    collectionName3 = dbName["APYDaily"]
    collectionName4 = dbName["BAVAPrice"]
    collectionName5 = dbName["BAVATVL"]
    collectionName6 = dbName["BAVAAPR"]
    collectionName7 = dbName["BAVAAPYDaily"]
    collectionName8 = dbName["ReturnRatio"]
    collectionName9 = dbName["TVLV2_2"]
    collectionName10 = dbName["APRV2_2"]
    collectionName11 = dbName["APYDailyV2_2"]
    collectionName12 = dbName["ReturnRatioV2_2"]
    print("start")

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

    with open("BAVAPrice.json") as bavaPrice:
        data4 = json.load(bavaPrice)
        print(data4)
        collectionName4.delete_many({})
        if isinstance(data4, list):
            collectionName4.insert_many(data4)  
        else:
            collectionName4.insert_one(data4)

    with open('BAVATVL.json') as tvl:
        data5 = json.load(tvl)
        print(data5)
        collectionName5.delete_many({})
        if isinstance(data5, list):
            collectionName5.insert_many(data5)  
        else:
            collectionName5.insert_one(data5)
    
    with open('BAVAAPR.json') as apr:
        data6 = json.load(apr)
        print(data6)
        collectionName6.delete_many({})
        if isinstance(data6, list):
            collectionName6.insert_many(data6)  
        else:
            collectionName6.insert_one(data6)

    with open('BAVAAPYDaily.json') as apyDaily:
        data7 = json.load(apyDaily)
        print(data7)
        collectionName7.delete_many({})
        if isinstance(data7, list):
            collectionName7.insert_many(data7)  
        else:
            collectionName7.insert_one(data7)

    with open('ReturnRatio.json') as returnRatio:
        data8 = json.load(returnRatio)
        print(data8)
        collectionName8.delete_many({})
        if isinstance(data8, list):
            collectionName8.insert_many(data8)  
        else:
            collectionName8.insert_one(data8)

    with open('TVLV2_2.json') as tvl:
        data9 = json.load(apr)
        print(data9)
        collectionName9.delete_many({})
        if isinstance(data9, list):
            collectionName9.insert_many(data9)  
        else:
            collectionName9.insert_one(data9)

    with open('APRV2_2.json') as apr:
        data10 = json.load(apyDaily)
        print(data10)
        collectionName10.delete_many({})
        if isinstance(data10, list):
            collectionName10.insert_many(data10)  
        else:
            collectionName10.insert_one(data10)

    with open('APYDailyV2_2.json') as apyDaily:
        data11 = json.load(tvl)
        print(data11)
        collectionName11.delete_many({})
        if isinstance(data11, list):
            collectionName11.insert_many(data11)  
        else:
            collectionName11.insert_one(data11)

    with open('ReturnRatioV2_2.json') as returnRatio:
        data12 = json.load(returnRatio)
        print(data12)
        collectionName12.delete_many({})
        if isinstance(data12, list):
            collectionName12.insert_many(data12)
        else:
            collectionName12.insert_one(data12)

def getDB():
    dbName = connectDB() 
    collectionName1 = dbName["TVL"]
    collectionName2 = dbName["APR"]
    collectionName3 = dbName["APYDaily"]
    collectionName4 = dbName["BAVAPrice"]
    collectionName5 = dbName["BAVATVL"]
    collectionName6 = dbName["BAVAAPR"]
    collectionName7 = dbName["BAVAAPYDaily"]
    collectionName8 = dbName["ReturnRatio"]
    print("done")

    cursor1 = collectionName1.find({})
    for data1 in cursor1:
        tvl = data1["tvl"]
        print(tvl)

    cursor2 = collectionName2.find({})
    for data2 in cursor2:
        apr = data2["apr"]
        print(apr)
        
    cursor3 = collectionName3.find({})
    for data3 in cursor3:
        apy = data3["apyDaily"]
        print(apy)

    cursor4 = collectionName4.find({})
    for data4 in cursor4:
        bavaPrice = data4["bavaPrice"]
        print(bavaPrice)

    cursor5 = collectionName5.find({})
    for data5 in cursor5:
        tvl = data5["tvl"]
        print(tvl)

    cursor6 = collectionName6.find({})
    for data6 in cursor6:
        apr = data6["apr"]
        print(apr)
        
    cursor7 = collectionName7.find({})
    for data7 in cursor7:
        apy = data7["apyDaily"]
        print(apy)

    cursor8 = collectionName8.find({})
    for data8 in cursor8:
        returnRatio = data8["returnRatio"]
        print(returnRatio)

# #############################################################################################################
# Main code
# #############################################################################################################
def main():
    #11732360 (Oct-13-2021 09:01:59 AM +UTC) Purse contract created time
    
    queryData()
    connectDB()
    updateDB()
    # getDB()

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":     # __name__ is a built-in variable in Python which evaluates to the name of the current module.
    main()

schedule.every(15).minutes.do(queryData)
schedule.every(15).minutes.do(updateDB)

while True:
    schedule.run_pending()
    time.sleep(1)