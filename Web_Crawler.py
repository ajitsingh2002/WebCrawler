import requests                 # for sending http requests
from bs4 import BeautifulSoup   # for parsing html
from cfg import Config          # for configuration of inputs
from _datetime import datetime  # for getting current time
import time                     # for sleep
import pymongo                  # for accessing database
import random                   # for creating randomness
import string                   # for creating string from random
import os                       # for deleting html file on disk
import urllib.request           # for checking internet connection
import threading                # for implementing multi-threading
from logger import logger       # for logging output


# region Connecting to Database
connection = pymongo.MongoClient(Config["URI"])
db = connection[Config["Database_Name"]]
collection = db.Links
# endregion

headers = {'User-Agent': Config['User_Agent']}

# region Local variables
linkDicts = [{} for i in range(Config["Parallel_Threads"])]
linksList = []
sourceList = []
while True:
    try:
        count = collection.count_documents({})
        logger.info("Number of documents in database are " + str(count))
        break
    except KeyboardInterrupt:
        logger.info("Program closed manually")
        exit()
    except:
        logger.warning("Can not connect to database")
        logger.info("Retrying...")
# endregion


# region Initialize data from database
def InitializeData():
    try:
        logger.debug("Initializing data from database...")
        cursor = collection.find()
        for record in cursor:
            linksList.append(record.get("linksFound"))
            sourceList.append(record.get("sourceLink"))
        logger.debug("Data initialized")
    except KeyboardInterrupt:
        logger.info("Program closed manually")
        exit()
    except:
        logger.warning("Can not connect to database")
        logger.info("Retrying...")
        InitializeData()
InitializeData()
# endregion


websites = ['http://google.com', 'http://facebook.com', 'http://instagram.com',
            'http://youtube.com', 'http://flinkhub.com']
def isConnected(Id):
    try:
        urllib.request.urlopen(websites[random.randint(0, len(websites) - 1)])
        return True
    except KeyboardInterrupt:
        exit()
    except:
        try:
            if Id == 0:
                logger.warning("No access to internet!")
                logger.info("Retrying in " + str(Config["Wait_Time"]) + " seconds")
            time.sleep(Config["Wait_Time"])
        except KeyboardInterrupt:
            exit()
        return isConnected(Id)


def GetRandomName(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for j in range(length))
    return result_str


def AddData(Id, link, sourceLink, isCrawled, statusCode, contentType, contentLength, rText, linksFound):
    linkDicts[Id]["link"] = link
    linkDicts[Id]["sourceLink"] = sourceLink
    linkDicts[Id]["isCrawled"] = isCrawled

    timeStamp = datetime.now()

    lastCrawledDate = linkDicts[Id].get("crawlDate")
    linkDicts[Id]["crawlDate"] = timeStamp
    if lastCrawledDate is None:
        linkDicts[Id]["lastCrawledDate"] = "N/A"
    else:
        linkDicts[Id]["lastCrawledDate"] = lastCrawledDate

    linkDicts[Id]["responseStatus"] = statusCode
    linkDicts[Id]["contentType"] = contentType
    linkDicts[Id]["contentLength"] = contentLength

    filePath = linkDicts[Id].get("filePath")
    try:
        os.remove(filePath)
    except:
        pass
    randomName = GetRandomName(random.randint(5, 10))
    f = open("html_files/" + randomName + ".html", "w", encoding='utf-8')       # Used relative file path for for storing files
    f.write(rText)
    linkDicts[Id]["filePath"] = f.name

    createdAt = linkDicts[Id].get("createdAt")
    if createdAt is None:
        linkDicts[Id]["createdAt"] = timeStamp
    else:
        linkDicts[Id]["createdAt"] = createdAt

    linkDicts[Id]["linksFound"] = linksFound

    global count
    if count >= Config["Links_Limit"]:
        return
    while True:
        try:
            collection.insert_one(linkDicts[Id])
            linkDicts[Id].clear()
            break
        except KeyboardInterrupt:
            exit()
        except:
            if Id == 0:
                logger.warning("Can not connect to database")
                logger.info("Retrying...")

    count = count + 1


def WebScrapper(URL, sourceLink, Id=0):
    global count
    global linkDicts
    if URL[-1] == "/":
        URL = URL[:-1]

    cursor = collection.find()
    try:
        for record in cursor:
            if record.get("link") == URL:
                currentDate = datetime.now()
                crawledDate = record["crawlDate"]
                timeDifference = currentDate - crawledDate
                if record["isCrawled"] and timeDifference < Config["Crawling_Time"]:
                    logger.debug(URL + " Already Crawled")
                    return
                elif record["isCrawled"] and timeDifference >= Config["Crawling_Time"]:
                    linkDicts[Id] = record
                    del linkDicts[Id]["_id"]
                    if linkDicts[Id]["sourceLink"] != "Manual Input":
                        sourceLink = linkDicts[Id]["sourceLink"]
                    collection.delete_one(record)
                    count -= 1
                    break
                if not record["isCrawled"]:
                    linkDicts[Id] = record
                    del linkDicts[Id]["_id"]
                    sourceLink = linkDicts[Id]["sourceLink"]
                    collection.delete_one(record)
                    count -= 1
                    break
    except KeyboardInterrupt:
        exit()
    except:
        if Id == 0:
            logger.warning("Can not connect to database")
            logger.info("Retrying...")
        WebScrapper(URL, sourceLink, Id)


    try:
        r = requests.get(URL, headers=headers)
        logger.debug("Crawling " + URL)
    except KeyboardInterrupt:
        exit()
    except:
        connected = isConnected(Id)
        if connected:
            return
        elif not connected:
            WebScrapper(URL, sourceLink, Id)
    statusCode = r.status_code
    contentType = r.headers['content-type']
    contentLength = len(r.content)
    rText = r.text

    soup = BeautifulSoup(rText, 'html.parser')
    aTag = soup.find_all("a")
    Links = []

    for a in aTag:
        try:
            link = a['href']
        except:
            continue

        if len(link) > 1 and link[0] == "/":
            link = requests.compat.urljoin(URL, link)
            if link[-1] == "/":
                link = link[:-1]
        elif link[:4] == "http":
            link = link
            if link[-1] == "/":
                link = link[:-1]
        else:
            continue
        Links.append(link)

    AddData(Id, URL, sourceLink, True, statusCode, contentType, contentLength, rText, Links)
    linksList.append(Links)
    sourceList.append(URL)


if count >= Config["Links_Limit"]:
    print("Database contain maximum number of documents")
    exit()
WebScrapper(Config["Start_Link"], "Manual Input")

def Scrap(Id):
    global linksList
    global sourceList
    while True:
        try:
            if len(linksList) > 0 and len(linksList[0]) > 0:
                URL = linksList[0].pop(0)
                WebScrapper(URL, sourceList[0], Id)
                if len(linksList[0]) == 0:
                    linksList.pop(0)
                    sourceList.pop(0)
                    time.sleep(Config["Wait_Time"])

            elif len(linksList) > 0 and len(linksList[0]) == 0:
                linksList.pop(0)
                sourceList.pop(0)
                time.sleep(Config["Wait_Time"])
        except KeyboardInterrupt:
            exit()
        except:
            linksList.pop(0)
            sourceList.pop(0)

        if len(linksList) == 0:
            print("All links crawled")
            break

        if count >= Config["Links_Limit"]:
            print("Maximum limit reached")
            break


class MyThread(threading.Thread):
    def __init__(self, Id):
        threading.Thread.__init__(self)
        self.id = Id

    def run(self):
        Scrap(self.id)


threads = [MyThread(i) for i in range(Config["Parallel_Threads"])]
for thread in threads:
    thread.start()
