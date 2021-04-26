from datetime import timedelta

Config = {
    "Start_Link": "https://flinkhub.com/explore?category=product+management",

    "User_Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36",

    "Crawling_Time": timedelta(days=1, hours=0, minutes=0),

    "Links_Limit": 5000,

    "Parallel_Threads": 5,

    "Wait_Time": 5,

    "URI": 'mongodb://localhost:27017/', # on local host
    

    "Database_Name": "My_Database"
}
