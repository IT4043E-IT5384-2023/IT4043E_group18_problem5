# IT4043E_group18_problem5
- First, run Crawl_data.ipynb to crawl necessary fields of data
- Second, change the path of transaction data that you store in the cloud in data_processing.py file, then run data_processing.py file.
- Third, dowload the processed data and up load to kaggle, and also upload outlier_detection.ipynb to get outlier users file.
* Demo in telegram bot:
First, install necessary libraries:
- pip install -r requirements
- Download Postman from https://www.postman.com/downloads/
- Get your chat_id from telegram
- In Telegram search @WarningTransactionBot , enter "/start"
- Replace with the chat ID of the user you want to message at line 35 of api.py 
- Run interminal : python api.py
- Link api : http://127.0.0.1:5000/warning
- Create a New Request in Postman and set the options as shown below
- ![api_image](https://github.com/IT4043E-IT5384-2023/IT4043E_group18_problem5/assets/90127093/9108a6c1-bb78-43f7-83d4-270e8386a854)

- Key :
- transaction : send or recive (text)
- space : arbitrum or fantom or polygon or ethereum (text)
- with_user : user addresses in the outlier files of the respective space in folder Telegram_bot (text)
  





