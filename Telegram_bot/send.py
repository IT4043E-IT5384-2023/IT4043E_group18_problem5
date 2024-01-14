import requests

jokes = ["joke 1", "joke 2", "Joke 3"]

for joke in jokes:
 base_url = ""https://api.telegram.org/bot6507458271:AAHYhg1ilLrhrMSPb1zglG_rwNkalgN1jFo/sendMessage?chat_id=6048644912&text=joke"
 print("Message is sent")