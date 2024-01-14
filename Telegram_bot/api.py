from flask import Flask, jsonify, request
import json
import requests
import random
import string
from datetime import datetime
import asyncio
from telegram import Bot

import csv

def read_csv_to_list(file_path):
    data_list = []

    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            data_list = [row[0] for row in csv_reader]

    return data_list
async def send_telegram_message(api_token, chat_id, message):
    try:
        bot = Bot(token=api_token)
        await bot.send_message(chat_id=chat_id, text=message)
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
def construct_warning_message_recive(sender_address,net_):
    return f'Warning!!! The user address {sender_address} send transaction in {net_} space to you is at risk .'
def construct_warning_message_send(reciver_address,net_):
    return f'Warning!!! The user address {reciver_address} you send transaction in {net_} space to is at risk.'
async def send_warning_message(command,risk_address,net_):
    api_token = '6507458271:AAHYhg1ilLrhrMSPb1zglG_rwNkalgN1jFo'  # Replace with your Telegram Bot API token
    chat_id = '6977671358'  # Replace with the chat ID of the user you want to message
    if(str(command) == "recive"):
        message = construct_warning_message_recive(risk_address,net_)
    else:
        message = construct_warning_message_send(risk_address,net_)

    check_ = await send_telegram_message(api_token, chat_id, message)
    print(check_)
def generate_random_address(list):
    return random.choice(list)

def generate_random_transaction(command,risk_):
    # address_risk = ["0xda26bd4f89dff5dfa0a84f0f0ccfc3c73bd0b7d7","0x57469550b9a42d2fd964e67a9dd1de3d9169b291","0x57f4d3071e99d0a4baef0b274526215f939a6575","0xab324146c49b23658e5b3930e641bdbdf089cbac"]
    address_user =  ["0xda26bd4f89dff5dfa0a84f0f0ccfc3c73bd0b7d7","0x57469550b9a42d2fd964e67a9dd1de3d9169b291","0x57f4d3071e99d0a4baef0b274526215f939a6575","0xab324146c49b23658e5b3930e641bdbdf089cbac"]
    # risk_ = generate_random_address(address_risk)
    user_ = generate_random_address(address_user)
    if(str(command) == "recive"):
        data = {
            "_id": f"transaction_{random.randint(100000, 999999)}",
            "type": "transaction",
            "hash": f"0x{random.getrandbits(256):x}",
            "nonce": random.randint(0, 100000),
            "transaction_index": 0,
            "from_address": risk_,
            "to_address": user_,
            "value": "0",
            "gas": str(random.randint(100000, 2000000)),
            "gas_price": str(random.randint(1000000000, 2000000000)),
            "input": f"0x{random.getrandbits(256):x}",
            "block_timestamp": int(datetime.timestamp(datetime.now())),
            "block_number": random.randint(1000000, 2000000),
            "block_hash": f"0x{random.getrandbits(256):x}",
            "receipt_cumulative_gas_used": str(random.randint(1000, 5000)),
            "receipt_gas_used": str(random.randint(100000, 1000000)),
            "receipt_contract_address": None,
            "receipt_root": None,
            "receipt_status": 1,
            "item_timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        }
    else:
        data = {
            "_id": f"transaction_{random.randint(100000, 999999)}",
            "type": "transaction",
            "hash": f"0x{random.getrandbits(256):x}",
            "nonce": random.randint(0, 100000),
            "transaction_index": 0,
            "from_address": user_,
            "to_address": risk_,
            "value": "0",
            "gas": str(random.randint(100000, 2000000)),
            "gas_price": str(random.randint(1000000000, 2000000000)),
            "input": f"0x{random.getrandbits(256):x}",
            "block_timestamp": int(datetime.timestamp(datetime.now())),
            "block_number": random.randint(1000000, 2000000),
            "block_hash": f"0x{random.getrandbits(256):x}",
            "receipt_cumulative_gas_used": str(random.randint(1000, 5000)),
            "receipt_gas_used": str(random.randint(100000, 1000000)),
            "receipt_contract_address": None,
            "receipt_root": None,
            "receipt_status": 1,
            "item_timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        }
    return data,risk_,user_

app = Flask(__name__)



@app.route('/warning', methods=['POST'])
async def warningTransaction():
    try:
        # Check if the POST request has a file part
        command = request.values['transaction']
        net_ = request.values['space']
        partner = request.values['with_user']
        # print(command)
        data, risk_, user_ = generate_random_transaction(command, partner)
        if(str(net_) == 'arbitrum'):
            risk_address = read_csv_to_list('final_outliers_arbi.csv')
            if(str(partner) in risk_address):
                check_ = await send_warning_message(command, risk_,net_)
        elif (str(net_) == 'fantom'):
            risk_address = read_csv_to_list('final_outliers_ftm.csv')
            if (str(partner) in risk_address):
                check_ = await send_warning_message(command, risk_,net_)
        elif (str(net_) == 'polygon'):
            risk_address = read_csv_to_list('final_outliers_poly.csv')
            if (str(partner) in risk_address):
                check_ = await send_warning_message(command, risk_,net_)
        else:
            print("ok")
            risk_address = read_csv_to_list('final_outliers_ether.csv')
            print(risk_address[1])
            print(str(partner))
            if (str(partner) in risk_address):
                print("ok")
                check_ = await send_warning_message(command, risk_,net_)



        return jsonify({'success': True,'warning': 'Are you sure you want to make this transaction?','transaction': data})

    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    # Replace '0.0.0.0' with your desired host IP and 5000 with your desired port

    app.run(host='0.0.0.0', port=5000)
