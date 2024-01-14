import asyncio
from telegram import Bot

async def send_telegram_message(api_token, chat_id, message):
    try:
        bot = Bot(token=api_token)
        await bot.send_message(chat_id=chat_id, text=message)
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
def construct_warning_message_1(sender_address):
    return f'Warning!!! This transaction from {sender_address} is risky.'
def construct_warning_message_2(reciver_address):
    return f'Warning!!! This transaction to {reciver_address} is risky.'
async def main():
    address_rík = ["0xda26bd4f89dff5dfa0a84f0f0ccfc3c73bd0b7d7","0x57469550b9a42d2fd964e67a9dd1de3d9169b291","0x57f4d3071e99d0a4baef0b274526215f939a6575","0xab324146c49b23658e5b3930e641bdbdf089cbac"]
    api_token = '6507458271:AAHYhg1ilLrhrMSPb1zglG_rwNkalgN1jFo'  # Replace with your Telegram Bot API token
    chat_id = '6977671358'  # Replace with the chat ID of the user you want to message
    address = "0xda26bd4f89dff5dfa0a84f0f0ccfc3c73bd0b7d7"
    message = construct_warning_message_1(address)

    check_ = await send_telegram_message(api_token, chat_id, message)
    print(check_)

if __name__ == "__main__":
    asyncio.run(main())

