import asyncio
from telegram import Bot

async def get_chat_id(api_token, username):
    try:
        bot = Bot(token=api_token)
        user = await bot.get_chat(username)
        return user.id
    except Exception as e:
        print(f"Error: {e}")
        return None

async def send_message_to_user(api_token, chat_id, message):
    try:
        bot = Bot(token=api_token)
        await bot.send_message(chat_id=chat_id, text=message)
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
async def main():
    api_token = '6507458271:AAHYhg1ilLrhrMSPb1zglG_rwNkalgN1jFo'  # Replace with your actual Telegram Bot API token
    username = 'cuong1i05'  # Replace with the username of the user you want to message

    chat_id = await get_chat_id(api_token, username)
    message = "warning"
    if chat_id:
        print(f"Chat ID: {chat_id}")
        # Send a message to the user
        result = await send_message_to_user(api_token, chat_id, message)
        print(f"Message sent: {result}")
    else:
        print("User not found")


if __name__ == "__main__":
    asyncio.run(main())