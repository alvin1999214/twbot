import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

async def main():
    print("=== Userbot Session 產生工具 ===")
    api_id_input = input("請輸入 API ID (USERBOT_KEY): ").strip()
    api_hash_input = input("請輸入 API HASH (USERBOT_HASH): ").strip()

    if not api_id_input or not api_hash_input:
        print("❌ 錯誤: API ID 和 API HASH 不可為空")
        return

    try:
        api_id = int(api_id_input)
    except ValueError:
        print("❌ 錯誤: API ID 必須是數字")
        return

    client = TelegramClient(StringSession(), api_id, api_hash_input)
    await client.start()

    print("\n" + "="*50)
    print("✅ 授權成功！請複製下方的 Session String：\n")
    print(client.session.save())
    print("\n" + "="*50)
    print("請將上方這段字串填入 docker-compose.yml 的 USERBOT_SESSION 變數中。")
    
    await client.disconnect()

if __name__ == '__main__':
    # 請確保本地端已安裝 telethon 套件: pip install telethon
    asyncio.run(main())