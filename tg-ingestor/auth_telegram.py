#!/usr/bin/env python3
"""
Telegram authentication utility for tg-ingestor.
This script helps authenticate and create a Telegram session file.
Run this BEFORE starting the main service.
"""

import asyncio
import sys
import os
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, ApiIdInvalidError, PhoneCodeInvalidError


async def authenticate_telegram(
    api_id: int,
    api_hash: str,
    session_path: str,
    phone_number: str
) -> bool:
    """
    Authenticate with Telegram and create session file.
    
    Args:
        api_id: Telegram API ID
        api_hash: Telegram API hash
        session_path: Path where to save session file
        phone_number: Phone number for authentication
        
    Returns:
        True if authentication successful, False otherwise
    """
    # Ensure session directory exists
    session_file = Path(session_path)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    
    client = TelegramClient(str(session_file), api_id, api_hash)
    
    try:
        print(f"Connecting to Telegram...")
        await client.connect()
        
        if await client.is_user_authorized():
            print("✅ Already authenticated!")
            me = await client.get_me()
            print(f"Logged in as: {me.first_name} (@{me.username})")
            return True
        
        print(f"Starting authentication for phone: {phone_number}")
        
        # Send code request
        print("Sending authentication code...")
        await client.send_code_request(phone_number)
        
        # Get verification code from user
        code = input("Enter the verification code you received: ").strip()
        
        try:
            # Sign in with code
            await client.sign_in(phone_number, code)
            
        except SessionPasswordNeededError:
            # Two-factor authentication enabled
            print("Two-factor authentication detected.")
            password = input("Enter your 2FA password: ").strip()
            await client.sign_in(password=password)
        
        except PhoneCodeInvalidError:
            print("❌ Invalid verification code!")
            return False
        
        # Check if authentication successful
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ Authentication successful!")
            print(f"Logged in as: {me.first_name} (@{me.username})")
            print(f"Session saved to: {session_path}")
            return True
        else:
            print("❌ Authentication failed!")
            return False
            
    except ApiIdInvalidError:
        print("❌ Invalid API ID or API hash!")
        print("Please check your credentials from https://my.telegram.org")
        return False
        
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return False
        
    finally:
        await client.disconnect()


def get_credentials_from_env():
    """Get Telegram credentials from environment variables."""
    api_id = os.getenv('TELEGRAM_API_ID')
    api_hash = os.getenv('TELEGRAM_API_HASH')
    session_path = os.getenv('TELEGRAM_SESSION', './data/session.session')
    
    if not api_id or not api_hash:
        return None, None, session_path
    
    try:
        api_id = int(api_id)
    except ValueError:
        return None, None, session_path
    
    return api_id, api_hash, session_path


def get_credentials_interactive():
    """Get Telegram credentials interactively from user."""
    print("Telegram API Credentials Setup")
    print("=" * 40)
    print("Get your credentials from: https://my.telegram.org")
    print()
    
    try:
        api_id = int(input("Enter your API ID: ").strip())
        api_hash = input("Enter your API Hash: ").strip()
        
        if len(api_hash) != 32:
            print("⚠️  Warning: API hash should be 32 characters long")
        
        session_path = input("Session file path [./data/session.session]: ").strip()
        if not session_path:
            session_path = "./data/session.session"
        
        return api_id, api_hash, session_path
        
    except ValueError:
        print("❌ Invalid API ID format!")
        return None, None, None

def get_credentials_from_secrets():
    """Get Telegram credentials from Docker secrets files."""
    try:
        # Try to load from Docker secrets first
        api_id_path = Path("/run/secrets/telegram_api_id")
        api_hash_path = Path("/run/secrets/telegram_api_hash")
        
        if api_id_path.exists() and api_hash_path.exists():
            with open(api_id_path, 'r') as f:
                api_id = int(f.read().strip())
            
            with open(api_hash_path, 'r') as f:
                api_hash = f.read().strip()
            
            session_path = os.getenv('TELEGRAM_SESSION', '/app/data/session.session')
            
            print(f"✅ Loaded credentials from Docker secrets")
            return api_id, api_hash, session_path
            
    except (FileNotFoundError, ValueError, OSError):
        pass
    
    return None, None, None

async def main():
    """Main function for the authentication script."""
    print("Telegram Authentication Setup for tg-ingestor")
    print("=" * 50)
    
    # Try to get credentials from Docker secrets first
    api_id, api_hash, session_path = get_credentials_from_secrets()
    
    if not api_id or not api_hash:
        print("Docker secrets not found, trying environment variables...")
        api_id, api_hash, session_path = get_credentials_from_env()
        
        if api_id and api_hash:
            print(f"✅ Loaded credentials from environment variables")
        else:
            print("Environment variables not found, using interactive mode...")
            api_id, api_hash, session_path = get_credentials_interactive()
            
            if not api_id or not api_hash:
                print("❌ Invalid credentials provided!")
                return 1
    
    print(f"\nUsing session path: {session_path}")
    print(f"API ID: {api_id}")
    print(f"API Hash: {api_hash[:8] if api_hash else 'None'}...{api_hash[-4:] if api_hash else ''}")  # Partially hide hash
    
    # Get phone number
    phone_number = input("\nEnter your phone number (with country code, e.g., +1234567890): ").strip()
    
    if not phone_number.startswith('+'):
        print("❌ Phone number should start with + and country code!")
        return 1
    
    print(f"\nStarting authentication process...")
    
    # Perform authentication
    success = await authenticate_telegram(api_id, api_hash, session_path, phone_number)
    
    if success:
        print("\n✅ Authentication completed successfully!")
        print("\nYou can now start the tg-ingestor service:")
        print("  docker-compose up tg-ingestor")
        print("\nOr run locally:")
        print("  python app.py")
        return 0
    else:
        print("\n❌ Authentication failed!")
        print("Please check your credentials and try again.")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Authentication cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)