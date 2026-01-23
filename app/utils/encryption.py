# app/utils/encryption.py

import base64
import hashlib
import os
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from dotenv import load_dotenv
load_dotenv()
SECRET_KEY = os.getenv("ENCRYPTION_KEY")  # SAME as frontend

def decrypt_password(encrypted_password: str) -> str:
    try:
        # CryptoJS uses OpenSSL-style format
        encrypted_bytes = base64.b64decode(encrypted_password)

        # Salt is stored in bytes 8–16
        salt = encrypted_bytes[8:16]
        ciphertext = encrypted_bytes[16:]

        # Key + IV derivation (EVP_BytesToKey)
        key_iv = b''
        prev = b''

        while len(key_iv) < 48:
            prev = hashlib.md5(prev + SECRET_KEY.encode() + salt).digest()
            key_iv += prev

        key = key_iv[:32]
        iv = key_iv[32:48]

        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypted = unpad(cipher.decrypt(ciphertext), AES.block_size)

        return decrypted.decode("utf-8")

    except Exception as e:
        raise ValueError(f"Password decryption failed: {str(e)}")
