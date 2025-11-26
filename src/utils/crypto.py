"""
工具模块 - 加密工具

提供AES加密和解密功能。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import base64
import hashlib
from Crypto.Cipher import AES
from Crypto.Cipher._mode_cbc import CbcMode
from Crypto.Util.Padding import pad, unpad


class AESCrypto:
    """AES加密工具类"""
    
    def __init__(self, key: str):
        """
        初始化AES加密器
        
        Args:
            key: 加密密钥（将使用SHA-256哈希）
        """
        # 使用SHA-256确保密钥为32字节
        self._key: bytes = hashlib.sha256(key.encode()).digest()
        # 使用密钥的MD5作为固定IV（确保加密结果一致性）
        self._iv: bytes = hashlib.md5(key.encode()).digest()
    
    def encrypt(self, plaintext: str) -> str:
        """
        AES加密（确定性加密）
        
        Args:
            plaintext: 明文字符串
            
        Returns:
            str: Base64编码的密文
        """
        cipher: CbcMode = AES.new(self._key, AES.MODE_CBC, self._iv)
        padded_data: bytes = pad(plaintext.encode('utf-8'), AES.block_size)
        encrypted_data: bytes = cipher.encrypt(padded_data)
        return base64.b64encode(encrypted_data).decode('utf-8')
    
    def decrypt(self, encrypted_text: str) -> str:
        """
        AES解密
        
        Args:
            encrypted_text: Base64编码的密文
            
        Returns:
            str: 解密后的明文
        """
        cipher: CbcMode = AES.new(self._key, AES.MODE_CBC, self._iv)
        encrypted_data: bytes = base64.b64decode(encrypted_text)
        decrypted_data: bytes = unpad(cipher.decrypt(encrypted_data), AES.block_size)
        return decrypted_data.decode('utf-8')
