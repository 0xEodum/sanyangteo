"""
Authentication module for queue-writer service.
Implements shared token validation using Docker secrets.
"""

import logging
from pathlib import Path
from typing import Optional

from ..domain.ports import TokenValidator, AuthenticationError


logger = logging.getLogger(__name__)


class SharedTokenValidator(TokenValidator):
    """
    Validates shared bearer tokens loaded from Docker secrets.
    Implements simple shared secret authentication.
    """
    
    def __init__(self, token_file_path: str):
        """
        Initialize token validator.
        
        Args:
            token_file_path: Path to file containing the shared token
            
        Raises:
            AuthenticationError: If token file cannot be read
        """
        self.token_file_path = Path(token_file_path)
        self._shared_token: Optional[str] = None
        self._load_token()
    
    def _load_token(self) -> None:
        """
        Load shared token from file.
        
        Raises:
            AuthenticationError: If token file cannot be read or is invalid
        """
        try:
            if not self.token_file_path.exists():
                raise AuthenticationError(f"Token file not found: {self.token_file_path}")
            
            with open(self.token_file_path, 'r', encoding='utf-8') as f:
                token = f.read().strip()
            
            if not token:
                raise AuthenticationError("Token file is empty")
            
            if len(token) < 16:
                logger.warning("Token is shorter than 16 characters, consider using a longer token")
            
            self._shared_token = token
            logger.info(f"Loaded shared token from {self.token_file_path}")
            
        except (OSError, IOError) as e:
            raise AuthenticationError(f"Failed to read token file: {e}") from e
    
    async def validate_token(self, token: Optional[str]) -> bool:
        """
        Validate the provided bearer token against shared secret.
        
        Args:
            token: The token to validate (from Authorization header)
            
        Returns:
            True if token is valid, False otherwise
        """
        if not token:
            logger.debug("No token provided")
            return False
        
        if not self._shared_token:
            logger.error("No shared token loaded")
            return False
        
        # Simple constant-time comparison to prevent timing attacks
        is_valid = self._constant_time_compare(token, self._shared_token)
        
        if not is_valid:
            logger.warning(
                "Invalid token provided",
                extra={
                    "component": "auth",
                    "token_prefix": token[:8] + "..." if len(token) > 8 else token
                }
            )
        
        return is_valid
    
    def reload_token(self) -> None:
        """
        Reload token from file.
        Useful for token rotation without service restart.
        
        Raises:
            AuthenticationError: If token reload fails
        """
        logger.info("Reloading shared token")
        self._load_token()
    
    @staticmethod
    def _constant_time_compare(a: str, b: str) -> bool:
        """
        Compare two strings in constant time to prevent timing attacks.
        
        Args:
            a: First string
            b: Second string
            
        Returns:
            True if strings are equal, False otherwise
        """
        if len(a) != len(b):
            return False
        
        result = 0
        for x, y in zip(a, b):
            result |= ord(x) ^ ord(y)
        
        return result == 0


def extract_bearer_token(authorization_header: Optional[str]) -> Optional[str]:
    """
    Extract bearer token from Authorization header.
    
    Args:
        authorization_header: Value of Authorization header
        
    Returns:
        Token string if valid Bearer format, None otherwise
    """
    if not authorization_header:
        return None
    
    parts = authorization_header.split()
    if len(parts) != 2:
        return None
    
    scheme, token = parts
    if scheme.lower() != "bearer":
        return None
    
    return token


# Dependency for FastAPI
class AuthDependency:
    """
    FastAPI dependency for token validation.
    """
    
    def __init__(self, token_validator: TokenValidator):
        self.token_validator = token_validator
    
    async def __call__(self, authorization: Optional[str] = None) -> bool:
        """
        Validate authorization header.
        
        Args:
            authorization: Authorization header value
            
        Returns:
            True if authenticated
            
        Raises:
            AuthenticationError: If authentication fails
        """
        token = extract_bearer_token(authorization)
        is_valid = await self.token_validator.validate_token(token)
        
        if not is_valid:
            raise AuthenticationError("Invalid or missing authentication token")
        
        return True