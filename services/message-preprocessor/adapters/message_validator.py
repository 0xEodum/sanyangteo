"""
Message validator for validating parsed message data.
Handles validation of ORDER and OFFER data structures.
"""

import re
import logging
from typing import Dict, Any, List

from domain.ports import MessageValidator, ValidationError
from domain.dto import TelegramEventDTO


logger = logging.getLogger(__name__)


class StandardMessageValidator(MessageValidator):
    """
    Standard message validator with comprehensive validation rules.
    
    Validates:
    - ORDER data: plant items with proper types and formats
    - OFFER data: contact information and company details  
    - Enhances OFFER data with telegram from sender if missing
    """
    
    def __init__(self):
        """Initialize validator with validation rules."""
        pass
    
    async def validate_order_data(self, order_data: Dict[str, Any]) -> List[str]:
        """
        Validate ORDER message data.
        
        Args:
            order_data: Parsed ORDER data dictionary
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        try:
            # Validate city (optional)
            city = order_data.get('city')
            if city is not None and not isinstance(city, str):
                errors.append("City must be a string or null")
            
            # Validate items (required)
            items = order_data.get('items', [])
            if not items:
                errors.append("At least one plant item is required")
            elif not isinstance(items, list):
                errors.append("Items must be a list")
            else:
                # Validate each item
                for i, item in enumerate(items):
                    item_errors = await self._validate_order_item(item, i)
                    errors.extend(item_errors)
            
            if errors:
                logger.warning(
                    f"ORDER validation failed",
                    extra={
                        "component": "message_validator",
                        "error_count": len(errors),
                        "errors": errors
                    }
                )
            
            return errors
            
        except Exception as e:
            logger.error(f"Error during ORDER validation: {e}")
            return [f"Validation error: {str(e)}"]
    
    async def _validate_order_item(self, item: Dict[str, Any], index: int) -> List[str]:
        """Validate individual ORDER item."""
        errors = []
        prefix = f"Item {index + 1}: "
        
        # Plant name (required)
        plant_name = item.get('plant_name')
        if not plant_name:
            errors.append(f"{prefix}plant_name is required")
        elif not isinstance(plant_name, str):
            errors.append(f"{prefix}plant_name must be a string")
        elif len(plant_name.strip()) < 2:
            errors.append(f"{prefix}plant_name too short")
        
        # Height (optional, number_range format)
        height = item.get('height')
        if height is not None:
            height_errors = self._validate_number_range(height, f"{prefix}height")
            errors.extend(height_errors)
        
        # Height unit (optional)
        height_unit = item.get('height_unit')
        if height_unit is not None and not isinstance(height_unit, str):
            errors.append(f"{prefix}height_unit must be a string or null")
        
        # Quantity (required, number)
        quantity = item.get('quantity')
        if quantity is None:
            errors.append(f"{prefix}quantity is required")
        elif not isinstance(quantity, (int, float)):
            errors.append(f"{prefix}quantity must be a number")
        elif quantity <= 0:
            errors.append(f"{prefix}quantity must be positive")
        
        # Quantity unit (required)
        quantity_unit = item.get('quantity_unit')
        if not quantity_unit:
            errors.append(f"{prefix}quantity_unit is required")
        elif not isinstance(quantity_unit, str):
            errors.append(f"{prefix}quantity_unit must be a string")
        
        return errors
    
    def _validate_number_range(self, value: Any, field_name: str) -> List[str]:
        """Validate number or number range format."""
        errors = []
        
        if not isinstance(value, str):
            return [f"{field_name} must be a string"]
        
        # Check for range format: "2.5-3.0" or single number: "2.5"
        if '-' in value:
            parts = value.split('-')
            if len(parts) != 2:
                errors.append(f"{field_name} invalid range format")
            else:
                for part in parts:
                    try:
                        float(part.strip())
                    except ValueError:
                        errors.append(f"{field_name} contains non-numeric values in range")
                        break
        else:
            try:
                float(value)
            except ValueError:
                errors.append(f"{field_name} is not a valid number")
        
        return errors
    
    async def validate_offer_data(self, offer_data: Dict[str, Any]) -> List[str]:
        """
        Validate OFFER message data.
        
        Args:
            offer_data: Parsed OFFER data dictionary
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        try:
            # Validate city (optional)
            city = offer_data.get('city')
            if city is not None and not isinstance(city, str):
                errors.append("City must be a string or null")
            
            # Validate company name (optional)
            company_name = offer_data.get('company_name')
            if company_name is not None and not isinstance(company_name, str):
                errors.append("Company name must be a string or null")
            
            # Validate contact information (required structure)
            contact = offer_data.get('contact')
            if not contact:
                errors.append("Contact information is required")
            elif not isinstance(contact, dict):
                errors.append("Contact must be an object")
            else:
                contact_errors = await self._validate_contact_info(contact)
                errors.extend(contact_errors)
            
            if errors:
                logger.warning(
                    f"OFFER validation failed",
                    extra={
                        "component": "message_validator",
                        "error_count": len(errors),
                        "errors": errors
                    }
                )
            
            return errors
            
        except Exception as e:
            logger.error(f"Error during OFFER validation: {e}")
            return [f"Validation error: {str(e)}"]
    
    async def _validate_contact_info(self, contact: Dict[str, Any]) -> List[str]:
        """Validate contact information structure."""
        errors = []
        
        # Phone (optional, but validate format if present)
        phone = contact.get('phone')
        if phone is not None:
            if not isinstance(phone, str):
                errors.append("Phone must be a string or null")
            elif not self._is_valid_phone(phone):
                errors.append(f"Invalid phone format: {phone}")
        
        # Telegram (optional, but validate format if present)
        telegram = contact.get('telegram')
        if telegram is not None:
            if not isinstance(telegram, str):
                errors.append("Telegram must be a string or null")
            elif not self._is_valid_telegram(telegram):
                errors.append(f"Invalid telegram format: {telegram}")
        
        # Email (optional, but validate format if present)
        email = contact.get('email')
        if email is not None:
            if not isinstance(email, str):
                errors.append("Email must be a string or null")
            elif not self._is_valid_email(email):
                errors.append(f"Invalid email format: {email}")
        
        # At least one contact method should be provided
        if not any([phone, telegram, email]):
            logger.info("No contact information provided in OFFER")
            # This is not necessarily an error, just noting it
        
        return errors
    
    def _is_valid_phone(self, phone: str) -> bool:
        """Validate phone number format."""
        # Russian phone format: +7XXXXXXXXXX
        return bool(re.match(r'^\+?[78]?\d{10,11}$', re.sub(r'[^\d+]', '', phone)))
    
    def _is_valid_telegram(self, telegram: str) -> bool:
        """Validate telegram username format."""
        # Telegram format: @username (5-32 characters)
        return bool(re.match(r'^@[a-zA-Z0-9_]{5,32}$', telegram))
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format."""
        # Basic email regex
        return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))
    
    async def enhance_offer_data(
        self, 
        offer_data: Dict[str, Any],
        original_message: TelegramEventDTO
    ) -> Dict[str, Any]:
        """
        Enhance OFFER data by filling missing telegram from sender.
        
        Args:
            offer_data: Parsed OFFER data
            original_message: Original telegram message
            
        Returns:
            Enhanced offer data with telegram from sender if missing
        """
        try:
            # Make a copy to avoid modifying original
            enhanced_data = offer_data.copy()
            
            # Get sender information
            sender = original_message.sender
            sender_username = sender.get('username') if isinstance(sender, dict) else None
            
            # Check if contact info exists and telegram is missing
            contact = enhanced_data.get('contact', {})
            if isinstance(contact, dict):
                current_telegram = contact.get('telegram')
                
                # Fill telegram from sender if missing and sender has username
                if not current_telegram and sender_username:
                    # Ensure @ prefix
                    telegram_username = f"@{sender_username}" if not sender_username.startswith('@') else sender_username
                    
                    # Update contact info
                    enhanced_contact = contact.copy()
                    enhanced_contact['telegram'] = telegram_username
                    enhanced_data['contact'] = enhanced_contact
                    
                    logger.info(
                        f"Enhanced OFFER with telegram from sender",
                        extra={
                            "component": "message_validator",
                            "sender_username": sender_username,
                            "idempotency_key": original_message.idempotency_key
                        }
                    )
                    
                    # Add metadata about enhancement
                    enhanced_data['_telegram_filled_from_sender'] = True
                else:
                    enhanced_data['_telegram_filled_from_sender'] = False
            
            return enhanced_data
            
        except Exception as e:
            logger.error(
                f"Error enhancing OFFER data: {e}",
                extra={
                    "component": "message_validator",
                    "idempotency_key": getattr(original_message, 'idempotency_key', 'unknown')
                }
            )
            # Return original data on error
            return offer_data


def create_message_validator() -> StandardMessageValidator:
    """
    Create standard message validator.
    
    Returns:
        Configured message validator
    """
    return StandardMessageValidator()