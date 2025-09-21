"""
Message parser for converting LLM responses into structured data.
Supports configurable schemas and custom field types for extensibility.
"""

import re
import logging
from typing import Dict, Any, List, Optional, Union
from email_validator import validate_email, EmailNotValidError

from domain.ports import MessageParser, ParsingError, SchemaNotFoundError
from domain.dto import (
    ClassificationResult, 
    ClassificationType,
    OrderData,
    OrderItem, 
    OfferData,
    ContactInfo,
    ParsingSchema,
    FieldConfig
)


logger = logging.getLogger(__name__)


class ConfigurableMessageParser(MessageParser):
    """
    Message parser with configurable schemas and custom field types.
    
    Supports:
    - ORDER parsing with plant items
    - OFFER parsing with contact information  
    - OTHER classification
    - Extensible field schemas
    - Custom field types (number_range, phone, email, telegram)
    """
    
    def __init__(self, parsing_schemas: Dict[str, Dict[str, ParsingSchema]]):
        """
        Initialize parser with schema configurations.
        
        Args:
            parsing_schemas: Dict with 'order' and 'offer' schema versions
        """
        self.parsing_schemas = parsing_schemas
        
        # Field type parsers
        self.field_parsers = {
            'string': self._parse_string,
            'number': self._parse_number,
            'number_range': self._parse_number_range,
            'phone': self._parse_phone,
            'email': self._parse_email,
            'telegram': self._parse_telegram
        }
    
    async def parse_llm_response(
        self, 
        llm_response: str,
        schema_version: str
    ) -> ClassificationResult:
        """
        Parse LLM response into structured classification result.
        
        Args:
            llm_response: Raw response from LLM
            schema_version: Schema version to use (e.g., "order_v1")
            
        Returns:
            Classification result with parsed data
        """
        try:
            response = llm_response.strip()
            
            logger.debug(
                f"Parsing LLM response",
                extra={
                    "component": "message_parser",
                    "response_length": len(response),
                    "schema_version": schema_version,
                    "response_preview": response[:100] + "..." if len(response) > 100 else response
                }
            )
            
            # Determine classification type
            if response.startswith("ORDER"):
                classification = ClassificationType.ORDER
                parsed_data = await self._parse_order_response(response, schema_version)
                
            elif response.startswith("OFFER"):
                classification = ClassificationType.OFFER
                parsed_data = await self._parse_offer_response(response, schema_version)
                
            elif response.startswith("OTHER") or response == "null":
                classification = ClassificationType.OTHER
                parsed_data = None
                
            else:
                # Unrecognized format
                logger.warning(
                    f"Unrecognized LLM response format",
                    extra={
                        "component": "message_parser",
                        "response_start": response[:50]
                    }
                )
                raise ParsingError(f"Unrecognized response format: {response[:50]}")
            
            logger.info(
                f"Successfully parsed LLM response",
                extra={
                    "component": "message_parser",
                    "classification": classification.value,
                    "schema_version": schema_version,
                    "has_data": parsed_data is not None
                }
            )
            
            return ClassificationResult(
                classification=classification,
                parsed_data=parsed_data,
                raw_llm_response=response,
                validation_passed=True,
                validation_errors=[]
            )
            
        except ParsingError:
            # Re-raise parsing errors
            raise
            
        except Exception as e:
            error_msg = f"Unexpected parsing error: {str(e)}"
            logger.error(
                error_msg,
                extra={
                    "component": "message_parser",
                    "schema_version": schema_version,
                    "error": str(e)
                }
            )
            raise ParsingError(error_msg) from e
    
    async def _parse_order_response(self, response: str, schema_version: str) -> OrderData:
        """Parse ORDER response into structured data."""
        try:
            lines = response.strip().split('\n')
            
            # Extract city from first line: "ORDER [City]" or just "ORDER"
            first_line = lines[0].strip()
            city = None
            
            # Match "ORDER [City]" pattern
            city_match = re.match(r'ORDER\s*\[([^\]]*)\]', first_line)
            if city_match:
                city_text = city_match.group(1).strip()
                city = city_text if city_text and city_text.lower() != 'null' else None
            elif first_line == "ORDER":
                city = None
            
            # Parse plant items from remaining lines
            items = []
            schema = self._get_order_schema(schema_version)
            
            for line_num, line in enumerate(lines[1:], 2):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    item = await self._parse_order_item(line, schema)
                    items.append(item)
                    
                except Exception as e:
                    logger.warning(
                        f"Failed to parse order item on line {line_num}",
                        extra={
                            "component": "message_parser",
                            "line": line,
                            "error": str(e)
                        }
                    )
                    # Continue parsing other items
            
            if not items:
                raise ParsingError("No valid plant items found in ORDER response")
            
            return OrderData(city=city, items=items)
            
        except ParsingError:
            raise
        except Exception as e:
            raise ParsingError(f"Failed to parse ORDER response: {str(e)}") from e
    
    async def _parse_order_item(self, line: str, schema: ParsingSchema) -> OrderItem:
        """Parse single plant item line using schema."""
        # Split by pipe separator
        parts = [part.strip() for part in line.split('|')]
        
        if len(parts) < len([f for f in schema.fields if f.required]):
            raise ParsingError(f"Insufficient fields in line: {line}")
        
        # Parse each field according to schema
        item_data = {}
        
        for i, field_config in enumerate(schema.fields):
            field_name = field_config.name
            
            if i < len(parts):
                raw_value = parts[i]
                parsed_value = await self._parse_field_value(raw_value, field_config)
                item_data[field_name] = parsed_value
            else:
                if field_config.required:
                    raise ParsingError(f"Required field {field_name} missing")
                item_data[field_name] = None
        
        return OrderItem(**item_data)
    
    async def _parse_offer_response(self, response: str, schema_version: str) -> OfferData:
        """Parse OFFER response into structured data."""
        try:
            # OFFER format: "OFFER [City] [Company] Телефон: +7... Telegram: @username Email: email"
            response_text = response.strip()
            
            # Remove "OFFER " prefix
            if response_text.startswith("OFFER "):
                content = response_text[6:].strip()
            else:
                content = response_text
            
            # Initialize data
            city = None
            company_name = None
            contact = ContactInfo(phone=None, telegram=None, email=None)
            
            # Extract structured fields using regex patterns
            
            # Extract city (first bracketed content)
            city_match = re.search(r'^\[([^\]]*)\]', content)
            if city_match:
                city_text = city_match.group(1).strip()
                city = city_text if city_text and city_text.lower() != 'null' else None
                content = content[city_match.end():].strip()
            
            # Extract company name (second bracketed content or text before contact info)
            company_match = re.search(r'^\[([^\]]*)\]', content)
            if company_match:
                company_text = company_match.group(1).strip()
                company_name = company_text if company_text and company_text.lower() != 'null' else None
                content = content[company_match.end():].strip()
            else:
                # Try to extract company name before contact fields
                contact_pattern = r'(Телефон:|Telegram:|Email:)'
                company_match = re.search(contact_pattern, content)
                if company_match:
                    company_text = content[:company_match.start()].strip()
                    company_name = company_text if company_text and company_text.lower() != 'null' else None
                    content = content[company_match.start():].strip()
            
            # Extract contact information
            
            # Phone number
            phone_match = re.search(r'Телефон:\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+(?:Telegram:|Email:)|$)', content)
            if phone_match:
                phone_text = phone_match.group(1).strip()
                contact.phone = await self._parse_field_value(phone_text, FieldConfig(name="phone", type="phone"))
            
            # Telegram
            telegram_match = re.search(r'Telegram:\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+Email:|$)', content)
            if telegram_match:
                telegram_text = telegram_match.group(1).strip()
                contact.telegram = await self._parse_field_value(telegram_text, FieldConfig(name="telegram", type="telegram"))
            
            # Email
            email_match = re.search(r'Email:\s*([^\s]+(?:\s+[^\s]+)*?)$', content)
            if email_match:
                email_text = email_match.group(1).strip()
                contact.email = await self._parse_field_value(email_text, FieldConfig(name="email", type="email"))
            
            return OfferData(
                city=city,
                company_name=company_name,
                contact=contact
            )
            
        except Exception as e:
            raise ParsingError(f"Failed to parse OFFER response: {str(e)}") from e
    
    async def _parse_field_value(self, raw_value: str, field_config: FieldConfig) -> Any:
        """Parse field value according to its type."""
        if raw_value.lower() == 'null' or not raw_value.strip():
            return None
        
        field_type = field_config.type
        if field_type in self.field_parsers:
            return self.field_parsers[field_type](raw_value)
        else:
            logger.warning(f"Unknown field type: {field_type}, treating as string")
            return raw_value.strip()
    
    def _parse_string(self, value: str) -> str:
        """Parse string field."""
        return value.strip()
    
    def _parse_number(self, value: str) -> Union[int, float]:
        """Parse numeric field."""
        try:
            # Try integer first
            if '.' not in value and ',' not in value:
                return int(value)
            else:
                # Handle decimal separator
                normalized = value.replace(',', '.')
                return float(normalized)
        except ValueError:
            raise ParsingError(f"Invalid number: {value}")
    
    def _parse_number_range(self, value: str) -> str:
        """Parse number range field (e.g., "2.5-3" or "2.5")."""
        value = value.strip()
        
        if '-' in value:
            # Range format: "2.5-3"
            parts = value.split('-')
            if len(parts) != 2:
                raise ParsingError(f"Invalid range format: {value}")
            
            try:
                # Validate that both parts are numbers
                float(parts[0].strip())
                float(parts[1].strip())
                return value  # Return as string
            except ValueError:
                raise ParsingError(f"Invalid range numbers: {value}")
        else:
            # Single number
            try:
                float(value)
                return value
            except ValueError:
                raise ParsingError(f"Invalid number: {value}")
    
    def _parse_phone(self, value: str) -> Optional[str]:
        """Parse and validate phone number."""
        if not value or value.lower() == 'null':
            return None
        
        # Clean phone number (remove spaces, dashes, etc.)
        cleaned = re.sub(r'[^\d+]', '', value)
        
        # Basic phone validation (Russian format)
        if re.match(r'^\+?[78]?\d{10,11}$', cleaned):
            # Normalize to +7 format
            if cleaned.startswith('8'):
                cleaned = '+7' + cleaned[1:]
            elif not cleaned.startswith('+'):
                cleaned = '+7' + cleaned
            return cleaned
        else:
            logger.warning(f"Invalid phone format: {value}")
            return value  # Return as-is if validation fails
    
    def _parse_email(self, value: str) -> Optional[str]:
        """Parse and validate email address."""
        if not value or value.lower() == 'null':
            return None
        
        try:
            # Use email-validator library
            valid = validate_email(value)
            return valid.email
        except EmailNotValidError:
            logger.warning(f"Invalid email format: {value}")
            return value  # Return as-is if validation fails
    
    def _parse_telegram(self, value: str) -> Optional[str]:
        """Parse and validate Telegram username."""
        if not value or value.lower() == 'null':
            return None
        
        # Ensure @ prefix
        if not value.startswith('@'):
            value = '@' + value
        
        # Basic telegram username validation
        if re.match(r'^@[a-zA-Z0-9_]{5,32}$', value):
            return value
        else:
            logger.warning(f"Invalid telegram format: {value}")
            return value  # Return as-is if validation fails
    
    def get_parsing_schema(self, schema_version: str) -> ParsingSchema:
        """
        Get parsing schema configuration.
        
        Args:
            schema_version: Schema version (e.g., "order_v1", "offer_v1")
            
        Returns:
            Schema configuration
        """
        try:
            # Parse schema version format: "type_version"
            if '_' not in schema_version:
                raise SchemaNotFoundError(f"Invalid schema version format: {schema_version}")
            
            schema_type, version = schema_version.split('_', 1)
            
            if schema_type not in self.parsing_schemas:
                raise SchemaNotFoundError(f"Schema type not found: {schema_type}")
            
            type_schemas = self.parsing_schemas[schema_type]
            if version not in type_schemas:
                raise SchemaNotFoundError(f"Schema version not found: {schema_version}")
            
            return type_schemas[version]
            
        except Exception as e:
            if isinstance(e, SchemaNotFoundError):
                raise
            raise SchemaNotFoundError(f"Error getting schema {schema_version}: {str(e)}") from e
    
    def _get_order_schema(self, schema_version: str) -> ParsingSchema:
        """Get ORDER parsing schema, handling version format."""
        # If schema_version is just "v1", assume "order_v1"
        if schema_version.startswith('v'):
            schema_version = f"order_{schema_version}"
        
        return self.get_parsing_schema(schema_version)
    
    def _get_offer_schema(self, schema_version: str) -> ParsingSchema:
        """Get OFFER parsing schema, handling version format."""
        # If schema_version is just "v1", assume "offer_v1"
        if schema_version.startswith('v'):
            schema_version = f"offer_{schema_version}"
        
        return self.get_parsing_schema(schema_version)
    
    def add_field_parser(self, field_type: str, parser_func: callable) -> None:
        """Add custom field type parser."""
        self.field_parsers[field_type] = parser_func
        logger.info(f"Added custom field parser: {field_type}")


def create_message_parser(parsing_schemas: Dict[str, Dict[str, ParsingSchema]]) -> ConfigurableMessageParser:
    """
    Create message parser with schema configuration.
    
    Args:
        parsing_schemas: Dictionary with schema configurations
        
    Returns:
        Configured message parser
    """
    return ConfigurableMessageParser(parsing_schemas)