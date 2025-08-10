"""
Structured logging setup with redaction and split logs.
Implements PII redaction and multiple log files per the master prompt.
"""
import logging
import logging.handlers
import re
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

from config import Config


class PIIRedactingFormatter(logging.Formatter):
    """Formatter that redacts PII from log messages"""
    
    # Patterns to redact
    PHONE_PATTERN = re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b')
    EMAIL_PATTERN = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    ADDRESS_PATTERN = re.compile(r'\b\d+\s+[A-Za-z0-9\s,]+(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Blvd|Boulevard|Ln|Lane|Ct|Court|Pl|Place|Way|Pkwy|Parkway)\b', re.IGNORECASE)
    TOKEN_PATTERN = re.compile(r'\b[A-Za-z0-9_-]{20,}\b')  # Generic tokens
    
    def __init__(self, *args, redact_pii: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.redact_pii = redact_pii
    
    def format(self, record: logging.LogRecord) -> str:
        # Format the record normally first
        formatted = super().format(record)
        
        if not self.redact_pii:
            return formatted
        
        # Redact PII
        formatted = self.PHONE_PATTERN.sub('[PHONE]', formatted)
        formatted = self.EMAIL_PATTERN.sub('[EMAIL]', formatted)
        formatted = self.ADDRESS_PATTERN.sub('[ADDRESS]', formatted)
        
        # Redact long tokens but preserve short identifiers
        def redact_token(match):
            token = match.group(0)
            if len(token) > 30:  # Only redact very long tokens
                return f'[TOKEN:{token[:4]}...{token[-4:]}]'
            return token
        
        formatted = self.TOKEN_PATTERN.sub(redact_token, formatted)
        
        return formatted


class StructuredJSONFormatter(PIIRedactingFormatter):
    """JSON formatter with PII redaction for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in {'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'lineno', 'funcName', 'created', 'msecs', 
                          'relativeCreated', 'thread', 'threadName', 'processName', 
                          'process', 'getMessage', 'exc_info', 'exc_text', 'stack_info'}:
                log_entry[key] = value
        
        json_str = json.dumps(log_entry, default=str)
        
        if self.redact_pii:
            # Apply PII redaction to the JSON string
            json_str = self.PHONE_PATTERN.sub('[PHONE]', json_str)
            json_str = self.EMAIL_PATTERN.sub('[EMAIL]', json_str)
            json_str = self.ADDRESS_PATTERN.sub('[ADDRESS]', json_str)
        
        return json_str


def setup_logging(config: Config) -> Dict[str, logging.Logger]:
    """Setup split logging with redaction according to master prompt specs"""
    
    # Create logs directory
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    # Base configuration
    log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    max_bytes = config.LOG_FILE_MAX_MB * 1024 * 1024
    backup_count = config.LOG_BACKUP_COUNT
    
    # Remove all existing handlers from root logger
    logging.root.handlers = []
    
    # Configure formatters
    console_formatter = PIIRedactingFormatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        redact_pii=True
    )
    
    file_formatter = StructuredJSONFormatter(redact_pii=True)
    
    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    
    # Dictionary to store specialized loggers
    loggers = {}
    
    # Define log files and their purposes
    log_configs = [
        ('bot.log', ['telegram_integration', '__main__', 'main']),
        ('error.log', []),  # Error-only log
        ('risk_alerts.log', ['cargo_risk_detection', 'risk_integration']),
        ('scheduler.log', ['group_update_scheduler']),
        ('qc_panel_sync.log', ['google_integration']),
        ('eta_alerts.log', ['eta_service']),
    ]
    
    # Setup file handlers for each log
    for log_file, modules in log_configs:
        file_path = logs_dir / log_file
        
        # Create rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(file_formatter)
        
        if log_file == 'error.log':
            # Error log gets errors from all loggers
            file_handler.setLevel(logging.ERROR)
            
            # Create error logger
            error_logger = logging.getLogger('error')
            error_logger.setLevel(logging.ERROR)
            error_logger.addHandler(file_handler)
            error_logger.propagate = False
            loggers['error'] = error_logger
        else:
            # Create logger for each module group
            logger_name = log_file.replace('.log', '')
            logger = logging.getLogger(logger_name)
            logger.setLevel(log_level)
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            logger.propagate = False
            loggers[logger_name] = logger
            
            # Associate modules with this logger
            for module in modules:
                module_logger = logging.getLogger(module)
                module_logger.setLevel(log_level)
                module_logger.addHandler(file_handler)
                module_logger.addHandler(console_handler)
                # Also send errors to error log
                if 'error' in loggers:
                    error_handler = logging.handlers.RotatingFileHandler(
                        logs_dir / 'error.log',
                        maxBytes=max_bytes,
                        backupCount=backup_count,
                        encoding='utf-8'
                    )
                    error_handler.setLevel(logging.ERROR)
                    error_handler.setFormatter(file_formatter)
                    module_logger.addHandler(error_handler)
                
                module_logger.propagate = False
                loggers[module] = module_logger
    
    # Setup root logger as fallback
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(console_handler)
    
    # Main bot log handler
    main_file_handler = logging.handlers.RotatingFileHandler(
        logs_dir / 'bot.log',
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    main_file_handler.setLevel(log_level)
    main_file_handler.setFormatter(file_formatter)
    root_logger.addHandler(main_file_handler)
    
    return loggers


def get_logger(name: str) -> logging.Logger:
    """Get a logger with proper configuration"""
    return logging.getLogger(name)