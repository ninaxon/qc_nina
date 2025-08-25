#!/usr/bin/env python3
"""
VIN Suggestion Integration Example
How to integrate the VIN suggestion system with your existing bot
"""

from telegram import Update
from telegram.ext import Application, ContextTypes, CallbackQueryHandler, CommandHandler
from vin_suggestion_handlers import (
    suggest_vin_on_group_join,
    auto_register_vin_on_group_join,
    on_vin_selected, 
    register_vin_handlers,
    save_group_vin
)
import logging

logger = logging.getLogger(__name__)

# Integration Step 1: Modify your existing button handler
async def handle_set_vin_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Your existing 🛠 Set VIN button handler - modify to call VIN suggestion
    """
    # Instead of your current logic, call the VIN suggestion system
    await suggest_vin_on_group_join(update, context)

# Integration Step 2: Initialize bot_data with your GoogleSheetsIntegration
def setup_bot_data(application: Application):
    """
    Set up bot_data with necessary components
    Call this during your bot initialization
    """
    # Example - adapt to your existing initialization
    from config import Config
    from google_integration import GoogleSheetsIntegration
    
    config = Config()
    google_integration = GoogleSheetsIntegration(config)
    
    # Store in bot_data for handlers to access
    application.bot_data['google_integration'] = google_integration
    application.bot_data['assets_ws'] = google_integration.assets_worksheet
    
    logger.info("Bot data initialized with Google Sheets integration")

# Integration Step 3: Implement save_group_vin for your system
async def save_group_vin_implementation(group_id: int, vin: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    REPLACE THE STUB in vin_suggestion_handlers.py with this implementation
    """
    try:
        if hasattr(context.bot_data, 'google_integration') and context.bot_data.google_integration:
            google = context.bot_data.google_integration
            
            # Use your existing save_group_vin method from google_integration.py
            # Adapt the method call to match your actual signature:
            
            # Option 1: If you have a direct save method
            # return google.save_group_vin(group_id, vin)
            
            # Option 2: If you use update_group_registration
            # return google.update_group_registration(
            #     group_id=group_id, 
            #     vin=vin, 
            #     status='ACTIVE'
            # )
            
            # Option 3: If you need to find and update existing record
            try:
                # Get current groups data
                groups_records = google._get_groups_records_safe()
                
                # Find existing group or create new record
                row_to_update = None
                for i, record in enumerate(groups_records):
                    if int(record.get('group_id', 0)) == group_id:
                        row_to_update = i + 2  # +2 for header and 1-based indexing
                        break
                
                if row_to_update:
                    # Update existing group's VIN
                    # Assuming VIN is in column C (index 2)
                    google.groups_worksheet.update(f'C{row_to_update}', [[vin]])
                    logger.info(f"Updated existing group {group_id} with VIN {vin}")
                else:
                    # Create new group record
                    from datetime import datetime
                    new_row = [
                        group_id,           # A: group_id
                        "",                 # B: group_title (will be updated later)
                        vin,                # C: vin
                        "ACTIVE",           # D: status
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # E: last_updated
                    ]
                    google.groups_worksheet.append_row(new_row)
                    logger.info(f"Created new group record {group_id} with VIN {vin}")
                
                return True
                
            except Exception as e:
                logger.error(f"Error in Google Sheets operation: {e}")
                return False
        
        return False
        
    except Exception as e:
        logger.error(f"Error saving VIN {vin} for group {group_id}: {e}")
        return False

# Integration Step 4: Main bot setup
def setup_vin_suggestion_system(application: Application):
    """
    Complete setup function - call this from your main bot initialization
    """
    # 1. Set up bot data
    setup_bot_data(application)
    
    # 2. Register handlers
    register_vin_handlers(application)
    
    # 3. If you want to trigger on group join (optional)
    # Add a ChatMemberHandler to trigger when bot is added to group
    from telegram.ext import ChatMemberHandler
    
    async def on_bot_added_to_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Trigger auto VIN registration when bot is added to group (with manual fallback)"""
        member_update = update.my_chat_member
        if member_update and member_update.new_chat_member:
            if member_update.new_chat_member.status in ['member', 'administrator']:
                # Bot was added to group, attempt auto-registration first
                await auto_register_vin_on_group_join(update, context)
                
                # Optional: Check if auto-registration succeeded, fallback to manual if not
                # from vin_suggestion_handlers import get_existing_group_vin
                # existing_vin = await get_existing_group_vin(update.effective_chat.id, context)
                # if not existing_vin:
                #     await suggest_vin_on_group_join(update, context)
    
    application.add_handler(ChatMemberHandler(on_bot_added_to_group, ChatMemberHandler.MY_CHAT_MEMBER))
    
    logger.info("VIN suggestion system fully configured")

# Example usage in your main bot file:
"""
# In your main bot initialization (e.g., main.py or telegram_integration.py):

from integration_example import setup_vin_suggestion_system

def main():
    # Your existing bot setup...
    application = Application.builder().token(TOKEN).build()
    
    # Add VIN suggestion system
    setup_vin_suggestion_system(application)
    
    # Your other handlers...
    
    # Start the bot
    application.run_polling()

if __name__ == "__main__":
    main()
"""