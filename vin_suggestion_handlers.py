#!/usr/bin/env python3
"""
VIN Suggestion Handlers - Telegram bot handlers for VIN suggestion and confirmation
"""

import logging
from typing import List, Tuple, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CallbackQueryHandler, CommandHandler
from telegram.constants import ParseMode

from fuzzy_vin_matcher import (
    extract_names_from_title,
    build_assets_index,
    shortlist_for_group_title,
    redact_phone
)

logger = logging.getLogger(__name__)

# Callback data prefixes
CB_VIN_SELECTED = "VINSEL"
CB_MANUAL_SEARCH = "MANUAL_SEARCH"

# Score threshold for auto-suggestions
CONFIDENCE_THRESHOLD = 70
# Score threshold for auto-registration (higher threshold for automatic
# actions)
AUTO_REGISTER_THRESHOLD = 85


async def auto_register_vin_on_group_join(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    """
    Attempt auto VIN registration when bot joins group, with fallback to manual selection
    This is the main entry point for automatic VIN registration
    """
    chat = update.effective_chat
    user = update.effective_user

    if not chat or not chat.id:
        return

    # Only work in groups
    if chat.type not in ['group', 'supergroup']:
        return

    group_id = chat.id
    group_title = chat.title or "Untitled Group"

    # Log the trigger (redact phone if present)
    safe_title = redact_phone(group_title)
    logger.info(
        f"Auto VIN registration triggered - Group: {group_id}, Title: '{safe_title}'")

    # Check if VIN is already set
    try:
        existing_vin = await get_existing_group_vin(group_id, context)
        if existing_vin:
            # Escape special Markdown characters in group title
            safe_title = group_title.replace(
                '*', '\\*').replace(
                '_', '\\_').replace(
                '[', '\\[').replace(
                ']', '\\]').replace(
                    '`', '\\`') if group_title else "Unknown Group"

            message = (
                f"✅ **VIN Already Registered**\n\n"
                f"🚛 **VIN:** `{existing_vin}`\n"
                f"👥 **Group:** {safe_title}\n\n"
                f"🎉 Location tracking is ready!\n\n"
                f"_VIN was previously registered for this group._"
            )
            keyboard = [
                [InlineKeyboardButton("🛰 Get Location Update", callback_data="get_update")],
                [InlineKeyboardButton("🛠 Change VIN", callback_data="set_vin")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            if update.message:
                await update.message.reply_text(message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
            return
    except Exception as e:
        logger.error(f"Error checking existing VIN for group {group_id}: {e}")

    # Load assets data
    try:
        assets_data = await load_assets_data(context)
        if not assets_data:
            logger.warning("Unable to load driver data for auto-registration")
            return

        assets_index = build_assets_index(assets_data, driver_col=3, vin_col=4)
        logger.debug(f"Built assets index with {len(assets_index)} entries")

    except Exception as e:
        logger.error(f"Error loading assets data: {e}")
        return

    # Generate shortlist
    try:
        shortlist = shortlist_for_group_title(
            group_title, assets_index, k_each=3)
        extracted_names = extract_names_from_title(group_title)
        logger.info(f"Auto-registration - Extracted names: {extracted_names}")

        if shortlist:
            logger.info(
                f"Auto-registration - Top matches: {[(d, v[-4:], s) for d, v, s in shortlist[:3]]}")

    except Exception as e:
        logger.error(f"Error generating shortlist for auto-registration: {e}")
        return

    # Attempt auto-registration with very high confidence matches only
    if shortlist:
        auto_register_candidates = [
            item for item in shortlist if item[2] >= AUTO_REGISTER_THRESHOLD]

        if auto_register_candidates and len(auto_register_candidates) == 1:
            # Single high-confidence match - attempt auto-registration
            driver_name, vin, score = auto_register_candidates[0]
            logger.info(
                f"Auto-registering VIN {vin} for group {group_id} with confidence {score}%")

            # Double-check that VIN isn't already registered (race condition
            # protection)
            existing_vin_check = await get_existing_group_vin(group_id, context)
            if existing_vin_check:
                logger.info(
                    f"VIN already registered during auto-registration attempt: {existing_vin_check}")
                return

            try:
                success = await save_group_vin(group_id, vin.upper(), context, group_title)
                if success:
                    # Escape special Markdown characters in driver name
                    safe_driver = driver_name.replace(
                        '*', '\\*').replace(
                        '_', '\\_').replace(
                        '[', '\\[').replace(
                        ']', '\\]').replace(
                        '`', '\\`') if driver_name else "Unknown Driver"

                    message = (
                        f"🎉 **VIN Auto-Registered!**\n\n"
                        f"🤖 **Bot detected:** High confidence match\n"
                        f"🎯 **Confidence:** {score}%\n"
                        f"👤 **Driver:** {safe_driver}\n"
                        f"🚛 **VIN:** `{vin.upper()}`\n\n"
                        f"✅ **Ready for location tracking!**\n\n"
                        f"_Auto-registered based on group name. Use 🛠 Change VIN if incorrect._")

                    keyboard = [
                        [InlineKeyboardButton("🛰 Get Location Update", callback_data="get_update")],
                        [InlineKeyboardButton("🛠 Change VIN", callback_data="set_vin")]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)

                    try:
                        if update.message:
                            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
                        elif update.callback_query:
                            await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

                        logger.info(
                            f"Successfully auto-registered VIN {vin.upper()} for group {group_id}")
                        return  # Success!

                    except Exception as e:
                        logger.error(
                            f"Error sending auto-registration success message: {e}")
                        return

                else:
                    logger.warning(
                        f"Auto-registration failed for VIN {vin}, will not show manual options")

            except Exception as e:
                logger.error(f"Error during auto-registration: {e}")

    # Auto-registration didn't work - don't fallback to manual selection
    # This keeps the bot quiet unless there's a clear match
    logger.info(
        f"No auto-registration performed for group {group_id} - confidence too low or multiple matches")


async def suggest_vin_on_group_join(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    """
    Trigger VIN suggestion when bot is added to group or user taps 🛠 Set VIN
    """
    chat = update.effective_chat

    if not chat or not chat.id:
        return

    # Only work in groups
    if chat.type not in ['group', 'supergroup']:
        if update.message:
            await update.message.reply_text("🤖 VIN setup is only available in group chats.")
        return

    group_id = chat.id
    group_title = chat.title or "Untitled Group"

    # Log the trigger (redact phone if present)
    safe_title = redact_phone(group_title)
    logger.info(
        f"VIN suggestion triggered - Group: {group_id}, Title: '{safe_title}'")

    # Check if VIN is already set
    try:
        existing_vin = await get_existing_group_vin(group_id, context)
        if existing_vin:
            message = (
                f"✅ VIN already registered: `{existing_vin}`\n\n"
                f"You can now tap 🛰 **Get an Update** for location info.\n\n"
                f"_To change VIN, contact an admin._"
            )
            if update.message:
                await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            return
    except Exception as e:
        logger.error(f"Error checking existing VIN for group {group_id}: {e}")

    # Load assets data
    try:
        assets_data = await load_assets_data(context)
        if not assets_data:
            error_msg = "❌ Unable to load driver data. Please try again later."
            if update.message:
                await update.message.reply_text(error_msg)
            return

        assets_index = build_assets_index(
            assets_data, driver_col=3, vin_col=4)  # D=3, E=4 (0-indexed)
        logger.debug(f"Built assets index with {len(assets_index)} entries")

    except Exception as e:
        logger.error(f"Error loading assets data: {e}")
        error_msg = "❌ Error accessing driver database. Please contact support."
        if update.message:
            await update.message.reply_text(error_msg)
        return

    # Generate shortlist
    try:
        shortlist = shortlist_for_group_title(
            group_title, assets_index, k_each=3)

        # Log extracted names and matches
        extracted_names = extract_names_from_title(group_title)
        logger.info(f"Extracted names: {extracted_names}")
        if shortlist:
            logger.info(
                f"Top matches: {[(d, v[-4:], s) for d, v, s in shortlist[:3]]}")

    except Exception as e:
        logger.error(f"Error generating shortlist: {e}")
        shortlist = []

    # Build response
    if not shortlist:
        # Escape special Markdown characters in safe_title for message display
        escaped_title = safe_title.replace(
            '*', '\\*').replace(
            '_', '\\_').replace(
            '[', '\\[').replace(
                ']', '\\]').replace(
                    '`', '\\`')

        message = (
            f"📋 **VIN Registration Required**\n\n"
            f"🔍 **No driver suggestions found** based on group name '{escaped_title}'.\n\n"
            f"**To register a VIN:**\n"
            f"• Contact admin to manually register VIN\n"
            f"• Rename group to include driver name (e.g., 'John Doe Truck')\n"
            f"• Ensure driver is registered in the assets system\n\n"
            f"💡 **Tip:** Groups with driver names in the title get automatic VIN suggestions!")
        keyboard = [[InlineKeyboardButton(
            "🔍 Manual Search", callback_data=CB_MANUAL_SEARCH)]]

    else:
        # Check for auto-registration opportunity (very high confidence)
        auto_register_candidates = [
            item for item in shortlist if item[2] >= AUTO_REGISTER_THRESHOLD]

        if auto_register_candidates and len(auto_register_candidates) == 1:
            # Single high-confidence match - attempt auto-registration
            driver_name, vin, score = auto_register_candidates[0]
            logger.info(
                f"Auto-registering VIN {vin} for group {group_id} with confidence {score}%")

            # Double-check that VIN isn't already registered (race condition
            # protection)
            existing_vin_check = await get_existing_group_vin(group_id, context)
            if existing_vin_check:
                logger.info(
                    f"VIN already registered during manual VIN suggestion attempt: {existing_vin_check}")
                # Return early, but don't show error - fall through to normal
                # suggestion logic
            else:
                try:
                    success = await save_group_vin(group_id, vin.upper(), context, group_title)
                    if success:
                        # Escape special Markdown characters in group title and
                        # driver name
                        safe_title = group_title.replace(
                            '*', '\\*').replace(
                            '_', '\\_').replace(
                            '[', '\\[').replace(
                            ']', '\\]').replace(
                            '`', '\\`') if group_title else "Unknown Group"
                        safe_driver = driver_name.replace(
                            '*', '\\*').replace(
                            '_', '\\_').replace(
                            '[', '\\[').replace(
                            ']', '\\]').replace(
                            '`', '\\`') if driver_name else "Unknown Driver"

                        message = (
                            f"✅ **VIN Auto-Registered!**\n\n"
                            f"🎯 **High Confidence Match:** {score}%\n"
                            f"👤 **Driver:** {safe_driver}\n"
                            f"🚛 **VIN:** `{vin.upper()}`\n"
                            f"👥 **Group:** {safe_title}\n\n"
                            f"🎉 You can now use location tracking!\n\n"
                            f"_Auto-registered due to high confidence match. Contact admin if incorrect._")

                        # Still provide manual options in case of error
                        keyboard = [
                            [InlineKeyboardButton("🛰 Get Location Update", callback_data="get_update")],
                            [InlineKeyboardButton("🛠 Change VIN", callback_data="set_vin")]
                        ]

                        reply_markup = InlineKeyboardMarkup(keyboard)

                        try:
                            if update.message:
                                await update.message.reply_text(message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
                            elif update.callback_query:
                                await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
                        except Exception as e:
                            logger.error(
                                f"Error sending auto-registration success message: {e}")

                        return  # Exit early - auto-registration successful
                    else:
                        logger.warning(
                            f"Auto-registration failed for VIN {vin}, falling back to manual selection")

                except Exception as e:
                    logger.error(f"Error during auto-registration: {e}")
                # Fall through to manual selection

        # Filter by confidence threshold for manual selection
        high_conf = [item for item in shortlist if item[2]
                     >= CONFIDENCE_THRESHOLD]

        if high_conf:
            # Escape special Markdown characters in safe_title for message
            # display
            escaped_title = safe_title.replace(
                '*', '\\*').replace(
                '_', '\\_').replace(
                '[', '\\[').replace(
                ']', '\\]').replace(
                    '`', '\\`')

            message = f"🤖 **VIN Suggestions for:**\n`{escaped_title}`\n\nSelect your driver:"
            keyboard_items = []

            # Max 6 high-confidence
            for driver_name, vin, score in high_conf[:6]:
                # Truncate long names for button display
                display_name = driver_name[:15] + \
                    "..." if len(driver_name) > 15 else driver_name
                button_text = f"✅ {score}% • {display_name} • {vin[:8]}..."
                keyboard_items.append([InlineKeyboardButton(
                    button_text, callback_data=f"{CB_VIN_SELECTED}|{vin}")])

        else:
            # Escape special Markdown characters in safe_title for message
            # display
            escaped_title = safe_title.replace(
                '*', '\\*').replace(
                '_', '\\_').replace(
                '[', '\\[').replace(
                ']', '\\]').replace(
                    '`', '\\`')

            message = (
                f"🤖 **Low-confidence matches** — please confirm:\n"
                f"`{escaped_title}`\n\n"
                f"⚠️ _No high-confidence matches found_"
            )
            keyboard_items = []

            # Show top 3 even if low confidence
            for driver_name, vin, score in shortlist[:3]:
                display_name = driver_name[:15] + \
                    "..." if len(driver_name) > 15 else driver_name
                button_text = f"⚠️ {score}% • {display_name} • {vin[:8]}..."
                keyboard_items.append([InlineKeyboardButton(
                    button_text, callback_data=f"{CB_VIN_SELECTED}|{vin}")])

        # Add manual search option
        keyboard_items.append([InlineKeyboardButton(
            "🔍 Search Manually", callback_data=CB_MANUAL_SEARCH)])
        keyboard = keyboard_items

    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        if update.message:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
        elif update.callback_query:
            await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error sending VIN suggestion message: {e}")


async def on_vin_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle VIN selection from inline keyboard
    """
    query = update.callback_query
    if not query or not query.data:
        return

    await query.answer()  # Acknowledge the callback

    chat = query.message.chat if query.message else None
    user = query.from_user

    if not chat or not user:
        return

    group_id = chat.id
    callback_data = query.data

    logger.info(
        f"VIN selection callback: {callback_data} from user {user.id} in group {group_id}")

    try:
        if callback_data == CB_MANUAL_SEARCH:
            # Handle manual search
            message = (
                f"🔍 **Manual VIN Search**\n\n"
                f"Please contact your fleet manager or admin to:\n"
                f"1. Look up your VIN in the assets database\n"
                f"2. Set it manually for this group\n\n"
                f"_Group ID: `{group_id}`_"
            )
            await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)
            return

        elif callback_data.startswith(f"{CB_VIN_SELECTED}|"):
            # Extract VIN from callback data
            _, vin = callback_data.split("|", 1)

            if not vin or len(vin) != 17:
                await query.edit_message_text("❌ Invalid VIN selected. Please try again.")
                return

            # Validate VIN format
            import re
            if not re.match(r"^[A-Z0-9]{17}$", vin.upper()):
                await query.edit_message_text("❌ Invalid VIN format. Please contact support.")
                return

            # Save to groups table
            try:
                success = await save_group_vin(group_id, vin.upper(), context, chat.title)
                if success:
                    # Get driver name for confirmation
                    driver_name = await get_driver_name_for_vin(vin.upper(), context)
                    safe_driver = driver_name.replace(
                        '*', '\\*').replace(
                        '_', '\\_').replace(
                        '[', '\\[').replace(
                        ']', '\\]').replace(
                        '`', '\\`') if driver_name else ""
                    driver_display = f" ({safe_driver})" if safe_driver else ""

                    # Escape special Markdown characters in group title
                    safe_title = chat.title.replace(
                        '*', '\\*').replace(
                        '_', '\\_').replace(
                        '[', '\\[').replace(
                        ']', '\\]').replace(
                        '`', '\\`') if chat.title else "Unknown Group"

                    message = (
                        f"✅ **VIN Registered Successfully!**\n\n"
                        f"🚛 **VIN:** `{vin.upper()}`{driver_display}\n"
                        f"👥 **Group:** {safe_title}\n\n"
                        f"🎉 You can now tap 🛰 **Get an Update** for live location tracking!\n\n"
                        f"_VIN is locked for this group. Contact admin to change._")

                    logger.info(
                        f"VIN {vin.upper()} successfully registered for group {group_id}")

                else:
                    message = (
                        "❌ **Registration Failed**\n\n"
                        "Unable to save VIN to database. Please try again or contact support.\n\n"
                        "_Error code: DB_SAVE_FAILED_")

                await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)

            except Exception as e:
                logger.error(
                    f"Error saving VIN {vin} for group {group_id}: {e}")
                await query.edit_message_text(
                    "❌ Database error occurred. Please try again later or contact support.",
                    parse_mode=ParseMode.MARKDOWN
                )

        else:
            # Unknown callback data
            await query.edit_message_text("❌ Invalid selection. Please try again.")

    except Exception as e:
        logger.error(f"Error handling VIN selection: {e}")
        try:
            await query.edit_message_text("❌ An error occurred. Please try again later.")
        except Exception:
            pass  # Message may have been deleted


# Helper functions - integrate with your existing system

async def load_assets_data(
        context: ContextTypes.DEFAULT_TYPE) -> Optional[List[List[str]]]:
    """
    Load assets data from Google Sheets
    Integrate with your existing google_integration.py
    """
    try:
        # Option 1: Use cached worksheet handle
        if context.bot_data and 'assets_ws' in context.bot_data and context.bot_data[
                'assets_ws']:
            return context.bot_data['assets_ws'].get_all_values()

        # Option 2: Use your existing GoogleSheetsIntegration
        if context.bot_data and 'google_integration' in context.bot_data and context.bot_data[
                'google_integration']:
            google = context.bot_data['google_integration']
            if hasattr(google, 'assets_worksheet') and google.assets_worksheet:
                return google.assets_worksheet.get_all_values()

        # Option 3: Initialize fresh connection (fallback)
        logger.warning(
            "Assets worksheet not found in context, attempting fresh connection")
        # Add your GoogleSheetsIntegration initialization here
        return None

    except Exception as e:
        logger.error(f"Error loading assets data: {e}")
        return None


async def get_existing_group_vin(
        group_id: int,
        context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    """
    Check if group already has a VIN registered
    Integrate with your existing groups storage
    """
    try:
        # Integrate with your existing google_integration groups method
        if context.bot_data and 'google_integration' in context.bot_data and context.bot_data[
                'google_integration']:
            google = context.bot_data['google_integration']
            # Use your existing _get_groups_records_safe method or similar
            groups_records = google._get_groups_records_safe()
            for record in groups_records:
                if int(record.get('group_id', 0)) == group_id:
                    return record.get('vin', '').strip().upper() or None
        return None
    except Exception as e:
        logger.error(f"Error checking existing VIN for group {group_id}: {e}")
        return None


async def save_group_vin(
        group_id: int,
        vin: str,
        context: ContextTypes.DEFAULT_TYPE,
        group_title: str = None) -> bool:
    """
    Save VIN to groups table using existing GoogleSheetsIntegration

    Args:
        group_id: Telegram group ID
        vin: 17-character VIN (already validated and uppercased)
        context: PTB context for accessing bot data

    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        logger.info(f"Saving VIN {vin} for group {group_id}")

        if not context.bot_data or 'google_integration' not in context.bot_data or not context.bot_data[
                'google_integration']:
            logger.error("Google integration not available in context")
            return False

        google = context.bot_data['google_integration']

        # Use your existing save_group_vin method
        try:
            # Use the group_title parameter if provided, otherwise derive it
            if not group_title:
                group_title = f"Group {group_id}"  # Fallback
                try:
                    # Try to get the group title from context if available
                    if hasattr(context,
                               'effective_chat') and context.effective_chat:
                        group_title = context.effective_chat.title or f"Group {group_id}"
                    elif 'current_group_title' in context.bot_data:
                        group_title = context.bot_data['current_group_title']
                except BaseException:
                    pass

            logger.debug(
                f"Calling google.save_group_vin({group_id}, '{group_title}', '{vin}')")
            success = google.save_group_vin(group_id, group_title, vin)

            if success:
                logger.info(
                    f"Successfully saved VIN {vin} for group {group_id}")
                return True
            else:
                logger.error(
                    f"save_group_vin returned False for group {group_id}")
                return False

        except Exception as e:
            logger.error(f"Error calling save_group_vin: {e}")
            return False

    except Exception as e:
        logger.error(f"Error saving VIN {vin} for group {group_id}: {e}")
        return False


async def get_driver_name_for_vin(
        vin: str,
        context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    """
    Get driver name for a VIN for display in confirmation message
    """
    try:
        assets_data = await load_assets_data(context)
        if not assets_data or len(assets_data) < 2:
            return None

        # Search for VIN in assets (column E = index 4)
        for row in assets_data[1:]:  # Skip header
            if len(row) > 4 and str(row[4]).strip().upper() == vin.upper():
                driver_name = str(row[3]).strip() if len(
                    row) > 3 else ""  # Column D = index 3
                return driver_name if driver_name else None

        return None
    except Exception as e:
        logger.error(f"Error getting driver name for VIN {vin}: {e}")
        return None


# Handler registration - add these to your main bot file
def register_vin_handlers(application):
    """
    Register VIN suggestion handlers with the PTB application
    Call this from your main bot initialization
    """
    # Command handler for manual trigger
    application.add_handler(
        CommandHandler(
            "setvin",
            suggest_vin_on_group_join))

    # Command handler for auto-registration trigger (for testing)
    application.add_handler(
        CommandHandler(
            "autovin",
            auto_register_vin_on_group_join))

    # Callback query handler for VIN selection
    application.add_handler(CallbackQueryHandler(
        on_vin_selected,
        pattern=f"^({CB_VIN_SELECTED}|{CB_MANUAL_SEARCH})"
    ))

    # If you have existing button handlers, you can trigger suggest_vin_on_group_join
    # from your 🛠 Set VIN button handler

    logger.info("VIN suggestion handlers registered")
