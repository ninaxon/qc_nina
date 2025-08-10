# 🚨 SECURITY INCIDENT RESPONSE PLAN

## **IMMEDIATE ACTIONS REQUIRED**

### **1. REVOKE ALL LEAKED CREDENTIALS IMMEDIATELY**

**A. Telegram Bot Token**
1. Go to [@BotFather](https://t.me/botfather) on Telegram
2. Send `/mybots`
3. Select your bot
4. Choose "API Token"
5. Select "Revoke current token"
6. Generate a new token
7. Update Railway/platform secrets with new token

**B. Google Service Account Key**
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to IAM & Admin > Service Accounts
3. Find service account: `data-warehouse-452216-cb7ee86d19ea.json`
4. Click on service account
5. Go to "Keys" tab
6. **DELETE the compromised key immediately**
7. Create new key (JSON format)
8. Upload to Railway/platform secrets (don't commit to repo)

**C. TMS API Credentials**
1. Contact TMS provider immediately
2. Revoke current API key and hash
3. Generate new credentials
4. Update Railway/platform secrets

### **2. REMOVE SECRETS FROM REPO HISTORY**

```bash
# Install BFG Repo-Cleaner
# Download from: https://rtyley.github.io/bfg-repo-cleaner/

# Remove all JSON files from history
java -jar bfg.jar --delete-files "*.json" --delete-folders "credentials"

# Remove .env files
java -jar bfg.jar --delete-files ".env"

# Clean up repository
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# Force push (WARNING: This rewrites history)
git push --force-with-lease --all
```

### **3. SECURE CONFIGURATION**

Create secure environment variable setup:

```bash
# On Railway/platform, set these secrets:
TELEGRAM_BOT_TOKEN=your_new_bot_token
GOOGLE_SERVICE_ACCOUNT_JSON=base64_encoded_json_content
TMS_API_KEY=new_tms_key
TMS_API_HASH=new_tms_hash
```

## **VERIFICATION CHECKLIST**

- [ ] All old tokens revoked and new ones generated
- [ ] Secrets removed from repo history using BFG
- [ ] `.gitignore` prevents future secret commits
- [ ] Platform environment variables updated
- [ ] Application tested with new credentials
- [ ] Security incident documented and reported

## **PREVENTION MEASURES**

1. **Never commit secrets to repos**
2. **Use environment variables exclusively**
3. **Regular security audits**
4. **Pre-commit hooks to prevent secret commits**
5. **Separate development and production credentials**

## **TIMELINE**

- **T+0**: Incident discovered
- **T+15min**: All credentials revoked
- **T+30min**: Secrets removed from repo history  
- **T+45min**: New credentials deployed
- **T+60min**: Application verified working
- **T+24hr**: Full security review completed