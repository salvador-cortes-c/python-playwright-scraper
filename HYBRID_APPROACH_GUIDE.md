# 🔄 Hybrid Approach Guide: Free GitHub Actions + Local Cookie Refresh

## **Overview**
This guide explains how to run the New World scraper for **free** using GitHub Actions, with periodic local cookie refreshes to bypass Cloudflare protection.

## **📊 How It Works**

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Local Machine │     │    GitHub Repo  │     │ GitHub Actions  │
│                 │     │                 │     │                 │
│ 1. Run scraper  │────▶│ 2. Upload       │────▶│ 3. Automated    │
│    with headed  │     │    cookies      │     │    scraping     │
│    browser      │     │    to repo      │     │    daily        │
│    (manual      │     │                 │     │                 │
│    Cloudflare   │     │                 │     │                 │
│    challenge)   │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        └───────────────────────┼───────────────────────┘
                                │
                        ┌───────▼───────┐
                        │  storage_     │
                        │  state.json   │
                        │  (cookies)    │
                        └───────────────┘
```

## **🚀 Quick Start**

### **Step 1: Initial Local Setup**
```bash
# Clone your repository
git clone https://github.com/salvador-cortes-c/python-playwright-scraper.git
cd python-playwright-scraper

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install playwright-stealth
python -m playwright install firefox
```

### **Step 2: First Cookie Generation**
```bash
# Run the refresh script
./refresh_cookies.sh

# OR use the Python script
python quick_refresh.py
```

**During execution:**
1. Firefox will open
2. Complete the Cloudflare "I'm not a robot" challenge
3. Wait 90 seconds for manual verification
4. Script will scrape a few test products
5. Cookies saved to `storage_state.json`

### **Step 3: Upload Cookies to GitHub**
```bash
git add storage_state.json
git commit -m "Initial Cloudflare session cookies"
git push origin main
```

### **Step 4: Automated Daily Scraping**
- GitHub Actions will run **daily at 2 AM UTC**
- Uses saved cookies from `storage_state.json`
- Results saved as artifacts
- **Completely automated!**

## **🔄 Maintenance Schedule**

### **Cookie Refresh Frequency**
- **Recommended**: Every 7-14 days
- **Cloudflare sessions** typically expire after 1-2 weeks
- **Signs cookies expired**: GitHub Actions fails with Cloudflare error

### **Quick Refresh Command**
```bash
# When cookies expire, simply run:
./refresh_cookies.sh
# Then commit and push the updated storage_state.json
```

## **📈 Monitoring & Results**

### **Checking Results**
1. Go to: https://github.com/salvador-cortes-c/python-playwright-scraper/actions
2. Click on latest "Daily Product Scrape" run
3. Download "scrape-results" artifact
4. Extract to get:
   - `products.json` - Current products
   - `price_snapshots.json` - Price history (grows over time)
   - `scrape_progress.json` - Progress tracking
   - `storage_state.json` - Current cookies

### **Success Indicators**
- ✅ Workflow completes without errors
- ✅ `products.json` has product data
- ✅ Artifacts are uploaded
- ✅ No Cloudflare errors in logs

## **⚠️ Troubleshooting**

### **Common Issues & Solutions**

#### **1. Cloudflare Challenge in GitHub Actions**
```
Error: Target page returned a bot challenge (Cloudflare)
```
**Solution**: Cookies expired. Refresh locally and upload new `storage_state.json`

#### **2. Manual Verification Takes Too Long**
**Solution**: Increase `--manual-wait-seconds` in refresh scripts

#### **3. Firefox Doesn't Open Locally**
**Solution**: 
- Ensure Firefox is installed
- Run with `--headed` flag
- Check display settings (if on remote server)

#### **4. Rate Limiting**
**Solution**: Increase `--delay-seconds` in GitHub Actions workflow

## **🔧 Customization**

### **Change Scraping Schedule**
Edit `.github/workflows/scrape.yml`:
```yaml
schedule:
  # Current: Daily at 2 AM UTC
  - cron: '0 2 * * *'
  
  # Alternatives:
  # - cron: '0 */6 * * *'  # Every 6 hours
  # - cron: '0 8 * * 1'    # Every Monday at 8 AM
```

### **Add More URLs/Stores**
Edit the workflow or use manual trigger with parameters:
1. Go to Actions → "Daily Product Scrape"
2. Click "Run workflow"
3. Enter custom URL, limit, or store name

### **Store-Specific Scraping**
```bash
# Refresh cookies for specific store
python scraper.py \
  --url "https://www.newworld.co.nz/" \
  --store-name "New World Rototuna" \
  --headed \
  --manual-wait-seconds 120 \
  --storage-state storage_state_rototuna.json
```

## **💰 Cost Analysis**

### **This Approach: FREE**
- GitHub Actions: Free for public repos
- Local execution: Minimal resources
- **Total cost: $0/month**

### **vs Paid Services**
- ScrapingBee: $49-99/month
- Residential proxies: $300+/month
- **Savings: $49-300+/month**

## **📅 Recommended Routine**

### **Weekly (5 minutes)**
1. Check GitHub Actions runs
2. Verify artifacts are being created
3. Note if cookies will expire soon

### **Bi-weekly (10 minutes)**
1. Run `./refresh_cookies.sh`
2. Commit updated `storage_state.json`
3. Push to GitHub

### **Monthly (15 minutes)**
1. Review scraped data quality
2. Adjust selectors if needed
3. Consider adding more stores/categories

## **🚀 Advanced Tips**

### **Multiple Store Sessions**
Create separate cookie files for different stores:
```bash
# Store A
python scraper.py ... --storage-state storage_store_a.json

# Store B  
python scraper.py ... --storage-state storage_store_b.json
```

### **Database Integration**
Add a step to upload to cloud database:
```yaml
- name: Upload to Database
  run: python db_uploader.py
  # Create db_uploader.py to process JSON and upload
```

### **Notifications**
Set up email/Discord notifications for:
- Workflow failures
- Successful runs with stats
- Cookie expiration warnings

## **✅ Success Checklist**

- [ ] Local environment setup complete
- [ ] Initial cookies generated and uploaded
- [ ] GitHub Actions runs successfully
- [ ] Artifacts contain product data
- [ ] Cookie refresh schedule established

## **🆘 Need Help?**

1. **Check workflow logs** in GitHub Actions
2. **Test locally** with `./refresh_cookies.sh`
3. **Review error messages** and adjust parameters
4. **Consider** [ScrapingBee free tier](https://www.scrapingbee.com/) if persistent issues

---

**Happy scraping!** 🕷️ Your automated price tracker is now running for **free** with minimal maintenance.