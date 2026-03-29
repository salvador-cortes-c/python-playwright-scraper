# 🔧 Troubleshooting Guide: Cookie Generation

## **Common Issues & Solutions**

### **❌ Issue 1: "Firefox not opening"**
**Symptoms:**
- No browser window appears
- Script hangs or fails

**Solutions:**
1. **Check Firefox installation:**
   ```bash
   # Linux/Mac
   which firefox
   
   # Windows
   where firefox
   ```

2. **Install Firefox if missing:**
   - Download from https://www.mozilla.org/firefox/
   - Or use package manager:
     ```bash
     # Ubuntu/Debian
     sudo apt install firefox
     
     # macOS
     brew install firefox
     ```

3. **Run with explicit Firefox path:**
   ```bash
   # Set Firefox binary location
   export PLAYWRIGHT_FIREFOX_BINARY=/path/to/firefox
   python scraper.py ...
   ```

### **❌ Issue 2: "Cloudflare challenge not appearing"**
**Symptoms:**
- Firefox opens but shows normal page
- No "I'm not a robot" challenge

**Solutions:**
1. **Clear browser cache first:**
   ```bash
   # Delete old cookies/storage
   rm -f storage_state.json
   rm -f storage_state.json.backup
   ```

2. **Use incognito/private mode equivalent:**
   ```python
   # The scraper already uses fresh context each time
   # Just delete old storage_state.json
   ```

3. **Try different URL:**
   ```bash
   # Some URLs trigger Cloudflare more reliably
   python scraper.py --url "https://www.newworld.co.nz/" --headed ...
   ```

### **❌ Issue 3: "Script times out waiting for manual verification"**
**Symptoms:**
- Script waits 2 minutes then fails
- Cloudflare challenge takes too long

**Solutions:**
1. **Increase wait time:**
   ```bash
   python scraper.py --manual-wait-seconds 180 ...
   ```

2. **Complete challenge faster:**
   - Stay at computer during execution
   - Complete challenge immediately when it appears

3. **Check internet connection:**
   - Ensure stable connection
   - Cloudflare may throttle slow connections

### **❌ Issue 4: "Playwright browser errors"**
**Symptoms:**
- `BrowserType.launch: Failed to launch browser`
- Various browser launch errors

**Solutions:**
1. **Install browser dependencies:**
   ```bash
   python -m playwright install-deps firefox
   ```

2. **Reinstall browsers:**
   ```bash
   python -m playwright uninstall firefox
   python -m playwright install firefox
   ```

3. **Run with different browser:**
   ```bash
   python -m playwright install chromium
   # Then modify scraper to use chromium
   ```

### **❌ Issue 5: "GitHub Actions still fails after uploading cookies"**
**Symptoms:**
- Cookies uploaded but GitHub Actions still shows Cloudflare error

**Solutions:**
1. **Check cookie freshness:**
   - Cookies expire after 7-14 days
   - Generate fresh cookies

2. **Verify file was committed:**
   ```bash
   git log --oneline -5
   git show --name-only HEAD
   ```

3. **Check GitHub Actions logs:**
   - Look for "Using storage state" message
   - Check if cookies are being loaded

### **❌ Issue 6: "No GUI available" (Headless servers)**
**Symptoms:**
- Running on server/VM without display
- Can't complete manual Cloudflare challenge

**Solutions:**
1. **Use VNC/RDP to access GUI:**
   ```bash
   # Install desktop environment
   sudo apt install ubuntu-desktop x11vnc
   
   # Start VNC server
   x11vnc -display :0 -forever -nopw
   ```

2. **Use cloud VM with GUI:**
   - DigitalOcean/Google Cloud with desktop
   - AWS Workspaces or similar

3. **Alternative: Use scraping service:**
   - ScrapingBee free tier (1,000 requests)
   - Test if bypasses Cloudflare

## **🔄 Quick Fix Checklist**

If nothing works, try this complete reset:

```bash
# 1. Clean everything
rm -rf .venv
rm -f storage_state.json
rm -f storage_state.json.backup

# 2. Fresh setup
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install playwright-stealth
python -m playwright install firefox
python -m playwright install-deps firefox

# 3. Generate cookies with extra time
python scraper.py \
  --url "https://www.newworld.co.nz/" \
  --limit 2 \
  --headed \
  --manual-wait-seconds 180 \
  --storage-state storage_state.json

# 4. Upload
git add storage_state.json
git commit -m "Fresh cookies"
git push origin main
```

## **📞 Still Stuck?**

### **Alternative Approaches:**

#### **1. Use Different Site for Testing**
Test with non-Cloudflare site first:
```bash
python scraper.py \
  --url "https://httpbin.org/html" \
  --product-selector "body" \
  --name-selector "h1" \
  --headless-only \
  --output test.json
```

#### **2. Manual Cookie Extraction**
1. Open Firefox manually
2. Go to https://www.newworld.co.nz/
3. Complete Cloudflare challenge
4. Use browser devtools to export cookies
5. Convert to Playwright storage_state.json format

#### **3. Community Help**
- GitHub Issues on your repository
- Playwright Discord community
- Stack Overflow with #playwright tag

## **✅ Success Signs**

When it works, you'll see:
1. Firefox opens automatically
2. Cloudflare challenge appears
3. You complete challenge (checkboxes, etc.)
4. Script continues and prints "Scraping URL: ..."
5. `storage_state.json` file is created
6. Test products appear in output JSON

---

**Remember:** The cookie generation ONLY needs to be done once every 1-2 weeks. After that, GitHub Actions runs completely automatically!