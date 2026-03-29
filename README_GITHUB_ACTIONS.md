# GitHub Actions Automation for Playwright Scraper

## Overview
This document explains how to set up automated scraping using GitHub Actions. The scraper can run on a schedule without human intervention.

## Setup Instructions

### 1. Push to GitHub
```bash
git init
git add .
git commit -m "Initial commit with GitHub Actions"
git remote add origin https://github.com/yourusername/your-repo-name.git
git push -u origin main
```

### 2. Enable GitHub Actions
- Go to your repository on GitHub
- Navigate to **Actions** tab
- The workflows will be automatically detected

### 3. Available Workflows

#### Daily Scrape (`scrape.yml`)
- **Schedule**: Daily at 2 AM UTC
- **Manual trigger**: Available from GitHub UI
- **Parameters**: URL, limit, store name
- **Output**: Artifacts with JSON data

#### Test Scraper (`scrape-test.yml`)
- **Trigger**: On push to main or pull requests
- **Purpose**: Test setup without actual scraping
- **Output**: Verification of Playwright installation

### 4. Manual Trigger
1. Go to **Actions** → **Daily Product Scrape**
2. Click **Run workflow**
3. Optional: Provide custom parameters
4. Click **Run workflow**

### 5. Access Results
After each run:
1. Go to the workflow run
2. Click **Artifacts**
3. Download `scrape-results`
4. Extract to get JSON files

## Customization

### Change Schedule
Edit `.github/workflows/scrape.yml`:
```yaml
schedule:
  # Every 6 hours
  - cron: '0 */6 * * *'
  
  # Daily at 8 AM
  - cron: '0 8 * * *'
  
  # Every Monday at 9 AM
  - cron: '0 9 * * 1'
```

### Change Scraper Arguments
Edit the workflow file to modify:
- Target URLs
- Selectors
- Limits
- Delay settings

## GitHub Actions Limits
- **Free tier**: 2,000 minutes/month (private), unlimited (public)
- **Max runtime**: 6 hours per job
- **Artifact retention**: 90 days (public), 7 days (private)
- **Concurrent jobs**: 20 (free), 180 (pro)

## Troubleshooting

### Common Issues

1. **Timeout errors**
   - Reduce scraping limits
   - Increase `timeout-minutes` in workflow
   - Use `--max-pages` to limit pages

2. **Browser launch failures**
   - Check system dependencies are installed
   - Ensure Firefox is properly installed
   - Try `--headless-only` flag

3. **Rate limiting**
   - Increase `--delay-seconds`
   - Use `--manual-wait-seconds` for manual verification
   - Consider rotating IPs or using proxies

### Monitoring
- Check workflow run logs in GitHub
- Set up email notifications for failures
- Monitor GitHub Actions usage in Settings → Billing

## Next Steps

### Database Integration
After scraping, add a step to upload to cloud database:
```yaml
- name: Upload to Database
  run: python db_uploader.py
```

### Notifications
Add notifications for success/failure:
```yaml
- name: Send notification
  uses: actions/github-script@v6
  with:
    script: |
      // Send Discord/Slack/Email notification
```

### Multiple Stores
Schedule different stores at different times:
```yaml
# Separate workflow for each store
- cron: '0 2 * * *'  # Store A at 2 AM
- cron: '0 4 * * *'  # Store B at 4 AM
```

## Support
For issues with GitHub Actions setup:
1. Check workflow run logs
2. Review GitHub Actions documentation
3. Test with `scrape-test.yml` first
4. Adjust timeouts and resource limits