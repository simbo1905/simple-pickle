const puppeteer = require('puppeteer');
const fs = require('fs');

(async () => {
  try {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    
    // Set viewport size
    await page.setViewport({ width: 1200, height: 800 });
    
    // Navigate to localhost:8080
    await page.goto('http://localhost:8080', { waitUntil: 'networkidle0' });
    
    // Take screenshot
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `benchmark-verification-${timestamp}.png`;
    
    await page.screenshot({ 
      path: filename,
      fullPage: true
    });
    
    console.log(`Screenshot saved as ${filename}`);
    
    await browser.close();
  } catch (error) {
    console.error('Error taking screenshot:', error);
    process.exit(1);
  }
})();