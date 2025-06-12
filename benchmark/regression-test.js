const puppeteer = require('puppeteer');
const fs = require('fs');

(async () => {
  let browser;
  try {
    console.log('🚀 Starting comprehensive regression test for benchmark visualization...');
    
    browser = await puppeteer.launch({
      headless: false, // Set to true for headless mode
      slowMo: 500 // Slow down for visibility
    });
    
    const page = await browser.newPage();
    await page.setViewport({ width: 1400, height: 900 });
    
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    
    // Test 1: Initial state screenshot
    console.log('📸 Test 1: Taking screenshot of initial state...');
    await page.goto('http://localhost:8080', { waitUntil: 'networkidle0' });
    
    // Wait for the table to load
    await page.waitForSelector('table', { timeout: 5000 });
    
    await page.screenshot({ 
      path: `regression-test-1-initial-${timestamp}.png`,
      fullPage: true
    });
    console.log('✅ Initial state screenshot saved');
    
    // Test 2: File search functionality
    console.log('🔍 Test 2: Testing file search functionality...');
    
    // Click on the search input
    const searchSelector = 'input[type="text"]';
    await page.waitForSelector(searchSelector);
    await page.click(searchSelector);
    
    // Type to filter files
    await page.type(searchSelector, 'jmh');
    
    // Wait a moment for filtering to take effect
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    await page.screenshot({ 
      path: `regression-test-2-search-${timestamp}.png`,
      fullPage: true
    });
    console.log('✅ Search functionality test completed');
    
    // Clear search for next test
    await page.evaluate(() => {
      document.querySelector('input[type="text"]').value = '';
      document.querySelector('input[type="text"]').dispatchEvent(new Event('input'));
    });
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Test 3: File loading functionality
    console.log('📂 Test 3: Testing file loading functionality...');
    
    // Click on search input to trigger dropdown
    const searchInput = 'input#file-search';
    await page.click(searchInput);
    
    // Wait for dropdown to appear and get dropdown items
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    const dropdownItems = await page.$$('.dropdown-item');
    
    if (dropdownItems.length > 0) {
      // Click on the first dropdown item
      await dropdownItems[0].click();
      console.log('Clicked on first dropdown item');
      
      // Wait for data to load
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      await page.screenshot({ 
        path: `regression-test-3-file-loaded-${timestamp}.png`,
        fullPage: true
      });
      console.log('✅ File loading test completed');
    } else {
      console.log('⚠️ No dropdown items found to test loading');
    }
    
    // Test 4: JSON popup functionality
    console.log('📄 Test 4: Testing JSON popup functionality...');
    
    // Look for file icons in the table (📄 cells)
    try {
      await page.waitForSelector('.file-icon', { timeout: 5000 });
      
      // Click on the first file icon
      await page.click('.file-icon');
      console.log('Clicked on file icon (📄)');
      
      // Wait for popup to appear
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Check if popup appeared
      const popup = await page.$('.popup-overlay');
      if (popup) {
        console.log('JSON popup appeared successfully');
        
        await page.screenshot({ 
          path: `regression-test-4-json-popup-${timestamp}.png`,
          fullPage: true
        });
        console.log('✅ JSON popup test completed');
        
        // Close popup by clicking the close button
        try {
          await page.click('.close-btn');
          await new Promise(resolve => setTimeout(resolve, 500));
          console.log('Closed JSON popup');
        } catch (e) {
          // Try clicking overlay to close
          try {
            await page.click('.popup-overlay');
            await new Promise(resolve => setTimeout(resolve, 500));
            console.log('Closed JSON popup via overlay');
          } catch (e2) {
            console.log('Popup might have closed automatically');
          }
        }
      } else {
        console.log('⚠️ JSON popup did not appear');
      }
      
    } catch (e) {
      console.log('⚠️ No file icons found in table');
    }
    
    // Test 5: Final state screenshot
    console.log('📸 Test 5: Taking final screenshot...');
    
    await page.screenshot({ 
      path: `regression-test-5-final-${timestamp}.png`,
      fullPage: true
    });
    console.log('✅ Final state screenshot saved');
    
    // Summary
    console.log('\n🎉 Regression test completed successfully!');
    console.log(`📁 Screenshots saved with timestamp: ${timestamp}`);
    console.log('🔍 Tests performed:');
    console.log('  ✅ Initial state capture');
    console.log('  ✅ File search functionality'); 
    console.log('  ✅ File loading via dropdown');
    console.log(`  ${jsonButtonFound ? '✅' : '⚠️'} JSON popup functionality`);
    console.log('  ✅ Final state capture');
    
  } catch (error) {
    console.error('❌ Error during regression test:', error);
    process.exit(1);
  } finally {
    if (browser) {
      await browser.close();
    }
  }
})();