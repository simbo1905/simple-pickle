#!/usr/bin/env node

const puppeteer = require('puppeteer');

async function runManualTest() {
    console.log('🚀 Starting manual regression test for benchmark visualization...');
    
    const browser = await puppeteer.launch({ 
        headless: false,
        defaultViewport: { width: 1200, height: 800 }
    });
    
    const page = await browser.newPage();
    
    try {
        // Test 1: Load initial page
        console.log('📱 Test 1: Loading initial page...');
        await page.goto('http://localhost:8080', { waitUntil: 'networkidle0' });
        await new Promise(resolve => setTimeout(resolve, 1000));
        await page.screenshot({ path: 'manual-test-1-initial.png', fullPage: true });
        console.log('✅ Initial page loaded and screenshot taken');
        
        // Test 2: Verify color coding is working
        console.log('🎨 Test 2: Verifying color coding...');
        const coloredElements = await page.evaluate(() => {
            const sources = Array.from(document.querySelectorAll('.source'));
            return sources.map(el => ({
                text: el.textContent.trim(),
                backgroundColor: window.getComputedStyle(el).backgroundColor
            }));
        });
        
        console.log('Color coding results:', coloredElements);
        const hasNFP = coloredElements.some(el => el.text === 'NFP');
        const hasJDK = coloredElements.some(el => el.text === 'JDK');
        const hasPTB = coloredElements.some(el => el.text === 'PTB');
        
        if (hasNFP && hasJDK && hasPTB) {
            console.log('✅ Color coding test passed - all source types present');
        } else {
            console.log('❌ Color coding test failed - missing source types');
        }
        
        // Test 3: Test search functionality
        console.log('🔍 Test 3: Testing search functionality...');
        await page.click('input[type="text"]');
        await page.type('input[type="text"]', 'Simple');
        await new Promise(resolve => setTimeout(resolve, 500));
        await page.screenshot({ path: 'manual-test-2-search.png', fullPage: true });
        console.log('✅ Search functionality tested and screenshot taken');
        
        // Test 4: Test status bar updates
        console.log('📊 Test 4: Testing status bar...');
        const statusText = await page.evaluate(() => {
            return document.querySelector('.status')?.textContent || 'No status found';
        });
        console.log('Status bar text:', statusText);
        if (statusText.includes('benchmark results')) {
            console.log('✅ Status bar test passed');
        } else {
            console.log('❌ Status bar test failed');
        }
        
        // Test 5: Clear search and test dropdown
        console.log('📂 Test 5: Testing file dropdown...');
        await page.click('input[type="text"]');
        await page.evaluate(() => document.querySelector('input[type="text"]').value = '');
        await page.click('.search-icon');
        await new Promise(resolve => setTimeout(resolve, 500));
        
        // Check if dropdown appears
        const dropdownVisible = await page.evaluate(() => {
            const dropdown = document.querySelector('.dropdown');
            return dropdown && dropdown.style.display !== 'none';
        });
        
        if (dropdownVisible) {
            console.log('✅ Dropdown test passed - dropdown is visible');
            await page.screenshot({ path: 'manual-test-3-dropdown.png', fullPage: true });
        } else {
            console.log('❌ Dropdown test failed - dropdown not visible');
        }
        
        console.log('🎉 Manual regression test completed!');
        console.log('📸 Screenshots saved: manual-test-1-initial.png, manual-test-2-search.png, manual-test-3-dropdown.png');
        
    } catch (error) {
        console.error('❌ Error during test:', error);
    } finally {
        await browser.close();
    }
}

runManualTest().catch(console.error);