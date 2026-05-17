import { test, expect } from "@playwright/test";

test.describe("Query Execution", () => {
  test.beforeEach(async ({ page }) => {
    // Login before each test
    await page.goto("/");
    await page.fill('input[type="text"]', "admin");
    await page.fill('input[type="password"]', "admin");
    await page.click('button[type="submit"]');
    await expect(page.locator(".sidebar")).toBeVisible();
  });

  test("should execute SELECT query", async ({ page }) => {
    // Navigate to query view
    await page.click('a[data-route="query"]');

    // Type a query
    await page.fill("textarea.query-editor", "SELECT 1");

    // Execute query
    await page.click('button:has-text("Execute")');

    // Check results
    await expect(page.locator(".result-grid")).toBeVisible();
    await expect(page.locator(".result-grid tbody tr")).toHaveCount(1);
  });

  test("should show query ID and timing", async ({ page }) => {
    // Navigate to query view
    await page.click('a[data-route="query"]');

    // Type a query
    await page.fill("textarea.query-editor", "SELECT 1");

    // Execute query
    await page.click('button:has-text("Execute")');

    // Check for query ID
    await expect(page.locator(".query-id")).toBeVisible();

    // Check for timing information
    await expect(page.locator(".timing-cards")).toBeVisible();
  });
});
