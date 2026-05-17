import { test, expect } from "@playwright/test";

test.describe("Explorer Navigation", () => {
  test.beforeEach(async ({ page }) => {
    // Login before each test
    await page.goto("/");
    await page.fill('input[type="text"]', "admin");
    await page.fill('input[type="password"]', "admin");
    await page.click('button[type="submit"]');
    await expect(page.locator(".sidebar")).toBeVisible();
  });

  test("should display databases", async ({ page }) => {
    // Navigate to explorer (assuming it's part of query view or a separate route)
    await page.click('a[data-route="query"]');

    // Check if explorer sidebar is visible
    await expect(page.locator(".explorer-tree")).toBeVisible();
  });

  test("should expand database to show schemas", async ({ page }) => {
    await page.click('a[data-route="query"]');

    // Click on a database to expand
    const dbNode = page.locator(".explorer-tree .database-node").first();
    await dbNode.click();

    // Should show schemas
    await expect(page.locator(".explorer-tree .schema-node")).toBeVisible();
  });

  test("should expand schema to show tables", async ({ page }) => {
    await page.click('a[data-route="query"]');

    // Expand database
    const dbNode = page.locator(".explorer-tree .database-node").first();
    await dbNode.click();

    // Expand schema
    const schemaNode = page.locator(".explorer-tree .schema-node").first();
    await schemaNode.click();

    // Should show tables/views
    await expect(page.locator(".explorer-tree .table-node")).toBeVisible();
  });
});
