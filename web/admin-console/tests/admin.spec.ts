import { test, expect } from "@playwright/test";

test.describe("Admin Views", () => {
  test.beforeEach(async ({ page }) => {
    // Login as admin
    await page.goto("/");
    await page.fill('input[type="text"]', "admin");
    await page.fill('input[type="password"]', "admin");
    await page.click('button[type="submit"]');
    await expect(page.locator(".sidebar")).toBeVisible();
  });

  test("should display users view", async ({ page }) => {
    await page.click('a[data-route="users"]');

    // Check if users table is visible
    await expect(page.locator(".users-table")).toBeVisible();
  });

  test("should display system settings view", async ({ page }) => {
    await page.click('a[data-route="settings"]');

    // Check if settings form is visible
    await expect(page.locator(".settings-form")).toBeVisible();
  });

  test("should display system information view", async ({ page }) => {
    await page.click('a[data-route="system"]');

    // Check if system info is visible
    await expect(page.locator(".system-info")).toBeVisible();
  });

  test("should be able to create a new user", async ({ page }) => {
    await page.click('a[data-route="users"]');

    // Click add user button
    await page.click('button:has-text("Add User")');

    // Fill in user details
    await page.fill('input[name="username"]', "testuser");
    await page.fill('input[name="password"]', "testpass123");

    // Submit
    await page.click('button[type="submit"]');

    // Should see new user in the list
    await expect(page.locator("text=testuser")).toBeVisible();
  });
});
