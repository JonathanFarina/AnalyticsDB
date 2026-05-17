import { test, expect } from "@playwright/test";

test.describe("Login", () => {
  test("should display login form", async ({ page }) => {
    await page.goto("/");

    // Check if login form is visible
    await expect(page.locator("form")).toBeVisible();
    await expect(page.locator('input[type="text"]')).toBeVisible();
    await expect(page.locator('input[type="password"]')).toBeVisible();
    await expect(page.locator('button[type="submit"]')).toBeVisible();
  });

  test("should show error on invalid credentials", async ({ page }) => {
    await page.goto("/");

    // Fill in invalid credentials
    await page.fill('input[type="text"]', "invalid");
    await page.fill('input[type="password"]', "wrong");
    await page.click('button[type="submit"]');

    // Check for error message
    await expect(page.locator(".error-message")).toBeVisible();
  });

  test("should login with valid credentials", async ({ page }) => {
    await page.goto("/");

    // Fill in valid credentials (assumes test user exists)
    await page.fill('input[type="text"]', "admin");
    await page.fill('input[type="password"]', "admin");
    await page.click('button[type="submit"]');

    // Should redirect to main app
    await expect(page.locator(".sidebar")).toBeVisible();
    await expect(page.locator(".view-outlet")).toBeVisible();
  });
});
