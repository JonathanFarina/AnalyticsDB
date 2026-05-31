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

    // Check for the login error message
    await expect(page.locator(".login-error")).toBeVisible();
  });

  // Valid credentials are environment-specific: a freshly initialized cluster
  // has only `analyticsdb_admin` with a random password (printed once at
  // `--init-cluster`). Set ADMIN_USER / ADMIN_PASSWORD to run this end-to-end
  // against a live gateway.
  const adminUser = process.env.ADMIN_USER;
  const adminPassword = process.env.ADMIN_PASSWORD;
  test(adminUser && adminPassword ? "should login with valid credentials" : "should login with valid credentials [skipped: set ADMIN_USER/ADMIN_PASSWORD]", async ({ page }) => {
    test.skip(!adminUser || !adminPassword, "ADMIN_USER/ADMIN_PASSWORD not set");
    await page.goto("/");

    await page.fill('input[type="text"]', adminUser!);
    await page.fill('input[type="password"]', adminPassword!);
    await page.click('button[type="submit"]');

    // Should render the authenticated shell.
    await expect(page.locator(".sidebar")).toBeVisible();
    await expect(page.locator(".view-outlet")).toBeVisible();
  });
});
