import {test, expect} from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import {fixture, signIn, thread} from './fixture';
import {comparisonFixture} from '../comparison-fixture.mjs';

for (const width of [1440, 390]) {
  test(`monthly comparison shows the finding, aligned lines and optional exact values at ${width}px`, async ({page}, testInfo) => {
    await page.setViewportSize({width, height: 1000});
    const data = comparisonFixture();
    const state = await fixture(page, {existing: true, exploratory: true, answerOverride: {
      summary: data.summary, summary_version: '3', exploratory_chart: data.intent,
      summary_notices: ['The current incomplete month and any future months are excluded from this comparison.'],
    }});
    state.result = data.result;
    state.runs[0].question = "Can you compare this year's revenue trend with last year's?";
    const errors: string[] = []; page.on('pageerror', error => errors.push(error.message));
    await signIn(page, `/conversations/${thread}`);
    const card = page.getByRole('region', {name: data.result.provenance.title});
    await expect(card.getByLabel('Summary', {exact: true})).toHaveText(data.summary);
    await expect(card.locator('.chart svg')).toBeVisible();
    await expect(card.locator('.mark-line path')).toHaveCount(2);
    await expect(card.locator('.chart')).toContainText('Nov 2025 – Sept 2026');
    await expect(card.locator('.chart')).toContainText('Nov 2024 – Sept 2025');
    await expect(card.locator('.chart')).not.toContainText('Oct 2026');
    await expect(card.getByRole('table')).toHaveCount(0);
    const summaryBox = await card.getByLabel('Summary', {exact: true}).boundingBox();
    const chartBox = await card.locator('.chart').boundingBox();
    expect(summaryBox!.y + summaryBox!.height).toBeLessThan(chartBox!.y);
    await page.screenshot({path: testInfo.outputPath(`monthly-comparison-${width}.png`), fullPage: true});
    await card.getByRole('button', {name: 'View 12 values'}).click();
    await expect(card.getByRole('table')).toContainText('£709,552.98');
    await card.getByRole('button', {name: 'Next rows'}).click();
    await expect(card.getByRole('table')).toContainText('2026-10-01'); // Original evidence is retained.
    await card.getByRole('button', {name: 'Hide values'}).click();
    await expect(card.getByRole('table')).toHaveCount(0);
    expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
    expect(errors).toEqual([]);
  });
}
