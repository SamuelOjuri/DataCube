import {test, expect} from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import {readFileSync} from 'node:fs';
import {fixture, runId, signIn, thread} from './fixture';

const presentations = JSON.parse(readFileSync(new URL('../summary-fixtures.json', import.meta.url), 'utf8'));

for (const width of [1440, 390]) {
  test(`shared summaries and analytical charts at ${width}px`, async ({page}, testInfo) => {
    await page.setViewportSize({width, height: 1000});
    const errors: string[] = [];
    page.on('pageerror', error => errors.push(error.message));
    const state = await fixture(page, {existing: true, exploratory: true});
    for (const name of ['ranking', 'comparison', 'daily', 'quarterly', 'distribution', 'relationship', 'fallback', 'unknown', 'records', 'empty', 'total']) {
      const {answer, result} = presentations[name];
      state.result = structuredClone(result);
      state.workflow[runId].answer = structuredClone(answer);
      state.runs[0].question = result.provenance.title;
      // This fixture uses an exchange-only session; authenticate each navigation.
      await signIn(page, `/conversations/${thread}`);
      const card = page.getByRole('region', {name: result.provenance.title, exact: true});
      const summary = card.getByLabel('Summary', {exact: true});
      await expect(summary).toHaveText(answer.summary);
      if (answer.exploratory_chart) {
        const chart = card.locator('.chart svg');
        await expect(chart).toBeVisible();
        await expect(card.getByRole('table')).toHaveCount(0);
        const summaryBox = await summary.boundingBox(), chartBox = await chart.boundingBox();
        expect(summaryBox!.y + summaryBox!.height).toBeLessThan(chartBox!.y);
        await card.getByRole('button', {name: `View ${result.rows.length} values`}).click();
        await expect(card.getByRole('table')).toBeVisible();
        await card.getByRole('button', {name: 'Hide values'}).click();
        await expect(card.getByText('Chart unavailable.', {exact: false})).toHaveCount(0);
      } else {
        await expect(card.locator('.chart svg')).toHaveCount(0);
        if (name === 'fallback' || name === 'records') await expect(card.getByRole('table')).toBeVisible();
      }
      if (['distribution', 'relationship', 'quarterly', 'fallback'].includes(name)) {
        await page.screenshot({path: testInfo.outputPath(`${name}-${width}.png`), fullPage: true});
      }
    }
    expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
    expect(errors).toEqual([]);
  });
}

test('blank legacy answers and explanations still have a useful summary slot', async ({page}) => {
  const state = await fixture(page, {existing: true, exploratory: true, answerOverride: {summary: ' ', text: ' '}});
  await signIn(page, `/conversations/${thread}`);
  await expect(page.getByLabel('Summary', {exact: true})).toContainText("isn't enough information");
  state.workflow[runId].answer = {kind: 'explanation', title: 'Data unavailable', reason: 'data_unavailable',
    text: '', summary: '', suggestions: [], notices: []};
  await signIn(page, `/conversations/${thread}`);
  await expect(page.getByLabel('Summary', {exact: true})).toContainText("couldn't produce an answer");
});
