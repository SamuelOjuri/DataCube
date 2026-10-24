import {test, expect} from '@playwright/test';
import {fixture, runId, signIn, thread} from './fixture';

for (const chart of [false, true]) {
  test(`project references stay text in ${chart ? 'charts and tables' : 'reports'}`, async ({page}) => {
    const state = await fixture(page, {existing: true, exploratory: true});
    const title = 'Projects by order value';
    state.result = {...state.result, columns: ['project', 'order_value'],
      rows: [['0018952', '1200.00'], ['18951', '300.50']],
      provenance: {...state.result.provenance, title, returned_rows: 2, columns: [
        {name: 'project', label: 'Project', type: 'text', role: 'dimension', unit: 'none'},
        {name: 'order_value', label: 'Order value', type: 'decimal', role: 'measure', unit: 'source_currency'},
      ]}};
    state.workflow[runId].answer = {...state.workflow[runId].answer,
      summary: 'Among the returned groups, 0018952 had the highest order value at £1,200.00.',
      ...(chart ? {exploratory_chart: {kind: 'bar', x: 'project', series: ['order_value'], sort: 'descending'}} : {})};
    await signIn(page, `/conversations/${thread}`);
    const card = page.getByRole('region', {name: title, exact: true});
    await expect(card.getByLabel('Summary', {exact: true})).toContainText('0018952');
    if (chart) {
      await expect(card.locator('.chart svg text').filter({hasText: /^0018952$/})).toBeVisible();
      await card.getByRole('button', {name: 'View 2 values'}).click();
    }
    await expect(card.getByRole('columnheader', {name: /Project/})).toBeVisible();
    await expect(card.getByRole('cell', {name: '0018952', exact: true})).toBeVisible();
    await expect(card.getByRole('cell', {name: '18951', exact: true})).toBeVisible();
    await expect(card.getByRole('cell', {name: '£1,200.00', exact: true})).toBeVisible();
  });
}

test('contributing projects display item names and retain numeric value formatting', async ({page}) => {
  await fixture(page, {existing: true});
  await page.route('http://127.0.0.1:4174/v1/results/*/projects?*', route => route.fulfill({
    headers: {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': 'http://127.0.0.1:4173',
      'Access-Control-Allow-Headers': 'authorization,content-type', 'Access-Control-Allow-Methods': 'GET,OPTIONS'},
    body: JSON.stringify({columns: ['project_id', 'source_id', 'source_value', 'project'],
      rows: [['1778334088', '1778334088', '1200.00', '0018952']],
      has_more: false, offset: 0, queried_at: '2026-10-24T12:00:00Z', limitation: 'Live project evidence.'}),
  }));
  await signIn(page, `/conversations/${thread}`);
  await page.getByRole('button', {name: 'Explore contributing projects'}).click();
  const detail = page.getByRole('region', {name: 'Contributing projects', exact: true});
  await expect(detail.getByRole('cell', {name: '0018952', exact: true})).toBeVisible();
  await expect(detail.getByRole('cell', {name: '£1,200.00', exact: true})).toBeVisible();
  await expect(detail.getByRole('columnheader', {name: /project_id|source_id/})).toHaveCount(0);
});
