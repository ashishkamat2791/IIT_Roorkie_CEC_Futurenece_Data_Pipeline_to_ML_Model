// Headless capture of the Airflow 3 UI (real local instance). Usage:
//   SHOT=docs/screenshots/x.png MODE=graph node <browser.mjs> <run-url> --script scripts/screenshots/airflow_graph.mjs
// MODE=graph clicks "Show Graph" on a DAG-run page; MODE=page captures the page as loaded.
export default async function run(page, ui) {
  await page.setViewportSize({ width: 1700, height: 1050 })
  await page.waitForTimeout(2500)
  if ((process.env.MODE || 'graph') === 'graph') {
    const snap = await ui.snapshot()
    const ref = snap.match(/@(e\d+) button "Show Graph/)?.[1]
    if (!ref) return { error: 'no Show Graph button', snap: snap.slice(0, 800) }
    await ui.click('@' + ref)
    await page.waitForTimeout(3500)
  }
  await page.screenshot({ path: process.env.SHOT, fullPage: false })
  return { saved: process.env.SHOT, url: page.url() }
}
