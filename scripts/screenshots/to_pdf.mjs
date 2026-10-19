// Print the loaded page to PDF (headless Chromium). PDF_OUT = output path.
export default async function run(page) {
  await page.waitForLoadState('load')
  await page.evaluate(async () => {
    await Promise.all([...document.images].map(i => i.complete ? null : new Promise(r => { i.onload = i.onerror = r })))
  })
  await page.emulateMedia({ media: 'print' })
  await page.pdf({
    path: process.env.PDF_OUT, format: 'A4', landscape: true, printBackground: true, preferCSSPageSize: true,
    displayHeaderFooter: true, headerTemplate: '<span></span>',
    footerTemplate: '<div style="font-size:8px;width:100%;text-align:center;color:#666">NammaMart ETL &amp; ELT — Ashish Kamat · page <span class="pageNumber"></span> of <span class="totalPages"></span></div>',
    margin: { top: '12mm', bottom: '14mm', left: '12mm', right: '12mm' },
  })
  const broken = await page.evaluate(() => [...document.images].filter(i => !i.naturalWidth).map(i => i.src))
  return { pdf: process.env.PDF_OUT, images: (await page.evaluate(() => document.images.length)), broken }
}
