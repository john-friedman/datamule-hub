import { sec_filings_lookup } from '../dist/index.js';

// Set DATAMULE_API_KEY in your environment, then: node examples/quickstart.mjs
for await (const page of sec_filings_lookup.stream_sgml({
  ticker: 'MSFT',
  submission_type: ['8-K', '8-K/A'],
  filing_date: ['2026-01-01', '2026-10-01'],
  page_size: 100,
})) {
  console.log(page);
}
