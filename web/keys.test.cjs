const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync(require('node:path').join(__dirname, 'templates/keys-script.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];

function fixture(withExport = true, timeOptions = {}) {
  const el = () => ({ checked: false, textContent: '', handlers: {}, closest() { return null; }, focus() {}, addEventListener(n, fn) { this.handlers[n] = fn; } });
  const items = [el(), el(), el()];
  const form = el(); form.querySelectorAll = () => items;
  const exportValue = el(); exportValue.value = 'gwp_k_test-first\ngwp_k_test-second';
  exportValue.focus = () => { exportValue.focused = true; };
  exportValue.select = () => { exportValue.selected = true; };
  const elements = {
    'key-batch': form, 'select-keys': el(), 'key-selection-count': el(),
    'created-keys': withExport ? exportValue : null, 'copy-keys': el(),
    'download-keys': el(), 'key-export-status': el(),
    'key-batchbar': el(), 'clear-key-selection': el(),
  };
  const out = {};
  const context = {
    document: {
      getElementById: id => elements[id],
      querySelectorAll: selector => selector === '[data-key-last-time]' ? (timeOptions.nodes || []) : selector === '[data-key-timezone]' ? (timeOptions.labels || []) : [],
      createElement: () => ({ click() { out.filename = this.download; out.url = this.href; } }),
    },
    navigator: { clipboard: { writeText: async text => { out.copied = text; } } },
    Blob, URL: { createObjectURL(blob) { out.blob = blob; return 'blob:offline'; }, revokeObjectURL(url) { out.revoked = url; } },
    setTimeout: fn => fn(),
  };
  if (Object.hasOwn(timeOptions, 'intl')) context.Intl = timeOptions.intl;
  else if (timeOptions.zone) context.Intl = {
    DateTimeFormat: function (locale, options) { return new Intl.DateTimeFormat(locale, {...options, timeZone:timeOptions.zone}); },
  };
  vm.runInNewContext(script, context);
  return { elements, items, out, context, exportValue };
}

test('selection is limited to this page and empty submit is blocked', () => {
  const f = fixture(false);
  let prevented = false;
  f.elements['key-batch'].handlers.submit({ preventDefault() { prevented = true; } });
  assert.equal(prevented, true);
  f.elements['select-keys'].checked = true;
  f.elements['select-keys'].handlers.change();
  assert.equal(f.items.filter(item => item.checked).length, 3);
  f.items[0].checked = false;
  f.items[0].handlers.change();
  assert.equal(f.elements['select-keys'].indeterminate, true);
  assert.equal(f.elements['key-selection-count'].textContent, '已选 2 把');
  assert.equal(f.elements['key-batchbar'].hidden, false);
  f.elements['clear-key-selection'].handlers.click();
  assert.equal(f.items.some(item => item.checked), false);
  assert.equal(f.elements['key-batchbar'].hidden, true);
});

test('copy and download preserve only one plaintext key per line', async () => {
  const f = fixture();
  await f.elements['copy-keys'].handlers.click();
  assert.equal(f.out.copied, f.exportValue.value);
  f.elements['download-keys'].handlers.click();
  assert.equal(await f.out.blob.text(), f.exportValue.value + '\n');
  assert.equal(f.out.filename, 'gwpool-keys.txt');
  assert.equal(f.out.revoked, 'blob:offline');
});

test('clipboard failure selects original text for manual copying', async () => {
  const f = fixture();
  f.context.navigator.clipboard.writeText = async () => { throw new Error('offline permission'); };
  await f.elements['copy-keys'].handlers.click();
  assert.equal(f.exportValue.selected, true);
  assert.equal(f.elements['key-export-status'].textContent, '请手动复制选中文本');
});

function timeNode(iso, full = false) {
  const clock = {textContent:'UTC clock'}, date = {textContent:'UTC date'};
  return {
    dataset:{keyLastTime:iso}, clock, date, textContent:'UTC full',
    hasAttribute: name => name === 'data-key-full-time' && full,
    querySelector: selector => selector === '[data-key-clock]' ? clock : date,
  };
}

test('last requests use browser timezone with midnight, date rollover and DST', () => {
  for (const [zone,iso,date,clock] of [
    ['Asia/Taipei','2026-10-07T20:30:45Z','2026-10-08','04:30:45'],
    ['America/Los_Angeles','2026-01-01T00:30:45Z','2025-12-31','16:30:45'],
    ['America/Los_Angeles','2026-03-08T09:59:59Z','2026-03-08','01:59:59'],
    ['America/Los_Angeles','2026-03-08T10:00:00Z','2026-03-08','03:00:00'],
    ['America/Los_Angeles','2026-11-01T08:59:59Z','2026-11-01','01:59:59'],
    ['America/Los_Angeles','2026-11-01T09:00:00Z','2026-11-01','01:00:00'],
    ['UTC','2026-10-08T00:00:00Z','2026-10-08','00:00:00'],
  ]) {
    const row = timeNode(iso), detail = timeNode(iso, true), label = {textContent:'UTC'};
    fixture(false, {zone,nodes:[row,detail],labels:[label]});
    assert.equal(row.clock.textContent,clock,zone+' '+iso);
    assert.equal(row.date.textContent,date);
    assert.equal(detail.textContent,date+' '+clock);
    assert.equal(row.dataset.keyLastTime,iso);
    assert.equal(label.textContent,zone);
  }
});

test('missing timestamps stay empty and failed localization retains UTC', () => {
  const empty = timeNode('');
  fixture(false,{zone:'Asia/Taipei',nodes:[empty]});
  assert.equal(empty.clock.textContent,'UTC clock');
  assert.equal(empty.date.textContent,'UTC date');
  for (const options of [{intl:undefined},{zone:'Asia/Taipei',invalid:true}]) {
    const row = timeNode('2026-10-07T20:30:45Z'), label = {textContent:'UTC'};
    const nodes = options.invalid ? [row,timeNode('invalid')] : [row];
    fixture(false,{...options,nodes,labels:[label]});
    assert.equal(row.clock.textContent,'UTC clock');
    assert.equal(label.textContent,'UTC');
  }
});
