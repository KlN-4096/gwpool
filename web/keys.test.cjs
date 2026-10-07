const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync(require('node:path').join(__dirname, 'templates/keys.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];

function fixture(withExport = true) {
  const el = () => ({ checked: false, textContent: '', handlers: {}, addEventListener(n, fn) { this.handlers[n] = fn; } });
  const items = [el(), el(), el()];
  const form = el(); form.querySelectorAll = () => items;
  const exportValue = el(); exportValue.value = 'gwp_k_test-first\ngwp_k_test-second';
  exportValue.focus = () => { exportValue.focused = true; };
  exportValue.select = () => { exportValue.selected = true; };
  const elements = {
    'key-batch': form, 'select-keys': el(), 'key-selection-count': el(),
    'created-keys': withExport ? exportValue : null, 'copy-keys': el(),
    'download-keys': el(), 'key-export-status': el(),
  };
  const out = {};
  const context = {
    document: { getElementById: id => elements[id], createElement: () => ({ click() { out.filename = this.download; out.url = this.href; } }) },
    navigator: { clipboard: { writeText: async text => { out.copied = text; } } },
    Blob, URL: { createObjectURL(blob) { out.blob = blob; return 'blob:offline'; }, revokeObjectURL(url) { out.revoked = url; } },
    setTimeout: fn => fn(),
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
