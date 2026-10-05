const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, 'templates/layout.html'), 'utf8');
const script = html.slice(html.indexOf('  const gatewayDialog ='), html.indexOf('  // 概览卡片里的分类计数'));

test('details clone safe content without removing searchable source, then restore focus', () => {
  const handlers = {};
  const title = { textContent: '' };
  const body = { children: [], replaceChildren(...items) { this.children = items; } };
  const close = { focus() { this.focused = true; } };
  const content = { cloneNode(deep) { assert.equal(deep, true); return { clone: true }; } };
  const summary = {
    isConnected: true, focus() { this.focused = true; },
    parentElement: {
      querySelector() { return content; },
      closest() { return { dataset: { gateway: '<unsafe & literal>' } }; }
    }
  };
  const dialog = {
    open: false,
    querySelector(selector) { return selector === '#gateway-detail-title' ? title : selector === '[data-close-gateway]' ? close : body; },
    showModal() { this.open = true; },
    close() { this.open = false; handlers.close(); },
    addEventListener(name, callback) { handlers[name] = callback; }
  };
  const document = { getElementById() { return dialog; }, addEventListener(name, callback) { handlers[name] = callback; } };
  vm.runInNewContext(script, { document });
  let prevented = false;
  handlers.click({ target: { closest(selector) { return selector.startsWith('#t-gateways') ? summary : null; } }, preventDefault() { prevented = true; } });
  assert.equal(prevented, true);
  assert.equal(dialog.open, true);
  assert.equal(title.textContent, '<unsafe & literal>');
  assert.equal(body.children[0].clone, true);
  assert.equal(summary.parentElement.querySelector(), content);
  assert.equal(close.focused, true);
  handlers.click({ target: { closest(selector) { return selector === '[data-close-gateway]' ? close : null; } } });
  assert.equal(dialog.open, false);
  assert.equal(summary.focused, true);
  assert.deepEqual(body.children, []);
});

test('unsupported dialog leaves native details untouched', () => {
  vm.runInNewContext(script, { document: {
    getElementById() { return {}; },
    addEventListener() { assert.fail('native details must remain unmodified'); }
  } });
});
