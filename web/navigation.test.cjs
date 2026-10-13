const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, 'templates/layout.html'), 'utf8');
const script = html.slice(html.indexOf('  const groups = [];'), html.indexOf('  // 表格增强：'));

function fixture(names, definitions, initial) {
  const panels = definitions.map(([name, group]) => ({
    id: `tab-${name}`, dataset: { tabGroup: group }, hidden: false,
    detail: name === 'owners' ? { open: false } : null,
    querySelector() { return this.detail; }, scrollIntoView() { this.scrolled = true; },
  }));
  const links = names.map(name => ({
    hash: `#${name}`, attrs: {},
    setAttribute(k, v) { this.attrs[k] = v; },
    removeAttribute(k) { delete this.attrs[k]; },
  }));
  const callbacks = {};
  const document = {
    querySelectorAll(selector) {
      return selector === '[data-tabs]' ? [{ querySelectorAll: () => links }] : panels;
    },
    getElementById: id => panels.find(panel => panel.id === id),
    addEventListener: (name, cb) => { callbacks[name] = cb; },
  };
  const location = { hash: initial };
  const context = { document, location,
    history: { pushState: (_, __, hash) => { location.hash = hash; } },
    addEventListener: (name, cb) => { callbacks[name] = cb; },
  };
  vm.runInNewContext(script, context);
  return {
    visible: () => panels.filter(p => !p.hidden).map(p => p.id),
    click(name) {
      const a = { hash: `#${name}` };
      callbacks.click({ target: { closest: () => a }, preventDefault() {} });
    },
    pop(name) { location.hash = `#${name}`; callbacks.popstate(); },
    links,
    panels,
  };
}

test('admin groups preserve legacy account and record links, including back navigation', () => {
  const f = fixture(['overview', 'gateways', 'accounts', 'proxies', 'samples', 'config'], [
    ['overview'], ['gateways'], ['accounts', 'accounts'], ['owners', 'accounts'],
    ['external', 'accounts'], ['proxies'], ['slots', 'records'], ['samples', 'records'],
    ['fails', 'records'], ['config'],
  ], '#external');
  assert.deepEqual(f.visible(), ['tab-accounts', 'tab-owners', 'tab-external']);
  assert.equal(f.links[2].attrs['aria-current'], 'page');
  assert.equal(f.panels.find(p => p.id === 'tab-external').scrolled, true);
  f.pop('owners');
  assert.equal(f.panels.find(p => p.id === 'tab-owners').detail.open, true);
  assert.equal(f.panels.find(p => p.id === 'tab-owners').scrolled, true);
  f.click('fails');
  assert.deepEqual(f.visible(), ['tab-slots', 'tab-samples', 'tab-fails']);
  assert.equal(f.panels.find(p => p.id === 'tab-fails').scrolled, true);
  f.click('config');
  assert.deepEqual(f.visible(), ['tab-config']);
  f.pop('gateways');
  assert.deepEqual(f.visible(), ['tab-gateways']);
  f.pop('not-a-tab');
  assert.deepEqual(f.visible(), ['tab-overview']);
});

test('pool prefixed anchors and ordinary tabs stay compatible', () => {
  const f = fixture(['gateways', 'usage', 'submit'], [
    ['gateways'], ['usage'], ['submit'],
  ], '#tab-gateways');
  assert.deepEqual(f.visible(), ['tab-gateways']);
  f.click('usage');
  assert.deepEqual(f.visible(), ['tab-usage']);
  f.pop('tab-submit');
  assert.deepEqual(f.visible(), ['tab-submit']);
  f.click('tab-gateways');
  assert.deepEqual(f.visible(), ['tab-gateways']);
});

test('owner integration guide supports direct links and return to accounts', () => {
  const f = fixture(['accounts', 'usage', 'import'], [
    ['accounts'], ['usage'], ['import'],
  ], '#usage');
  assert.deepEqual(f.visible(), ['tab-usage']);
  f.click('accounts');
  assert.deepEqual(f.visible(), ['tab-accounts']);
  f.pop('tab-usage');
  assert.deepEqual(f.visible(), ['tab-usage']);
  f.click('import');
  assert.deepEqual(f.visible(), ['tab-import']);
});
