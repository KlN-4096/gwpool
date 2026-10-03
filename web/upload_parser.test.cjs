const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const parserSource = readFileSync(path.join(__dirname, 'templates/upload-parser.html'), 'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
const parser = vm.runInNewContext(parserSource + '\ngatewayPoolUpload;');
const parse = input => JSON.parse(JSON.stringify(parser.parse(typeof input === 'string' ? input : JSON.stringify(input))));
// 仅用于离线格式测试的假 JWT，不是有效凭据。
const token = 'eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.c2ln';
const refresh = 'rt.offline-test-token-not-a-secret';
const account = (name, credentials = {}) => ({
  name, platform: 'openai', type: 'oauth',
  credentials: { access_token: token, refresh_token: refresh, chatgpt_account_id: 'test-account', ...credentials }
});

test('matches the sub2api export contract snapshot without importing runtime settings', () => {
  // 人工假凭据，结构独立核对 sub2api ced7aa37b：
  // account_data.go DataPayload/DataAccount + ExportData；
  // openai_oauth_service.go BuildAccountCredentials；AccountsView.vue handleExportData。
  // 不是线上文件，不能把此测试称为真实账号导入或真实刷新验收。
  const result = parse(readFileSync(path.join(__dirname, 'testdata/sub2api-export.json'), 'utf8'));
  assert.equal(result.items.length, 1);
  assert.equal(result.items[0].label, 'offline-oauth-fixture');
  assert.deepEqual(Object.keys(JSON.parse(result.items[0].raw)).sort(), ['access_token', 'account_id', 'refresh_token']);
  assert.equal(result.skipped.length, 1);
  assert.equal(result.skipped[0].label, 'offline-pat-fixture');
});

test('expands sub2api export and only maps the required credential fields', () => {
  const result = parse({
    exported_at: '2026-10-03T00:00:00Z', proxies: [{ url: 'not-to-be-imported' }],
    accounts: [
      { ...account('原名称'), expires_at: 1, extra: { openai_gwpool: true }, credentials: { ...account('').credentials, expires_at: '2026-10-04T00:00:00Z' } },
      account('第二个')
    ]
  });
  assert.equal(result.fromSub2api, true);
  assert.equal(result.error, '');
  assert.equal(result.items.length, 2);
  assert.equal(result.items[0].label, '原名称');
  assert.deepEqual(JSON.parse(result.items[0].raw), {
    access_token: token, refresh_token: refresh, account_id: 'test-account'
  });
  assert.deepEqual(result.skipped, []);
  assert.doesNotMatch(result.items[0].retry, /expires_at|extra|proxies/);
  assert.deepEqual(parse(result.items[0].retry).items[0], result.items[0]);
});

test('skips unsupported platforms, credential modes, types and broken records without echoing tokens', () => {
  const records = [
    { ...account('非 OpenAI'), platform: 'anthropic' },
    ...['apikey', 'setup-token', 'cpr'].map(type => ({ ...account(type), type })),
    account('PAT', { auth_mode: 'personalAccessToken' }),
    account('legacy PAT', { auth_mode: 'oauth', openai_auth_mode: 'personal_access_token' }),
    account('agent', { auth_mode: 'agentIdentity' }),
    account('other client', { client_id: 'other-client' }),
    { ...account('missing'), credentials: {} },
    { ...account('bad type'), credentials: { access_token: 123 } },
    account('bad access', { access_token: 'sensitive-invalid-value' }),
    account('bad refresh', { refresh_token: 'invalid token with whitespace' }),
    null
  ];
  const result = parse({ accounts: records });
  assert.equal(result.items.length, 0);
  assert.equal(result.skipped.length, records.length);
  assert.equal(result.skipped[5].reason.includes('授权方式'), true, '不能被 auth_mode=oauth 遮住旧 PAT 键');
  assert.doesNotMatch(JSON.stringify(result.skipped), /sensitive-invalid-value|invalid token with whitespace|eyJ|rt\.offline/);
});

test('accepts refresh-only exports and the current public OAuth client ID', () => {
  const oauth = readFileSync(path.join(__dirname, '../internal/oauth/oauth.go'), 'utf8');
  assert.equal(parser.clientID, oauth.match(/clientID = "([^"]+)"/)[1]);
  const result = parse({ accounts: [account('refresh only', { access_token: '', client_id: parser.clientID })] });
  assert.equal(result.items.length, 1);
  assert.deepEqual(JSON.parse(result.items[0].raw), { refresh_token: refresh, account_id: 'test-account' });
});

test('keeps auth.json, JSON arrays, adjacent JSON and bare-token input compatible', () => {
  const nested = JSON.stringify({ tokens: { access_token: token }, note: 'brace } and quote " inside string' }, null, 2);
  const result = parse(nested + '\n' + JSON.stringify({ access_token: token }) + '\n' + refresh);
  assert.equal(result.items.length, 3);
  assert.equal(result.items[0].raw, nested);
  assert.equal(result.items[2].raw, refresh);
  assert.equal(result.fromSub2api, false);
  assert.equal(parse(JSON.stringify([token, refresh])).items.length, 2);
  assert.equal(parse('{"tokens":').items[0].raw, '{"tokens":');
  assert.equal(parse('  ').items.length, 0);
});

test('rejects empty/malformed exports, bounds expanded batches and preserves Unicode names', () => {
  for (const accounts of [[], null, {}]) {
    const result = parse({ accounts });
    assert.equal(result.items.length, 0);
    assert.equal(result.skipped.length, 1);
  }
  assert.equal(parse({ accounts: Array.from({ length: 100 }, () => account('ok')) }).items.length, 100);
  const tooMany = parse({ accounts: Array.from({ length: 101 }, () => account('ok')) });
  assert.match(tooMany.error, /100/);
  assert.deepEqual(tooMany.items, [], '超限整批不发，不偷偷截到前 100 项');
  const mixed = JSON.stringify({ accounts: Array.from({ length: 100 }, () => account('ok')) }) + '\n' + refresh;
  assert.match(parse(mixed).error, /100/);
  const named = parse({ accounts: [account('\n' + '😀'.repeat(70) + '\x00')] }).items[0];
  assert.equal(Array.from(named.label).length, 64);
  assert.doesNotMatch(named.label, /[\x00-\x1f]/);
});

// 运行真实 layout 中的上传表单块，只用离线 DOM/fetch 替身，验证解析结果确实被接到串行上传。
function uploadHarness(input, responses) {
  class Element {
    constructor() {
      this.listeners = {};
      this.children = [];
      this.textContent = '';
      this.value = '';
      this.hidden = false;
    }
    addEventListener(name, fn) { this.listeners[name] = fn; }
    append(child) { this.children.push(child); }
    replaceChildren() { this.children = []; }
    get lastChild() { return this.children.at(-1); }
    querySelector() { return this.children.find(child => child.tag === 'a'); }
    set innerHTML(_) { throw new Error('不得用凭据或名称构造 HTML'); }
  }
  const ids = Object.fromEntries(['upload', 'upload-err', 'upload-results', 'creds-file', 'pick'].map(id => [id, new Element()]));
  const box = new Element(), button = new Element();
  box.value = input;
  button.textContent = '上传';
  const form = ids.upload;
  form.action = '/upload';
  form.elements = { creds: box };
  form.querySelector = () => button;
  form.reset = () => { box.value = ''; };
  const document = {
    getElementById: id => ids[id],
    createElement: tag => Object.assign(new Element(), { tag })
  };
  const requests = [], redirects = [];
  const formSource = readFileSync(path.join(__dirname, 'templates/layout.html'), 'utf8');
  const start = formSource.indexOf("  const form = document.getElementById('upload');");
  const end = formSource.indexOf('\n})();', start);
  assert.ok(start > 0 && end > start);
  const context = {
    document, gatewayPoolUpload: parser, URLSearchParams,
    FormData: class extends Map {
      constructor() { super([['creds', box.value], ['label', 'manual label'], ['kind', 'auto']]); }
    },
    fetch: async (url, options) => {
      requests.push({ url, data: Object.fromEntries(options.body) });
      const response = responses[requests.length - 1];
      assert.ok(response, 'unexpected upload');
      if (response.error) return { ok: false, status: 400, text: async () => response.error };
      return { ok: true, json: async () => response };
    },
    location: { pathname: '/', assign: value => redirects.push(value), reload: () => redirects.push('reload') },
    history: { replaceState() {} }
  };
  vm.runInNewContext(formSource.slice(start, end), context);
  return { box, ids, requests, redirects, submit: () => form.listeners.submit({ preventDefault() {} }) };
}

test('uploads mapped credentials serially, carries owner token, shows skips and retains only failures with names', async () => {
  const payload = { accounts: [
    account('<img onerror=alert(1)>'),
    { ...account('API key'), type: 'apikey' },
    account('retry me')
  ] };
  const ui = uploadHarness(JSON.stringify(payload), [
    { owner_url: '/u/test-owner', owner_token: 'owner-test', label: payload.accounts[0].name, url: '/a/test', status: 'created' },
    { error: 'offline mock refusal' },
    { error: 'offline retry refusal' }
  ]);
  await ui.submit();
  assert.equal(ui.requests.length, 2);
  assert.equal(ui.requests[0].data.label, payload.accounts[0].name);
  assert.deepEqual(JSON.parse(ui.requests[0].data.creds), {
    access_token: token, refresh_token: refresh, account_id: 'test-account'
  });
  assert.equal(ui.requests[1].data.owner_token, 'owner-test');
  assert.equal(ui.requests[1].data.label, 'retry me');
  assert.equal(parse(ui.box.value).items.length, 1);
  assert.equal(parse(ui.box.value).items[0].label, 'retry me');
  assert.equal(ui.ids['upload-results'].children.length, 4, '跳过、成功、失败与总览链接均保留');
  assert.deepEqual(ui.redirects, [], '不能在导入完成时跳走、丢掉逐项结果');
  assert.equal(ui.box.readOnly, false);
  const originalRows = [...ui.ids['upload-results'].children];
  await ui.submit();
  assert.equal(ui.requests.length, 3, '只重试失败项');
  assert.equal(ui.requests[2].data.owner_token, 'owner-test', '重试仍属于原上传者，不只依赖浏览器 cookie');
  for (const row of originalRows) assert.ok(ui.ids['upload-results'].children.includes(row), '不能清掉原来每项结果');
  const failedRows = ui.ids['upload-results'].children.filter(row => row.className === 'bad');
  assert.equal(failedRows.at(-1).children[0].textContent, '✗ 第 3 项 · retry me');
});

test('unsupported-only and over-limit exports make no requests; ordinary single upload keeps its label and redirect', async () => {
  const skipped = uploadHarness(JSON.stringify({ accounts: [{ ...account('skip'), type: 'cpr' }] }), []);
  await skipped.submit();
  assert.equal(skipped.requests.length, 0);
  assert.equal(skipped.ids['upload-results'].children.length, 1);
  const tooMany = uploadHarness(JSON.stringify({ accounts: Array.from({ length: 101 }, () => account('x')) }), []);
  await tooMany.submit();
  assert.equal(tooMany.requests.length, 0);
  assert.equal(tooMany.ids['upload-err'].hidden, false);
  const legacy = uploadHarness(token, [{ owner_url: '/u/test', label: 'legacy', url: '/a/test', status: 'created' }]);
  await legacy.submit();
  assert.equal(legacy.requests[0].data.creds, token);
  assert.equal(legacy.requests[0].data.label, 'manual label');
  assert.deepEqual(legacy.redirects, ['/u/test']);
});
