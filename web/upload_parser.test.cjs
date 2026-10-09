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

// Run the actual workspace script against an offline task API/DOM.
function uploadHarness(input, responses, options = {}) {
  class Element {
    constructor() {
      this.listeners = {};
      this.children = [];
      this.textContent = '';
      this.value = '';
      this.hidden = false;
      this.readOnly = false;
    }
    addEventListener(name, fn) { this.listeners[name] = fn; }
    append(...children) { for (const child of children) { this.children = this.children.filter(x => x !== child); this.children.push(child); } }
    replaceChildren(...children) { this.children = children; }
    get lastChild() { return this.children.at(-1); }
    querySelector() { return this.children.find(child => child.tag === 'a'); }
    querySelectorAll() { return []; }
    setAttribute(key, value) { this[key] = value; }
    focus() {}
    set innerHTML(_) { throw new Error('不得用凭据或名称构造 HTML'); }
  }
  const ids = Object.fromEntries(['upload', 'upload-err', 'upload-results', 'creds-file', 'pick',
    'upload-summary', 'upload-leave-note', 'upload-mode-credentials', 'upload-mode-login',
    'upload-credentials-panel', 'upload-login-panel', 'login-batch', 'upload-editor', 'upload-editor-title'].map(id => [id, new Element()]));
  const box = new Element(), button = new Element();
  box.value = input;
  button.textContent = '上传';
  const form = ids.upload;
  form.action = '/upload';
  form.elements = { creds: box, kind: { value: 'auto' }, label: { value: 'manual label' },
    login_batch: new Element(), login_email: new Element(), login_password: new Element(), login_totp: new Element() };
  if (options.ownerToken) form.elements.owner_token = { value: options.ownerToken };
  form.querySelector = () => button;
  form.querySelectorAll = () => [box, form.elements.login_batch, form.elements.login_email,
    form.elements.login_password, form.elements.login_totp,
    ...descendants(ids['upload-results']).filter(x => x.tag === 'input')];
  form.reset = () => { box.value = ''; };
  const document = {
    getElementById: id => ids[id],
    createElement: tag => Object.assign(new Element(), { tag })
  };
  const requests = [], redirects = [], timers = new Map(), jobs = new Map();
  const formSource = readFileSync(path.join(__dirname, 'templates/upload-script.html'), 'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
  const context = {
    document, gatewayPoolUpload: parser, URLSearchParams, AbortController,
    window: { addEventListener() {} },
    setTimeout: (fn, ms) => { const id = { ms }; timers.set(id, fn); return id; },
    clearTimeout: id => timers.delete(id),
    FormData: class extends Map {
      constructor() {
        super([['creds', box.value], ['label', 'manual label'], ['kind', 'auto']]);
        if (options.ownerToken) this.set('owner_token', options.ownerToken);
      }
    },
    fetch: async (url, options) => {
      if (url !== '/upload/jobs') {
        const job = jobs.get(url.split('/')[3]);
        assert.ok(job, 'unexpected status query');
        const customized = harnessOptions.onTaskRequest ? await harnessOptions.onTaskRequest(url, options, job) : null;
        if (customized?.networkError) throw new TypeError('offline network interruption');
        if (customized) return {
          ok: !customized.error, status: customized.status || (customized.error ? 400 : 200),
          headers: { get: () => customized.error ? 'text/plain' : 'application/json' },
          text: async () => customized.error || JSON.stringify(customized.data)
        };
        return { ok: true, status: 200, headers: { get: () => 'application/json' },
          text: async () => JSON.stringify({ id: job.id, state: 'completed', stage: 'done',
            stages: ['queued', 'validate', 'store', 'done'], receipt: job.receipt }) };
      }
      requests.push({ url, data: Object.fromEntries(options.body) });
      let response = responses[requests.length - 1];
      assert.ok(response, 'unexpected upload');
      if (typeof response === 'function') response = await response(options);
      if (response.networkError) throw new TypeError('offline network failure');
      let raw = response.raw ?? response.error;
      if (raw == null) {
        const id = String(requests.length).padStart(48, 'a');
        const key = 'b'.repeat(48);
        jobs.set(id, { id, receipt: response });
        raw = JSON.stringify({ id, key });
      }
      return {
        ok: !response.error, status: response.status || (response.error ? 400 : 200),
        headers: { get: name => name.toLowerCase() === 'content-type' ? response.contentType || (response.error ? 'text/plain' : 'application/json') : null },
        text: async () => raw, json: async () => JSON.parse(raw)
      };
    },
    location: { pathname: '/', assign: value => redirects.push(value), reload: () => redirects.push('reload') },
    history: { replaceState() {} }
  };
  const harnessOptions = options;
  vm.runInNewContext(formSource, context);
  return { box, button, form, ids, requests, redirects, timers,
    poll: async () => {
      for (const [id, fn] of [...timers]) if (id.ms === 1000) { timers.delete(id); fn(); }
      await flushPromises();
    },
    submit: () => form.listeners.submit({ preventDefault() {} }) };
}

test('uploads mapped credentials serially, carries owner token, shows skips and retains only failures with names', async () => {
  const payload = { accounts: [
    account('<img onerror=alert(1)>'),
    { ...account('API key'), type: 'apikey' },
    account('retry me', { chatgpt_account_id: 'different-offline-account' })
  ] };
  const ui = uploadHarness(JSON.stringify(payload), [
    { owner_url: '/u/owner-test', owner_token: 'owner-test', label: payload.accounts[0].name, url: '/a/test', status: 'created' },
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
  assert.equal(ui.ids['upload-results'].children.length, 3, '逐项跳过、成功与失败均保留；成功卡内有总览链接');
  assert.deepEqual(ui.redirects, [], '不能在导入完成时跳走、丢掉逐项结果');
  assert.equal(ui.box.readOnly, false);
  const originalRows = [...ui.ids['upload-results'].children];
  await ui.submit();
  assert.equal(ui.requests.length, 3, '只重试失败项');
  assert.equal(ui.requests[2].data.owner_token, 'owner-test', '重试仍属于原上传者，不只依赖浏览器 cookie');
  for (const row of originalRows) assert.ok(ui.ids['upload-results'].children.includes(row), '不能清掉原来每项结果');
  const failedRows = ui.ids['upload-results'].children.filter(row => row.className.endsWith('failed'));
  assert.equal(failedRows.at(-1).children[0].children[0].textContent, '第 3 项 · retry me');
});

test('unsupported-only and over-limit exports make no requests; ordinary single upload keeps its label and visible receipt', async () => {
  const skipped = uploadHarness(JSON.stringify({ accounts: [{ ...account('skip'), type: 'cpr' }] }), []);
  await skipped.submit();
  assert.equal(skipped.requests.length, 0);
  assert.equal(skipped.ids['upload-results'].children.length, 1);
  const tooMany = uploadHarness(JSON.stringify({ accounts: Array.from({ length: 101 }, () => account('x')) }), []);
  await tooMany.submit();
  assert.equal(tooMany.requests.length, 0);
  assert.equal(tooMany.ids['upload-err'].hidden, false);
  const legacy = uploadHarness(token, [{ owner_url: '/u/test', owner_token: 'test', label: 'legacy', url: '/a/test', status: 'created' }]);
  await legacy.submit();
  assert.equal(legacy.requests[0].data.creds, token);
  assert.equal(legacy.requests[0].data.label, 'manual label');
  assert.deepEqual(legacy.redirects, [], '完成后保留进度与结果，不自动跳走');
});

const uploadReceipt = id => ({
  owner_url: '/u/owner-test', owner_token: 'owner-test',
  label: 'offline ' + id, url: '/a/offline-' + id, status: 'created'
});
const flushPromises = () => new Promise(resolve => setImmediate(resolve));
const nodeText = node => node.textContent + '\n' + node.children.map(nodeText).join('\n');
const rowMessages = ui => nodeText(ui.ids['upload-results']) + ui.ids['upload-err'].textContent;

test('distinguishes invalid success receipts from network failures without claiming nothing was saved', async () => {
  const ui = uploadHarness(refresh, [{ raw: '<html>offline proxy page</html>', contentType: 'text/html' }]);
  await ui.submit();
  assert.match(rowMessages(ui), /响应|回执/);
  assert.doesNotMatch(rowMessages(ui), /网络错误|没有提交成功|<html>/);
  assert.equal(ui.requests.length, 1, 'unknown results must not be automatically retried');
  assert.equal(ui.box.value, '', '未确认项只通过卡片重新查询，不放进重新提交列表');
  assert.equal(ui.timers.size, 0);
});

test('bootstraps one owner, then uses at most three independent requests and prevents resubmission', async () => {
  const tokens = Array.from({ length: 6 }, (_, i) => refresh + i);
  const releases = [];
  let active = 0, maximum = 0;
  const responses = tokens.map((_, i) => () => new Promise(resolve => {
    active++;
    maximum = Math.max(maximum, active);
    releases[i] = () => { active--; resolve(uploadReceipt(i)); };
  }));
  const ui = uploadHarness(tokens.join('\n'), responses);
  const batch = ui.submit();
  await flushPromises();
  assert.equal(ui.requests.length, 1, 'first owner is established serially');
  await ui.submit();
  assert.equal(ui.requests.length, 1, 'double submit must not create another batch');
  releases[0]();
  await flushPromises();
  assert.equal(ui.requests.length, 4, 'remaining uploads should use bounded parallelism');
  assert.equal(maximum, 3);
  releases[2](); releases[1](); releases[3]();
  await flushPromises();
  releases[5](); releases[4]();
  await batch;
  assert.equal(new Set(ui.requests.map(x => x.data.creds)).size, 6, 'each worker must own its form body');
  assert.ok(ui.requests.slice(1).every(x => x.data.owner_token === 'owner-test'));
  assert.equal(ui.button.disabled, false, '完成后保留结果并允许新的批次');
  assert.equal(ui.timers.size, 0);
});

test('deduplicates identical credentials and retains failures in their original order', async () => {
  const tokens = [refresh + 'a', refresh + 'b', refresh + 'a', refresh + 'c'];
  const ui = uploadHarness(tokens.join('\n'), [
    { error: 'offline rejection' }, uploadReceipt(2), { error: 'offline rejection' }
  ], { ownerToken: 'owner-test' });
  await ui.submit();
  assert.equal(ui.requests.length, 3);
  assert.equal(ui.box.value, tokens[0] + '\n\n' + tokens[3]);
  assert.match(rowMessages(ui), /重复/);
  assert.match(rowMessages(ui), /HTTP 400/);
});

test('unknown first result stops ownerless batch rather than possibly creating multiple owners', async () => {
  const tokens = [refresh + 'a', refresh + 'b'];
  const ui = uploadHarness(tokens.join('\n'), [{ networkError: true }, uploadReceipt(2)]);
  await ui.submit();
  assert.equal(ui.requests.length, 1);
  assert.equal(ui.box.value, tokens[1], '只保留确实未提交的项目');
  assert.match(rowMessages(ui), /未确认|不确定/);
  assert.doesNotMatch(rowMessages(ui), /没有提交成功/);
  assert.equal(ui.button.disabled, false);
});

test('single request timeout releases the batch and does not retry a potentially rotated refresh token', async () => {
  const ui = uploadHarness(refresh, [options => new Promise((_, reject) => {
    options.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')));
  })]);
  const batch = ui.submit();
  await flushPromises();
  assert.equal(ui.timers.size, 1);
  for (const expire of ui.timers.values()) expire();
  await batch;
  assert.equal(ui.requests.length, 1);
  assert.match(rowMessages(ui), /超时/);
  assert.equal(ui.button.disabled, false);
  assert.equal(ui.timers.size, 0);
});

test('normalizes owner whitespace before bootstrap and receipt comparison', async () => {
  for (const ownerToken of ['   ', ' owner-test \n']) {
    let resolveFirst;
    const ui = uploadHarness(refresh + 'a\n' + refresh + 'b', [
      () => new Promise(resolve => { resolveFirst = resolve; }),
      uploadReceipt(2)
    ], { ownerToken });
    const batch = ui.submit();
    await flushPromises();
    assert.equal(ui.requests.length, ownerToken.trim() ? 2 : 1);
    resolveFirst(uploadReceipt(1));
    await batch;
    assert.equal(ui.ids['upload-results'].children.filter(x => x.className.endsWith('failed')).length, 0);
    assert.ok(ui.requests.every(x => !x.data.owner_token || x.data.owner_token === 'owner-test'));
  }
});

test('HTTP 504 before owner confirmation stops the batch and reports an unconfirmed result', async () => {
  const input = refresh + 'a\n' + refresh + 'b';
  const ui = uploadHarness(input, [{ error: 'offline proxy timeout', status: 504 }, uploadReceipt(2)]);
  await ui.submit();
  assert.equal(ui.requests.length, 1);
  assert.match(rowMessages(ui), /HTTP 504/);
  assert.match(rowMessages(ui), /未确认/);
  assert.equal(ui.box.value, refresh + 'b');
});

test('whitespace owner input cannot shadow the confirmed owner on partial retry', async () => {
  const ui = uploadHarness(refresh + 'a\n' + refresh + 'b', [
    uploadReceipt(1), { error: 'offline refusal' }, uploadReceipt(2)
  ], { ownerToken: '  \n ' });
  await ui.submit();
  assert.equal(ui.requests[1].data.owner_token, 'owner-test');
  await ui.submit();
  assert.equal(ui.requests[2].data.owner_token, 'owner-test');
});

test('login parser supports optional TOTP and preserves password whitespace without interpreting surrounding text', () => {
  const parsed = JSON.parse(JSON.stringify(parser.parseLogins(
    '\ufefftest@example.com---- password \nsecond@example.com----secret----\nthird@example.com----pw----otpauth://totp/test?secret=TEST\nan unrelated instruction'
  )));
  assert.equal(parsed.items.length, 3);
  assert.equal(parsed.items[0].login.password, ' password ');
  assert.equal(parsed.items[1].login.totp, '');
  assert.equal(parsed.items[2].login.totp, 'otpauth://totp/test?secret=TEST');
  assert.equal(parsed.skipped.length, 1);
  assert.doesNotMatch(JSON.stringify(parsed.skipped), /password|instruction/);
  assert.match(parser.parseLogins(Array.from({ length: 101 }, () => 'x@example.com----pw').join('\n')).error, /100/);
});

test('login mode deduplicates email, supports empty 2FA, and clears secrets instead of retaining failed login retries', async () => {
  const ui = uploadHarness('', [uploadReceipt(1), { error: 'offline rejection' }]);
  ui.ids['upload-mode-login'].listeners.click();
  ui.form.elements.login_batch.value = 'first@example.com----pw-one\nfirst@example.com----pw-two----\nlast@example.com----pw-three----';
  await ui.submit();
  assert.equal(ui.requests.length, 2);
  assert.equal(ui.requests[0].data.mode, 'login');
  assert.equal(ui.requests[0].data.password, 'pw-one');
  assert.equal(ui.requests[0].data.totp, '');
  assert.equal(ui.requests[1].data.owner_token, 'owner-test');
  assert.equal(ui.form.elements.login_batch.value, '');
  assert.equal(ui.form.elements.login_password.value, '');
  assert.doesNotMatch(rowMessages(ui), /pw-one|pw-two|pw-three/);
  await ui.submit();
  assert.equal(ui.requests.length, 2, '点击提交不能暗中保留并重试密码');
});

test('single login accepts delimiters in password and rejects simultaneous batch input', async () => {
  const ui = uploadHarness('', [uploadReceipt(1)]);
  ui.ids['upload-mode-login'].listeners.click();
  ui.form.elements.login_email.value = 'test@example.com';
  ui.form.elements.login_password.value = ' password----with-separator ';
  ui.form.elements.login_batch.value = 'other@example.com----pw';
  await ui.submit();
  assert.equal(ui.requests.length, 0);
  assert.match(rowMessages(ui), /不要同时填写/);
  ui.form.elements.login_batch.value = '';
  await ui.submit();
  assert.equal(ui.requests[0].data.password, ' password----with-separator ');
  assert.equal(ui.form.elements.login_password.value, '');
  assert.deepEqual(ui.redirects, []);
});

test('receipt identity guard rejects another owner without providing its links', async () => {
  const ui = uploadHarness(refresh, [uploadReceipt(1)], { ownerToken: 'different-owner' });
  await ui.submit();
  assert.match(rowMessages(ui), /归属异常/);
  assert.equal(ui.box.value, '', 'unknown result is not automatically eligible for retry');
  assert.doesNotMatch(rowMessages(ui), /打开账号总览/);
});

const descendants = node => [node, ...node.children.flatMap(descendants)];
const buttonNamed = (node, name) => descendants(node).find(x => x.tag === 'button' && x.textContent === name);
const waitingView = job => ({ id: job.id, state: 'waiting', stage: 'password', stages: ['queued', 'password'],
  expires: new Date(Date.now() + 60000).toISOString(), prompt_id: 1, submitted: false, challenge: { kind: 'email_code' } });

test('same email prompt becomes visible after status connection recovers', async () => {
  let reads = 0;
  const ui = uploadHarness('', [uploadReceipt(1)], { onTaskRequest: (_url, _options, job) => {
    reads++;
    return reads === 2 ? { networkError: true } : { data: waitingView(job) };
  } });
  ui.ids['upload-mode-login'].listeners.click();
  ui.form.elements.login_batch.value = 'test@example.com----pw';
  const batch = ui.submit();
  await flushPromises();
  await ui.poll();
  await batch;
  const root = ui.ids['upload-results'].children[0];
  buttonNamed(root, '重新查询此任务').listeners.click();
  await flushPromises();
  const challenge = root.children.find(x => x.className === 'upload-challenge');
  assert.equal(challenge.hidden, false, '同一 prompt 必须恢复显示');
});

test('unsent OTP can be retried only after status confirms it was not accepted', async () => {
  const ui = uploadHarness('', [uploadReceipt(1)], { onTaskRequest: (url, _options, job) =>
    url.endsWith('/input') ? { networkError: true } : { data: waitingView(job) } });
  ui.ids['upload-mode-login'].listeners.click();
  ui.form.elements.login_batch.value = 'test@example.com----pw';
  ui.submit();
  await flushPromises();
  const root = ui.ids['upload-results'].children[0];
  const input = descendants(root).find(x => x.tag === 'input');
  input.value = '123456';
  const send = buttonNamed(root, '确认并继续');
  await send.listeners.click();
  assert.equal(send.disabled, true, '结果未确认时不可盲目重复');
  await ui.poll();
  assert.equal(send.disabled, false, '服务端明确尚未接收后可人工重试');
});

test('new batch preserves the only controls for an unresolved job', async () => {
  const ui = uploadHarness(refresh + 'one', [uploadReceipt(1), uploadReceipt(2)], { onTaskRequest: () => ({ networkError: true }) });
  await ui.submit();
  const original = ui.ids['upload-results'].children[0];
  assert.ok(buttonNamed(original, '取消任务'), '未确认任务仍可请求取消');
  ui.box.value = refresh + 'two';
  await ui.submit();
  assert.ok(ui.ids['upload-results'].children.includes(original), '新批次不能清掉仍活跃的旧卡片');
});

test('late owner confirmation binds the unsubmitted remainder even without a cookie', async () => {
  let reads = 0;
  const ui = uploadHarness(refresh + 'one\n' + refresh + 'two', [uploadReceipt(1), uploadReceipt(2)], {
    onTaskRequest: () => ++reads === 1 ? { networkError: true } : null
  });
  await ui.submit();
  const original = ui.ids['upload-results'].children[0];
  await buttonNamed(original, '重新查询此任务').listeners.click();
  await ui.submit();
  assert.equal(ui.requests[1].data.owner_token, 'owner-test', '延迟确认的归属不能丢给可能已变化的 cookie');
});

test('a new batch cannot make the restored older OTP input read-only', async () => {
  let reads = 0;
  const ui = uploadHarness('', [uploadReceipt(1), () => new Promise(() => {})], {
    onTaskRequest: (_url, _options, job) => ++reads === 2 ? { networkError: true } : { data: waitingView(job) }
  });
  ui.ids['upload-mode-login'].listeners.click();
  ui.form.elements.login_batch.value = 'first@example.com----pw';
  const first = ui.submit();
  await flushPromises();
  await ui.poll();
  await first;
  const original = ui.ids['upload-results'].children[0];
  buttonNamed(original, '重新查询此任务').listeners.click();
  await flushPromises();
  const input = descendants(original).find(x => x.tag === 'input');
  ui.form.elements.login_batch.value = 'second@example.com----pw';
  ui.submit();
  await flushPromises();
  assert.equal(input.readOnly, false, '新批次只能锁上传字段，不能锁已有验证码框');
});
