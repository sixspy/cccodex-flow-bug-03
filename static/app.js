/* 编排流台：组件化 SPA（Svelte 式状态驱动渲染，无构建步骤便于容器验收） */

const state = {
  user: '',
  lab: '',
  items: [],
  selected: null,
  panel: null,
  view: 'flow', // flow | batch | rollback
  batchKey: '',
  flash: null,
  busy: false,
};

async function api(path, opts) {
  const r = await fetch(path, opts);
  const j = await r.json().catch(() => ({}));
  if (!r.ok) {
    const err = new Error(j.detail || j.error || '请求失败');
    err.payload = j;
    err.status = r.status;
    throw err;
  }
  return j;
}

const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
}[c]));

async function boot() {
  try {
    const me = await api('/api/me');
    state.user = me.user;
    state.lab = me.lab;
    await refresh();
    render();
  } catch (e) {
    location.href = '/login';
  }
}

async function refresh() {
  state.items = (await api('/api/flows')).items;
  state.panel = await api('/api/idempo/panel');
}

async function select(id) {
  state.selected = await api('/api/flows/' + id);
  state.view = 'flow';
  state.batchKey = '';
  state.flash = null;
  render();
}

function flash(kind, text) {
  state.flash = { kind, text };
}

async function doExtract() {
  try {
    await api('/api/flows/' + state.selected.id + '/extract', { method: 'POST' });
    state.selected = await api('/api/flows/' + state.selected.id);
    await refresh();
    flash('ok', '已抽取，旧批次库存整笔撤销，列表与库存保持一致');
    state.view = 'flow';
  } catch (e) {
    flash('err', e.message);
  }
  render();
}

async function doLand() {
  if (!state.selected) return;
  state.busy = true;
  render();
  try {
    const out = await api('/api/flows/' + state.selected.id + '/land', { method: 'POST' });
    state.selected = await api('/api/flows/' + state.selected.id);
    state.panel = out.landing;
    await refresh();
    const card = (out.landing.batches || []).find((b) => b.flow_id === state.selected.id);
    if (card && card.last_verdict === 'skipped') {
      const n = card.rows;
      flash('skip', `整批撞钥：${n} 行整批同一判定为跳过，跳过计数 +${n}，写入 0 行`);
    } else {
      flash('ok', `整批落库完成：写入 ${card ? card.written : '—'} 行，库存与列表一致`);
    }
  } catch (e) {
    flash('err', e.message);
  }
  state.busy = false;
  render();
}

async function landBatch(batchKey, flowId) {
  state.busy = true;
  render();
  try {
    const out = await api('/api/idempo/action', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'land', flow_id: flowId, batch_key: batchKey }),
    });
    await refresh();
    if (out.verdict === 'skipped') {
      flash('skip', `整批撞钥：${out.total} 行整批跳过，跳过计数 +${out.skipped}，写入 ${out.written} 行`);
    } else {
      flash('ok', `整批落库：写入 ${out.written} 行（${out.batch_key}）`);
    }
    state.view = 'batch';
    state.batchKey = batchKey;
  } catch (e) {
    flash('err', e.message);
  }
  state.busy = false;
  render();
}

async function rollbackBatch(batchKey, flowId) {
  state.busy = true;
  render();
  try {
    const out = await api('/api/idempo/action', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'rollback', flow_id: flowId, batch_key: batchKey }),
    });
    await refresh();
    flash('ok', `整笔回滚完成：撤销 ${out.removed} 行，库存 ${out.inventory} 与列表 ${out.list_landed} 一致`);
  } catch (e) {
    flash(e.status === 409 ? 'warn' : 'err', e.message);
  }
  state.busy = false;
  state.view = 'rollback';
  render();
}

async function rollbackFlowAll(flowId) {
  state.busy = true;
  render();
  try {
    const out = await api('/api/idempo/action', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'rollback', flow_id: flowId }),
    });
    await refresh();
    flash('ok', `整笔回滚完成：撤销 ${out.removed} 行，库存 ${out.inventory} 与列表 ${out.list_landed} 一致`);
  } catch (e) {
    flash(e.status === 409 ? 'warn' : 'err', e.message);
  }
  state.busy = false;
  state.view = 'rollback';
  render();
}

async function logout() {
  await api('/api/logout', { method: 'POST' });
  location.href = '/login';
}

function chipFor(batch) {
  const cls = {
    landed: 'chip-ok',
    skipped: 'chip-warn',
    rolled_back: 'chip-muted',
    pending: 'chip-pending',
  }[batch.state] || 'chip';
  return `<span class="chip ${cls}">${esc(batch.state_label)}</span>`;
}

function consistencyBanner(panel) {
  if (!panel) return '';
  const ok = panel.consistency && panel.consistency.ok;
  if (ok) {
    return `<div class="banner ok">一致性：列表已落库合计 ${panel.list_landed_total} = 库存 ${panel.landed_total}，无半写入</div>`;
  }
  const parts = [];
  for (const m of panel.consistency.mismatches || []) {
    parts.push(`流 ${m.flow_id}：列表 ${m.list_landed} ≠ 库存 ${m.inventory}`);
  }
  for (const b of panel.consistency.half_writes || []) parts.push(`批次 ${b} 存在半写入`);
  return `<div class="banner err">不一致：${parts.map(esc).join('；')}</div>`;
}

function strategyCard(panel) {
  const s = (panel && panel.strategy) || {};
  return `<section class="strategy">
    <div class="strategy-head">
      <h2>${esc(s.title || '撞钥跳过 · 整批原子')}</h2>
      <span class="mono">${esc(s.mode_label || '')}</span>
      <span class="mono">跳过合计 ${panel ? panel.skip_total : 0} · 争用 ${panel ? panel.contention_total : 0}</span>
    </div>
    <ul class="rules">
      <li>${esc(s.skip_policy || '')}</li>
      <li>${esc(s.rollback_policy || '')}</li>
      <li>${esc(s.consistency || '')}</li>
    </ul>
  </section>`;
}

function listPanel(panel) {
  const batches = (panel && panel.batches) || [];
  const rows = batches.map((b) => `
    <tr class="batch-row" data-key="${esc(b.batch_key)}" data-flow="${b.flow_id}">
      <td><strong>${esc(b.batch_key)}</strong><div class="mono">${esc(b.flow_title)} · ${esc(b.flow_code)}</div></td>
      <td>${chipFor(b)}</td>
      <td class="num">${b.rows}</td>
      <td class="num">${b.written}</td>
      <td class="num">${b.skipped}</td>
      <td class="num">${b.contention}</td>
      <td class="num">${b.inventory} / ${b.list_landed}</td>
      <td class="mono">${esc(b.last_reason || '—')}</td>
    </tr>`).join('') || '<tr><td colspan="8" class="hint">暂无批次，先抽取再落库</td></tr>';
  return `<section class="detail list-panel">
    <div class="panel-head">
      <h3>落库列表</h3>
      <span class="strategy-tag">策略：${esc((panel && panel.strategy && panel.strategy.title) || '撞钥跳过 · 整批原子')}</span>
      <span class="mono">点击任一行进入撞钥详情 / 回滚</span>
    </div>
    <table class="tbl">
      <thead><tr><th>批次 / 流</th><th>状态</th><th>行数</th><th>写入</th><th>跳过</th><th>争用</th><th>库存/列表</th><th>最近判定</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </section>`;
}

function flowDetail() {
  if (!state.selected) return '<section class="detail"><p class="hint">从左侧选择一条编排流</p></section>';
  const f = state.selected;
  const card = ((f.landing && f.landing.batches) || []).find((b) => b.flow_id === f.id);
  const landingBlock = card ? `
    <div class="subcard">
      <div class="panel-head"><h4>当前批次 ${esc(card.batch_key)}</h4>${chipFor(card)}</div>
      <p class="mono">行数 ${card.rows} · 写入 ${card.written} · 跳过计 ${card.skipped} · 争用 ${card.contention}</p>
      <p class="mono">库存 ${card.inventory} · 列表已落库 ${card.list_landed} · ${card.inventory === card.list_landed ? '一致' : '不一致'}</p>
      <p>${esc(card.last_reason || '无撞钥')}</p>
      <div class="actions">
        <button type="button" class="ghost" data-open-batch="${esc(card.batch_key)}">撞钥详情</button>
        <button type="button" class="danger" data-rollback="${esc(card.batch_key)}" data-rollback-flow="${f.id}" ${card.inventory ? '' : 'disabled'}>整笔回滚</button>
      </div>
    </div>` : '';
  return `<section class="detail">
    <h2>${esc(f.title)}</h2>
    <p class="mono">${esc(f.source_uri)}</p>
    <p>状态 <span class="chip">${esc(f.status)}</span> · 已抽取 ${f.extracted} · 已落库 ${f.landed}</p>
    <div class="actions">
      <button type="button" id="btn-ex" ${state.busy ? 'disabled' : ''}>执行抽取</button>
      <button type="button" id="btn-land" class="ghost" ${state.busy ? 'disabled' : ''}>执行落库</button>
    </div>
    ${landingBlock}
  </section>`;
}

function batchDetail() {
  const panel = state.panel || {};
  const batch = (panel.batches || []).find((b) => b.batch_key === state.batchKey) || null;
  if (!batch) {
    return `<section class="detail"><p class="hint">批次 ${esc(state.batchKey)} 不存在</p><button type="button" id="back-list">返回落库列表</button></section>`;
  }
  const keys = (batch.keys || []).map((k) => `
    <li class="keyrow ${esc(k.verdict)}">
      <span class="mono">${esc(k.key)}</span>
      <span class="chip ${k.verdict === 'skipped' ? 'chip-warn' : 'chip-ok'}">${esc(k.verdict === 'skipped' ? '跳过' : '已落库')}</span>
    </li>`).join('') || '<li class="hint">无钥匙记录</li>';
  const events = (batch.events || []).map((e) => `
    <li class="mono">${esc(e.created_at)} · ${esc(e.kind)} · ${e.count} · ${esc(e.note)}</li>`).join('') || '<li class="hint">无事件</li>';
  return `<section class="detail">
    <div class="crumbs"><button type="button" class="link" id="back-list">落库列表</button> / 撞钥详情</div>
    <h2>${esc(batch.batch_key)} · ${esc(batch.flow_title)}</h2>
    <p>${chipFor(batch)} <span class="mono">行数 ${batch.rows} · 写入 ${batch.written} · 跳过计 ${batch.skipped} · 争用 ${batch.contention}</span></p>
    <p class="mono">库存 ${batch.inventory} · 列表已落库 ${batch.list_landed} · ${batch.inventory === batch.list_landed ? '一致' : '不一致'}</p>
    <p class="mono">${esc(batch.last_reason || '无撞钥')}</p>
    <h3>整批判定</h3>
    <p class="hint">同批任一行撞钥 ⇒ 整批同一判定；本批 ${batch.rows} 行一律 ${batch.last_verdict === 'skipped' ? '跳过（零写入）' : '落库'}。</p>
    <ul class="keylist">${keys}</ul>
    <h3>批次事件</h3>
    <ul class="events">${events}</ul>
    <div class="actions">
      <button type="button" class="ghost" data-land-batch="${esc(batch.batch_key)}" data-land-flow="${batch.flow_id}" ${state.busy ? 'disabled' : ''}>整批落库</button>
      <button type="button" class="danger" data-rollback="${esc(batch.batch_key)}" data-rollback-flow="${batch.flow_id}" ${batch.inventory ? '' : 'disabled'}>整笔回滚</button>
    </div>
  </section>`;
}

function rollbackPage() {
  const panel = state.panel || {};
  const rbs = (panel.rollbacks || []).map((r) => `
    <tr><td class="mono">${esc(r.created_at)}</td><td class="mono">${esc(r.batch_key)}</td><td class="num">${r.removed}</td><td>${r.done ? '整笔完成' : '未完成'}</td><td class="mono">${esc(r.note)}</td></tr>`).join('') || '<tr><td colspan="5" class="hint">暂无回滚记录</td></tr>';
  const inv = (panel.inventory || []).map((i) => `
    <tr><td>${esc(i.title)} <span class="mono">${esc(i.code)}</span></td><td>${esc(i.status)}</td>
    <td class="num">${i.list_landed}</td><td class="num">${i.inventory}</td>
    <td><span class="chip ${i.consistent ? 'chip-ok' : 'chip-err'}">${i.consistent ? '一致' : '不一致'}</span></td></tr>`).join('');
  const flowIds = (panel.inventory || []).map((i) => i.flow_id);
  const invById = {};
  for (const i of panel.inventory || []) invById[i.flow_id] = i;
  const actions = flowIds.map((id) => {
    const i = invById[id];
    return `<button type="button" class="danger" data-rollback-flow-all="${id}" ${i && i.inventory ? '' : 'disabled'}>${esc(i ? i.title : '流 ' + id)} 整笔回滚</button>`;
  }).join(' ');
  return `<section class="detail">
    <div class="crumbs"><button type="button" class="link" id="back-list">落库列表</button> / 回滚</div>
    <h2>回滚 · 原子整笔</h2>
    <p class="hint">回滚以整笔为单位撤销批次库存，撤销后强制把列表与库存对齐，不做部分留存。</p>
    <div class="actions">${actions || '<span class="hint">无可回滚批次</span>'}</div>
    <h3>库存与列表对照</h3>
    <table class="tbl"><thead><tr><th>流</th><th>状态</th><th>列表已落库</th><th>库存</th><th>核对</th></tr></thead><tbody>${inv}</tbody></table>
    <h3>回滚记录</h3>
    <table class="tbl"><thead><tr><th>时间</th><th>批次</th><th>撤销行</th><th>状态</th><th>说明</th></tr></thead><tbody>${rbs}</tbody></table>
  </section>`;
}

function flashBlock() {
  if (!state.flash) return '';
  const cls = state.flash.kind === 'err' ? 'err' : state.flash.kind === 'skip' ? 'warn' : 'ok';
  return `<div class="banner ${cls}">${esc(state.flash.text)}</div>`;
}

function render() {
  const el = document.getElementById('app');
  const panel = state.panel;
  const rows = state.items.map((it) => `
    <button type="button" class="row ${state.selected && state.selected.id === it.id ? 'on' : ''}" data-id="${it.id}">
      <strong>${esc(it.title)}</strong>
      <span class="mono">${esc(it.code)}</span>
      <span class="chip">${esc(it.status)}</span>
    </button>`).join('');
  let main;
  if (state.view === 'batch') main = batchDetail();
  else if (state.view === 'rollback') main = rollbackPage();
  else main = flowDetail();
  el.innerHTML = `
    <header class="bar">
      <div class="brand">${esc(state.lab)}</div>
      <div>${esc(state.user)} · <button type="button" id="out" class="link">退出</button></div>
    </header>
    ${flashBlock()}
    <div class="layout">
      <aside>${rows}</aside>
      <main>
        ${strategyCard(panel)}
        ${consistencyBanner(panel)}
        ${main}
        ${listPanel(panel)}
        <div class="actions">
          <button type="button" class="ghost" id="open-rollback">回滚页</button>
          <button type="button" class="link" id="reload">刷新</button>
        </div>
      </main>
    </div>`;

  el.querySelectorAll('.row').forEach((b) => { b.onclick = () => select(+b.dataset.id); });
  el.querySelectorAll('[data-open-batch]').forEach((b) => {
    b.onclick = () => { state.view = 'batch'; state.batchKey = b.dataset.openBatch; render(); };
  });
  el.querySelectorAll('.batch-row').forEach((tr) => {
    tr.onclick = () => { state.view = 'batch'; state.batchKey = tr.dataset.key; render(); };
  });
  el.querySelectorAll('[data-land-batch]').forEach((b) => {
    b.onclick = () => landBatch(b.dataset.landBatch, +b.dataset.landFlow);
  });
  el.querySelectorAll('[data-rollback]').forEach((b) => {
    b.onclick = () => rollbackBatch(b.dataset.rollback, +b.dataset.rollbackFlow);
  });
  el.querySelectorAll('[data-rollback-flow-all]').forEach((b) => {
    b.onclick = () => rollbackFlowAll(+b.dataset.rollbackFlowAll);
  });
  el.querySelectorAll('#back-list').forEach((b) => {
    b.onclick = () => { state.view = 'flow'; render(); };
  });
  const out = el.querySelector('#out'); if (out) out.onclick = logout;
  const be = el.querySelector('#btn-ex'); if (be) be.onclick = doExtract;
  const bl = el.querySelector('#btn-land'); if (bl) bl.onclick = doLand;
  const orb = el.querySelector('#open-rollback'); if (orb) orb.onclick = () => { state.view = 'rollback'; render(); };
  const rl = el.querySelector('#reload'); if (rl) rl.onclick = async () => { await refresh(); if (state.selected) state.selected = await api('/api/flows/' + state.selected.id); render(); };
}

boot();
