(function () {
  // ---- Boot config ----
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase = cfg.apiBase || '';
  const HOME_BASE = (cfg.baseAirport || 'DFW').toUpperCase();

  const state = {
    lastPullIso: null,
    nextRefreshIso: null,
    clockMode: cfg.clockMode === '24' ? 24 : 12,
    onlyReports: cfg.onlyReports !== false,
    _zeroKick: 0,
  };

  // ---- Public actions ----
  window.dwManualRefresh = async function () {
    try { await fetchJSON(apiBase + '/api/refresh', { method: 'POST' }); }
    catch (e) { console.error(e); }
  };

  // ---- Controls ----
  const refreshSel = document.getElementById('refresh-mins');
  if (refreshSel) {
    if (cfg.refreshMinutes) refreshSel.value = String(cfg.refreshMinutes);
    refreshSel.addEventListener('change', async () => {
      const minutes = parseInt(refreshSel.value, 10);
      try {
        await fetchJSON(apiBase + '/api/settings/refresh-seconds', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ seconds: minutes * 60 }),
        });
      } catch (e) { console.error(e); }
    });
  }

  const clockSel = document.getElementById('clock-mode');
  if (clockSel) {
    clockSel.value = String(state.clockMode);
    clockSel.addEventListener('change', async () => {
      state.clockMode = parseInt(clockSel.value, 10) === 12 ? 12 : 24;
      await renderOnce();
    });
  }

  // ---- SSE hookup ----
  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('hello', () => {});
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
  } catch {}

  // ---- 1s ticker ----
  setInterval(tickStatusLine, 1000);

  // ---- Summary row toggle ----
  document.addEventListener('click', (e) => {
    const sum = e.target.closest('tr.summary');
    // ignore clicks on check-in box (we handle those separately)
    if (!sum || e.target.closest('.ck')) return;
    sum.classList.toggle('open');

    const details = sum.nextElementSibling;
    if (!details || !details.classList.contains('details')) return;
    const open = sum.classList.contains('open');
    details.querySelectorAll('.day .legs').forEach(tbl => {
      tbl.style.display = open ? 'table' : 'none';
    });
    details.querySelectorAll('.day .helper').forEach(h => {
      h.textContent = open ? 'click to hide legs' : 'click to show legs';
    });
  });

  // ---- Day toggle ----
  document.addEventListener('click', (e) => {
    const hdr = e.target.closest('.dayhdr');
    if (!hdr) return;
    const day = hdr.closest('.day');
    const legs = day?.querySelector('.legs');
    if (!legs) return;
    const shown = legs.style.display !== 'none';
    legs.style.display = shown ? 'none' : 'table';
    const helper = day.querySelector('.helper');
    if (helper) helper.textContent = shown ? 'click to show legs' : 'click to hide legs';
  });

  // ---- Check-in icon interactions ----
  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('.ck');
    if (!btn) return;

    const pairingId = btn.dataset.pairingId;
    const reportIso = btn.dataset.reportIso;

    const mode = btn.dataset.mode; // "out" | "pending" | "ok"
    if (mode === 'out') {
      // window not open -> show plan
      openPlanModal({ pairing_id: pairingId, report_local_iso: reportIso });
      return;
    }
    if (mode === 'ok') {
      // already acknowledged -> show plan (optional)
      openPlanModal({ pairing_id: pairingId, report_local_iso: reportIso });
      return;
    }
    // pending -> acknowledge then refresh
    try {
      await fetchJSON(apiBase + '/api/ack/acknowledge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pairing_id: pairingId, report_local_iso: reportIso }),
      });
      await renderOnce();
    } catch (err) {
      console.error('ack error', err);
      // still show plan modal for context
      openPlanModal({ pairing_id: pairingId, report_local_iso: reportIso });
    }
  });

  // ---- Modal wiring ----
  const modal = qs('#plan-modal');
  const planClose1 = qs('#plan-close-1');
  const planClose2 = qs('#plan-close-2');
  [planClose1, planClose2].forEach(el => el && el.addEventListener('click', closePlanModal));
  modal?.addEventListener('click', (e) => { if (e.target === modal) closePlanModal(); });

  async function openPlanModal({ pairing_id, report_local_iso }) {
    const metaEl = qs('#plan-meta');
    const rowsEl = qs('#plan-rows');
    if (!metaEl || !rowsEl) return;

    metaEl.textContent = 'Loading…';
    rowsEl.innerHTML = '';
    modal?.classList.remove('hidden');

    try {
      const data = await fetchJSON(`${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairing_id)}&report_local_iso=${encodeURIComponent(report_local_iso)}`);
      metaEl.textContent = `Pairing ${pairing_id} · Report ${report_local_iso}`;
      rowsEl.innerHTML = (data.attempts || []).map(a => {
        const when = new Date(a.at_iso);
        return `<tr><td>${esc(when.toLocaleString())}</td><td>${esc(a.kind)}</td><td>${esc(a.label)}</td></tr>`;
      }).join('') || `<tr><td colspan="3" class="muted">No attempts.</td></tr>`;
    } catch (e) {
      metaEl.textContent = 'Failed to load plan';
      rowsEl.innerHTML = `<tr><td colspan="3" class="muted">${esc(String(e))}</td></tr>`;
    }
  }

  function closePlanModal() {
    modal?.classList.add('hidden');
  }

  // ---- Core render ----
  async function renderOnce() {
    const params = new URLSearchParams({
      is_24h: String(state.clockMode === 24 ? 1 : 0),
      only_reports: String(state.onlyReports ? 1 : 0),
    });

    let data;
    try {
      const res = await fetch(`${apiBase}/api/pairings?${params.toString()}`, { cache: 'no-store' });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`HTTP ${res.status}: ${text.slice(0, 140)}`);
      }
      data = await res.json();
    } catch (e) {
      console.error('Failed to fetch /api/pairings', e);
      return;
    }

    state.lastPullIso    = data.last_pull_local_iso || null;
    state.nextRefreshIso = data.next_pull_local_iso || null;

    if (refreshSel && data.refresh_minutes) {
      refreshSel.value = String(data.refresh_minutes);
    }

    setText('#looking-through', data.looking_through ?? '—');
    setText('#last-pull', data.last_pull_local ?? '—');

    const base = (data.next_pull_local && data.tz_label)
      ? `${data.next_pull_local} (${data.tz_label})`
      : '—';
    const nextEl = qs('#next-refresh');
    if (nextEl) nextEl.innerHTML = `${esc(base)} <span id="next-refresh-eta"></span>`;

    const tbody = qs('#pairings-body');
    tbody.innerHTML = (data.rows || []).map(row => renderRowHTML(row, HOME_BASE)).join('');
  }

  // ---- Helpers for start airport / out-of-base pill ----
  function firstDepartureAirport(row) {
    const days = row?.days || [];
    for (const d of days) {
      const legs = d?.legs || [];
      if (legs.length && legs[0].dep) {
        return String(legs[0].dep).toUpperCase();
      }
    }
    return null;
  }

  function renderRowHTML(row, homeBase) {
    if (row.kind === 'off') {
      return `
        <tr class="off">
          <td></td>
          <td><span class="off-label">OFF</span></td>
          <td class="muted"></td>
          <td class="muted"></td>
          <td><span class="off-dur">${esc(row.display?.off_dur || '')}</span></td>
        </tr>`;
    }

    const daysCount = row.days ? row.days.length : 0;
    const inProg = row.in_progress ? `<span class="progress">(In progress)</span>` : '';

    // Out-of-base pill
    const startDep = firstDepartureAirport(row);
    const showOOB = !!(startDep && startDep !== homeBase);
    const oobPill = showOOB ? `<span class="pill pill-red">${esc(startDep)}</span>` : '';

    // Check-in icon state
    const ack = row.ack || {};
    let ckClass = 'ck ck--out';
    let ckLabel = '•';
    let ckMode = 'out';
    if (ack.acknowledged) {
      ckClass = 'ck ck--ok'; ckLabel = '✓'; ckMode = 'ok';
    } else if (ack.window_open) {
      ckClass = 'ck ck--pending'; ckLabel = '!'; ckMode = 'pending';
    } else {
      ckClass = 'ck ck--out'; ckLabel = '•'; ckMode = 'out';
    }

    const details = (row.days || []).map((day, i) => renderDayHTML(day, i)).join('');

    return `
      <tr class="summary">
        <td>
          <span class="${ckClass}" title="${ckMode === 'out' ? 'Window not open — click to view plan' : (ckMode === 'pending' ? 'Click to acknowledge' : 'Acknowledged')}"
                data-mode="${ckMode}" data-pairing-id="${esc(row.pairing_id || '')}" data-report-iso="${esc(ack.report_local_iso || row.report_local_iso || '')}">
            ${ckLabel}
          </span>
        </td>
        <td class="sum-first">
          <strong>${esc(row.pairing_id || '')}</strong>
          <span class="pill">${daysCount} day</span>
          ${oobPill}
          ${inProg}
        </td>
        <td>${esc(row.display?.report_str || '')}</td>
        <td>${esc(row.display?.release_str || '')}</td>
        <td class="muted">click to expand days</td>
      </tr>
      <tr class="details">
        <td colspan="5">
          <div class="daysbox">${details}</div>
        </td>
      </tr>`;
  }

  function renderDayHTML(day, idx) {
    const legs = (day.legs || []).map(leg => `
      <tr class="leg-row ${leg.done ? 'leg-done' : ''}">
        <td>${esc(leg.flight || '')}</td>
        <td>${esc(leg.dep || '')}–${esc(leg.arr || '')}</td>
        <td>${esc(leg.dep_time_str || leg.dep_time || '')}
            &nbsp;→&nbsp;
            ${esc(leg.arr_time_str || leg.arr_time || '')}</td>
      </tr>`).join('');

    return `
      <div class="day">
        <div class="dayhdr">
          <span class="dot"></span>
          <span class="daytitle">Day ${idx + 1}</span>
          ${day.report ? `· Report ${esc(day.report)}` : ''}
          ${day.release ? `· Release ${esc(day.release)}` : ''}
          ${day.hotel ? `· ${esc(day.hotel)}` : ''}
          <span class="helper">click to show legs</span>
        </div>
        ${legs ? `
          <div class="legs-wrap">
            <table class="legs" style="display:none">
              <thead><tr><th>Flight</th><th>Route</th><th>Block Times</th></tr></thead>
              <tbody>${legs}</tbody>
            </table>
          </div>` : `<div class="muted subnote">No legs parsed.</div>`}
      </div>`;
  }

  // ---- Status ticker ----
  function tickStatusLine() {
    if (state.lastPullIso) {
      const ago = preciseAgo(new Date(state.lastPullIso));
      setText('#last-pull', ago);
    }

    const etaEl = qs('#next-refresh-eta');
    if (!etaEl || !state.nextRefreshIso) return;

    const diff = Math.floor((new Date(state.nextRefreshIso).getTime() - Date.now()) / 1000);
    const left = Math.max(0, diff);
    if (left > 0) {
      const m = Math.floor(left / 60), s = left % 60;
      etaEl.textContent = `in ${m}m ${s}s`;
    } else {
      etaEl.textContent = '(refreshing…)';
      const now = Date.now();
      if (!state._zeroKick || now - state._zeroKick > 4000) {
        state._zeroKick = now;
        renderOnce();
      }
    }
  }

  // ---- utils ----
  function qs(sel) { return document.querySelector(sel); }
  async function fetchJSON(url, opts) {
    const res = await fetch(url, opts);
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`);
    }
    return res.json();
  }
  function setText(sel, v) { const el = qs(sel); if (el) el.textContent = v; }
  function esc(s) {
    return String(s).replace(/[&<>"'`=\/]/g, (ch) =>
      ({'&':'&amp;','<':'&lt;','>':'&#x3E;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'}[ch])
    );
  }
  function preciseAgo(d){
    const sec = Math.max(0, Math.floor((Date.now() - d.getTime())/1000));
    const m = Math.floor(sec/60), s = sec%60;
    return m ? `${m}m ${s}s ago` : `${s}s ago`;
  }
  function safeParseJSON(s) { try { return JSON.parse(s || '{}'); } catch { return null; } }

  // ---- First paint ----
  renderOnce();
})();
