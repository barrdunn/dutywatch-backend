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

  // API helpers for ack
  const API = {
    ackPlan(pairing_id, report_local_iso){
      const q = new URLSearchParams({ pairing_id, report_local_iso }).toString();
      return fetch(`${apiBase}/api/ack/plan?${q}`).then(r=>r.json());
    },
    acknowledge(pairing_id, report_local_iso){
      return fetch(`${apiBase}/api/ack/acknowledge`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ pairing_id, report_local_iso })
      }).then(r=>r.json());
    }
  };

  // ---- Public actions ----
  window.dwManualRefresh = async function () {
    try { await fetch(apiBase + '/api/refresh', { method: 'POST' }); }
    catch (e) { console.error(e); }
  };

  // ---- Controls wiring ----
  const refreshSel = document.getElementById('refresh-mins');
  if (refreshSel) {
    if (cfg.refreshMinutes) refreshSel.value = String(cfg.refreshMinutes);
    refreshSel.addEventListener('change', async () => {
      const minutes = parseInt(refreshSel.value, 10);
      try {
        await fetch(apiBase + '/api/settings/refresh-seconds', {
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

  // ---- First paint ----
  renderOnce();

  // ---- SSE hookup with fallbacks ----
  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('hello', () => {/* initial */});
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
    es.onerror = () => { /* passive */ };
  } catch {}

  // ---- 1s ticker for mm:ss + countdown & zero-kick ----
  setInterval(tickStatusLine, 1000);

  // ---- Global click handlers ----
  document.addEventListener('click', (e) => {
    const sum = e.target.closest('tr.summary');
    if (!sum) return;
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

  // ---- Modal controls ----
  const modal = document.getElementById('plan-modal');
  const planRows = document.getElementById('plan-rows');
  const planMeta = document.getElementById('plan-meta');
  const close1 = document.getElementById('plan-close-1');
  const close2 = document.getElementById('plan-close-2');
  function openPlan(pairing_id, report_iso){
    modal.classList.remove('hidden');
    planRows.innerHTML = '';
    planMeta.textContent = 'Loading plan…';
    API.ackPlan(pairing_id, report_iso).then(data => {
      planMeta.textContent = `Pairing ${pairing_id} • Report ${new Date(report_iso).toLocaleString()}`;
      planRows.innerHTML = (data.attempts || []).map(at => {
        const when = new Date(at.at_iso).toLocaleString();
        const typ = at.kind === 'call' ? 'Call' : 'Push';
        return `<tr><td>${esc(when)}</td><td>${esc(typ)}</td><td>${esc(at.label || '')}</td></tr>`;
      }).join('');
    }).catch(()=>{ planMeta.textContent = 'Failed to load plan.'; });
  }
  function closePlan(){ modal.classList.add('hidden'); }
  close1?.addEventListener('click', closePlan);
  close2?.addEventListener('click', closePlan);

  // ---- Core render ----
  async function renderOnce() {
    const params = new URLSearchParams({
      is_24h: String(state.clockMode === 24 ? 1 : 0),
      only_reports: String(state.onlyReports ? 1 : 0),
    });

    let data;
    try {
      const res = await fetch(`${apiBase}/api/pairings?${params.toString()}`, { cache: 'no-store' });
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
          <td class="checkcell"></td>
          <td><span class="off-label">OFF</span></td>
          <td class="muted"></td>
          <td class="muted"></td>
          <td><span class="off-dur">${esc(row.display?.off_dur || '')}</span></td>
        </tr>`;
    }

    const daysCount = row.days ? row.days.length : 0;
    const inProg = row.in_progress ? `<span class="progress">(In progress)</span>` : '';
    const startDep = firstDepartureAirport(row);
    const showOOB = !!(startDep && startDep !== homeBase);
    const oobPill = showOOB ? `<span class="pill pill-red">${esc(startDep)}</span>` : '';

    // NEW: check icon state from server
    const ack = row.ack || {};
    const isAck = !!ack.acknowledged;
    const inWindow = !!ack.window_open;
    const iconCls = isAck ? 'check-icon done' : (inWindow ? 'check-icon pending' : 'check-icon');
    const iconText = isAck ? '✓' : '•';

    const onClick = isAck
      ? `openPlan('${js(row.pairing_id)}','${js(row.report_local_iso||'')}');event.stopPropagation();`
      : (inWindow
          ? `ackNow('${js(row.pairing_id)}','${js(row.report_local_iso||'')}');event.stopPropagation();`
          : `openPlan('${js(row.pairing_id)}','${js(row.report_local_iso||'')}');event.stopPropagation();`);

    const details = (row.days || []).map((day, i) => renderDayHTML(day, i)).join('');

    return `
      <tr class="summary">
        <td class="checkcell"><span class="${iconCls}" onclick="${onClick}" title="Check-in">${iconText}</span></td>
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
        <td class="checkcell"></td>
        <td colspan="4">
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

  // ---- actions bound from inline handlers ----
  window.openPlan = openPlan;
  window.ackNow = async function(pairing_id, report_iso){
    try {
      await API.acknowledge(pairing_id, report_iso);
      await renderOnce();
    } catch(e){ console.error(e); }
  };

  // ---- Live status ticker ----
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
  function setText(sel, v) { const el = qs(sel); if (el) el.textContent = v; }
  function esc(s) {
    return String(s).replace(/[&<>"'`=\/]/g, (ch) =>
      ({'&':'&amp;','<':'&lt;','>':'&#x3E;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'}[ch])
    );
  }
  function js(s){ return String(s).replace(/\\/g,'\\\\').replace(/'/g,"\\'"); }
  function preciseAgo(d){
    const sec = Math.max(0, Math.floor((Date.now() - d.getTime())/1000));
    const m = Math.floor(sec/60), s = sec%60;
    return m ? `${m}m ${s}s ago` : `${s}s ago`;
  }
  function safeParseJSON(s) { try { return JSON.parse(s || '{}'); } catch { return null; } }
})();
