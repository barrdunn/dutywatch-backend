(function () {
  // ---- Boot config ----
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase = cfg.apiBase || '';
  const BASE_AIRPORT = (cfg.baseAirport || 'DFW').toUpperCase();

  const state = {
    lastPullIso: null,
    nextRefreshIso: null,
    clockMode: cfg.clockMode === '24' ? 24 : 12, // default 12h
    onlyReports: cfg.onlyReports !== false,
    _zeroKick: 0,
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
      await renderOnce();  // re-render rows with new time format
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
    es.onerror = () => { /* fallback via ticker zero-kick */ };
  } catch { /* non-fatal */ }

  // ---- 1s ticker for mm:ss + countdown & zero-kick ----
  setInterval(tickStatusLine, 1000);

  // ---- Global click handlers ----
  // Toggle a pairing's details row (expand/collapse)
  document.addEventListener('click', (e) => {
    const sum = e.target.closest('tr.summary');
    if (!sum) return;
    sum.classList.toggle('open');

    // If opening, expand all day legs; if closing, collapse all
    const details = sum.nextElementSibling;
    if (!details || !details.classList.contains('details')) return;
    const open = sum.classList.contains('open');
    details.querySelectorAll('.day .legs').forEach(tbl => {
      tbl.style.display = open ? 'table' : 'none';
    });
    // Update helper labels
    details.querySelectorAll('.day .helper').forEach(h => {
      h.textContent = open ? 'click to hide legs' : 'click to show legs';
    });
  });

  // Toggle a single day legs table
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

    // Save stamps for live ticker
    state.lastPullIso    = data.last_pull_local_iso || null;
    state.nextRefreshIso = data.next_pull_local_iso || null;

    // Reflect actual server schedule in the select (avoid stale defaults)
    if (refreshSel && data.refresh_minutes) {
      refreshSel.value = String(data.refresh_minutes);
    }

    // Status chips
    setText('#looking-through', data.looking_through ?? '—');
    setText('#last-pull', data.last_pull_local ?? '—');

    const base = (data.next_pull_local && data.tz_label)
      ? `${data.next_pull_local} (${data.tz_label})`
      : '—';
    const nextEl = qs('#next-refresh');
    if (nextEl) nextEl.innerHTML = `${esc(base)} <span id="next-refresh-eta"></span>`;

    // Table
    const tbody = qs('#pairings-body');
    tbody.innerHTML = (data.rows || []).map(row => renderRowHTML(row, state.clockMode, BASE_AIRPORT)).join('');
  }

  // ---- Out-of-base detection moved to pairing level ----
  function pairingOutOfBase(row, baseAirport) {
    const days = row?.days || [];
    for (const d of days) {
      const legs = d?.legs || [];
      if (legs.length) {
        const dep = String(legs[0].dep || '').toUpperCase();
        return dep && dep !== baseAirport;
      }
    }
    return false;
  }

  function renderRowHTML(row, clockMode, baseAirport) {
    if (row.kind === 'off') {
      return `
        <tr class="off">
          <td><span class="off-label">OFF</span></td>
          <td class="muted"></td>
          <td class="muted"></td>
          <td><span class="off-dur">${esc(row.display?.off_dur || '')}</span></td>
        </tr>`;
    }

    const daysCount = row.days ? row.days.length : 0;
    const inProg = row.in_progress ? `<span class="progress">(In progress)</span>` : '';
    const oob = pairingOutOfBase(row, baseAirport);
    const basePill = oob ? `<span class="pill pill-red">${esc(baseAirport)}</span>` : '';

    const details = (row.days || []).map((day, i) => renderDayHTML(day, i)).join('');

    return `
      <tr class="summary">
        <td class="sum-first">
          <strong>${esc(row.pairing_id || '')}</strong>
          ${basePill}
          <span class="pill">${daysCount} day</span> ${inProg}
        </td>
        <td>${esc(row.display?.report_str || '')}</td>
        <td>${esc(row.display?.release_str || '')}</td>
        <td class="muted">click to expand days</td>
      </tr>
      <tr class="details">
        <td colspan="4">
          <div class="daysbox">${details}</div>
        </td>
      </tr>`;
  }

  // NOTE: red pill removed from day rows (kept clean as sub-rows)
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
      ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'}[ch])
    );
  }
  function preciseAgo(d){
    const sec = Math.max(0, Math.floor((Date.now() - d.getTime())/1000));
    const m = Math.floor(sec/60), s = sec%60;
    return m ? `${m}m ${s}s ago` : `${s}s ago`;
  }
  function safeParseJSON(s) { try { return JSON.parse(s || '{}'); } catch { return null; } }
})();
