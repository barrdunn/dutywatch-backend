(function () {
  // ---- Boot config ----
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase = cfg.apiBase || '';

  // ===== Base settings =====
  // Hard-coded per request. If you want this dynamic later, we can pass via dw_boot_json.
  const BASE = 'DFW';

  const state = {
    lastPullIso: null,
    nextRefreshIso: null,
    clockMode: cfg.clockMode === '12' ? 12 : 24,
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
    refreshSel.value = String(cfg.refreshMinutes || 30);
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
    es.onerror = () => { /* fallback via ticker zero-kick */ };
  } catch { /* non-fatal */ }

  // ---- 1s ticker for mm:ss + countdown & zero-kick ----
  setInterval(tickStatusLine, 1000);

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
    tbody.innerHTML = (data.rows || []).map(renderRowHTML).join('');

    // bind expand/collapse on summaries and per-day rows
    bindExpanders();
  }

  function renderRowHTML(row) {
    if (row.kind === 'off') {
      return `
        <tr class="off">
          <td><span class="off-label">OFF</span></td>
          <td class="muted"></td>
          <td class="muted"></td>
          <td><span class="off-dur">${esc(row.display?.off_dur || '')}</span></td>
        </tr>`;
    }
    const days = row.days || [];
    const daysCount = days.length;
    const inProg = row.in_progress ? `<span class="progress">(In progress)</span>` : '';

    // day mini-rows (always present)
    const dayMiniRows = days.map((day, i) => renderDayMiniRow(day, i)).join('');

    // legs panels for each day (collapsed by default)
    const dayPanels = days.map((day, i) => renderDayPanel(day, i)).join('');

    return `
      <tr class="summary">
        <td><strong>${esc(row.pairing_id || '')}</strong>
            <span class="pill">${daysCount} day</span> ${inProg}</td>
        <td>${esc(row.display?.report_str || '')}</td>
        <td>${esc(row.display?.release_str || '')}</td>
        <td class="muted">click to expand days</td>
      </tr>
      ${dayMiniRows}
      ${dayPanels}`;
  }

  // ----- Day mini row (green, indented; no icon; OOB pill on Day 1 when dep ≠ DFW) -----
  function renderDayMiniRow(day, idx) {
    const legs = Array.isArray(day.legs) ? day.legs : [];
    const firstDep = (legs[0]?.dep || '').toUpperCase();

    // Only flag Day 1 if its first leg doesn't depart from BASE
    const isOutOfBase = (idx === 0) && firstDep && firstDep !== BASE;

    const oobPill = isOutOfBase
      ? `<span class="pill pill-red">${esc(BASE)}</span>`
      : '';

    const subTitleParts = [];
    if (day.report)  subTitleParts.push(`Report ${esc(day.report)}`);
    if (day.release) subTitleParts.push(`Release ${esc(day.release)}`);
    if (day.hotel)   subTitleParts.push(`${esc(day.hotel)}`);

    return `
      <tr class="day-mini" data-day="${idx}">
        <td colspan="4">
          <div class="day-mini-inner">
            <div class="day-mini-header">
              <span class="day-title">Day ${idx + 1}</span>
              ${oobPill}
              <span class="day-sub">&middot; ${subTitleParts.join(' · ')}</span>
              <button class="link day-toggle" data-day="${idx}" type="button">click to show legs</button>
            </div>
          </div>
        </td>
      </tr>`;
  }

  // ----- Day legs panel (indent more; toggled by mini row) -----
  function renderDayPanel(day, idx) {
    const legs = Array.isArray(day.legs) ? day.legs : [];
    const legsRows = legs.map(leg => `
      <tr class="leg-row ${leg.done ? 'leg-done' : ''}">
        <td>${esc(leg.flight || '')}</td>
        <td>${esc(leg.dep || '')}–${esc(leg.arr || '')}</td>
        <td>${esc(leg.dep_time_str || leg.dep_time || '')}
            &nbsp;→&nbsp;
            ${esc(leg.arr_time_str || leg.arr_time || '')}</td>
      </tr>`).join('');

    const table = legs.length ? `
      <table class="legs">
        <thead><tr><th>Flight</th><th>Route</th><th>Block Times</th></tr></thead>
        <tbody>${legsRows}</tbody>
      </table>` : `<div class="muted">No legs parsed.</div>`;

    return `
      <tr class="details" data-day-panel="${idx}">
        <td colspan="4">
          <div class="daysbox">${table}</div>
        </td>
      </tr>`;
  }

  function bindExpanders() {
    // Expand/collapse all day panels for a pairing when summary clicked
    document.querySelectorAll('tr.summary').forEach(sum => {
      sum.addEventListener('click', () => {
        // find following rows until next summary/off
        let n = sum.nextElementSibling;
        let toggleOpen = false;
        // decide target by first panel state
        while (n && !n.classList.contains('summary') && !n.classList.contains('off')) {
          if (n.matches('[data-day-panel]')) {
            toggleOpen = !n.classList.contains('open');
            break;
          }
          n = n.nextElementSibling;
        }
        n = sum.nextElementSibling;
        while (n && !n.classList.contains('summary') && !n.classList.contains('off')) {
          if (n.matches('[data-day-panel]')) {
            if (toggleOpen) n.classList.add('open'); else n.classList.remove('open');
          }
          n = n.nextElementSibling;
        }
      });
    });

    // Toggle a single day panel when its "click to show legs" link is clicked
    document.querySelectorAll('.day-toggle').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const idx = btn.getAttribute('data-day');
        const panel = document.querySelector(`tr.details[data-day-panel="${idx}"]`);
        if (panel) panel.classList.toggle('open');
      });
    });
  }

  // ---- Live status ticker ----
  function tickStatusLine() {
    // last pull mm:ss
    if (state.lastPullIso) {
      const ago = preciseAgo(new Date(state.lastPullIso));
      setText('#last-pull', ago);
    }

    // next refresh countdown
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
        renderOnce(); // single retry in case SSE is late
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
