(function () {
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  let state = {
    apiBase: cfg.apiBase || '',
    clockMode: (cfg.clockMode || '24'),   // '12' | '24'
    refreshMinutes: cfg.refreshMinutes || 5,
    lastPullIso: null,
    nextRefreshIso: null,
    tickTimer: null,
  };

  // Manual refresh
  window.dwManualRefresh = async function () {
    try {
      await fetch(state.apiBase + '/api/refresh', { method: 'POST' });
      // UI will update via SSE; but also fall back:
      await renderOnce();
    } catch (e) { console.error(e); }
  };

  // Controls
  const refreshSel = document.getElementById('refresh-mins');
  if (refreshSel) {
    refreshSel.value = String(state.refreshMinutes);
    refreshSel.addEventListener('change', async () => {
      const minutes = parseInt(refreshSel.value, 10);
      state.refreshMinutes = minutes;
      try {
        await fetch(state.apiBase + '/api/settings/refresh-seconds', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ seconds: minutes * 60 }),
        });
      } catch (e) { console.error(e); }
      // reflect new schedule immediately
      await renderOnce();
    });
  }

  const clockSel = document.getElementById('clock-mode');
  if (clockSel) {
    clockSel.value = state.clockMode;
    clockSel.addEventListener('change', async () => {
      state.clockMode = clockSel.value;
      await renderOnce(); // ask backend to format legs accordingly
    });
  }

  // First paint
  renderOnce();

  // SSE for live updates
  try {
    const es = new EventSource(state.apiBase + '/api/events');
    es.addEventListener('hello', () => {});
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
    es.onerror = () => {};
  } catch {
    setInterval(renderOnce, (state.refreshMinutes || 5) * 60 * 1000);
  }

  async function renderOnce() {
    // stop any ticking; we’ll restart with new timestamps
    if (state.tickTimer) { clearInterval(state.tickTimer); state.tickTimer = null; }

    const query = new URLSearchParams({
      is_24h: state.clockMode === '24' ? '1' : '0',
      only_reports: '1',
    }).toString();

    let data;
    try {
      const res = await fetch(`${state.apiBase}/api/pairings?${query}`, { cache: 'no-store' });
      data = await res.json();
    } catch (e) {
      console.error('Failed to fetch /api/pairings', e);
      return;
    }

    // header/status
    setText('#looking-through', data.looking_through ?? '—');
    setText('#last-pull', data.last_pull_local ?? '—');

    // Wall clock + remember ISO stamps for ticking
    setText('#next-refresh', data.next_pull_local && data.tz_label
      ? `${data.next_pull_local} (${data.tz_label})` : '—');

    state.lastPullIso = data.last_pull_local_iso || null;
    state.nextRefreshIso = data.next_pull_local_iso || null;

    // render table
    const tbody = qs('#pairings-body');
    tbody.innerHTML = (data.rows || []).map(renderRowHTML).join('');

    // collapse all by default
    for (const tr of tbody.querySelectorAll('tr.summary')) {
      tr.classList.remove('open');
      const det = tr.nextElementSibling;
      if (det && det.classList.contains('details')) det.style.display = 'none';
    }
    // auto-open in-progress
    for (const tr of tbody.querySelectorAll('tr.summary')) {
      if (/\(In progress\)/.test(tr.innerHTML)) toggleRow(tr, true);
    }

    // (Re)start 1s ticker for “last pull” and “next in …”
    tickStatusLine();
    state.tickTimer = setInterval(tickStatusLine, 1000);
  }

    function tickStatusLine() {
    // last pull precise
    if (state.lastPullIso) {
        const s = preciseAgo(new Date(state.lastPullIso));
        setText('#last-pull', s);
    }

    // next refresh countdown
    const etaEl = qs('#next-refresh-eta');
    if (!etaEl) return;

    if (!state.nextRefreshIso) { etaEl.textContent = ''; return; }

    const diff = Math.floor((new Date(state.nextRefreshIso).getTime() - Date.now()) / 1000);
    const clamped = Math.max(0, diff);
    const m = Math.floor(clamped / 60);
    const s = clamped % 60;
    etaEl.textContent = clamped > 0 ? `in ${m}m ${s}s` : '(refreshing…)';

    // When we hit zero, trigger a one-shot re-render to pick up the new schedule
        if (diff <= 0) {
            const now = Date.now();
            if (!state._zeroKick || now - state._zeroKick > 4000) { // throttle 4s
            state._zeroKick = now;
            renderOnce();
            }
        }
    }

  // Click-to-toggle details (delegated)
  document.addEventListener('click', (ev) => {
    const tr = ev.target.closest('tr.summary');
    if (!tr) return;
    toggleRow(tr, !tr.classList.contains('open'));
  });

  function toggleRow(summaryTr, open) {
    summaryTr.classList.toggle('open', open);
    const det = summaryTr.nextElementSibling;
    if (det && det.classList.contains('details')) {
      det.style.display = open ? 'table-row' : 'none';
    }
  }

  // Rendering helpers
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
    const daysCount = row.days ? row.days.length : 0;
    const inProg = row.in_progress ? `<span class="progress">(In progress)</span>` : '';
    const details = (row.days || []).map((day, i) => renderDayHTML(day, i)).join('');

    return `
      <tr class="summary">
        <td><strong>${esc(row.pairing_id || '')}</strong>
            <span class="pill">${daysCount} day</span> ${inProg}</td>
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
          Day ${idx + 1}
          ${day.report ? `&middot; Report ${esc(day.report)}` : ''}
          ${day.release ? `&middot; Release ${esc(day.release)}` : ''}
          ${day.hotel ? `&middot; ${esc(day.hotel)}` : ''}
        </div>
        ${legs ? `
          <table class="legs">
            <thead><tr><th>Flight</th><th>Route</th><th>Block Times</th></tr></thead>
            <tbody>${legs}</tbody>
          </table>` : `<div class="muted">No legs parsed.</div>`}
      </div>`;
  }

  // utils
  function preciseAgo(then) {
    const d = Math.max(0, Math.floor((Date.now() - then.getTime()) / 1000));
    const m = Math.floor(d / 60), s = d % 60;
    if (m && s) return `${m}m ${s}s ago`;
    if (m) return `${m}m ago`;
    return `${s}s ago`;
  }
  function qs(sel) { return document.querySelector(sel); }
  function setText(sel, v) { const el = qs(sel); if (el) el.textContent = v; }
  function esc(s) {
    return String(s).replace(/[&<>"'`=\/]/g, (ch) =>
      ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'}[ch])
    );
  }
  function safeParseJSON(s) { try { return JSON.parse(s || '{}'); } catch { return null; } }
})();
