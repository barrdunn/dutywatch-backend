(function () {
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase = cfg.apiBase || '';
  const HOME_BASE = (cfg.baseAirport || 'DFW').toUpperCase();

  // iOS detection for minor CSS tweaks (kept)
  const IS_IOS = /iP(ad|hone|od)/.test(navigator.platform)
    || (navigator.userAgent.includes('Mac') && 'ontouchend' in document);
  document.documentElement.classList.toggle('ios', IS_IOS);

  const state = {
    lastPullIso: null,
    nextRefreshIso: null,
    clockMode: cfg.clockMode === '24' ? 24 : 12,
    onlyReports: cfg.onlyReports !== false,
    _zeroKick: 0,
  };

  // Public action
  window.dwManualRefresh = async function () {
    try { await fetch(apiBase + '/api/refresh', { method: 'POST' }); }
    catch (e) { console.error(e); }
  };

  // Controls
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
      // Re-render by re-fetching all rows with is_24h flag to get report/release/legs in correct format
      await renderOnce();
      // If the plan modal is open, re-open/redraw it with new time format on next click.
      // (We purposely keep modal content simple: it is regenerated every time you open it.)
    });
  }

  // First paint
  renderOnce();

  // SSE
  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('hello', () => {});
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
  } catch {}

  // Status ticker
  setInterval(tickStatusLine, 1000);

  // Global click handlers
  document.addEventListener('click', async (e) => {
    // Expand/collapse pairing rows (ignore clicks on checkbox)
    const sum = e.target.closest('tr.summary');
    if (sum && !e.target.closest('[data-ck]')) {
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
      return;
    }

    // Day header toggles its legs
    const hdr = e.target.closest('.dayhdr');
    if (hdr) {
      const day = hdr.closest('.day');
      const legs = day?.querySelector('.legs');
      if (!legs) return;
      const shown = legs.style.display !== 'none';
      legs.style.display = shown ? 'none' : 'table';
      const helper = day.querySelector('.helper');
      if (helper) helper.textContent = shown ? 'click to show legs' : 'click to hide legs';
      return;
    }

    // Check-in checkbox behavior
    const ckBtn = e.target.closest('[data-ck]');
    if (ckBtn) {
      const stateAttr = ckBtn.getAttribute('data-ck');
      const pairingId = ckBtn.getAttribute('data-pairing') || '';
      const reportIso = ckBtn.getAttribute('data-report') || '';

      if (stateAttr === 'off') {
        // Show plan modal when window is not open
        await openPlan(pairingId, reportIso);
        return;
      }

      if (stateAttr === 'pending') {
        // Acknowledge immediately
        try {
          await fetch(`${apiBase}/api/ack/acknowledge`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ pairing_id: pairingId, report_local_iso: reportIso }),
          });
        } catch (err) {
          console.error('ack failed', err);
        }
        await renderOnce();
        return;
      }

      // 'ok' -> do nothing
      return;
    }

    // Plan modal close buttons
    if (e.target.id === 'plan-close-1' || e.target.id === 'plan-close-2') {
      showModal(false);
      return;
    }

    // Click on backdrop closes
    if (e.target.classList.contains('modal')) {
      showModal(false);
      return;
    }
  });

  // ESC key closes modal
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') showModal(false);
  });

  // Core render
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

    // header label from API (single source of truth)
    const label = (data.window && data.window.label) || data.looking_through || '—';
    setText('#looking-through', label);
    setText('#last-pull', data.last_pull_local ?? '—');

    const base = (data.next_pull_local && data.tz_label)
      ? `${data.next_pull_local} (${data.tz_label})`
      : '—';
    const nextEl = qs('#next-refresh');
    if (nextEl) nextEl.innerHTML = `${esc(base)} <span id="next-refresh-eta"></span>`;

    const tbody = qs('#pairings-body');
    tbody.innerHTML = (data.rows || []).map(row => renderRowHTML(row, HOME_BASE)).join('');
  }

  // Helpers for start airport / out-of-base pill
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
    /* OFF row: put duration in Release column (kept) */
    if (row.kind === 'off') {
      return `
        <tr class="off">
          <td class="ck"></td>
          <td class="sum-first"><span class="off-label">OFF</span></td>
          <td class="muted"></td>
          <td class="off-dur">${esc(row.display?.off_dur || '')}</td>
          <td class="muted"></td>
        </tr>`;
    }

    const daysCount = row.days ? row.days.length : 0;
    const inProg = row.in_progress ? `<span class="progress">(In progress)</span>` : '';

    const startDep = firstDepartureAirport(row);
    const showOOB = !!(startDep && startDep !== homeBase);
    const oobPill = showOOB ? `<span class="pill pill-red">${esc(startDep)}</span>` : '';

    const checkCell = renderCheckCell(row);
    const details = (row.days || []).map((day, i) => renderDayHTML(day, i)).join('');

    return `
      <tr class="summary">
        ${checkCell}
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

  function renderCheckCell(row) {
    const ack = row.ack || {};
    const acknowledged = !!ack.acknowledged;
    const windowOpen = !!ack.window_open;
    const stateAttr = acknowledged ? 'ok' : (windowOpen ? 'pending' : 'off');

    const ariaChecked = acknowledged ? 'true' : 'false';
    const ariaDisabled = acknowledged ? 'true' : 'false';

    return `
      <td class="ck ${stateAttr}">
        <div class="ck-wrapper">
          <button class="ckbtn"
                  type="button"
                  role="checkbox"
                  aria-checked="${ariaChecked}"
                  aria-disabled="${ariaDisabled}"
                  title="${stateAttr === 'ok' ? 'Acknowledged' : (stateAttr === 'pending' ? 'Click to acknowledge now' : 'Click to view reminder plan')}"
                  data-ck="${stateAttr}"
                  data-pairing="${esc(row.pairing_id || '')}"
                  data-report="${esc((ack && ack.report_local_iso) || '')}">
            <span class="ckbox" aria-hidden="true"></span>
          </button>
        </div>
      </td>`;
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

  // ---- Time + Date helpers for the plan modal (12h / 24h aware)
  function fmtTime(d, is24) {
    if (is24) {
      // 24h HH:MM
      const hh = String(d.getHours()).padStart(2, '0');
      const mm = String(d.getMinutes()).padStart(2, '0');
      return `${hh}:${mm}`;
    }
    // 12h h:MM AM/PM
    const h = d.getHours();
    const hh12 = (h % 12) || 12;
    const mm = String(d.getMinutes()).padStart(2, '0');
    const ampm = h < 12 ? 'AM' : 'PM';
    return `${hh12}:${mm} ${ampm}`;
  }
  const fmtDate = (d) => `${d.getMonth() + 1}/${d.getDate()}`;

  // Plan modal (When column shows TIME then DATE; 12/24h according to state.clockMode)
  async function openPlan(pairingId, reportIso) {
    try {
      const res = await fetch(`${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairingId)}&report_local_iso=${encodeURIComponent(reportIso)}`);
      const data = await res.json();

      const use24 = state.clockMode === 24;

      let lastDateKey = null;
      const rows = (data.attempts || []).map(a => {
        const d = new Date(a.at_iso);
        const timeStr = fmtTime(d, use24);
        const dateStr = fmtDate(d);
        const showDate = dateStr !== lastDateKey;
        if (showDate) lastDateKey = dateStr;

        // TIME first; show DATE only when it changes (and always after time)
        const whenHTML = showDate
          ? `<span class="plan-when"><span class="plan-time">${esc(timeStr)}</span><span class="plan-date"> ${esc(dateStr)}</span></span>`
          : `<span class="plan-when"><span class="plan-time">${esc(timeStr)}</span></span>`;

        const type = (a.kind || '').toUpperCase();
        const det  = a.label || '';
        return `<tr>
          <td>${whenHTML}</td>
          <td>${esc(type)}</td>
          <td>${esc(det)}</td>
        </tr>`;
      }).join('');

      qs('#plan-rows').innerHTML = rows || `<tr><td colspan="3" class="muted">No upcoming attempts.</td></tr>`;
      qs('#plan-meta').textContent =
        `Policy: push at T-${data.policy.window_open_hours}h and T-${data.policy.second_push_at_hours}h; ` +
        `calls from T-${data.policy.call_start_hours}h every ${data.policy.call_interval_minutes}m (2 rings/attempt)`;
      showModal(true);
    } catch (e) {
      console.error(e);
    }
  }

  function showModal(show) {
    const m = qs('#plan-modal');
    if (!m) return;
    m.classList.toggle('hidden', !show);
    document.body.classList.toggle('modal-open', show);
  }

  // Status line tick
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

  // utils
  function qs(sel) { return document.querySelector(sel); }
  function setText(sel, v) { const el = qs(sel); if (el) el.textContent = v; }
  function esc(s) {
    return String(s).replace(/[&<>"'`=\/]/g, (ch) =>
      ({'&':'&amp;','<':'&#x3E;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'}[ch])
    );
  }
  function preciseAgo(d){
    const sec = Math.max(0, Math.floor((Date.now() - d.getTime())/1000));
    const m = Math.floor(sec/60), s = sec%60;
    return m ? `${m}m ${s}s ago` : `${s}s ago`;
  }
  function safeParseJSON(s) { try { return JSON.parse(s || '{}'); } catch { return null; } }
})();
