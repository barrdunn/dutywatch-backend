(function () {
  // ========= Boot config =========
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase   = cfg.apiBase || '';
  const HOME_BASE = (cfg.baseAirport || 'DFW').toUpperCase();

  // iOS class hook (used by CSS only)
  const IS_IOS = /iP(ad|hone|od)/.test(navigator.platform)
    || (navigator.userAgent.includes('Mac') && 'ontouchend' in document);
  document.documentElement.classList.toggle('ios', IS_IOS);

  // ========= State =========
  const state = {
    lastPullIso: null,
    nextRefreshIso: null,
    clockMode: cfg.clockMode === '24' ? 24 : 12,   // default 12h
    onlyReports: cfg.onlyReports !== false,
    _zeroKick: 0,
  };

  // ========= Public actions =========
  window.dwManualRefresh = async function () {
    try { await fetch(apiBase + '/api/refresh', { method: 'POST' }); }
    catch (e) { console.error(e); }
  };

  // ========= Controls =========
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
      state.clockMode = parseInt(clockSel.value, 10) === 24 ? 24 : 12;
      repaintTimesOnly();             // <-- no reflow, no re-render
    });
  }

  // ========= First paint & live updates =========
  renderOnce();

  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('hello', () => {});
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
  } catch {}

  setInterval(tickStatusLine, 1000);

  // ========= Global click handlers =========
  document.addEventListener('click', async (e) => {
    // Expand/collapse pairing rows (ignore clicks on checkbox or plan button)
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

    // Check-in cell behavior
    const ckBtn = e.target.closest('[data-ck]');
    if (ckBtn) {
      const stateAttr = ckBtn.getAttribute('data-ck');
      const pairingId = ckBtn.getAttribute('data-pairing') || '';
      const reportIso = ckBtn.getAttribute('data-report') || '';

      if (stateAttr === 'off') {
        await openPlan(pairingId, reportIso);
        return;
      }

      if (stateAttr === 'pending') {
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

      // 'ok' -> no-op
      return;
    }

    // Plan modal close buttons / backdrop
    if (e.target.id === 'plan-close-1' || e.target.id === 'plan-close-2') {
      showModal(false);
      return;
    }
    if (e.target.classList.contains('modal')) {
      showModal(false);
      return;
    }
  });

  // ESC key closes modal
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') showModal(false);
  });

  // ========= Core render =========
  async function renderOnce() {
    const params = new URLSearchParams({
      is_24h: '0',                                // server can send any, we format on client
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

    // after DOM is in place, paint times to current mode without reflow
    repaintTimesOnly();
  }

  // ========= Front-end time utils (format + parsing for repaint) =========
  function fmtMin(min, is24) {
    if (min == null || isNaN(min)) return '';
    let h = Math.floor(min / 60), m = min % 60;
    if (is24) return `${String(h).padStart(2,'0')}${String(m).padStart(2,'0')}`; // 24h: no colon
    const ap = h < 12 ? 'AM' : 'PM';
    const hh = (h % 12) || 12;
    return `${hh}:${String(m).padStart(2,'0')} ${ap}`;
  }
  const dateMD = (d) => `${d.getMonth()+1}/${d.getDate()}`;
  function dateStrFromISO(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    return isNaN(d) ? '' : dateMD(d);
  }
  function isoToMinutes(iso) {
    if (!iso) return null;
    const d = new Date(iso);
    if (isNaN(d)) return null;
    return d.getHours()*60 + d.getMinutes();
  }
  function parseClockishToMin(s) {
    // Handles "07:15", "7:15 AM", "715", "0715", "07:15 PM", "1115"
    if (!s) return null;
    const str = String(s).trim();
    // AM/PM?
    const ampm = /am|pm/i.test(str) ? str.match(/(am|pm)/i)[1].toLowerCase() : null;
    let digits = str.replace(/[^0-9]/g,'');
    if (digits.length < 3 || digits.length > 4) return null;
    if (digits.length === 3) digits = '0' + digits;
    const h = parseInt(digits.slice(0,2),10);
    const m = parseInt(digits.slice(2),10);
    if (isNaN(h) || isNaN(m)) return null;
    if (ampm) {
      let hh = h % 12;
      if (ampm === 'pm') hh += 12;
      return hh*60 + m;
    }
    // 24h interpretation
    return h*60 + m;
  }

  // Create a span that we can repaint later, with data-min minutes and optional date text
  function timeSpanHTML(minOrNull, fallback, dateText /* optional */) {
    const hasMin = minOrNull != null && !isNaN(minOrNull);
    const initText = hasMin ? fmtMin(minOrNull, state.clockMode === 24) : (fallback || '');
    const dateHTML = dateText ? ` <span class="js-date muted">${esc(dateText)}</span>` : '';
    return `<span class="js-time tabnums"${hasMin ? ` data-min="${minOrNull}"` : ''}>${esc(initText)}</span>${dateHTML}`;
  }

  // Repaint only the time text, preserving appended dates and layout
  function repaintTimesOnly() {
    const is24 = state.clockMode === 24;
    document.querySelectorAll('.js-time').forEach(el => {
      const v = el.getAttribute('data-min');
      if (v == null || v === '') return;
      el.textContent = fmtMin(parseInt(v,10), is24);
    });
  }

  // ========= Helpers for rendering =========
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

  // ========= Row renderers =========
  function renderRowHTML(row, homeBase) {
    // OFF row: duration goes in Release column; capsule aligns with summary
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

    // Summary: report/release minutes + dates (date appears after time)
    const reportISO = row.report_local_iso || (row.days && row.days[0]?.report_local_iso) || '';
    const releaseISO = (row.days && row.days[row.days.length-1]?.release_local_iso) || row.release_local_iso || '';

    const repMin  = isoToMinutes(reportISO) ?? parseClockishToMin(row.display?.report_str || row.report || '');
    const relMin  = isoToMinutes(releaseISO) ?? parseClockishToMin(row.display?.release_str || row.release || '');
    const repDate = dateStrFromISO(reportISO);
    const relDate = dateStrFromISO(releaseISO);

    const repSpan = timeSpanHTML(repMin, row.display?.report_str ?? row.report ?? '', repDate);
    const relSpan = timeSpanHTML(relMin, row.display?.release_str ?? row.release ?? '', relDate);

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
        <td>${repSpan}</td>
        <td>${relSpan}</td>
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
    const windowOpen   = !!ack.window_open;
    const stateAttr    = acknowledged ? 'ok' : (windowOpen ? 'pending' : 'off');

    const ariaChecked  = acknowledged ? 'true' : 'false';
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
    // Day header: time spans (report / release) repainted on mode switch
    const repMin  = isoToMinutes(day.report_local_iso)  ?? parseClockishToMin(day.report);
    const relMin  = isoToMinutes(day.release_local_iso) ?? parseClockishToMin(day.release);
    const repHTML = day.report ? `· Report ${timeSpanHTML(repMin, day.report)}`   : '';
    const relHTML = day.release ? `· Release ${timeSpanHTML(relMin, day.release)}` : '';

    const legs = (day.legs || []).map(leg => {
      const depMin = parseClockishToMin(leg.dep_time_str || leg.dep_time || leg.dep_hhmm);
      const arrMin = parseClockishToMin(leg.arr_time_str || leg.arr_time || leg.arr_hhmm);
      const dep = timeSpanHTML(depMin, leg.dep_time_str || leg.dep_time || '');
      const arr = timeSpanHTML(arrMin, leg.arr_time_str || leg.arr_time || '');
      return `
      <tr class="leg-row ${leg.done ? 'leg-done' : ''}">
        <td>${esc(leg.flight || '')}</td>
        <td>${esc(leg.dep || '')}–${esc(leg.arr || '')}</td>
        <td class="tabnums">${dep}&nbsp;→&nbsp;${arr}</td>
      </tr>`;
    }).join('');

    return `
      <div class="day">
        <div class="dayhdr">
          <span class="dot"></span>
          <span class="daytitle">Day ${idx + 1}</span>
          ${repHTML}
          ${relHTML}
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

  // ========= Plan modal (When = time then optional date; also repaints on mode) =========
  async function openPlan(pairingId, reportIso) {
    try {
      const res = await fetch(`${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairingId)}&report_local_iso=${encodeURIComponent(reportIso)}`);
      const data = await res.json();

      let lastDateKey = null;
      const rows = (data.attempts || []).map(a => {
        const d = new Date(a.at_iso);
        const dateKey = isNaN(d) ? '' : dateMD(d);
        const showDate = dateKey !== lastDateKey;
        if (showDate) lastDateKey = dateKey;

        const min = isNaN(d) ? null : d.getHours()*60 + d.getMinutes();
        const whenHTML = showDate
          ? `<span class="js-time tabnums" data-min="${min || ''}">${fmtMin(min, state.clockMode === 24)}</span><span class="js-date muted"> ${esc(dateKey)}</span>`
          : `<span class="js-time tabnums" data-min="${min || ''}">${fmtMin(min, state.clockMode === 24)}</span>`;

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

      // ensure mode-consistent right after inject
      repaintTimesOnly();
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

  // ========= Status line =========
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

  // ========= tiny utils =========
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
