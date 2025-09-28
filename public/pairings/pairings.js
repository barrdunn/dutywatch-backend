(function () {
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase = cfg.apiBase || '';
  const HOME_BASE = (cfg.baseAirport || 'DFW').toUpperCase();

  const IS_IOS = /iP(ad|hone|od)/.test(navigator.platform)
    || (navigator.userAgent.includes('Mac') && 'ontouchend' in document);
  document.documentElement.classList.toggle('ios', IS_IOS);

  const state = {
    lastPullIso: null,
    nextRefreshIso: null,
    clockMode: cfg.clockMode === '24' ? 24 : 12,  // default 12h
    onlyReports: cfg.onlyReports !== false,
    _zeroKick: 0,
  };

  // ---------------- Time helpers ----------------
  function hhmmTo12(hhmm) {
    if (!hhmm) return '';
    let h = parseInt(hhmm.slice(0,2),10);
    const m = hhmm.slice(2,4);
    const ampm = h < 12 ? 'AM' : 'PM';
    h = h % 12 || 12;
    return `${h}:${m} ${ampm}`;
  }
  function hhmmTo24(hhmm) {
    if (!hhmm) return '';
    // If you want a colon, change to `${hhmm.slice(0,2)}:${hhmm.slice(2,4)}`
    return `${hhmm.slice(0,2)}${hhmm.slice(2,4)}`;
  }
  function formatHHMM(hhmm, mode) {
    return (parseInt(mode,10) === 24) ? hhmmTo24(hhmm) : hhmmTo12(hhmm);
  }
  function normalizeToHHMM(txt) {
    if (!txt) return '';
    const s = String(txt).trim();
    // Try detect AM/PM anywhere
    const ampmMatch = s.match(/(\d{1,2})\D?(\d{2})\s*(AM|PM)/i);
    if (ampmMatch) {
      let H = parseInt(ampmMatch[1],10);
      const M = ampmMatch[2];
      const AP = ampmMatch[3].toUpperCase();
      if (AP === 'AM') { if (H === 12) H = 0; }
      else { if (H !== 12) H += 12; }
      return `${String(H).padStart(2,'0')}${M}`;
    }
    // Strip to digits; accept 3–4 digits like 815 or 0815
    const digits = s.replace(/[^0-9]/g,'');
    if (digits.length >= 4) return digits.slice(0,4);
    if (digits.length === 3) return `0${digits}`;
    return '';
  }
  function makeTimeSpan(candidates) {
    // candidates: array of possible values in priority order
    const raw = (candidates.find(v => v) ?? '').toString();
    const hhmm = normalizeToHHMM(raw);
    const text = hhmm ? formatHHMM(hhmm, state.clockMode) : '—';
    return `<span class="js-time" data-hhmm="${esc(hhmm || '')}">${esc(text)}</span>`;
  }
  function applyClockMode(mode) {
    const use24 = (parseInt(mode,10) === 24);
    document.querySelectorAll('.js-time').forEach(el => {
      const hhmm = el.getAttribute('data-hhmm') || '';
      if (!hhmm) return; // leave as-is if we couldn't normalize
      el.textContent = use24 ? hhmmTo24(hhmm) : hhmmTo12(hhmm);
    });
  }

  // ---------------- Actions/controls ----------------
  window.dwManualRefresh = async function () {
    try { await fetch(apiBase + '/api/refresh', { method: 'POST' }); }
    catch (e) { console.error(e); }
  };

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
    clockSel.addEventListener('change', () => {
      state.clockMode = parseInt(clockSel.value, 10) === 12 ? 12 : 24;
      // Update all visible times IN PLACE (no re-render, keep rows open)
      applyClockMode(state.clockMode);
    });
  }

  // ---------------- Render cycle ----------------
  renderOnce();

  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('hello', () => {});
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
  } catch {}

  setInterval(tickStatusLine, 1000);

  document.addEventListener('click', async (e) => {
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
        } catch (err) { console.error('ack failed', err); }
        await renderOnce();
      }
      return;
    }

    if (e.target.id === 'plan-close-1' || e.target.id === 'plan-close-2' || e.target.classList.contains('modal')) {
      showModal(false);
      return;
    }
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') showModal(false);
  });

  async function renderOnce() {
    // Ask server for raw strings; we’ll format on client
    const params = new URLSearchParams({
      is_24h: '0',
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

    // Apply current clock mode after DOM is in place
    applyClockMode(state.clockMode);
  }

  function firstDepartureAirport(row) {
    const days = row?.days || [];
    for (const d of days) {
      const legs = d?.legs || [];
      if (legs.length && legs[0].dep) return String(legs[0].dep).toUpperCase();
    }
    return null;
  }

  function renderRowHTML(row, homeBase) {
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

    // Use ANY available value; normalize to HHMM and show formatted text now
    const reportSpan = makeTimeSpan([
      row.display?.report_hhmm,
      row.report_hhmm,
      row.display?.report_str,
      row.report
    ]);
    const releaseSpan = makeTimeSpan([
      row.display?.release_hhmm,
      row.release_hhmm,
      row.display?.release_str,
      row.release
    ]);

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
        <td>${reportSpan}</td>
        <td>${releaseSpan}</td>
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
    const legsRows = (day.legs || []).map(leg => {
      const depSpan = makeTimeSpan([leg.dep_hhmm, leg.dep_time, leg.dep_time_str]);
      const arrSpan = makeTimeSpan([leg.arr_hhmm, leg.arr_time, leg.arr_time_str]);
      const route = `${esc(leg.dep || '')}–${esc(leg.arr || '')}`;
      return `
        <tr class="leg-row ${leg.done ? 'leg-done' : ''}">
          <td>${esc(leg.flight || '')}</td>
          <td class="route-cell"><span class="route-code">${route}</span></td>
          <td class="bt">${depSpan} &nbsp;→&nbsp; ${arrSpan}</td>
        </tr>`;
    }).join('');

    const dayRep = makeTimeSpan([day.report_hhmm, day.report_time, day.report]);
    const dayRel = makeTimeSpan([day.release_hhmm, day.release_time, day.release]);

    return `
      <div class="day">
        <div class="dayhdr">
          <span class="dot"></span>
          <span class="daytitle">Day ${idx + 1}</span>
          ${day.report ? `· Report&nbsp;${dayRep}` : ''}
          ${day.release ? `· Release&nbsp;${dayRel}` : ''}
          ${day.hotel ? `· ${esc(day.hotel)}` : ''}
          <span class="helper">click to show legs</span>
        </div>
        <div class="legs-wrap">
          <table class="legs" style="display:none">
            <colgroup>
              <col style="width:22%">
              <col style="width:34%">
              <col style="width:44%">
            </colgroup>
            <thead>
              <tr>
                <th>Flight</th>
                <th>Route</th>
                <th>Block Times</th>
              </tr>
            </thead>
            <tbody>${legsRows}</tbody>
          </table>
        </div>
      </div>`;
  }

  async function openPlan(pairingId, reportIso) {
    try {
      const res = await fetch(`${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairingId)}&report_local_iso=${encodeURIComponent(reportIso)}`);
      const data = await res.json();

      const toHHMM  = (d) => `${String(d.getHours()).padStart(2,'0')}${String(d.getMinutes()).padStart(2,'0')}`;
      const fmtDate = (d) => `${d.getMonth()+1}/${d.getDate()}`;

      let lastDateKey = null;
      const rows = (data.attempts||[]).map(a => {
        const d = new Date(a.at_iso);
        const dateKey = fmtDate(d);
        const hhmm = toHHMM(d);
        const showDate = dateKey !== lastDateKey;
        if (showDate) lastDateKey = dateKey;

        const whenHTML = showDate
          ? `<span class="js-time" data-hhmm="${hhmm}">${esc(formatHHMM(hhmm, state.clockMode))}</span> <span class="muted">${dateKey}</span>`
          : `<span class="js-time" data-hhmm="${hhmm}">${esc(formatHHMM(hhmm, state.clockMode))}</span>`;

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
      applyClockMode(state.clockMode);
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