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
    clockMode: cfg.clockMode === '24' ? 24 : 12,
    onlyReports: cfg.onlyReports !== false,
    _zeroKick: 0,
    rowsCache: null,
  };

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
      state.clockMode = parseInt(clockSel.value, 10) === 24 ? 24 : 12;
      repaintTimesOnly(); // no DOM rebuild
    });
  }

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
        } catch (err) {
          console.error('ack failed', err);
        }
        await renderOnce();
        return;
      }
      return;
    }

    if (e.target.id === 'plan-close-1' || e.target.id === 'plan-close-2') {
      showModal(false);
      return;
    }
    if (e.target.classList.contains('modal')) {
      showModal(false);
      return;
    }
  });

  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') showModal(false); });

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

    if (refreshSel && data.refresh_minutes) refreshSel.value = String(data.refresh_minutes);

    const label = (data.window && data.window.label) || data.looking_through || '—';
    setText('#looking-through', label);
    setText('#last-pull', data.last_pull_local ?? '—');

    const base = (data.next_pull_local && data.tz_label) ? `${data.next_pull_local} (${data.tz_label})` : '—';
    const nextEl = qs('#next-refresh');
    if (nextEl) nextEl.innerHTML = `${esc(base)} <span id="next-refresh-eta"></span>`;

    const tbody = qs('#pairings-body');
    state.rowsCache = (data.rows || []).slice();
    tbody.innerHTML = state.rowsCache.map(row => renderRowHTML(row, HOME_BASE)).join('');

    repaintTimesOnly();
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

    const repIso = row.report_local_iso || row.ack?.report_local_iso || row.display?.report_iso || '';
    const relIso = row.release_local_iso || row.display?.release_iso || '';

    return `
      <tr class="summary">
        ${checkCell}
        <td class="sum-first">
          <strong>${esc(row.pairing_id || '')}</strong>
          <span class="pill">${daysCount} day</span>
          ${oobPill}
          ${inProg}
        </td>
        <td><span class="js-dt" data-iso="${esc(repIso)}"></span></td>
        <td><span class="js-dt" data-iso="${esc(relIso)}"></span></td>
        <td class="muted">click to expand days</td>
      </tr>
      <tr class="details">
        <td colspan="5">
          <div class="daysbox">${(row.days || []).map((day, i) => renderDayHTML(day, i)).join('')}</div>
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
    const legs = (day.legs || []).map(leg => {
      const depMin = parseClockishToMin(leg.dep_time_str || leg.dep_time || leg.dep_hhmm || '');
      const arrMin = parseClockishToMin(leg.arr_time_str || leg.arr_time || leg.arr_hhmm || '');
      const depAttr = depMin != null ? ` data-min="${depMin}"` : '';
      const arrAttr = arrMin != null ? ` data-min="${arrMin}"` : '';

      return `
        <tr class="leg-row ${leg.done ? 'leg-done' : ''}">
          <td>${esc(leg.flight || '')}</td>
          <td class="route-cell">
            <span class="route-code">${esc((leg.dep || '') + '–' + (leg.arr || ''))}</span>
            <span class="route-times">
              <span class="js-time"${depAttr}></span>
              &nbsp;→&nbsp;
              <span class="js-time"${arrAttr}></span>
            </span>
          </td>
        </tr>`;
    }).join('');

    const repMin = parseClockishToMin(day.report_time || day.report || '');
    const relMin = parseClockishToMin(day.release_time || day.release || '');
    const repAttr = repMin != null ? ` data-min="${repMin}"` : '';
    const relAttr = relMin != null ? ` data-min="${relMin}"` : '';

    const hotel = day.hotel ? ` · ${esc(day.hotel)}` : '';

    return `
      <div class="day">
        <div class="dayhdr">
          <span class="dot"></span>
          <span class="daytitle">Day ${idx + 1}</span>
          ${repMin != null ? ` · Report <span class="js-time"${repAttr}></span>` : ''}
          ${relMin != null ? ` · Release <span class="js-time"${relAttr}></span>` : ''}
          ${hotel}
          <span class="helper">click to show legs</span>
        </div>
        ${legs ? `
          <div class="legs-wrap">
            <table class="legs" style="display:none">
              <thead><tr><th>Flight</th><th>Route</th></tr></thead>
              <tbody>${legs}</tbody>
            </table>
          </div>` : `<div class="muted subnote">No legs parsed.</div>`}
      </div>`;
  }

  /* ===== Plan modal ===== */
  async function openPlan(pairingId, reportIso) {
    try {
      const res = await fetch(`${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairingId)}&report_local_iso=${encodeURIComponent(reportIso)}`);
      const data = await res.json();

      const fmtDate = (d) => `${d.getMonth()+1}/${d.getDate()}`;
      let lastDate = null;

      const rows = (data.attempts||[]).map(a => {
        const d = new Date(a.at_iso);
        const t = formatMinutesToString(d.getHours()*60 + d.getMinutes(), state.clockMode);
        const date = fmtDate(d);
        const showDate = (date !== lastDate);
        if (showDate) lastDate = date;

        const whenHTML = showDate
          ? `<span class="plan-when"><span class="plan-time">${esc(t)}</span> <span class="plan-date">${esc(date)}</span></span>`
          : `<span class="plan-when"><span class="plan-time">${esc(t)}</span></span>`;

        return `<tr>
          <td>${whenHTML}</td>
          <td>${esc((a.kind||'').toUpperCase())}</td>
          <td>${esc(a.label||'')}</td>
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

  /* ===== Repaint times (summary + day header + legs) ===== */
  function repaintTimesOnly() {
    document.querySelectorAll('.js-dt').forEach(el => {
      const iso = el.getAttribute('data-iso') || '';
      if (!iso) { el.textContent = '—'; return; }
      const d = new Date(iso);
      if (isNaN(d.getTime())) { el.textContent = '—'; return; }
      el.textContent = formatSummaryDateTime(d, state.clockMode);
    });

    document.querySelectorAll('.js-time').forEach(el => {
      const minAttr = el.getAttribute('data-min');
      if (minAttr == null || minAttr === '') { el.textContent = ''; return; }
      const mins = parseInt(minAttr, 10);
      el.textContent = formatMinutesToString(mins, state.clockMode);
    });
  }

  /* ===== Formatting helpers ===== */
  function formatSummaryDateTime(d, mode) {
    const wd = d.toLocaleDateString(undefined, { weekday:'short' });
    const mo = d.toLocaleDateString(undefined, { month:'short' });
    const da = d.getDate().toString().padStart(2,'0');
    const time = formatMinutesToString(d.getHours()*60 + d.getMinutes(), mode);
    return `${wd} ${mo} ${da} ${time}`;
  }

  function parseClockishToMin(s) {
    if (!s) return null;
    const str = String(s).trim().toUpperCase();

    let m = str.match(/^(\d{1,2}):?(\d{2})\s*(AM|PM)$/);
    if (m) {
      let h = parseInt(m[1], 10);
      const mm = parseInt(m[2], 10);
      const ap = m[3];
      if (ap === 'PM' && h !== 12) h += 12;
      if (ap === 'AM' && h === 12) h = 0;
      return h*60 + mm;
    }

    m = str.match(/^(\d{2}):(\d{2})$/);
    if (m) return parseInt(m[1],10)*60 + parseInt(m[2],10);

    m = str.match(/^(\d{3,4})$/);
    if (m) {
      const v = m[1].padStart(4,'0');
      return parseInt(v.slice(0,2),10)*60 + parseInt(v.slice(2),10);
    }
    return null;
  }

  // 12h -> "7:05 PM", 24h -> "1905" (no colon)
  function formatMinutesToString(mins, mode) {
    if (mins == null || isNaN(mins)) return '';
    mins = ((mins % (24*60)) + (24*60)) % (24*60);
    const h24 = Math.floor(mins/60);
    const mm = (mins % 60).toString().padStart(2,'0');
    if (mode === 24) return `${h24.toString().padStart(2,'0')}${mm}`;
    const ap = h24 < 12 ? 'AM' : 'PM';
    let h12 = h24 % 12; if (h12 === 0) h12 = 12;
    return `${h12}:${mm} ${ap}`;
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
