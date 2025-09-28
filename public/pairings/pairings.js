(function () {
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase = cfg.apiBase || '';
  const HOME_BASE = (cfg.baseAirport || 'DFW').toUpperCase();

  // iOS class (kept)
  const IS_IOS = /iP(ad|hone|od)/.test(navigator.platform)
    || (navigator.userAgent.includes('Mac') && 'ontouchend' in document);
  document.documentElement.classList.toggle('ios', IS_IOS);

  const state = {
    lastPullIso: null,
    nextRefreshIso: null,
    clockMode: cfg.clockMode === '24' ? 24 : 12, // default 12h
    onlyReports: cfg.onlyReports !== false,
    rows: [],       // server data
    rowsNorm: [],   // server data + parsed minute fields
    rendered: false
  };

  // ============== Time helpers ==============
  // minutes -> "HH:MM"/"h:MM AM"
  function fmtMin(min, is24) {
    if (min == null || isNaN(min)) return '';
    let h = Math.floor(min / 60), m = min % 60;
    if (is24) return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
    const ampm = h < 12 ? 'AM' : 'PM';
    const hh = (h % 12) || 12;
    return `${hh}:${String(m).padStart(2, '0')} ${ampm}`;
  }

  // parse many common formats to minutes since midnight (local)
  function parseToMin(any) {
    if (any == null) return null;
    let s = String(any).trim();
    if (!s) return null;

    // strip lone timezone tokens at end (CT, CST, Z), keep AM/PM
    s = s.replace(/\s([A-Z]{1,4})$/i, (m, tz) => (/AM|PM/i.test(tz) ? ` ${tz}` : ''));

    // "HHMM"/"HMM"
    if (/^\d{3,4}$/.test(s)) {
      const mm = parseInt(s.slice(-2), 10);
      const hh = parseInt(s.slice(0, s.length - 2), 10);
      if (mm >= 0 && mm < 60 && hh >= 0 && hh < 24) return hh * 60 + mm;
    }
    // "H:MM" (opt AM/PM)
    let m = s.match(/^(\d{1,2}):(\d{2})\s*([AP]M)?$/i);
    if (m) {
      let hh = parseInt(m[1], 10), mm = parseInt(m[2], 10);
      if (mm >= 0 && mm < 60 && hh >= 0 && hh <= 23) {
        const ap = (m[3] || '').toUpperCase();
        if (ap) { if (hh === 12) hh = 0; if (ap === 'PM') hh += 12; }
        return hh * 60 + mm;
      }
    }
    // "h:MMam"/"h:MMpm"
    m = s.match(/^(\d{1,2}):(\d{2})\s*([ap]m)$/i);
    if (m) {
      let hh = parseInt(m[1], 10), mm = parseInt(m[2], 10);
      if (hh === 12) hh = 0;
      if (m[3].toUpperCase() === 'PM') hh += 12;
      return hh * 60 + mm;
    }
    return null;
  }

  // pick best source field(s) for a time
  function chooseTimeToMin(obj, fields) {
    for (const f of fields) {
      const v = obj && obj[f];
      if (v == null || v === '') continue;
      const min = parseToMin(v);
      if (min != null) return min;
    }
    return null;
  }

  // Normalize all rows once so we have minute fields to format on demand
  function normalizeRows(rows) {
    return (rows || []).map(r => {
      if (r.kind === 'off') return r;

      const disp = r.display || {};
      const repMin =
        chooseTimeToMin(disp, ['report_hhmm','report','report_str']) ??
        chooseTimeToMin(r,    ['report_hhmm','report','report_str']);
      const relMin =
        chooseTimeToMin(disp, ['release_hhmm','release','release_str']) ??
        chooseTimeToMin(r,    ['release_hhmm','release','release_str']);

      const days = (r.days || []).map(d => {
        const reportMin = chooseTimeToMin(d, ['report','report_time','report_str']);
        const releaseMin = chooseTimeToMin(d, ['release','release_time','release_str']);
        const legs = (d.legs || []).map(leg => ({
          ...leg,
          _depMin: chooseTimeToMin(leg, ['dep_hhmm','dep_time','dep_time_str','dep']),
          _arrMin: chooseTimeToMin(leg, ['arr_hhmm','arr_time','arr_time_str','arr']),
        }));
        return { ...d, _reportMin: reportMin, _releaseMin: releaseMin, legs };
      });

      return { ...r, _reportMin: repMin, _releaseMin: relMin, days };
    });
  }

  // ============== Public actions ==============
  window.dwManualRefresh = async function () {
    try { await fetch(apiBase + '/api/refresh', { method: 'POST' }); }
    catch (e) { console.error(e); }
  };

  // ============== Controls ==============
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
      repaintTimesOnly();           // <<< no DOM rebuild, just rewrite time text
    });
  }

  // ============== Initial load + SSE ==============
  renderOnce();

  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('hello', () => {});
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
  } catch {}

  setInterval(tickStatusLine, 1000);

  // ============== Click handlers (unchanged UX) ==============
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

    if (e.target.id === 'plan-close-1' || e.target.id === 'plan-close-2' || e.target.classList.contains('modal')) {
      showModal(false);
      return;
    }
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') showModal(false);
  });

  // ============== Fetch + render (one-time structure) ==============
  async function renderOnce() {
    const params = new URLSearchParams({
      is_24h: 0, // we handle on client
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
    const base = (data.next_pull_local && data.tz_label)
      ? `${data.next_pull_local} (${data.tz_label})`
      : '—';
    const nextEl = qs('#next-refresh');
    if (nextEl) nextEl.innerHTML = `${esc(base)} <span id="next-refresh-eta"></span>`;

    state.rows = data.rows || [];
    state.rowsNorm = normalizeRows(state.rows);

    // Build table structure only once per fetch
    buildTable();
    state.rendered = true;

    // Then write times according to current clock mode
    repaintTimesOnly();
  }

  function buildTable() {
    const tbody = qs('#pairings-body');
    const parts = [];

    for (const row of state.rowsNorm) {
      if (row.kind === 'off') {
        parts.push(`
          <tr class="off">
            <td class="ck"></td>
            <td class="sum-first"><span class="off-label">OFF</span></td>
            <td class="muted"></td>
            <td class="off-dur">${esc(row.display?.off_dur || '')}</td>
            <td class="muted"></td>
          </tr>
        `);
        continue;
      }

      const daysCount = row.days ? row.days.length : 0;
      const inProg = row.in_progress ? `<span class="progress">(In progress)</span>` : '';

      // out-of-base pill
      const startDep = firstDepartureAirport(row);
      const oobPill = (startDep && startDep !== HOME_BASE) ? `<span class="pill pill-red">${esc(startDep)}</span>` : '';

      // summary: report/release spans with data-min for live toggle
      const repSpan = timeSpanHTML(row._reportMin, row.display?.report_str || row.report || '');
      const relSpan = timeSpanHTML(row._releaseMin, row.display?.release_str || row.release || '');

      parts.push(`
        <tr class="summary" data-key="${esc(row.pairing_id || '')}">
          ${renderCheckCell(row)}
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
            <div class="daysbox">
              ${renderDaysHTML(row.days || [])}
            </div>
          </td>
        </tr>
      `);
    }

    tbody.innerHTML = parts.join('');
  }

  function renderDaysHTML(days) {
    return days.map((day, i) => {
      const rep = timeSpanHTML(day._reportMin, day.report || '');
      const rel = timeSpanHTML(day._releaseMin, day.release || '');
      const legs = (day.legs || []).map(leg => {
        const dep = timeSpanHTML(leg._depMin,  leg.dep_time_str || leg.dep || '');
        const arr = timeSpanHTML(leg._arrMin,  leg.arr_time_str || leg.arr || '');
        const route = `${esc(leg.dep || '')}–${esc(leg.arr || '')}`;
        return `
          <tr class="leg-row ${leg.done ? 'leg-done' : ''}">
            <td>${esc(leg.flight || '')}</td>
            <td>${route}</td>
            <td>${dep}&nbsp;→&nbsp;${arr}</td>
          </tr>
        `;
      }).join('');

      return `
        <div class="day">
          <div class="dayhdr">
            <span class="dot"></span>
            <span class="daytitle">Day ${i + 1}</span>
            ${day.report ? `· Report ${rep}` : ''}
            ${day.release ? `· Release ${rel}` : ''}
            ${day.hotel ? `· ${esc(day.hotel)}` : ''}
            <span class="helper">click to show legs</span>
          </div>
          ${legs
            ? `<div class="legs-wrap">
                 <table class="legs" style="display:none">
                   <thead><tr><th>Flight</th><th>Route</th><th>Block Times</th></tr></thead>
                   <tbody>${legs}</tbody>
                 </table>
               </div>`
            : `<div class="muted subnote">No legs parsed.</div>`
          }
        </div>
      `;
    }).join('');
  }

  // a single <span> that can be reformatted in-place later
  function timeSpanHTML(minOrNull, fallback) {
    const hasMin = minOrNull != null && !isNaN(minOrNull);
    const initText = hasMin ? fmtMin(minOrNull, state.clockMode === 24) : (fallback || '');
    return `<span class="js-time" ${hasMin ? `data-min="${minOrNull}"` : ''}>${esc(initText)}</span>`;
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

  function firstDepartureAirport(row) {
    const days = row?.days || [];
    for (const d of days) {
      const legs = d?.legs || [];
      if (legs.length && legs[0].dep) return String(legs[0].dep).toUpperCase();
    }
    return null;
  }

  // ============== Repaint only times (no layout change) ==============
  function repaintTimesOnly() {
    const is24 = state.clockMode === 24;
    // Summary + details + legs (anything with .js-time and data-min)
    document.querySelectorAll('.js-time').forEach(el => {
      const v = el.getAttribute('data-min');
      if (v == null || v === '') return; // no parsed minutes; leave fallback text
      const min = parseInt(v, 10);
      el.textContent = fmtMin(min, is24);
    });

    // Also refresh Plan modal times if open (they are rebuilt on open, so skip here)
  }

  // ============== Plan modal ==============
  function fmtModalTime(d, is24) {
    if (is24) return `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
    const h = d.getHours(), mm = String(d.getMinutes()).padStart(2,'0');
    const ap = h < 12 ? 'AM' : 'PM';
    return `${(h % 12) || 12}:${mm} ${ap}`;
  }
  const fmtDate = (d) => `${d.getMonth() + 1}/${d.getDate()}`;

  async function openPlan(pairingId, reportIso) {
    try {
      const res = await fetch(`${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairingId)}&report_local_iso=${encodeURIComponent(reportIso)}`);
      const data = await res.json();
      const use24 = state.clockMode === 24;

      let lastDateKey = null;
      const rows = (data.attempts || []).map(a => {
        const d = new Date(a.at_iso);
        const t = fmtModalTime(d, use24);
        const date = fmtDate(d);
        const showDate = date !== lastDateKey;
        if (showDate) lastDateKey = date;

        const whenHTML = showDate
          ? `<span class="plan-when"><span class="plan-time">${esc(t)}</span><span class="plan-date"> ${esc(date)}</span></span>`
          : `<span class="plan-when"><span class="plan-time">${esc(t)}</span></span>`;

        const type = (a.kind || '').toUpperCase();
        const det  = a.label || '';
        return `<tr><td>${whenHTML}</td><td>${esc(type)}</td><td>${esc(det)}</td></tr>`;
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

  // ============== Status line ==============
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
      repaintTimesOnly(); // keep UI fresh even if we didn't fetch yet
    }
  }

  // ============== utils ==============
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
