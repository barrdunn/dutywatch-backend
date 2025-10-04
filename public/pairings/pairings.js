// public/pairings/pairings.js
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
    clockMode: cfg.clockMode === '24' ? 24 : 12,   // default 12h
    onlyReports: cfg.onlyReports !== false,
    _zeroKick: 0,
  };

  // ====== TIME/DATE FORMAT HELPERS (no column reflow) ======
  const TIME_12_RE = /\b([0-9]{1,2}):([0-9]{2})\s?(AM|PM)\b/i;
  const TIME_24_RE = /\b([01]?\d|2[0-3]):?([0-5]\d)\b/;

  function to12(h, m) {
    const hh = (h % 12) || 12;
    const ampm = h < 12 ? 'AM' : 'PM';
    return `${hh}:${m.toString().padStart(2,'0')} ${ampm}`;
  }
  function to24(h, m, withColon) {
    const hh = h.toString().padStart(2,'0');
    const mm = m.toString().padStart(2,'0');
    return withColon ? `${hh}:${mm}` : `${hh}${mm}`;
  }

  // ✅ robust raw HHMM selector — prefers numeric sources, never formatted strings
  function pickHHMM(...candidates) {
    for (const c of candidates) {
      if (c == null) continue;
      const s = String(c).trim();

      // 1) pure 4-digit HHMM (e.g., "1513")
      if (/^\d{4}$/.test(s)) return s;

      // 2) "HH:MM"
      const mColon = s.match(/^(\d{1,2}):(\d{2})$/);
      if (mColon) {
        const h = mColon[1].padStart(2,'0');
        const m = mColon[2];
        return `${h}${m}`;
      }

      // 3) "H:MM AM/PM"
      const m12 = s.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
      if (m12) {
        let h = parseInt(m12[1],10) % 12;
        const m = parseInt(m12[2],10);
        if (m12[3].toUpperCase() === 'PM') h += 12;
        return `${h.toString().padStart(2,'0')}${m.toString().padStart(2,'0')}`;
      }
    }
    return '';
  }

  // Replace the FIRST time in str with desired format; keep weekday+date prefix intact.
  function swapFirstTime(str, want24, colon24=true) {
    if (!str) return str;

    const m12 = str.match(TIME_12_RE);
    if (m12) {
      const h = parseInt(m12[1],10), m = parseInt(m12[2],10);
      const h24 = (m12[3].toUpperCase()==='PM' ? (h%12)+12 : (h%12));
      const repl = want24 ? to24(h24,m, colon24) : `${h}:${m.toString().padStart(2,'0')} ${m12[3].toUpperCase()}`;
      return str.replace(TIME_12_RE, repl);
    }

    const m24 = str.match(TIME_24_RE);
    if (m24) {
      const h = parseInt(m24[1],10), m = parseInt(m24[2],10);
      const repl = want24 ? to24(h,m, colon24) : to12(h,m);
      return str.replace(TIME_24_RE, repl);
    }

    return str;
  }

  // Format a pure HHMM like "0715" -> desired string
  function fmtHHMM(hhmm, want24, colon24=true) {
    if (!hhmm) return '';
    const s = String(hhmm).replace(':','').padStart(4,'0');
    const hh = parseInt(s.slice(0,2),10), mm = parseInt(s.slice(2),10);
    return want24 ? to24(hh,mm, colon24) : to12(hh,mm);
  }

  // ====== Public action ======
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
      state.clockMode = parseInt(clockSel.value, 10) === 24 ? 24 : 12;
      // reformat IN PLACE without changing layout / open rows
      repaintTimesOnly();
    });
  }

  // First paint + live updates
  renderOnce();
  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
    es.addEventListener('hidden_update', async () => { await renderOnce(); }); /* ADDED */
  } catch {}

  setInterval(tickStatusLine, 1000);

  // Clicks
  document.addEventListener('click', async (e) => {
    // expand/collapse
    const sum = e.target.closest('tr.summary');
    if (sum && !e.target.closest('[data-ck]')) {
      sum.classList.toggle('open');
      const details = sum.nextElementSibling;
      if (!details || !details.classList.contains('details')) return;
      const open = sum.classList.contains('open');

      // If this is a zero-legs row, details body is just the hide panel; nothing to toggle per-day.
      const zeroPanel = details.querySelector('[data-zero-legs-panel]');
      if (!zeroPanel) {
        details.querySelectorAll('.day .legs').forEach(tbl => { tbl.style.display = open ? 'table' : 'none'; });
        details.querySelectorAll('.day .helper').forEach(h => { h.textContent = open ? 'click to hide legs' : 'click to show legs'; });
      }
      return;
    }

    // day header own toggle
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

    // checkbox behavior
    const ckBtn = e.target.closest('[data-ck]');
    if (ckBtn) {
      const stateAttr = ckBtn.getAttribute('data-ck');
      const pairingId = ckBtn.getAttribute('data-pairing') || '';
      const reportIso = ckBtn.getAttribute('data-report') || '';

      if (stateAttr === 'off') { await openPlan(pairingId, reportIso); return; }
      if (stateAttr === 'pending') {
        try {
          await fetch(`${apiBase}/api/ack/acknowledge`, {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ pairing_id: pairingId, report_local_iso: reportIso }),
          });
        } catch (err) { console.error('ack failed', err); }
        await renderOnce();
      }
      return;
    }

    // plan modal close
    if (e.target.id === 'plan-close-1' || e.target.id === 'plan-close-2' || e.target.classList.contains('modal')) {
      showModal(false);
      return;
    }

    // ADDED: Hide button inside zero-legs panel
    const hideBtn = e.target.closest('[data-action="hide-row"]'); /* ADDED */
    if (hideBtn) { /* ADDED */
      try { /* ADDED */
        const rowJson = hideBtn.getAttribute('data-row') || '{}'; /* ADDED */
        const row = JSON.parse(rowJson); /* ADDED */
        const payload = { uid: row.uid || '', row }; /* ADDED */
        await fetch(`${apiBase}/api/hide`, { /* ADDED */
          method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) /* ADDED */
        }); /* ADDED */
      } catch (err) { console.error('hide failed', err); } /* ADDED */
      await renderOnce(); /* ADDED */
      return; /* ADDED */
    } /* ADDED */

    // ADDED: Unhide all chip button
    if (e.target && e.target.id === 'unhide-all') { /* ADDED */
      try { await fetch(`${apiBase}/api/unhide_all`, { method:'POST' }); } catch (e) { console.error(e); } /* ADDED */
      await renderOnce(); /* ADDED */
      return; /* ADDED */
    } /* ADDED */
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') showModal(false);
  });

  // ====== Render from API ======
  async function renderOnce() {
    const params = new URLSearchParams({
      is_24h: '0',                      // server can send raw; we’ll format on client
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

    // ADDED: hidden chip
    const hChip = qs('#hidden-chip'); /* ADDED */
    const hCountEl = qs('#hidden-count'); /* ADDED */
    if (hCountEl && Number.isFinite(data.hidden_count)) { hCountEl.textContent = String(data.hidden_count); } /* ADDED */
    if (hChip) hChip.classList.toggle('hidden', !(data.hidden_count > 0)); /* ADDED */

    if (refreshSel && data.refresh_minutes) refreshSel.value = String(data.refresh_minutes);

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

    // format times to current mode after HTML exists
    repaintTimesOnly();
  }

  // ====== Repaint any time strings in-place (no layout or open/close changes) ======
  function repaintTimesOnly() {
    const want24 = state.clockMode === 24;

    // main Report/Release cells (preserve weekday+date)
    document.querySelectorAll('[data-dw="report"]').forEach(el => {
      const orig = el.getAttribute('data-orig') || el.textContent;
      if (!el.hasAttribute('data-orig')) el.setAttribute('data-orig', orig);
      el.textContent = swapFirstTime(orig, want24, /*colon24*/false);   // 24h no colon as requested
    });
    document.querySelectorAll('[data-dw="release"]').forEach(el => {
      const orig = el.getAttribute('data-orig') || el.textContent;
      if (!el.hasAttribute('data-orig')) el.setAttribute('data-orig', orig);
      el.textContent = swapFirstTime(orig, want24, /*colon24*/false);
    });

    // day headers “Report XX:YY / Release …”
    document.querySelectorAll('[data-dw="day-report"]').forEach(el => {
      const raw = el.getAttribute('data-hhmm') || el.textContent;
      const text = fmtHHMM(raw, want24, /*colon24*/false);
      el.textContent = text;
    });
    document.querySelectorAll('[data-dw="day-release"]').forEach(el => {
      const raw = el.getAttribute('data-hhmm') || el.textContent;
      const text = fmtHHMM(raw, want24, /*colon24*/false);
      el.textContent = text;
    });

    // block times in legs table — use stored raw HHMMs
    document.querySelectorAll('[data-dw="bt"]').forEach(el => {
      const dep = el.getAttribute('data-dep') || '';
      const arr = el.getAttribute('data-arr') || '';
      const left  = fmtHHMM(dep, want24, /*colon24*/false);
      const right = fmtHHMM(arr, want24, /*colon24*/false);
      el.textContent = arr ? `${left} → ${right}` : left;
    });
  }

  // ====== Helpers ======
  function firstDepartureAirport(row) {
    const days = row?.days || [];
    for (const d of days) {
      const legs = d?.legs || [];
      if (legs.length && legs[0].dep) return String(legs[0].dep).toUpperCase();
    }
    return null;
  }

  function totalLegs(row){ /* ADDED */
    let n = 0;
    for (const d of (row?.days||[])) n += (d?.legs||[]).length;
    return n;
  } /* ADDED */

  function plural(n, one, many){ return n === 1 ? one : many; } /* ADDED */

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
    const legsCount = totalLegs(row); /* ADDED */
    const inProg = row.in_progress ? `<span class="progress">(In progress)</span>` : '';

    const startDep = firstDepartureAirport(row);
    const showOOB = !!(startDep && startDep !== homeBase);
    const oobPill = showOOB ? `<span class="pill pill-red">${esc(startDep)}</span>` : '';

    const checkCell = renderCheckCell(row);

    // Preserve server-provided date+time string, but mark for client-time swapping
    const repStr = esc(row.display?.report_str || '');
    const relStr = esc(row.display?.release_str || '');

    // CHANGED: show day pill only if > 0, and pluralize
    const dayPill = daysCount > 0
      ? `<span class="pill">${daysCount} ${plural(daysCount,'day','days')}</span>` /* ADDED */
      : ''; /* ADDED */

    // Details area:
    const details = (legsCount === 0) /* ADDED: Zero-flight “hide” panel */
      ? `
        <div class="daysbox" data-zero-legs-panel>
          <div class="muted" style="margin-bottom:8px;">No flights found for this item.</div>
          <button class="btn" type="button"
                  data-action="hide-row"
                  data-row="${esc(JSON.stringify(row))}">
            Hide this item
          </button>
        </div>`
      : (row.days || []).map((day, i) => renderDayHTML(day, i)).join('');

    // CHANGED: tweak details hint
    const hint = (legsCount === 0) ? 'click to hide' : 'click to expand days'; /* ADDED */

    return `
      <tr class="summary">
        ${checkCell}
        <td class="sum-first">
          <strong>${esc(row.pairing_id || '')}</strong>
          ${dayPill}  <!-- CHANGED -->
          ${oobPill}
          ${inProg}
        </td>
        <td data-dw="report">${repStr}</td>
        <td data-dw="release">${relStr}</td>
        <td class="muted">${esc(hint)}</td> <!-- CHANGED -->
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
    const want24 = state.clockMode === 24; // initial paint uses current mode for leg attrs

    const legs = (day.legs || []).map(leg => {
      // ✅ Use raw HHMM sources; never parse *_time_str (preformatted)
      const depHHMM = pickHHMM(leg.dep_hhmm, leg.dep_time);
      const arrHHMM = pickHHMM(leg.arr_hhmm, leg.arr_time);

      const left  = fmtHHMM(depHHMM, want24, /*colon24*/false);
      const right = fmtHHMM(arrHHMM, want24, /*colon24*/false);

      return `
        <tr class="leg-row ${leg.done ? 'leg-done' : ''}">
          <td>${esc(leg.flight || '')}</td>
          <td>${esc(leg.dep || '')}–${esc(leg.arr || '')}</td>
          <td class="bt" data-dw="bt" data-dep="${esc(depHHMM)}" data-arr="${esc(arrHHMM)}">${left} → ${right}</td>
        </tr>`;
    }).join('');

    // Day header report/release — keep raw HHMM in data-hhmm for repaint
    const dayRepRaw = pickHHMM(day.report_hhmm, day.report);
    const dayRelRaw = pickHHMM(day.release_hhmm, day.release);
    const repDisp = fmtHHMM(dayRepRaw, want24, /*colon24*/false);
    const relDisp = fmtHHMM(dayRelRaw, want24, /*colon24*/false);

    return `
      <div class="day">
        <div class="dayhdr">
          <span class="dot"></span>
          <span class="daytitle">Day ${idx + 1}</span>
          ${dayRepRaw ? `· Report <span data-dw="day-report" data-hhmm="${esc(dayRepRaw)}">${esc(repDisp)}</span>` : ''}
          ${dayRelRaw ? ` · Release <span data-dw="day-release" data-hhmm="${esc(dayRelRaw)}">${esc(relDisp)}</span>` : ''}
          ${day.hotel ? ` · ${esc(day.hotel)}` : ''}
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

  // ====== Plan modal (unchanged except table is equal thirds) ======
  async function openPlan(pairingId, reportIso) {
    try {
      const res = await fetch(`${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairingId)}&report_local_iso=${encodeURIComponent(reportIso)}`);
      const data = await res.json();

      // build rows; When cell will be repainted by repaintTimesOnly via data attrs
      const rows = (data.attempts||[]).map(a => {
        const d = new Date(a.at_iso);
        const hh = d.getHours(), mm = d.getMinutes();
        const hhmm = `${hh.toString().padStart(2,'0')}${mm.toString().padStart(2,'0')}`;
        const want24 = state.clockMode === 24;
        const timeText = fmtHHMM(hhmm, want24, /*colon24*/false);
        const dateText = `${d.toLocaleDateString(undefined,{month:'short'})} ${d.getDate()}`;
        return `<tr>
          <td><span data-dw="bt" data-dep="${esc(hhmm)}" data-arr="">${esc(timeText)}</span> ${esc(dateText)}</td>
          <td>${esc((a.kind||'').toUpperCase())}</td>
          <td>${esc(a.label||'')}</td>
        </tr>`;
      }).join('');

      qs('#plan-rows').innerHTML = rows || `<tr><td colspan="3" class="muted">No upcoming attempts.</td></tr>`;
      qs('#plan-meta').textContent =
        `Calls from T-${data.policy.call_start_hours}h every ${data.policy.call_interval_minutes}m (2 rings/attempt)`; /* CHANGED: shortened to match current server policy keys */

      showModal(true);
      repaintTimesOnly();
    } catch (e) { console.error(e); }
  }

  function showModal(show) {
    const m = qs('#plan-modal'); if (!m) return;
    m.classList.toggle('hidden', !show);
    document.body.classList.toggle('modal-open', show);
  }

  // status line
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
  function qs(sel){return document.querySelector(sel)}
  function setText(sel,v){const el=qs(sel); if(el) el.textContent=v;}
  function esc(s){return String(s).replace(/[&<>"'`=\/]/g,(ch)=>({'&':'&amp;','<':'&#x3E;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'}[ch]))}
  function preciseAgo(d){const sec=Math.max(0,Math.floor((Date.now()-d.getTime())/1000));const m=Math.floor(sec/60),s=sec%60;return m?`${m}m ${s}s ago`:`${s}s ago`}
  function safeParseJSON(s){try{return JSON.parse(s||'{}')}catch{return null}}
})();
