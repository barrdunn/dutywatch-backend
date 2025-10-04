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
    onlyReports: false, // include non-pairings like N/L
    hiddenCount: 0,
    _zeroKick: 0,
  };

  // ====== TIME HELPERS ======
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
  function pickHHMM(...candidates) {
    for (const c of candidates) {
      if (c == null) continue;
      const s = String(c).trim();
      if (/^\d{4}$/.test(s)) return s;
      const mColon = s.match(/^(\d{1,2}):(\d{2})$/);
      if (mColon) return `${mColon[1].padStart(2,'0')}${mColon[2]}`;
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
  function fmtHHMM(hhmm, want24, colon24=true) {
    if (!hhmm) return '';
    const s = String(hhmm).replace(':','').padStart(4,'0');
    const hh = parseInt(s.slice(0,2),10), mm = parseInt(s.slice(2),10);
    return want24 ? to24(hh,mm, colon24) : to12(hh,mm);
  }

  // ====== ACTIONS ======
  window.dwManualRefresh = async function () {
    try { await fetch(apiBase + '/api/refresh', { method: 'POST' }); }
    catch (e) { console.error(e); }
  };

  // ====== CONTROLS ======
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
      repaintTimesOnly();
    });
  }

  // ====== HIDDEN CHIP ======
  function applyHiddenCount(n) {
    state.hiddenCount = Number(n)||0;
    const chip = qs('#hidden-chip');
    const countEl = qs('#hidden-count');
    if (!chip || !countEl) return;
    countEl.textContent = `Hidden: ${state.hiddenCount}`;
    chip.classList.toggle('hidden', !(state.hiddenCount > 0));
  }

  // ====== LIVE UPDATES ======
  renderOnce();
  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
    es.addEventListener('hidden_update', async () => { await renderOnce(); });
  } catch {}

  setInterval(tickStatusLine, 1000);

  // ====== CLICKS ======
  document.addEventListener('click', async (e) => {
    const unhide = e.target.closest('[data-unhide-all]');
    if (unhide) {
      e.preventDefault();
      try { await fetch(`${apiBase}/api/hidden/unhide_all`, { method:'POST' }); }
      catch (err) { console.error('unhide all failed', err); }
      await renderOnce();
      return;
    }

    // prevent summary toggle from clicks inside the details message
    if (e.target.closest('[data-stop-toggle]')) {
      e.stopPropagation();
    }

    const sum = e.target.closest('tr.summary');
    if (sum && !e.target.closest('[data-ck]')) {
      sum.classList.toggle('open');
      const details = sum.nextElementSibling;
      if (!details || !details.classList.contains('details')) return;
      const open = sum.classList.contains('open');
      details.querySelectorAll('.day .legs').forEach(tbl => { tbl.style.display = open ? 'table' : 'none'; });
      details.querySelectorAll('.day .helper').forEach(h => { h.textContent = open ? 'click to hide legs' : 'click to show legs'; });
      return;
    }

    // Hide button for “no legs” events
    const hideBtn = e.target.closest('[data-hide-pairing]');
    if (hideBtn) {
      e.preventDefault();
      e.stopPropagation();
      const pairingId = hideBtn.getAttribute('data-hide-pairing') || '';
      const reportIso = hideBtn.getAttribute('data-report') || '';

      // optimistic UI remove
      const details = hideBtn.closest('tr.details');
      const summary = details?.previousElementSibling;
      if (summary?.classList.contains('summary')) {
        summary.remove();
        details.remove();
        applyHiddenCount(state.hiddenCount + 1);
      }

      try {
        await fetch(`${apiBase}/api/hidden/hide`, {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ pairing_id: pairingId, report_local_iso: reportIso }),
        });
      } catch (err) {
        console.error('hide failed', err);
      }
      // pull fresh list/count
      await renderOnce();
      return;
    }
  });

  // ====== RENDER ======
  async function renderOnce() {
    const params = new URLSearchParams({
      is_24h: '0',
      only_reports: '0',
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

    const hintedHidden = (data && (data.hidden_count ?? (data.hidden && data.hidden.count)));
    applyHiddenCount(typeof hintedHidden === 'number' ? hintedHidden : state.hiddenCount);

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

    repaintTimesOnly();
  }

  function repaintTimesOnly() {
    const want24 = state.clockMode === 24;
    document.querySelectorAll('[data-dw="report"]').forEach(el => {
      const orig = el.getAttribute('data-orig') || el.textContent;
      if (!el.hasAttribute('data-orig')) el.setAttribute('data-orig', orig);
      el.textContent = swapFirstTime(orig, want24, false);
    });
    document.querySelectorAll('[data-dw="release"]').forEach(el => {
      const orig = el.getAttribute('data-orig') || el.textContent;
      if (!el.hasAttribute('data-orig')) el.setAttribute('data-orig', orig);
      el.textContent = swapFirstTime(orig, want24, false);
    });
    document.querySelectorAll('[data-dw="day-report"]').forEach(el => {
      const raw = el.getAttribute('data-hhmm') || el.textContent;
      el.textContent = fmtHHMM(raw, want24, false);
    });
    document.querySelectorAll('[data-dw="day-release"]').forEach(el => {
      const raw = el.getAttribute('data-hhmm') || el.textContent;
      el.textContent = fmtHHMM(raw, want24, false);
    });
    document.querySelectorAll('[data-dw="bt"]').forEach(el => {
      const dep = el.getAttribute('data-dep') || '';
      const arr = el.getAttribute('data-arr') || '';
      const left  = fmtHHMM(dep, want24, false);
      const right = fmtHHMM(arr, want24, false);
      el.textContent = arr ? `${left} → ${right}` : left;
    });
  }

  // ====== TEMPLATES ======
  function firstDepartureAirport(row) {
    const days = row?.days || [];
    for (const d of days) {
      const legs = d?.legs || [];
      if (legs.length && legs[0].dep) return String(legs[0].dep).toUpperCase();
    }
    return null;
  }
  function wrapNotBoldBits(text, token) {
    if (!text) return '';
    const re = new RegExp(`\\s*\\(${token}\\)`,`i`);
    if (re.test(text)) {
      const base = text.replace(re, '').trim();
      return `${esc(base)} <span class="fw-normal" style="font-weight:400">(${token})</span>`;
    }
    return esc(text);
  }
  function legsCount(row) {
    let n = 0;
    const days = row?.days || [];
    for (const d of days) n += (d.legs || []).length;
    return n;
  }

  function renderRowHTML(row, homeBase) {
    if (row.kind === 'off') {
      const rawLabel = (row.display && row.display.off_label) ? String(row.display.off_label) : 'OFF';
      const labelHTML = wrapNotBoldBits(rawLabel, 'Current');
      const rawDur = String(row.display?.off_dur || '');
      const durHTML = wrapNotBoldBits(rawDur, 'Remaining');
      return `
        <tr class="off">
          <td class="ck"></td>
          <td class="sum-first"><span class="off-label">${labelHTML}</span></td>
          <td class="muted"></td>
          <td class="off-dur">${durHTML}</td>
          <td class="muted"></td>
        </tr>`;
    }

    const totalLegs = legsCount(row);
    const hasLegs = totalLegs > 0;

    const startDep = firstDepartureAirport(row);
    const showOOB = !!(startDep && startDep !== homeBase);
    const oobPill = showOOB ? `<span class="pill pill-red">${esc(startDep)}</span>` : '';

    const detailsDays = hasLegs ? (row.days || []).map((day, i) => renderDayHTML(day, i)).join('') : '';

    // Centered message + Hide button for no-legs rows
    const noLegsBlock = !hasLegs
      ? `<div data-stop-toggle style="display:flex;justify-content:center;align-items:center;gap:12px;padding:18px;text-align:center">
           <span class="muted">No legs found.</span>
           <button class="btn" data-hide-pairing="${esc(row.pairing_id || '')}" data-report="${esc(row.report_local_iso || '')}">
             Hide Event
           </button>
         </div>`
      : '';

    return `
      <tr class="summary" data-row-id="${esc(row.pairing_id || '')}">
        ${renderCheckCell(row)}
        <td class="sum-first">
          <strong>${esc(row.pairing_id || '')}</strong>
          ${hasLegs ? `<span class="pill">${(row.days||[]).length} day</span>` : ``}
          ${oobPill}
        </td>
        <td data-dw="report">${esc(row.display?.report_str || '')}</td>
        <td data-dw="release">${esc(row.display?.release_str || '')}</td>
        <td class="muted">${hasLegs ? 'click to expand days' : 'click to hide'}</td>
      </tr>
      <tr class="details">
        <td colspan="5">
          <div class="daysbox">${hasLegs ? detailsDays : noLegsBlock}</div>
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
    const want24 = state.clockMode === 24;
    const legs = (day.legs || []).map(leg => {
      const depHHMM = pickHHMM(leg.dep_hhmm, leg.dep_time);
      const arrHHMM = pickHHMM(leg.arr_hhmm, leg.arr_time);
      const left  = fmtHHMM(depHHMM, want24, false);
      const right = fmtHHMM(arrHHMM, want24, false);
      return `
        <tr class="leg-row ${leg.done ? 'leg-done' : ''}">
          <td>${esc(leg.flight || '')}</td>
          <td>${esc(leg.dep || '')}–${esc(leg.arr || '')}</td>
          <td class="bt" data-dw="bt" data-dep="${esc(depHHMM)}" data-arr="${esc(arrHHMM)}">${left} → ${right}</td>
        </tr>`;
    }).join('');
    const dayRepRaw = pickHHMM(day.report_hhmm, day.report);
    const dayRelRaw = pickHHMM(day.release_hhmm, day.release);
    const repDisp = fmtHHMM(dayRepRaw, want24, false);
    const relDisp = fmtHHMM(dayRelRaw, want24, false);
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
          </div>` : ``}
      </div>`;
  }

  // ====== STATUS LINE ======
  function tickStatusLine() {
    if (state.lastPullIso) setText('#last-pull', preciseAgo(new Date(state.lastPullIso)));
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

  // ====== UTILS ======
  function qs(sel){return document.querySelector(sel)}
  function setText(sel,v){const el=qs(sel); if(el) el.textContent=v;}
  function esc(s){return String(s).replace(/[&<>"'`=\/]/g,(ch)=>({'&':'&amp;','<':'&lt;','>':'&#x3E;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60','=':'&#x3D;'}[ch]))}
  function preciseAgo(d){const sec=Math.max(0,Math.floor((Date.now()-d.getTime())/1000));const m=Math.floor(sec/60),s=sec%60;return m?`${m}m ${s}s ago`:`${s}s ago`}
  function safeParseJSON(s){try{return JSON.parse(s||'{}')}catch{return null}}
})();
