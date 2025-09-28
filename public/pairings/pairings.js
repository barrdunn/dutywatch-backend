(function () {
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase = cfg.apiBase || '';
  const HOME_BASE = (cfg.baseAirport || 'DFW').toUpperCase();

  // iOS hint (kept from before)
  const IS_IOS = /iP(ad|hone|od)/.test(navigator.platform)
    || (navigator.userAgent.includes('Mac') && 'ontouchend' in document);
  document.documentElement.classList.toggle('ios', IS_IOS);

  const state = {
    lastPullIso: null,
    nextRefreshIso: null,
    clockMode: 12,                    // default to 12h
    onlyReports: cfg.onlyReports !== false,
    _zeroKick: 0,
    rows: [],                         // raw from server
    rowsNorm: [],                     // normalized (times parsed once)
  };

  // If boot config requests 24, honor it
  if (cfg.clockMode === '24') state.clockMode = 24;

  // ===== Time parsing/formatting (client-only) =====
  // minutes 0..1439 -> string
  function fmtMin(min, is24) {
    if (min == null || isNaN(min)) return '';
    let h = Math.floor(min / 60), m = min % 60;
    if (is24) return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
    const ampm = h < 12 ? 'AM' : 'PM';
    const hh = (h % 12) || 12;
    return `${hh}:${String(m).padStart(2, '0')} ${ampm}`;
  }

  // Accept: "1730", "730", "17:30", "7:30 PM", "07:30PM", optional tz tail (ignored)
  function parseToMin(any) {
    if (any == null) return null;
    let s = String(any).trim();
    if (!s) return null;

    // Strip trailing timezone labels, keep AM/PM
    // e.g. "7:30 PM CT" -> "7:30 PM"
    s = s.replace(/\s([A-Z]{1,4})$/, (m, tz) => (/AM|PM/i.test(tz) ? ` ${tz}` : ''));

    // "HHMM" or "HMM"
    if (/^\d{3,4}$/.test(s)) {
      const mm = parseInt(s.slice(-2), 10);
      const hh = parseInt(s.slice(0, s.length - 2), 10);
      if (mm >= 0 && mm < 60 && hh >= 0 && hh < 24) return hh * 60 + mm;
    }

    // "H:MM" or "HH:MM" with optional AM/PM
    const m = s.match(/^(\d{1,2}):(\d{2})\s*([AP]M)?$/i);
    if (m) {
      let hh = parseInt(m[1], 10);
      const mm = parseInt(m[2], 10);
      const ampm = (m[3] || '').toUpperCase();
      if (mm >= 0 && mm < 60 && hh >= 0 && hh <= 23) {
        if (ampm) {
          // 12h -> 24h
          if (hh === 12) hh = 0;
          if (ampm === 'PM') hh += 12;
        }
        return hh * 60 + mm;
      }
    }

    // "H:MMam" / "h:mmPM"
    const m2 = s.match(/^(\d{1,2}):(\d{2})\s*([ap]m)$/i);
    if (m2) {
      let hh = parseInt(m2[1], 10);
      const mm = parseInt(m2[2], 10);
      const ampm = m2[3].toUpperCase();
      if (hh === 12) hh = 0;
      if (ampm === 'PM') hh += 12;
      return hh * 60 + mm;
    }

    return null; // unparseable -> UI will show original string if we ever need it
  }

  // Prefer raw HHMM-like fields; else parse any stringy display we got
  function chooseTimeToMin(obj, bases) {
    for (const b of bases) {
      const v = obj?.[b];
      if (v == null) continue;
      const min = parseToMin(v);
      if (min != null) return min;
    }
    return null;
  }

  // Normalize all times in rows once (we mutate a copy so originals stay intact)
  function normalizeRows(rows) {
    const out = [];
    for (const r of (rows || [])) {
      if (r.kind === 'off') { out.push(r); continue; }

      const disp = r.display || {};
      const reportMin = chooseTimeToMin(disp, ['report_hhmm','report','report_str']) ??
                        chooseTimeToMin(r,    ['report_hhmm','report','report_str']);
      const releaseMin = chooseTimeToMin(disp, ['release_hhmm','release','release_str']) ??
                         chooseTimeToMin(r,    ['release_hhmm','release','release_str']);

      const days = (r.days || []).map(d => {
        const legs = (d.legs || []).map(leg => ({
          ...leg,
          _depMin: chooseTimeToMin(leg, ['dep_hhmm','dep_time','dep_time_str','dep']),
          _arrMin: chooseTimeToMin(leg, ['arr_hhmm','arr_time','arr_time_str','arr']),
        }));
        return { ...d, legs };
      });

      out.push({ ...r, _reportMin: reportMin, _releaseMin: releaseMin, days });
    }
    return out;
  }

  // ===== Public action =====
  window.dwManualRefresh = async function () {
    try { await fetch(apiBase + '/api/refresh', { method: 'POST' }); }
    catch (e) { console.error(e); }
  };

  // ===== Controls =====
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
      repaintTablePreservingOpen();   // no fetch, no collapsing summaries
    });
  }

  // ===== First paint & live updates =====
  renderOnce();

  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('hello', () => {});
    es.addEventListener('change', async () => { await renderOnce(); });
    es.addEventListener('schedule_update', async () => { await renderOnce(); });
  } catch {}

  // ===== Status ticker =====
  setInterval(tickStatusLine, 1000);

  // ===== Global clicks =====
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

    // Check-in checkbox behavior (unchanged)
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

    // Modal close
    if (e.target.id === 'plan-close-1' || e.target.id === 'plan-close-2' || e.target.classList.contains('modal')) {
      showModal(false);
      return;
    }
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') showModal(false);
  });

  // ===== Fetch & normalize on server change =====
  async function renderOnce() {
    const params = new URLSearchParams({
      is_24h: 0, // backend can ignore; we control formatting on client
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

    // Header text
    const label = (data.window && data.window.label) || data.looking_through || '—';
    setText('#looking-through', label);
    setText('#last-pull', data.last_pull_local ?? '—');

    const base = (data.next_pull_local && data.tz_label)
      ? `${data.next_pull_local} (${data.tz_label})`
      : '—';
    const nextEl = qs('#next-refresh');
    if (nextEl) nextEl.innerHTML = `${esc(base)} <span id="next-refresh-eta"></span>`;

    // Cache + normalize
    state.rows = data.rows || [];
    state.rowsNorm = normalizeRows(state.rows);

    repaintTablePreservingOpen();
  }

  // ===== Render helpers =====
  function repaintTablePreservingOpen() {
    const tbody = qs('#pairings-body');
    // remember open summaries by pairing_id
    const openKeys = Array.from(tbody.querySelectorAll('tr.summary.open'))
      .map(tr => tr.dataset.key)
      .filter(Boolean);

    tbody.innerHTML = (state.rowsNorm || []).map(row => renderRowHTML(row, HOME_BASE)).join('');

    // restore open state
    openKeys.forEach(key => {
      const tr = tbody.querySelector(`tr.summary[data-key="${cssEsc(key)}"]`);
      if (!tr) return;
      tr.classList.add('open');
      const details = tr.nextElementSibling;
      if (details && details.classList.contains('details')) {
        details.querySelectorAll('.day .legs').forEach(tbl => tbl.style.display = 'table');
        details.querySelectorAll('.day .helper').forEach(h => h.textContent = 'click to hide legs');
      }
    });
  }

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

    // Always format from normalized minutes so toggle affects UI
    const is24 = state.clockMode === 24;
    const reportStr  = fmtMin(row._reportMin,  is24) || '';
    const releaseStr = fmtMin(row._releaseMin, is24) || '';

    const checkCell = renderCheckCell(row);
    const details = (row.days || []).map((day, i) => renderDayHTML(day, i, is24)).join('');

    const key = String(row.pairing_id || '');

    return `
      <tr class="summary" data-key="${esc(key)}">
        ${checkCell}
        <td class="sum-first">
          <strong>${esc(row.pairing_id || '')}</strong>
          <span class="pill">${daysCount} day</span>
          ${oobPill}
          ${inProg}
        </td>
        <td>${esc(reportStr)}</td>
        <td>${esc(releaseStr)}</td>
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

  function renderDayHTML(day, idx, is24) {
    const legs = (day.legs || []).map(leg => {
      const depStr = fmtMin(leg._depMin, is24) || esc(leg.dep_time_str || leg.dep || '');
      const arrStr = fmtMin(leg._arrMin, is24) || esc(leg.arr_time_str || leg.arr || '');
      const route = `${esc(leg.dep || '')}–${esc(leg.arr || '')}`;
      const times = `${depStr}&nbsp;→&nbsp;${arrStr}`;
      return `
        <tr class="leg-row ${leg.done ? 'leg-done' : ''}">
          <td>${esc(leg.flight || '')}</td>
          <td>${route}</td>
          <td>${times}</td>
        </tr>`;
    }).join('');

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

  // ===== Plan modal (still client-formatted from ISO) =====
  function fmtModalTime(d, is24) {
    if (is24) {
      return `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
    }
    const h = d.getHours();
    const hh = (h % 12) || 12;
    const mm = String(d.getMinutes()).padStart(2,'0');
    const ampm = h < 12 ? 'AM' : 'PM';
    return `${hh}:${mm} ${ampm}`;
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
        const timeStr = fmtModalTime(d, use24);
        const dateStr = fmtDate(d);
        const showDate = dateStr !== lastDateKey;
        if (showDate) lastDateKey = dateStr;

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

  // ===== Status line tick =====
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

  // ===== utils =====
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
  function cssEsc(s){ return String(s).replace(/"/g,'\\"'); }
})();
