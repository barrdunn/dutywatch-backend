// === DutyWatch Pairings page script ===
(function () {
  const CT_TZ = 'America/Chicago';

  // Boot params from server (via JSON block)
  const BOOT = (window.DW_BOOT || {});
  const DEFAULT_MINUTES = Number(BOOT.refreshMinutes || 30);
  const ONLY_INIT = (BOOT.onlyReports === true) || (Number(BOOT.onlyReports) === 1);
  const CLOCK_INIT = String(BOOT.clockMode || '12');

  // Elements
  const statusEl = document.getElementById('status-line');
  const lastPullSpan = document.getElementById('last-pull');
  const nextRefreshSpan = document.getElementById('next-refresh');
  const refreshSelect = document.getElementById('refresh-mins');
  const onlyBtn = document.getElementById('toggle-only');
  const onlyState = document.getElementById('toggle-only-state');
  const clkBtn = document.getElementById('toggle-clock');
  const clkState = document.getElementById('toggle-clock-state');

  // ===== Helpers =====
  function fmtTimeCT(d) {
    try {
      return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit', hour12: true, timeZone: CT_TZ });
    } catch {
      return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit', hour12: true });
    }
  }
  function minutesAgo(iso) {
    if (!iso) return null;
    const t = new Date(iso);
    if (isNaN(t.getTime())) return null;
    const now = new Date();
    const ms = now - t;
    return Math.max(0, Math.round(ms / 60000));
  }
  function setQueryParam(url, key, val) {
    const u = new URL(url, window.location.href);
    u.searchParams.set(key, String(val));
    return u.toString();
  }

  // ===== Live "Last pull" =====
  const lastPullISO = statusEl?.dataset?.lastpull || '';
  function tickRelative() {
    if (!lastPullSpan || !lastPullISO) return;
    const mins = minutesAgo(lastPullISO);
    if (mins == null) return;
    lastPullSpan.textContent = mins === 0 ? 'just now' : `${mins} min${mins === 1 ? '' : 's'} ago`;
  }

  // ===== Auto-reload scheduling (UI only) =====
  // The server refreshes itself in the background on the saved cadence.
  // The page just reloads to show the latest data.
  const REF_KEY = 'dw_refresh_minutes';
  function getRefreshMinutes() {
    const saved = Number(localStorage.getItem(REF_KEY)) || 0;
    if (saved >= 1 && saved <= 60) return saved;
    return DEFAULT_MINUTES;
  }
  function setSelectTo(minutes) {
    if (!refreshSelect) return;
    const opts = Array.from(refreshSelect.options).map(o => Number(o.value));
    refreshSelect.value = String(opts.includes(minutes) ? minutes : DEFAULT_MINUTES);
  }

  let loopTimer = null;

  async function scheduleLoop() {
    if (loopTimer) {
      clearInterval(loopTimer);
      loopTimer = null;
    }
    const minutes = getRefreshMinutes();
    localStorage.setItem(REF_KEY, String(minutes));

    // Keep URL param in sync (lets server render consistent UI)
    const withParam = setQueryParam(window.location.href, 'refresh_minutes', minutes);
    if (withParam !== window.location.href) {
      history.replaceState(null, '', withParam);
    }

    // Update "Next refresh" display on the page (client-side hint)
    const now = new Date();
    const nextAt = new Date(now.getTime() + minutes * 60000);
    if (nextRefreshSpan) {
      nextRefreshSpan.textContent = `${fmtTimeCT(nextAt)} (CT)`;
    }

    // Reload the page every N minutes to reflect fresh server data
    loopTimer = setInterval(() => {
      const url = setQueryParam(window.location.href, 'refresh_minutes', minutes);
      window.location.href = url;
    }, minutes * 60000);
  }

  // Initialize select and handler
  if (refreshSelect) {
    setSelectTo(getRefreshMinutes());
    refreshSelect.addEventListener('change', async () => {
      const minutes = Number(refreshSelect.value);
      if (![1, 5, 10, 15, 30].includes(minutes)) return;
      try {
        // Persist server-side schedule
        await fetch('/settings/refresh', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({ minutes: String(minutes) }).toString()
        });
      } catch (_) {}
      localStorage.setItem(REF_KEY, String(minutes));
      scheduleLoop(); // reschedule & update label
    });
  }

  // ===== Table expand/collapse =====
  document.querySelectorAll('tr.summary').forEach(sum => {
    sum.addEventListener('click', () => {
      const idx = sum.getAttribute('data-idx');
      const details = document.querySelector(`tr.details[data-idx="${idx}"]`);
      if (details) details.classList.toggle('hide');
    });
  });

  // ===== Only-with-report filter (presentation-only) =====
  function applyOnlyReportFilter(on) {
    const rows = Array.from(document.querySelectorAll('tbody > tr'));
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (!r.classList.contains('summary')) continue;
      const hasReport = r.getAttribute('data-has-report') === '1';
      const show = !on || hasReport;
      const idx = r.getAttribute('data-idx');
      const detail = document.querySelector(`tr.details[data-idx="${idx}"]`);
      const off = rows[i + 2] && rows[i + 2].classList.contains('off') ? rows[i + 2] : null;
      r.style.display = show ? '' : 'none';
      if (detail) detail.style.display = show ? '' : 'none';
      if (off) off.style.display = show ? '' : 'none';
    }
  }
  applyOnlyReportFilter(ONLY_INIT);
  if (onlyBtn && onlyState) {
    onlyBtn.addEventListener('click', () => {
      const on = onlyBtn.getAttribute('data-on') === '1';
      const next = !on;
      onlyBtn.setAttribute('data-on', next ? '1' : '0');
      onlyState.textContent = next ? 'ON' : 'OFF';
      applyOnlyReportFilter(next);
    });
  }

  // ===== Clock toggle (label only) =====
  if (clkBtn && clkState) {
    clkBtn.addEventListener('click', () => {
      const cur = clkBtn.getAttribute('data-clock') || CLOCK_INIT;
      const next = cur === '12' ? '24' : '12';
      clkBtn.setAttribute('data-clock', next);
      clkState.textContent = next === '24' ? '24h' : '12h';
    });
  }

  // ===== Kick-off =====
  tickRelative();
  setInterval(tickRelative, 15000);
  scheduleLoop();
})();
