(function () {
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase = cfg.apiBase || '';

  window.dwManualRefresh = async function () {
    try { await fetch(apiBase + '/api/refresh', { method: 'POST' }); }
    catch (e) { console.error(e); }
  };

  const refreshSel = document.getElementById('refresh-mins');
  if (refreshSel) {
    refreshSel.value = String(cfg.refreshMinutes || 5);
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

  // First paint
  renderOnce();

  // Live updates via SSE; fallback to timer if SSE fails
  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('hello', () => {/* initial */});
    es.addEventListener('change', async () => { await renderOnce(); });
    es.onerror = () => { /* will rely on fallback below */ };
  } catch {
    setInterval(renderOnce, (cfg.refreshMinutes || 5) * 60 * 1000);
  }

  async function renderOnce() {
    let data;
    try {
      const res = await fetch(apiBase + '/api/pairings', { cache: 'no-store' });
      data = await res.json();
    } catch (e) {
      console.error('Failed to fetch /api/pairings', e);
      return;
    }
    setText('#looking-through', data.looking_through ?? '—');
    setText('#last-pull', data.last_pull_local ?? '—');
    setText('#next-refresh', data.next_pull_local && data.tz_label
      ? `${data.next_pull_local} (${data.tz_label})` : '—');

    const tbody = qs('#pairings-body');
    tbody.innerHTML = (data.rows || []).map(renderRowHTML).join('');
    // TODO: attach expand/collapse if you want
  }

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
  function qs(sel) { return document.querySelector(sel); }
  function setText(sel, v) { const el = qs(sel); if (el) el.textContent = v; }
  function esc(s) {
    return String(s).replace(/[&<>"'`=\/]/g, (ch) =>
      ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'}[ch])
    );
  }
  function safeParseJSON(s) { try { return JSON.parse(s || '{}'); } catch { return null; } }
})();
