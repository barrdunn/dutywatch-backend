(function () {
  const cfg = window.dwConfig || {};
  const apiBase = cfg.apiBase || '';
  const HOME_BASE = (cfg.baseAirport || 'DFW').toUpperCase();

  const IS_IOS = /iP(ad|hone|od)/.test(navigator.platform)
    || (navigator.userAgent.includes('Mac') && 'ontouchend' in document);
  document.documentElement.classList.toggle('ios', IS_IOS);

  const state = {
    lastPullIso: null,
    clockMode: cfg.clockMode === '24' ? 24 : 12,
    onlyReports: !!cfg.onlyReports,
    hiddenCount: 0,
    layoutMode: null,
    isRefreshing: false
  };

  // =========================
  // FullCalendar instances
  // =========================
  let calendarCurrent = null;
  let calendarNext = null;

  function stripYearFromTitle(el) {
    if (!el) return;
    el.textContent = (el.textContent || '').replace(/\s*\b(19|20)\d{2}\b\s*/g, ' ').trim();
  }

  function buildCalendarEvents(rows) {
    const events = [];
    
    if (!rows || !Array.isArray(rows)) return events;
    
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    
    const tripEvents = [];
    
    rows.forEach(row => {
      if (!row || row.kind === 'off' || !row.has_legs) return;
      if (!row.report_local_iso) return;
      
      const reportDate = row.report_local_iso.split('T')[0];
      const hasHotel = row.days && row.days.some(day => day.hotel);
      
      let startDate, endDate;
      
      if (hasHotel) {
        const tripDates = [];
        row.days.forEach(day => {
          const dayDate = day.actual_date || (day.date_local_iso ? day.date_local_iso.split('T')[0] : null);
          if (dayDate && !tripDates.includes(dayDate)) {
            tripDates.push(dayDate);
          }
        });
        
        if (tripDates.length > 0) {
          tripDates.sort();
          startDate = tripDates[0];
          endDate = tripDates[tripDates.length - 1];
        } else {
          startDate = reportDate;
          endDate = reportDate;
        }
      } else {
        startDate = reportDate;
        endDate = reportDate;
      }
      
      const [endYear, endMonth, endDay] = endDate.split('-').map(Number);
      const endDateObj = new Date(endYear, endMonth - 1, endDay);
      endDateObj.setHours(23, 59, 59, 999);
      if (endDateObj < today) return;
      
      tripEvents.push({
        pairingId: row.pairing_id,
        startDate: startDate,
        endDate: endDate
      });
    });
    
    tripEvents.forEach(trip => {
      const [endYear, endMonth, endDay] = trip.endDate.split('-').map(Number);
      const endDateObj = new Date(endYear, endMonth - 1, endDay + 1);
      const endDateExclusive = endDateObj.toISOString().split('T')[0];
      
      const bgColor = 'rgba(151, 245, 167, 0.7)';
      const borderColor = 'rgba(151, 245, 167, 0.7)';
      
      events.push({
        id: trip.pairingId || 'event-' + Math.random(),
        title: trip.pairingId || 'Event',
        start: trip.startDate,
        end: endDateExclusive,
        allDay: true,
        backgroundColor: bgColor,
        borderColor: borderColor,
        textColor: '#ffffff',
        extendedProps: {
          pairingId: trip.pairingId
        }
      });
    });
    
    return events;
  }

  function applyCalendarLayout() {
    // Layout handled by CSS media queries only
  }

  function ensureCalendars() {
    const calendarElCurrent = document.getElementById('calendar-current');
    const calendarElNext = document.getElementById('calendar-next');

    if (calendarElCurrent && !calendarCurrent) {
      const now = new Date();
      const currentDate = new Date(now.getFullYear(), now.getMonth(), 1);
      
      calendarCurrent = new FullCalendar.Calendar(calendarElCurrent, {
        initialView: 'dayGridMonth',
        initialDate: currentDate,
        headerToolbar: { left: '', center: 'title', right: '' },
        titleFormat: { month: 'long' },
        height: 'auto',
        fixedWeekCount: false,
        showNonCurrentDates: false,
        eventDisplay: 'block',
        dayMaxEvents: 3,
        moreLinkClick: 'popover',
        firstDay: 0,
        locale: 'en-US',
        dayHeaderFormat: { weekday: 'narrow' },
        eventClick(info) {
          const pairingId = info.event.extendedProps.pairingId;
          const row = document.querySelector(`[data-row-id="${pairingId}"]`);
          if (row) {
            row.scrollIntoView({ behavior: 'smooth', block: 'center' });
            row.classList.add('highlight');
            setTimeout(() => row.classList.remove('highlight'), 2000);
          }
        },
        eventMouseEnter(info) {
          info.el.style.cursor = 'pointer';
          info.el.setAttribute('title', info.event.extendedProps.pairingId || 'Pairing');
        },
        datesSet(info) {
          setTimeout(() => {
            stripYearFromTitle(calendarElCurrent.querySelector('.fc-toolbar-title'));
            
            const weeks = calendarElCurrent.querySelectorAll('.fc-daygrid-body tbody tr');
            
            if (weeks.length === 6) {
              const today = new Date();
              const currentMonth = info.view.currentStart.getMonth();
              const currentYear = info.view.currentStart.getFullYear();
              
              weeks.forEach(w => w.classList.remove('week-hidden'));
              
              if (today.getMonth() === currentMonth && today.getFullYear() === currentYear) {
                if (today.getDate() <= 2) {
                  const lastWeek = weeks[weeks.length - 1];
                  if (lastWeek) {
                    lastWeek.classList.add('week-hidden');
                  }
                }
              }
            }
            
            applyCalendarLayout();
          }, 0);
        }
      });
      calendarCurrent.render();
      stripYearFromTitle(calendarElCurrent.querySelector('.fc-toolbar-title'));
    }

    if (calendarElNext && !calendarNext) {
      const now = new Date();
      const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1);

      calendarNext = new FullCalendar.Calendar(calendarElNext, {
        initialView: 'dayGridMonth',
        initialDate: nextMonth,
        headerToolbar: { left: '', center: 'title', right: '' },
        titleFormat: { month: 'long' },
        height: 'auto',
        fixedWeekCount: false,
        showNonCurrentDates: false,
        eventDisplay: 'block',
        dayMaxEvents: 3,
        moreLinkClick: 'popover',
        firstDay: 0,
        locale: 'en-US',
        dayHeaderFormat: { weekday: 'narrow' },
        eventClick(info) {
          const pairingId = info.event.extendedProps.pairingId;
          const row = document.querySelector(`[data-row-id="${pairingId}"]`);
          if (row) {
            row.scrollIntoView({ behavior: 'smooth', block: 'center' });
            row.classList.add('highlight');
            setTimeout(() => row.classList.remove('highlight'), 2000);
          }
        },
        eventMouseEnter(info) {
          info.el.style.cursor = 'pointer';
          info.el.setAttribute('title', info.event.extendedProps.pairingId || 'Pairing');
        },
        datesSet(info) {
          setTimeout(() => {
            stripYearFromTitle(calendarElNext.querySelector('.fc-toolbar-title'));
            
            const weeks = calendarElNext.querySelectorAll('.fc-daygrid-body tbody tr');
            
            if (weeks.length === 6) {
              weeks.forEach(w => w.classList.remove('week-hidden'));
              
              const lastWeek = weeks[weeks.length - 1];
              if (lastWeek) {
                const days = lastWeek.querySelectorAll('.fc-daygrid-day');
                let nextMonthDays = 0;
                days.forEach(day => {
                  if (day.classList.contains('fc-day-other')) {
                    nextMonthDays++;
                  }
                });
                if (nextMonthDays >= 5) {
                  lastWeek.classList.add('week-hidden');
                }
              }
            }
            
            applyCalendarLayout();
          }, 0);
        }
      });
      calendarNext.render();
      stripYearFromTitle(calendarElNext.querySelector('.fc-toolbar-title'));
    }

    applyCalendarLayout();
  }

  function updateCalendarsWithRows(rows) {
    ensureCalendars();
    
    if (!rows || !Array.isArray(rows)) return;
    
    const events = buildCalendarEvents(rows);
    
    if (calendarCurrent) {
      calendarCurrent.removeAllEvents();
      events.forEach(event => {
        calendarCurrent.addEvent(event);
      });
    }
    
    if (calendarNext) {
      calendarNext.removeAllEvents();
      events.forEach(event => {
        calendarNext.addEvent(event);
      });
    }
    
    setTimeout(() => {
      markDaysWithEvents();
    }, 50);
    
    setTimeout(applyCalendarLayout, 0);
  }
  
  function markDaysWithEvents() {
    const eventDates = new Set();
    
    [calendarCurrent, calendarNext].forEach(cal => {
      if (!cal) return;
      cal.getEvents().forEach(event => {
        let d = new Date(event.start);
        const end = event.end ? new Date(event.end) : d;
        while (d < end) {
          const dateStr = d.toISOString().split('T')[0];
          eventDates.add(dateStr);
          d.setDate(d.getDate() + 1);
        }
      });
    });
    
    document.querySelectorAll('.fc-daygrid-day').forEach(day => {
      const dateStr = day.getAttribute('data-date');
      if (dateStr && eventDates.has(dateStr)) {
        day.classList.add('has-flying-event');
      } else {
        day.classList.remove('has-flying-event');
      }
    });
  }

  // ===== Time helpers =====
  const TIME_12_RE = /\b([0-9]{1,2}):([0-9]{2})\s?(AM|PM)\b/i;
  const TIME_24_RE = /\b([01]?\d|2[0-3]):?([0-5]\d)\b/;
  function to12(h, m){const hh=(h%12)||12;return `${hh}:${m.toString().padStart(2,'0')} ${h<12?'AM':'PM'}`;}
  function to24(h, m, c){const hh=String(h).padStart(2,'0'),mm=String(m).padStart(2,'0');return c?`${hh}:${mm}`:`${hh}${mm}`;}
  function pickHHMM(...c){for(const x of c){if(x==null)continue;const s=String(x).trim();if(/^\d{4}$/.test(s))return s;const m1=s.match(/^(\d{1,2}):(\d{2})$/);if(m1)return `${m1[1].padStart(2,'0')}${m1[2]}`;const m2=s.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);if(m2){let h=parseInt(m2[1],10)%12;const m=parseInt(m2[2],10);if(m2[3].toUpperCase()==='PM')h+=12;return `${String(h).padStart(2,'0')}${String(m).padStart(2,'0')}`;}}return '';}
  function swapFirstTime(str,w,c=true){if(!str)return str;const m12=str.match(TIME_12_RE);if(m12){const h=parseInt(m12[1],10),m=parseInt(m12[2],10),h24=(m12[3].toUpperCase()==='PM'?(h%12)+12:(h%12));return str.replace(TIME_12_RE,w?to24(h24,m,c):`${h}:${String(m).padStart(2,'0')} ${m12[3].toUpperCase()}`);}const m24=str.match(TIME_24_RE);if(m24){const h=parseInt(m24[1],10),m=parseInt(m24[2],10);return str.replace(TIME_24_RE,w?to24(h,m,c):to12(h,m));}return str;}
  function fmtHHMM(hhmm,w,c=true){if(!hhmm)return'';const s=String(hhmm).replace(':','').padStart(4,'0');const hh=parseInt(s.slice(0,2),10),mm=parseInt(s.slice(2),10);return w?to24(hh,mm,c):to12(hh,mm);}

  // ===== Refresh UI helpers =====
  function setRefreshingState(isRefreshing) {
    state.isRefreshing = isRefreshing;
    const btn = qs('#refresh-btn');
    const chip = qs('#last-pull-chip');
    
    if (btn) {
      btn.disabled = isRefreshing;
      btn.textContent = isRefreshing ? 'Refreshing…' : 'Refresh';
      btn.classList.toggle('refreshing', isRefreshing);
    }
    
    if (chip) {
      chip.classList.toggle('refreshing', isRefreshing);
    }
  }

  function flashLastPullChip() {
    const chip = qs('#last-pull-chip');
    if (chip) {
      chip.classList.remove('flash');
      void chip.offsetWidth;
      chip.classList.add('flash');
    }
  }

  // ===== Actions =====
  window.dwManualRefresh = async function() {
    if (state.isRefreshing) return;
    
    setRefreshingState(true);
    
    try {
      const res = await fetch(apiBase + '/api/refresh', { method: 'POST' });
      const data = await res.json();
      
      if (data.ok) {
        await renderOnce();
        flashLastPullChip();
      }
    } catch (e) {
      console.error('Refresh failed:', e);
    } finally {
      setRefreshingState(false);
    }
  };

  // ===== Clock mode (controlled by settings.js) =====
  function setClockMode(mode) {
    state.clockMode = mode;
    repaintTimesOnly();
  }

  function getClockMode() {
    return state.clockMode;
  }

  // Expose for settings module
  window.dwPairings = {
    setClockMode,
    getClockMode
  };

  // ===== Hidden chip =====
  function applyHiddenCount(n) {
    state.hiddenCount = Number(n) || 0;
    const chip = qs('#hidden-chip');
    const countEl = qs('#hidden-count');
    if (!chip || !countEl) return;
    countEl.textContent = `Hidden: ${state.hiddenCount}`;
    chip.classList.toggle('hidden', !(state.hiddenCount > 0));
  }

  // ===== Live updates =====
  renderOnce();
  try {
    const es = new EventSource(apiBase + '/api/events');
    es.addEventListener('change', async (e) => {
      if (!state.isRefreshing) {
        await renderOnce();
        flashLastPullChip();
      }
    });
    es.addEventListener('schedule_update', async (e) => {
      // Settings modal will query backend when opened
    });
    es.addEventListener('hidden_update', async () => {
      await renderOnce();
    });
  } catch {}

  setInterval(tickStatusLine, 1000);

  // ===== Plan modal =====
  const planModal = qs('#plan-modal');
  const planClose1 = qs('#plan-close-1');
  const planClose2 = qs('#plan-close-2');
  [planClose1, planClose2].forEach(b => b && b.addEventListener('click', closePlan));
  
  function openPlan(pairingId, reportIso) {
    if (!planModal) return;
    planModal.classList.remove('hidden');
    document.body.classList.add('modal-open');
    const url = `${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairingId)}&report_local_iso=${encodeURIComponent(reportIso || '')}`;
    setText('#plan-meta', 'Loading…');
    qs('#plan-rows').innerHTML = '';
    fetch(url).then(r => r.json()).then(data => {
      const attempts = data.attempts || [];
      setText('#plan-meta', `Window: starts ${(data.policy?.push_start_hours || 12)}h before report; includes calls during non-quiet hours.`);
      qs('#plan-rows').innerHTML = attempts.map(a => {
        const when = new Date(a.at_iso);
        const label = a.kind === 'push' ? 'Push' : 'Call';
        const details = a.kind === 'call' ? `Ring ${a?.meta?.ring || 1}` : '';
        return `<tr><td>${when.toLocaleString()}</td><td class="text-center">${label}</td><td>${details}</td></tr>`;
      }).join('');
    }).catch(() => setText('#plan-meta', 'Unable to load plan.'));
  }
  
  function closePlan() {
    if (!planModal) return;
    planModal.classList.add('hidden');
    document.body.classList.remove('modal-open');
  }

  // ===== Click handlers =====
  let lastRowToggle = 0;
  const ROW_TOGGLE_DEBOUNCE = 300;
  
  document.addEventListener('click', async (e) => {
    const unhide = e.target.closest('[data-unhide-all]');
    if (unhide) {
      e.preventDefault();
      try {
        await fetch(`${apiBase}/api/hidden/unhide_all`, { method: 'POST' });
      } catch (err) {
        console.error('unhide all failed', err);
      }
      await renderOnce();
      return;
    }
    
    if (e.target.closest('[data-stop-toggle]')) {
      e.stopPropagation();
    }
    
    const sum = e.target.closest('tr.summary');
    if (sum && !e.target.closest('[data-ck]')) {
      const now = Date.now();
      if (now - lastRowToggle < ROW_TOGGLE_DEBOUNCE) {
        e.preventDefault();
        return;
      }
      lastRowToggle = now;
      
      document.querySelectorAll('tr.summary').forEach(row => {
        row.blur();
        row.querySelectorAll('td').forEach(td => td.blur());
      });
      
      sum.classList.toggle('open');
      const details = sum.nextElementSibling;
      if (!details || !details.classList.contains('details')) return;
      const open = sum.classList.contains('open');
      details.querySelectorAll('.day .legs').forEach(tbl => {
        tbl.classList.toggle('table-visible', open);
      });
      
      if (document.activeElement) {
        document.activeElement.blur();
      }
      return;
    }
    
    const ck = e.target.closest('[data-ck]');
    if (ck) {
      e.preventDefault();
      const pairingId = ck.getAttribute('data-pairing') || '';
      const reportIso = ck.getAttribute('data-report') || '';
      openPlan(pairingId, reportIso);
      return;
    }
    
    const hideBtn = e.target.closest('[data-hide-pairing]');
    if (hideBtn) {
      e.preventDefault();
      e.stopPropagation();
      const pairingId = hideBtn.getAttribute('data-hide-pairing') || '';
      const reportIso = hideBtn.getAttribute('data-report') || '';
      const details = hideBtn.closest('tr.details');
      const summary = details?.previousElementSibling;
      if (summary?.classList.contains('summary')) {
        summary.remove();
        details.remove();
        applyHiddenCount(state.hiddenCount + 1);
      }
      try {
        await fetch(`${apiBase}/api/hidden/hide`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pairing_id: pairingId, report_local_iso: reportIso })
        });
      } catch (err) {
        console.error('hide failed', err);
      }
      await renderOnce();
      return;
    }
  });

  // ===== Render function =====
  async function renderOnce() {
    const params = new URLSearchParams({ is_24h: '0', only_reports: state.onlyReports ? '1' : '0' });
    let data;
    try {
      const res = await fetch(`${apiBase}/api/pairings?${params.toString()}`, { cache: 'no-store' });
      data = await res.json();
    } catch (e) {
      console.error('Failed to fetch /api/pairings', e);
      return;
    }

    state.lastPullIso = data.last_pull_local_iso || null;

    const hintedHidden = (data && (data.hidden_count ?? (data.hidden && data.hidden.count)));
    applyHiddenCount(typeof hintedHidden === 'number' ? hintedHidden : state.hiddenCount);

    setText('#last-pull', minutesOnlyAgo(state.lastPullIso));

    const calendarData = data.calendar_rows || [];
    const tableRows = data.rows || [];
    
    updateCalendarsWithRows(calendarData);
    
    const tbody = qs('#pairings-body');
    tbody.innerHTML = tableRows.map((row) => renderRowHTML(row, HOME_BASE)).join('');

    repaintTimesOnly();
    applyCalendarLayout();
  }

  function repaintTimesOnly() {
    const want24 = state.clockMode === 24;
    document.querySelectorAll('[data-dw="report"]').forEach(el => {
      if (el.classList.contains('off-dur')) return;
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
      const left = fmtHHMM(dep, want24, false);
      const right = fmtHHMM(arr, want24, false);
      el.textContent = arr ? `${left} → ${right}` : left;
    });
  }

  function pairingNowTag(row) {
    return row?.in_progress ? ' <span class="text-muted-normal">(Now)</span>' : '';
  }

  function renderRowHTML(row, homeBase) {
    if (row.kind === 'off') {
      const display = row.display || {};
      const offLabel = display.off_label || 'OFF';
      const offDur = display.off_dur || '';
      const offDuration = display.off_duration || '';
      const showRemaining = display.show_remaining || display.off_remaining;
      const isCurrentOff = row.is_current === true;
      
      let labelHtml;
      if (isCurrentOff || offLabel.includes('(Now)')) {
        const baseLabel = offLabel.replace('(Now)', '').trim();
        labelHtml = `<span class="off-label">${esc(baseLabel)}</span> <span class="off-text-normal">(Now)</span>`;
      } else {
        labelHtml = `<span class="off-label">${esc(offLabel)}</span>`;
      }
      
      let durHtml;
      if (offDuration) {
        durHtml = esc(offDuration);
        if (showRemaining) {
          durHtml += ' <span class="off-text-normal"><span class="remaining-full">(Remaining)</span><span class="remaining-short">(Rem.)</span></span>';
        }
      } else if (offDur.includes('Remaining:')) {
        const match = offDur.match(/^(\d+h)(?:\s*\(Remaining:[^)]+\))?/);
        if (match) {
          durHtml = esc(match[1]) + ' <span class="off-text-normal"><span class="remaining-full">(Remaining)</span><span class="remaining-short">(Rem.)</span></span>';
        } else {
          durHtml = esc(offDur);
        }
      } else {
        durHtml = esc(offDur);
      }

      return `
        <tr class="off">
          <td class="ckcol"></td>
          <td class="sum-first">${labelHtml}</td>
          <td class="off-dur" data-dw="report">${durHtml}</td>
          <td data-dw="release"></td>
        </tr>`;
    }

    const hasLegs = row.has_legs || false;
    const totalLegs = row.total_legs || 0;
    const isNonPairing = !hasLegs;
    
    const showOOB = row.out_of_base || false;
    const oobAirport = row.out_of_base_airport || '';
    const oobPill = showOOB ? `<span class="pill pill-red">${esc(oobAirport)}</span>` : '';

    const days = row.days || [];
    const detailsDays = hasLegs ? days.map((day, i) => renderDayHTML(row, day, i, days)).join('') : '';

    const noLegsBlock = !hasLegs ? `<div data-stop-toggle class="no-legs-block">
           <span class="muted">No legs found.</span>
           <button class="btn" data-hide-pairing="${esc(row.pairing_id || '')}" data-report="${esc(row.report_local_iso || '')}">
             Hide Event
           </button>
         </div>` : '';

    const numDays = row.num_days || 1;

    return `
      <tr class="summary ${isNonPairing ? 'non-pairing' : ''}" data-row-id="${esc(row.pairing_id || '')}">
        ${renderCheckCell(row)}
        <td class="sum-first">
          <strong>${esc(row.pairing_id || '')}</strong>${pairingNowTag(row)}
          ${hasLegs ? `<span class="pill">${numDays} day</span>` : ``}
          ${oobPill}
        </td>
        <td data-dw="report">${esc(row.display?.report_str || '')}</td>
        <td data-dw="release">${isNonPairing ? '' : esc(row.display?.release_str || '')}</td>
      </tr>
      <tr class="details">
        <td colspan="4">
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
      <td class="ckcol">
        <button class="ckbtn ck ${stateAttr}"
                type="button"
                role="checkbox"
                aria-checked="${ariaChecked}"
                aria-disabled="${ariaDisabled}"
                title="${stateAttr === 'ok' ? 'Acknowledged' : (stateAttr === 'pending' ? 'Click to view plan / acknowledge' : 'Click to view reminder plan')}"
                data-ck="${stateAttr}"
                data-pairing="${esc(row.pairing_id || '')}"
                data-report="${esc((ack && ack.report_local_iso) || row.report_local_iso || '')}">
          <span class="ckbox" aria-hidden="true"></span>
        </button>
      </td>`;
  }

  function renderDayHTML(row, day, idx, days) {
    const want24 = state.clockMode === 24;
    const isMobile = window.matchMedia && window.matchMedia('(max-width: 640px)').matches;
    const dayISO = dayDateFromRow(row, idx, day);
    const dow = weekdayFromISO(dayISO);
    
    if (day.is_layover) {
      const layoverLocation = day.layover_location || '';
      const hotel = day.hotel || '';
      const noFlightsMsg = day.no_flights_message || '';
      
      return `
        <div class="day">
          <div class="dayhdr">
            <span class="dot"></span>
            <span class="daytitle">Day ${day.day_index || (idx + 1)}</span>
            ${layoverLocation ? ` · Layover: ${esc(layoverLocation)}` : ''}
            ${hotel ? ` · Hotel: ${esc(hotel)}` : ''}
          </div>
          ${noFlightsMsg ? `
          <div class="legs-wrap">
            <table class="legs table-visible">
              <tbody>
                <tr>
                  <td colspan="4" style="text-align: center; color: var(--muted); padding: 12px;">
                    ${esc(noFlightsMsg)}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>` : ''}
        </div>`;
    }
    
    const legs = (day.legs || []).map(leg => {
      const route = leg.route_display || '';
      const depTime = leg.dep_time || '';
      const arrTime = leg.arr_time || '';
      const blockTime = leg.block_display || '';
      
      let trackCell = '';
      if (leg.tracking_url) {
        trackCell = `<a href="${esc(leg.tracking_url)}" target="_blank" style="color: #0066cc; text-decoration: underline; cursor: pointer;">Track →</a>`;
      } else if (leg.tracking_display) {
        trackCell = `<span style="color: var(--muted);">${esc(leg.tracking_display)}</span>`;
      } else {
        trackCell = '';
      }
      
      const isDeadhead = leg.deadhead || false;
      
      return `
        <tr class="leg-row ${leg.done ? 'leg-done' : ''}${isDeadhead ? ' leg-deadhead' : ''}">
          <td>${esc(leg.flight || '')}</td>
          <td>${route}</td>
          <td class="bt" data-dw="bt" data-dep="${esc(depTime)}" data-arr="${esc(arrTime)}">${blockTime}</td>
          <td>${trackCell}</td>
        </tr>`;
    }).join('');
    
    const dayRepRaw = pickHHMM(day.report_hhmm, day.report);
    const dayRelRaw = pickHHMM(day.release_hhmm, day.release);
    const repDisp = fmtHHMM(dayRepRaw, want24, false);
    const relDisp = fmtHHMM(dayRelRaw, want24, false);
    const blockLabel = 'Block';
    const trackLabel = isMobile ? 'Track' : 'Tracking';
    
    return `
      <div class="day">
        <div class="dayhdr">
          <span class="dot"></span>
          <span class="daytitle">Day ${day.day_index || (idx + 1)}</span>
          ${dayRepRaw ? ` · Report: ${esc(dow)} <span data-dw="day-report" data-hhmm="${esc(dayRepRaw)}">${esc(repDisp)}</span>` : ''}
          ${dayRelRaw ? ` · Release: <span data-dw="day-release" data-hhmm="${esc(dayRelRaw)}">${esc(relDisp)}</span>` : ''}
          ${day.hotel ? ` · Hotel: ${esc(day.hotel)}` : ''}
        </div>
        ${legs ? `
          <div class="legs-wrap">
            <table class="legs">
              <thead><tr><th>Flight</th><th>Route</th><th>${blockLabel}</th><th>${trackLabel}</th></tr></thead>
              <tbody>${legs}</tbody>
            </table>
          </div>` : ``}
      </div>`;
  }

  function dayDateFromRow(row, dayIndex, dayObj) {
    const keys = ['date_local_iso', 'local_iso', 'start_local_iso', 'date_iso'];
    for (const k of keys) {
      if (dayObj && dayObj[k]) return dayObj[k];
    }
    const leg = (dayObj && dayObj.legs && dayObj.legs[0]) || null;
    if (leg) {
      const legKeys = ['dep_local_iso', 'dep_iso', 'dep_dt_iso', 'local_iso'];
      for (const k of legKeys) {
        if (leg[k]) return leg[k];
      }
    }
    if (row && row.report_local_iso) {
      const shifted = new Date(row.report_local_iso);
      if (!isNaN(shifted)) {
        shifted.setDate(shifted.getDate() + dayIndex);
        return shifted.toISOString();
      }
    }
    return null;
  }
  
  function weekdayFromISO(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    if (isNaN(d)) return '';
    return d.toLocaleDateString(undefined, { weekday: 'short' });
  }

  function tickStatusLine() {
    if (state.lastPullIso) setText('#last-pull', minutesOnlyAgo(state.lastPullIso));
  }

  // Utilities
  function qs(sel) { return document.querySelector(sel); }
  function setText(sel, v) { const el = qs(sel); if (el) el.textContent = v; }
  function esc(s) { return String(s).replace(/[&<>"'`=\/]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', '/': '&#x2F;', '`': '&#x60;', '=': '&#x3D;' }[ch])); }
  function minutesOnlyAgo(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d)) return '—';
    const sec = Math.max(0, Math.floor((Date.now() - d.getTime()) / 1000));
    const m = Math.max(0, Math.floor(sec / 60));
    if (m <= 0) return 'just now';
    return `${m}m ago`;
  }
  function safeParseJSON(s) { try { return JSON.parse(s || '{}'); } catch { return null; } }

  // === Resize handling ===
  let _rzTimer = null;
  function debouncedApply() {
    if (_rzTimer) clearTimeout(_rzTimer);
    _rzTimer = setTimeout(() => {
      applyCalendarLayout();
    }, 100);
  }

  window.addEventListener('resize', debouncedApply);

  document.addEventListener('touchend', () => {
    setTimeout(() => {
      if (document.activeElement && document.activeElement !== document.body) {
        document.activeElement.blur();
      }
    }, 100);
  }, { passive: true });

  function handleOrientationChange() {
    applyCalendarLayout();
    setTimeout(applyCalendarLayout, 120);
  }
  
  window.addEventListener('orientationchange', handleOrientationChange);
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) handleOrientationChange();
  });
  window.addEventListener('pageshow', handleOrientationChange);
})();