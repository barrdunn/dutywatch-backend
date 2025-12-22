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
    onlyReports: !!cfg.onlyReports,
    hiddenCount: 0,
    _zeroKick: 0,
    layoutMode: null
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
    
    rows.forEach(row => {
      // Only show pairings with legs
      if (!row || row.kind === 'off' || !row.has_legs) return;
      
      // Need both report and release dates
      if (!row.report_local_iso || !row.release_local_iso) return;
      
      // Extract date parts (YYYY-MM-DD)
      const startDate = row.report_local_iso.split('T')[0];
      const releaseDate = row.release_local_iso.split('T')[0];
      
      // FullCalendar end dates are EXCLUSIVE for all-day events
      // So to include the release day, we need to add 1 day
      const [year, month, day] = releaseDate.split('-').map(Number);
      const endDateObj = new Date(year, month - 1, day + 1);
      const endDate = endDateObj.toISOString().split('T')[0];
      
      events.push({
        id: row.pairing_id || 'event-' + Math.random(),
        title: row.pairing_id || 'Event',
        start: startDate,
        end: endDate,
        allDay: true,
        backgroundColor: '#49b37c',
        borderColor: '#49b37c',
        textColor: '#ffffff',
        extendedProps: {
          pairingId: row.pairing_id
        }
      });
    });
    
    return events;
  }

  function findBelowAnchor() {
    const settingsInline = document.getElementById('settings-inline');
    if (settingsInline) return settingsInline;

    const controls = document.querySelector('.controls');
    if (controls) return controls;

    const pairings = document.getElementById('pairings');
    if (pairings && pairings.previousElementSibling) return pairings.previousElementSibling;

    const card = document.querySelector('.card');
    return card || document.body;
  }

  function moveCalendarsBelow() {
    const cc = document.getElementById('calendar-container');
    if (!cc) return;

    const anchor = findBelowAnchor();
    if (anchor && anchor.parentNode) {
      anchor.insertAdjacentElement('afterend', cc);
    }

    cc.classList.remove('calendar-desktop');
    cc.classList.add('calendar-mobile');
    
    const calCurrent = document.getElementById('calendar-current');
    const calNext = document.getElementById('calendar-next');
    if(calCurrent) calCurrent.style.display = 'block';
    if(calNext) {
      calNext.style.cssText = 'display: block !important; visibility: visible !important; opacity: 1 !important;';
    }
    
    cc.style.height = 'auto';
    cc.style.overflow = 'visible';
  }

  function moveCalendarsTopRight() {
    const cc = document.getElementById('calendar-container');
    const card = cc?.closest('.card') || document.body;
    if (!cc) return;

    if (cc.parentNode !== card || cc !== card.firstElementChild) {
      card.insertBefore(cc, card.firstElementChild || null);
    }

    cc.classList.remove('calendar-mobile');
    cc.classList.add('calendar-desktop');
  }

  function applyCalendarLayout() {
    const vw = Math.min(window.innerWidth || 0, document.documentElement.clientWidth || 0) || window.innerWidth;
    const body = document.body;
    
    body.classList.remove(
      'layout-desktop',
      'layout-desktop-locked',
      'layout-mobile',
      'layout-mobile-narrow',
      'cal-below'
    );

    if (vw > 990) {
      moveCalendarsTopRight();
      body.classList.add('layout-desktop');
      state.layoutMode = 'desktop-responsive';
    } else if (vw > 750) {
      moveCalendarsTopRight();
      body.classList.add('layout-desktop-locked');
      state.layoutMode = 'desktop-locked-990';
    } else {
      moveCalendarsBelow();
      body.classList.add('cal-below');
      
      const cc = document.getElementById('calendar-container');
      if(cc) {
        cc.style.display = 'flex';
        cc.style.flexDirection = 'row';
        cc.style.gap = '8px';
        cc.style.justifyContent = 'center';
      }
      
      if (vw <= 350) {
        body.classList.add('layout-mobile-narrow');
        state.layoutMode = 'mobile-locked-350';
      } else {
        body.classList.add('layout-mobile');
        state.layoutMode = 'mobile-responsive';
      }
    }
  }

  function ensureCalendars() {
    const calendarElCurrent = document.getElementById('calendar-current');
    const calendarElNext = document.getElementById('calendar-next');

    if (calendarElCurrent && !calendarCurrent) {
      // Use current month dynamically
      const now = new Date();
      const currentDate = new Date(now.getFullYear(), now.getMonth(), 1);
      
      calendarCurrent = new FullCalendar.Calendar(calendarElCurrent, {
        initialView: 'dayGridMonth',
        initialDate: currentDate,
        headerToolbar: { left: '', center: 'title', right: '' },
        titleFormat: { month: 'long' },
        height: 'auto',
        fixedWeekCount: false,
        showNonCurrentDates: true,
        eventDisplay: 'block',
        dayMaxEvents: 3,
        moreLinkClick: 'popover',
        firstDay: 0, // 0 = Sunday, 1 = Monday, etc.
        locale: 'en-US', // Force US locale which starts with Sunday
        dayHeaderFormat: { weekday: 'narrow' }, // Use narrow format for single letter
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
            
            // Hide last week if it's mostly next month days
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
      // Use next month dynamically (handles year rollover automatically)
      const now = new Date();
      const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1);

      calendarNext = new FullCalendar.Calendar(calendarElNext, {
        initialView: 'dayGridMonth',
        initialDate: nextMonth,
        headerToolbar: { left: '', center: 'title', right: '' },
        titleFormat: { month: 'long' },
        height: 'auto',
        fixedWeekCount: false,
        showNonCurrentDates: true,
        eventDisplay: 'block',
        dayMaxEvents: 3,
        moreLinkClick: 'popover',
        firstDay: 0, // 0 = Sunday, 1 = Monday, etc.
        locale: 'en-US', // Force US locale which starts with Sunday
        dayHeaderFormat: { weekday: 'narrow' }, // Use narrow format for single letter
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
    
    console.log('updateCalendarsWithRows called with rows:', rows);
    
    if (!rows || !Array.isArray(rows)) return;
    
    const events = buildCalendarEvents(rows);
    console.log('Built events for calendar:', events);
    
    if (calendarCurrent) {
      calendarCurrent.removeAllEvents();
      events.forEach(event => {
        calendarCurrent.addEvent(event);
      });
      console.log('Added', events.length, 'events to current month calendar');
    }
    
    if (calendarNext) {
      calendarNext.removeAllEvents();
      events.forEach(event => {
        calendarNext.addEvent(event);
      });
      console.log('Added', events.length, 'events to next month calendar');
    }
    
    setTimeout(applyCalendarLayout, 0);
    
    setTimeout(() => {
      const isMobile = window.innerWidth <= 640;
      if(isMobile) {
        const calNext = document.getElementById('calendar-next');
        if(calNext) {
          calNext.style.cssText = 'display: block !important; visibility: visible !important; opacity: 1 !important;';
        }
      }
    }, 100);
  }

  // ===== Time helpers =====
  const TIME_12_RE = /\b([0-9]{1,2}):([0-9]{2})\s?(AM|PM)\b/i;
  const TIME_24_RE = /\b([01]?\d|2[0-3]):?([0-5]\d)\b/;
  function to12(h, m){const hh=(h%12)||12;return `${hh}:${m.toString().padStart(2,'0')} ${h<12?'AM':'PM'}`;}
  function to24(h, m, c){const hh=String(h).padStart(2,'0'),mm=String(m).padStart(2,'0');return c?`${hh}:${mm}`:`${hh}${mm}`;}
  function pickHHMM(...c){for(const x of c){if(x==null)continue;const s=String(x).trim();if(/^\d{4}$/.test(s))return s;const m1=s.match(/^(\d{1,2}):(\d{2})$/);if(m1)return `${m1[1].padStart(2,'0')}${m1[2]}`;const m2=s.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);if(m2){let h=parseInt(m2[1],10)%12;const m=parseInt(m2[2],10);if(m2[3].toUpperCase()==='PM')h+=12;return `${String(h).padStart(2,'0')}${String(m).padStart(2,'0')}`;}}return '';}
  function swapFirstTime(str,w,c=true){if(!str)return str;const m12=str.match(TIME_12_RE);if(m12){const h=parseInt(m12[1],10),m=parseInt(m12[2],10),h24=(m12[3].toUpperCase()==='PM'?(h%12)+12:(h%12));return str.replace(TIME_12_RE,w?to24(h24,m,c):`${h}:${String(m).padStart(2,'0')} ${m12[3].toUpperCase()}`);}const m24=str.match(TIME_24_RE);if(m24){const h=parseInt(m24[1],10),m=parseInt(m24[2],10);return str.replace(TIME_24_RE,w?to24(h,m,c):to12(h,m));}return str;}
  function fmtHHMM(hhmm,w,c=true){if(!hhmm)return'';const s=String(hhmm).replace(':','').padStart(4,'0');const hh=parseInt(s.slice(0,2),10),mm=parseInt(s.slice(2),10);return w?to24(hh,mm,c):to12(hh,mm);}

  // ===== Actions =====
  window.dwManualRefresh = async function(){
    try{
      await fetch(apiBase+'/api/refresh',{method:'POST'});
    }catch(e){
      console.error(e);
    }
  };
  
  // Commute functions
  window.saveCommuteTime = async function(commuteId) {
    const input = document.getElementById(`time-${commuteId}`);
    if (!input || !input.value) {
      alert('Please select a time');
      return;
    }
    
    // Get the date from the row's data attribute
    const row = document.querySelector(`[data-commute-id="${commuteId}"]`);
    const dateStr = row ? row.getAttribute('data-report-date') : '';
    
    try {
      // Send just the time, backend will handle date
      const response = await fetch(`${apiBase}/api/commute/${commuteId}`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          report_time: input.value,
          date_context: dateStr
        })
      });
      
      if (response.ok) {
        alert('Commute time saved!');
        await renderOnce(); // Refresh the display
      } else {
        const error = await response.text();
        alert(`Failed to save: ${error}`);
      }
    } catch(e) {
      console.error('Failed to save commute time:', e);
      alert('Failed to save commute time');
    }
  };
  
  window.saveCommuteTracking = async function(commuteId) {
    const input = document.getElementById(`tracking-${commuteId}`);
    if (!input) return;
    
    try {
      const response = await fetch(`${apiBase}/api/commute/${commuteId}`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({tracking_url: input.value || null})
      });
      
      if (response.ok) {
        await renderOnce(); // Refresh the display
      } else {
        const error = await response.text();
        alert(`Failed to save: ${error}`);
      }
    } catch(e) {
      console.error('Failed to save tracking URL:', e);
      alert('Failed to save tracking URL');
    }
  };
  
  window.removeCommuteTracking = async function(commuteId) {
    if (!confirm('Remove tracking link?')) return;
    
    try {
      const response = await fetch(`${apiBase}/api/commute/${commuteId}`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({tracking_url: ''})  // Send empty string instead of null
      });
      
      if (response.ok) {
        await renderOnce();
      } else {
        console.error('Failed to remove tracking URL');
      }
    } catch(e) {
      console.error('Failed to remove tracking URL:', e);
    }
  };
  
  window.editCommuteTracking = function(commuteId) {
    // Just show the input field again
    const trackingDiv = document.querySelector(`#tracking-div-${commuteId}`);
    const currentUrl = trackingDiv.querySelector('a')?.href || '';
    
    trackingDiv.innerHTML = `
      <input type="url" id="tracking-${esc(commuteId)}" class="commute-tracking-input" 
             placeholder="https://flightaware.com/..." value="${esc(currentUrl)}" 
             style="width: 400px; padding: 6px; border-radius: 6px; border: 1px solid var(--border); background: var(--card); color: var(--text);">
      <button class="btn" style="margin-left: 8px;" onclick="saveCommuteTracking('${esc(commuteId)}')">Save</button>
      <button class="btn" style="margin-left: 4px;" onclick="renderOnce()">Cancel</button>
    `;
  };
  
  window.hideCommute = async function(commuteId) {
    if (!confirm('Hide this commute row?')) return;
    
    // For now, just hide it locally
    const row = document.querySelector(`[data-commute-id="${commuteId}"]`);
    const detailsRow = row ? row.nextElementSibling : null;
    
    if (row) row.style.display = 'none';
    if (detailsRow && detailsRow.classList.contains('details')) {
      detailsRow.style.display = 'none';
    }
    
    // TODO: Save hide preference to backend
  };

  // ===== Inline settings =====
  const refreshSel=document.getElementById('refresh-mins');
  if(refreshSel){
    if(cfg.refreshMinutes) refreshSel.value = String(cfg.refreshMinutes);
    refreshSel.addEventListener('change', async () => {
      const minutes = parseInt(refreshSel.value,10);
      try{
        await fetch(apiBase+'/api/settings/refresh-seconds',{
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body:JSON.stringify({seconds:minutes*60})
        });
      }catch(e){console.error(e);}
    });
  }

  const clockSel=document.getElementById('clock-mode');
  if(clockSel){
    clockSel.value = String(state.clockMode);
    clockSel.addEventListener('change', () => {
      state.clockMode = parseInt(clockSel.value,10)===24 ? 24 : 12;
      repaintTimesOnly();
    });
  }

  // ===== Hidden chip =====
  function applyHiddenCount(n){
    state.hiddenCount=Number(n)||0;
    const chip=qs('#hidden-chip');
    const countEl=qs('#hidden-count');
    if(!chip||!countEl) return;
    countEl.textContent=`Hidden: ${state.hiddenCount}`;
    chip.classList.toggle('hidden',!(state.hiddenCount>0));
  }

  // ===== Live updates =====
  renderOnce();
  try{
    const es=new EventSource(apiBase+'/api/events');
    es.addEventListener('change',async()=>{await renderOnce();});
    es.addEventListener('schedule_update',async()=>{await renderOnce();});
    es.addEventListener('hidden_update',async()=>{await renderOnce();});
  }catch{}

  setInterval(tickStatusLine,1000);

  // ===== Plan modal =====
  const planModal=qs('#plan-modal');
  const planClose1=qs('#plan-close-1');
  const planClose2=qs('#plan-close-2');
  [planClose1,planClose2].forEach(b=>b&&b.addEventListener('click',closePlan));
  
  function openPlan(pairingId,reportIso){
    if(!planModal) return;
    planModal.classList.remove('hidden');
    document.body.classList.add('modal-open');
    const url=`${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairingId)}&report_local_iso=${encodeURIComponent(reportIso||'')}`;
    setText('#plan-meta','Loading…');
    qs('#plan-rows').innerHTML='';
    fetch(url).then(r=>r.json()).then(data=>{
      const attempts=data.attempts||[];
      setText('#plan-meta',`Window: starts ${(data.policy?.push_start_hours||12)}h before report; includes calls during non-quiet hours.`);
      qs('#plan-rows').innerHTML=attempts.map(a=>{
        const when=new Date(a.at_iso);
        const label=a.kind==='push'?'Push':'Call';
        const details=a.kind==='call'?`Ring ${a?.meta?.ring||1}`:'';
        return `<tr><td>${when.toLocaleString()}</td><td class="text-center">${label}</td><td>${details}</td></tr>`;
      }).join('');
    }).catch(()=>setText('#plan-meta','Unable to load plan.'));
  }
  
  function closePlan(){
    if(!planModal)return;
    planModal.classList.add('hidden');
    document.body.classList.remove('modal-open');
  }

  // ===== Click handlers =====
  document.addEventListener('click',async(e)=>{
    const unhide=e.target.closest('[data-unhide-all]');
    if(unhide){
      e.preventDefault();
      try{
        await fetch(`${apiBase}/api/hidden/unhide_all`,{method:'POST'});
      }catch(err){
        console.error('unhide all failed',err);
      }
      await renderOnce();
      return;
    }
    
    if(e.target.closest('[data-stop-toggle]')){
      e.stopPropagation();
    }
    
    const sum=e.target.closest('tr.summary');
    if(sum&&!e.target.closest('[data-ck]')){
      sum.classList.toggle('open');
      const details=sum.nextElementSibling;
      if(!details||!details.classList.contains('details'))return;
      const open=sum.classList.contains('open');
      details.querySelectorAll('.day .legs').forEach(tbl=>{
        tbl.classList.toggle('table-visible', open);
      });
      return;
    }
    
    const ck=e.target.closest('[data-ck]');
    if(ck){
      e.preventDefault();
      const pairingId=ck.getAttribute('data-pairing')||'';
      const reportIso=ck.getAttribute('data-report')||'';
      openPlan(pairingId,reportIso);
      return;
    }
    
    const hideBtn=e.target.closest('[data-hide-pairing]');
    if(hideBtn){
      e.preventDefault();
      e.stopPropagation();
      const pairingId=hideBtn.getAttribute('data-hide-pairing')||'';
      const reportIso=hideBtn.getAttribute('data-report')||'';
      const details=hideBtn.closest('tr.details');
      const summary=details?.previousElementSibling;
      if(summary?.classList.contains('summary')){
        summary.remove();
        details.remove();
        applyHiddenCount(state.hiddenCount+1);
      }
      try{
        await fetch(`${apiBase}/api/hidden/hide`,{
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body:JSON.stringify({pairing_id:pairingId,report_local_iso:reportIso})
        });
      }catch(err){
        console.error('hide failed',err);
      }
      await renderOnce();
      return;
    }
  });

  // ===== Render function =====
  async function renderOnce(){
    const params=new URLSearchParams({is_24h:'0',only_reports:state.onlyReports?'1':'0'});
    let data;
    try{
      const res=await fetch(`${apiBase}/api/pairings?${params.toString()}`,{cache:'no-store'});
      data=await res.json();
    }catch(e){
      console.error('Failed to fetch /api/pairings',e);
      return;
    }

    console.log('API response data:', data);

    state.lastPullIso=data.last_pull_local_iso||null;
    state.nextRefreshIso=data.next_pull_local_iso||null;

    const hintedHidden=(data&&(data.hidden_count??(data.hidden&&data.hidden.count)));
    applyHiddenCount(typeof hintedHidden==='number'?hintedHidden:state.hiddenCount);

    setText('#last-pull', minutesOnlyAgo(state.lastPullIso));

    const nextEl = qs('#next-refresh');
    if (nextEl) {
      const base = (data.next_pull_local && data.tz_label) ? `${data.next_pull_local} (${data.tz_label})` : '—';
      nextEl.innerHTML = base;
    }

    // Backend now sends properly formatted rows with OFF calculations already included
    const calendarData = data.calendar_rows || [];
    const tableRows = data.rows || [];  // These already include OFF rows between pairings!
    
    console.log('Using for calendar:', calendarData.length, 'items');
    console.log('Using for table:', tableRows.length, 'items (including OFF rows)');
    
    // Update calendars with ALL events (including past)
    updateCalendarsWithRows(calendarData);
    
    // Render table - rows already have OFF times calculated by backend
    const tbody=qs('#pairings-body');
    tbody.innerHTML=tableRows.map((row)=>renderRowHTML(row,HOME_BASE)).join('');
    
    // Wrap table in scrollable container on mobile
    const isMobile = window.innerWidth <= 740;
    const table = qs('#pairings');
    if(isMobile && table && !table.parentElement.classList.contains('table-wrapper')) {
      const wrapper = document.createElement('div');
      wrapper.className = 'table-wrapper';
      table.parentNode.insertBefore(wrapper, table);
      wrapper.appendChild(table);
    }

    repaintTimesOnly();
    applyCalendarLayout();
  }

  function repaintTimesOnly(){
    const want24=state.clockMode===24;
    document.querySelectorAll('[data-dw="report"]').forEach(el=>{
      if(el.classList.contains('off-dur'))return;
      const orig=el.getAttribute('data-orig')||el.textContent;
      if(!el.hasAttribute('data-orig'))el.setAttribute('data-orig',orig);
      el.textContent=swapFirstTime(orig,want24,false);
    });
    document.querySelectorAll('[data-dw="release"]').forEach(el=>{
      const orig=el.getAttribute('data-orig')||el.textContent;
      if(!el.hasAttribute('data-orig'))el.setAttribute('data-orig',orig);
      el.textContent=swapFirstTime(orig,want24,false);
    });
    document.querySelectorAll('[data-dw="day-report"]').forEach(el=>{
      const raw=el.getAttribute('data-hhmm')||el.textContent;
      el.textContent=fmtHHMM(raw,want24,false);
    });
    document.querySelectorAll('[data-dw="day-release"]').forEach(el=>{
      const raw=el.getAttribute('data-hhmm')||el.textContent;
      el.textContent=fmtHHMM(raw,want24,false);
    });
    document.querySelectorAll('[data-dw="bt"]').forEach(el=>{
      const dep=el.getAttribute('data-dep')||'';
      const arr=el.getAttribute('data-arr')||'';
      const left=fmtHHMM(dep,want24,false);
      const right=fmtHHMM(arr,want24,false);
      el.textContent=arr?`${left} → ${right}`:left;
    });
  }

  function pairingNowTag(row){
    return row?.in_progress?' <span class="text-muted-normal">(Now)</span>':'';
  }

  function renderRowHTML(row,homeBase){
    if(row.kind==='off'){
      // Backend provides formatted OFF row with label and duration
      const display = row.display || {};
      const offLabel = display.off_label || 'OFF';
      const offDur = display.off_dur || '';
      const offDuration = display.off_duration || '';
      const showRemaining = display.show_remaining || display.off_remaining;
      const isCurrentOff = row.is_current === true;
      
      // Format label based on whether this is current OFF period
      let labelHtml;
      if (isCurrentOff || offLabel.includes('(Now)')) {
        // Remove any existing (Now) and add it with proper styling
        const baseLabel = offLabel.replace('(Now)', '').trim();
        labelHtml = `<span class="off-label">${esc(baseLabel)}</span> <span class="off-text-normal">(Now)</span>`;
      } else {
        labelHtml = `<span class="off-label">${esc(offLabel)}</span>`;
      }
      
      // Format duration - just show the time and "(Remaining)" if applicable
      let durHtml;
      if (offDuration) {
        // Use the clean duration if provided
        durHtml = esc(offDuration);
        if (showRemaining) {
          durHtml += ' <span class="off-text-normal">(Remaining)</span>';
        }
      } else if (offDur.includes('Remaining:')) {
        // Fix old format: "25h (Remaining: 25h)" -> "25h (Remaining)"
        const match = offDur.match(/^(\d+h)(?:\s*\(Remaining:[^)]+\))?/);
        if (match) {
          durHtml = esc(match[1]) + ' <span class="off-text-normal">(Remaining)</span>';
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
    
    // Handle commute rows
    if(row.kind==='commute'){
      const display = row.display || {};
      const commuteId = row.commute_id || '';
      const reportStr = display.report_str || 'Set report time';
      const reportIso = row.report_local_iso || '';
      const releaseIso = row.release_local_iso || '';  // This is the arrival time
      const trackingUrl = row.tracking_url || '';
      const label = display.label || 'Commute';
      
      // Extract times from ISO strings
      let timeValue = '';
      let dateStr = '';
      let arrivalStr = '';
      
      if (reportIso) {
        const dt = new Date(reportIso);
        const hours = String(dt.getHours()).padStart(2, '0');
        const mins = String(dt.getMinutes()).padStart(2, '0');
        timeValue = `${hours}:${mins}`;
        dateStr = dt.toISOString().split('T')[0];
      }
      
      if (releaseIso) {
        const arrDt = new Date(releaseIso);
        const arrHours = arrDt.getHours();
        const arrMins = String(arrDt.getMinutes()).padStart(2, '0');
        const ampm = arrHours >= 12 ? 'PM' : 'AM';
        const displayHours = arrHours % 12 || 12;
        arrivalStr = `Arrive: ${displayHours}:${arrMins} ${ampm}`;
      }
      
      // Create tracking HTML based on whether URL exists
      let trackingHtml = '';
      if (trackingUrl && trackingUrl !== 'null' && trackingUrl !== '') {
        trackingHtml = `
          <div id="tracking-div-${esc(commuteId)}" style="display: flex; align-items: center; gap: 8px;">
            <a href="${esc(trackingUrl)}" target="_blank" style="color: #0066cc; text-decoration: underline;">
              Track Flight →
            </a>
            <button class="btn btn-icon" style="padding: 4px 8px; font-size: 14px; opacity: 1.0;" onclick="editCommuteTracking('${esc(commuteId)}')" title="Edit">✏️</button>
            <button class="btn btn-icon" style="padding: 4px 8px; font-size: 14px; background: #aa3333; opacity: 1.0;" onclick="removeCommuteTracking('${esc(commuteId)}')" title="Remove">🗑️</button>
          </div>
        `;
      } else {
        trackingHtml = `
          <div id="tracking-div-${esc(commuteId)}">
            <input type="url" id="tracking-${esc(commuteId)}" class="commute-tracking-input" 
                   placeholder="https://flightaware.com/..." value="" 
                   style="width: 400px; padding: 6px; border-radius: 6px; border: 1px solid var(--border); background: var(--card); color: var(--text);">
            <button class="btn" style="margin-left: 8px;" onclick="saveCommuteTracking('${esc(commuteId)}')">Save</button>
          </div>
        `;
      }
      
      return `
        <tr class="summary commute" data-commute-id="${esc(commuteId)}" data-report-date="${esc(dateStr)}">
          ${renderCheckCell(row)}
          <td class="sum-first">
            <strong>${label}</strong>
          </td>
          <td data-dw="report">${esc(reportStr)}</td>
          <td data-dw="release">
            ${trackingUrl && trackingUrl !== 'null' && trackingUrl !== '' ? 
              `<a href="${esc(trackingUrl)}" target="_blank" style="color: #0066cc; text-decoration: underline;">Track →</a>` : ''}
            ${arrivalStr ? `<div style="font-size: 0.9em; color: var(--muted); margin-top: 2px;">${esc(arrivalStr)}</div>` : ''}
          </td>
        </tr>
        <tr class="details">
          <td colspan="4">
            <div class="daysbox" style="padding: 16px;">
              <div style="margin-bottom: 12px;">
                <label style="display: block; margin-bottom: 4px; font-weight: 600; color: var(--text);">Commute Report Time:</label>
                <input type="time" id="time-${esc(commuteId)}" class="commute-report-input" value="${esc(timeValue)}" style="padding: 6px; border-radius: 6px; border: 1px solid var(--border); background: var(--card); color: var(--text);">
                <button class="btn" style="margin-left: 8px;" onclick="saveCommuteTime('${esc(commuteId)}')">Save</button>
              </div>
              <div style="margin-bottom: 12px;">
                <label style="display: block; margin-bottom: 4px; font-weight: 600; color: var(--text);">Commute Arrival Time:</label>
                <div style="padding: 6px; color: var(--text);">
                  ${arrivalStr || 'Arrival time will be set based on pairing report time'}
                </div>
              </div>
              <div style="margin-bottom: 12px;">
                <label style="display: block; margin-bottom: 4px; font-weight: 600; color: var(--text);">FlightAware Tracking:</label>
                ${trackingHtml}
              </div>
              <div>
                <button class="btn" style="background: var(--accent); color: #031323;" onclick="hideCommute('${esc(commuteId)}')">Hide This Commute</button>
              </div>
            </div>
          </td>
        </tr>`;
    }

    // Backend provides has_legs and total_legs
    const hasLegs = row.has_legs || false;
    const totalLegs = row.total_legs || 0;
    const isNonPairing = !hasLegs;
    
    // Backend provides out_of_base detection
    const showOOB = row.out_of_base || false;
    const oobAirport = row.out_of_base_airport || '';
    const oobPill = showOOB ? `<span class="pill pill-red">${esc(oobAirport)}</span>` : '';

    const days=row.days||[];
    const detailsDays=hasLegs?days.map((day,i)=>renderDayHTML(row,day,i,days)).join(''):'';

    const noLegsBlock=!hasLegs?`<div data-stop-toggle class="no-legs-block">
           <span class="muted">No legs found.</span>
           <button class="btn" data-hide-pairing="${esc(row.pairing_id||'')}" data-report="${esc(row.report_local_iso||'')}">
             Hide Event
           </button>
         </div>`:'';

    // Use num_days from backend ONLY - no frontend calculations
    const numDays = row.num_days || 1;

    return `
      <tr class="summary ${isNonPairing?'non-pairing':''}" data-row-id="${esc(row.pairing_id||'')}">
        ${renderCheckCell(row)}
        <td class="sum-first">
          <strong>${esc(row.pairing_id||'')}</strong>${pairingNowTag(row)}
          ${hasLegs?`<span class="pill">${numDays} day</span>`:``}
          ${oobPill}
        </td>
        <td data-dw="report">${esc(row.display?.report_str||'')}</td>
        <td data-dw="release">${isNonPairing?'':esc(row.display?.release_str||'')}</td>
      </tr>
      <tr class="details">
        <td colspan="4">
          <div class="daysbox">${hasLegs?detailsDays:noLegsBlock}</div>
        </td>
      </tr>`;
  }

  function renderCheckCell(row){
    const ack=row.ack||{};
    const acknowledged=!!ack.acknowledged;
    const windowOpen=!!ack.window_open;
    const stateAttr=acknowledged?'ok':(windowOpen?'pending':'off');
    const ariaChecked=acknowledged?'true':'false';
    const ariaDisabled=acknowledged?'true':'false';
    return `
      <td class="ckcol">
        <button class="ckbtn ck ${stateAttr}"
                type="button"
                role="checkbox"
                aria-checked="${ariaChecked}"
                aria-disabled="${ariaDisabled}"
                title="${stateAttr==='ok'?'Acknowledged':(stateAttr==='pending'?'Click to view plan / acknowledge':'Click to view reminder plan')}"
                data-ck="${stateAttr}"
                data-pairing="${esc(row.pairing_id||'')}"
                data-report="${esc((ack&&ack.report_local_iso)||row.report_local_iso||'')}">
          <span class="ckbox" aria-hidden="true"></span>
        </button>
      </td>`;
  }

  function renderDayHTML(row,day,idx,days){
    const want24=state.clockMode===24;
    const isMobile=window.matchMedia&&window.matchMedia('(max-width: 640px)').matches;
    const dayISO=dayDateFromRow(row,idx,day);
    const dow=weekdayFromISO(dayISO);
    
    // Check if this is a layover day
    if (day.is_layover) {
      const layoverLocation = day.layover_location || '';
      const hotel = day.hotel || '';
      const noFlightsMsg = day.no_flights_message || '';
      
      return `
        <div class="day">
          <div class="dayhdr">
            <span class="dot"></span>
            <span class="daytitle">Day ${idx+1}</span>
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
    
    const legs=(day.legs||[]).map(leg=>{
      // Backend provides ALL formatted display strings
      const route = leg.route_display || '';
      
      // For block time: use backend formatting but store raw times for clock mode switching
      const depTime = leg.dep_time || '';
      const arrTime = leg.arr_time || '';
      const blockTime = leg.block_display || '';
      
      // Tracking: use backend-provided display and URL
      let trackCell = '';
      if (leg.tracking_url) {
        // Backend provided a URL - make it a clickable link
        trackCell = `<a href="${esc(leg.tracking_url)}" target="_blank" style="color: #0066cc; text-decoration: underline; cursor: pointer;">Track →</a>`;
      } else if (leg.tracking_display) {
        // Backend provided display text only (e.g., "Check FLICA" or "Tracking available Nov 12")
        trackCell = `<span style="color: var(--muted);">${esc(leg.tracking_display)}</span>`;
      } else {
        trackCell = '';
      }
      
      const isDeadhead = leg.deadhead || false;
      
      return `
        <tr class="leg-row ${leg.done?'leg-done':''}${isDeadhead?' leg-deadhead':''}">
          <td>${esc(leg.flight||'')}</td>
          <td>${route}</td>
          <td class="bt" data-dw="bt" data-dep="${esc(depTime)}" data-arr="${esc(arrTime)}">${blockTime}</td>
          <td>${trackCell}</td>
        </tr>`;
    }).join('');
    
    const dayRepRaw=pickHHMM(day.report_hhmm,day.report);
    const dayRelRaw=pickHHMM(day.release_hhmm,day.release);
    const repDisp=fmtHHMM(dayRepRaw,want24,false);
    const relDisp=fmtHHMM(dayRelRaw,want24,false);
    const blockLabel='Block';
    const trackLabel=isMobile?'Track':'Tracking';
    
    return `
      <div class="day">
        <div class="dayhdr">
          <span class="dot"></span>
          <span class="daytitle">Day ${idx+1}</span>
          ${dayRepRaw?` · Report: ${esc(dow)} <span data-dw="day-report" data-hhmm="${esc(dayRepRaw)}">${esc(repDisp)}</span>`:''}
          ${dayRelRaw?` · Release: <span data-dw="day-release" data-hhmm="${esc(dayRelRaw)}">${esc(relDisp)}</span>`:''}
          ${day.hotel?` · Hotel: ${esc(day.hotel)}`:''}
        </div>
        ${legs?`
          <div class="legs-wrap">
            <table class="legs">
              <thead><tr><th>Flight</th><th>Route</th><th>${blockLabel}</th><th>${trackLabel}</th></tr></thead>
              <tbody>${legs}</tbody>
            </table>
          </div>`:``}
      </div>`;
  }

  function dayDateFromRow(row,dayIndex,dayObj){
    const keys=['date_local_iso','local_iso','start_local_iso','date_iso'];
    for(const k of keys){
      if(dayObj&&dayObj[k])return dayObj[k];
    }
    const leg=(dayObj&&dayObj.legs&&dayObj.legs[0])||null;
    if(leg){
      const legKeys=['dep_local_iso','dep_iso','dep_dt_iso','local_iso'];
      for(const k of legKeys){
        if(leg[k])return leg[k];
      }
    }
    if(row&&row.report_local_iso){
      const shifted=new Date(row.report_local_iso);
      if(!isNaN(shifted)){
        shifted.setDate(shifted.getDate()+dayIndex);
        return shifted.toISOString();
      }
    }
    return null;
  }
  
  function weekdayFromISO(iso){
    if(!iso)return'';
    const d=new Date(iso);
    if(isNaN(d))return'';
    return d.toLocaleDateString(undefined,{weekday:'short'});
  }

  function tickStatusLine(){
    if(state.lastPullIso)setText('#last-pull',minutesOnlyAgo(state.lastPullIso));
    
    const etaEl=qs('#next-refresh-eta');
    if(!etaEl||!state.nextRefreshIso)return;
    const leftSec=Math.max(0,Math.floor((new Date(state.nextRefreshIso).getTime()-Date.now())/1000));
    if(leftSec>0){
      const m=Math.ceil(leftSec/60);
      etaEl.textContent=` (in ${m}m)`;
    }else{
      etaEl.textContent=' (refreshing…)';
      const now=Date.now();
      if(!state._zeroKick||now-state._zeroKick>4000){
        state._zeroKick=now;
        renderOnce();
      }
    }
  }

  // Utilities
  function qs(sel){return document.querySelector(sel)}
  function setText(sel,v){const el=qs(sel); if(el) el.textContent=v;}
  function esc(s){return String(s).replace(/[&<>"'`=\/]/g,(ch)=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'}[ch]));}
  function minutesOnlyAgo(iso){
    if(!iso)return'—';
    const d=new Date(iso);
    if(isNaN(d))return'—';
    const sec=Math.max(0,Math.floor((Date.now()-d.getTime())/1000));
    const m=Math.max(0,Math.floor(sec/60));
    if(m<=0)return'just now';
    return `${m}m ago`;
  }
  function safeParseJSON(s){try{return JSON.parse(s||'{}')}catch{return null}}

  // === Resize/orientation/visibility handling ===
  let _rzTimer = null;
  function debouncedApply() {
    if (_rzTimer) clearTimeout(_rzTimer);
    _rzTimer = setTimeout(() => {
      applyCalendarLayout();
    }, 100);
  }

  window.addEventListener('resize', debouncedApply);

  function handleOrientationChange() {
    applyCalendarLayout();
    setTimeout(applyCalendarLayout, 120);
    requestAnimationFrame(() => requestAnimationFrame(applyCalendarLayout));
  }
  
  window.addEventListener('orientationchange', handleOrientationChange);
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) handleOrientationChange();
  });
  window.addEventListener('pageshow', handleOrientationChange);

  const ccObsTarget = document.getElementById('calendar-container');
  if (window.ResizeObserver && ccObsTarget) {
    const ro = new ResizeObserver(() => applyCalendarLayout());
    ro.observe(ccObsTarget);
  }
})();