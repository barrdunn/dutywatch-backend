(function () {
  const cfg = safeParseJSON(document.getElementById('dw-boot')?.textContent) || {};
  const apiBase = cfg.apiBase || '';
  const HOME_BASE = (cfg.baseAirport || 'DFW').toUpperCase();

  const IS_IOS = /iP(ad|hone|od)/.test(navigator.platform)
    || (navigator.userAgent.includes('Mac') && 'ontouchend' in document);
  const IS_TOUCH = IS_IOS || (matchMedia && matchMedia('(pointer: coarse)').matches);
  document.documentElement.classList.toggle('ios', IS_IOS);

  const state = {
    lastPullIso: null,
    nextRefreshIso: null,
    clockMode: cfg.clockMode === '24' ? 24 : 12,
    onlyReports: !!cfg.onlyReports,
    hiddenCount: 0,
    _zeroKick: 0,
  };

  // ===== time helpers =====
  const TIME_12_RE = /\b([0-9]{1,2}):([0-9]{2})\s?(AM|PM)\b/i;
  const TIME_24_RE = /\b([01]?\d|2[0-3]):?([0-5]\d)\b/;
  function to12(h, m){const hh=(h%12)||12;return `${hh}:${m.toString().padStart(2,'0')} ${h<12?'AM':'PM'}`;}
  function to24(h, m, c){const hh=String(h).padStart(2,'0'),mm=String(m).padStart(2,'0');return c?`${hh}:${mm}`:`${hh}${mm}`;}
  function pickHHMM(...c){for(const x of c){if(x==null)continue;const s=String(x).trim();if(/^\d{4}$/.test(s))return s;const m1=s.match(/^(\d{1,2}):(\d{2})$/);if(m1)return `${m1[1].padStart(2,'0')}${m1[2]}`;const m2=s.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);if(m2){let h=parseInt(m2[1],10)%12;const m=parseInt(m2[2],10);if(m2[3].toUpperCase()==='PM')h+=12;return `${String(h).padStart(2,'0')}${String(m).padStart(2,'0')}`;}}return '';}
  function swapFirstTime(str,w,c=true){if(!str)return str;const m12=str.match(TIME_12_RE);if(m12){const h=parseInt(m12[1],10),m=parseInt(m12[2],10),h24=(m12[3].toUpperCase()==='PM'?(h%12)+12:(h%12));return str.replace(TIME_12_RE,w?to24(h24,m,c):`${h}:${String(m).padStart(2,'0')} ${m12[3].toUpperCase()}`);}const m24=str.match(TIME_24_RE);if(m24){const h=parseInt(m24[1],10),m=parseInt(m24[2],10);return str.replace(TIME_24_RE,w?to24(h,m,c):to12(h,m));}return str;}
  function fmtHHMM(hhmm,w,c=true){if(!hhmm)return'';const s=String(hhmm).replace(':','').padStart(4,'0');const hh=parseInt(s.slice(0,2),10),mm=parseInt(s.slice(2),10);return w?to24(hh,mm,c):to12(hh,mm);}
  function combineDateTimeISO(dateIso,hhmm){if(!dateIso||!hhmm)return null;const s=hhmm.replace(':','').padStart(4,'0');const h=parseInt(s.slice(0,2),10),m=parseInt(s.slice(2),10);const d=new Date(dateIso);if(isNaN(d))return null;d.setHours(h,m,0,0);return d;}
  function addDaysISO(iso,days){const d=new Date(iso);if(isNaN(d))return null;d.setDate(d.getDate()+days);return d.toISOString();}

  // ===== actions =====
  window.dwManualRefresh = async function(){try{await fetch(apiBase+'/api/refresh',{method:'POST'});}catch(e){console.error(e);}};

  // ===== controls =====
  const refreshSel=document.getElementById('refresh-mins');
  if(refreshSel){if(cfg.refreshMinutes)refreshSel.value=String(cfg.refreshMinutes);refreshSel.addEventListener('change',async()=>{const minutes=parseInt(refreshSel.value,10);try{await fetch(apiBase+'/api/settings/refresh-seconds',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({seconds:minutes*60})});}catch(e){console.error(e);}});}

  const clockSel=document.getElementById('clock-mode');
  if(clockSel){clockSel.value=String(state.clockMode);clockSel.addEventListener('change',()=>{state.clockMode=parseInt(clockSel.value,10)===24?24:12;repaintTimesOnly();});}

  // ===== hidden chip =====
  function applyHiddenCount(n){state.hiddenCount=Number(n)||0;const chip=qs('#hidden-chip');const countEl=qs('#hidden-count');if(!chip||!countEl)return;countEl.textContent=`Hidden: ${state.hiddenCount}`;chip.classList.toggle('hidden',!(state.hiddenCount>0));}

  // ===== live updates =====
  renderOnce();
  try{const es=new EventSource(apiBase+'/api/events');es.addEventListener('change',async()=>{await renderOnce();});es.addEventListener('schedule_update',async()=>{await renderOnce();});es.addEventListener('hidden_update',async()=>{await renderOnce();});}catch{}

  setInterval(tickStatusLine,1000);

  // ===== modal (plan) =====
  const planModal=qs('#plan-modal');const planClose1=qs('#plan-close-1');const planClose2=qs('#plan-close-2');[planClose1,planClose2].forEach(b=>b&&b.addEventListener('click',closePlan));
  function openPlan(pairingId,reportIso){if(!planModal)return;planModal.classList.remove('hidden');document.body.classList.add('modal-open');const url=`${apiBase}/api/ack/plan?pairing_id=${encodeURIComponent(pairingId)}&report_local_iso=${encodeURIComponent(reportIso||'')}`;setText('#plan-meta','Loading…');qs('#plan-rows').innerHTML='';fetch(url).then(r=>r.json()).then(data=>{const attempts=data.attempts||[];setText('#plan-meta',`Window: starts ${(data.policy?.push_start_hours||12)}h before report; includes calls during non-quiet hours.`);qs('#plan-rows').innerHTML=attempts.map(a=>{const when=new Date(a.at_iso);const label=a.kind==='push'?'Push':'Call';const details=a.kind==='call'?`Ring ${a?.meta?.ring||1}`:'';return `<tr><td>${when.toLocaleString()}</td><td style="text-align:center">${label}</td><td>${details}</td></tr>`;}).join('');}).catch(()=>setText('#plan-meta','Unable to load plan.')); }
  function closePlan(){if(!planModal)return;planModal.classList.add('hidden');document.body.classList.remove('modal-open');}

  // ===== clicks =====
  document.addEventListener('click',async(e)=>{
    const unhide=e.target.closest('[data-unhide-all]');if(unhide){e.preventDefault();try{await fetch(`${apiBase}/api/hidden/unhide_all`,{method:'POST'});}catch(err){console.error('unhide all failed',err);}await renderOnce();return;}
    if(e.target.closest('[data-stop-toggle]')){e.stopPropagation();}
    const sum=e.target.closest('tr.summary');if(sum&&!e.target.closest('[data-ck]')){sum.classList.toggle('open');const details=sum.nextElementSibling;if(!details||!details.classList.contains('details'))return;const open=sum.classList.contains('open');details.querySelectorAll('.day .legs').forEach(tbl=>{tbl.style.display=open?'table':'none';});return;}
    const ck=e.target.closest('[data-ck]');if(ck){e.preventDefault();const pairingId=ck.getAttribute('data-pairing')||'';const reportIso=ck.getAttribute('data-report')||'';openPlan(pairingId,reportIso);return;}
    const hideBtn=e.target.closest('[data-hide-pairing]');if(hideBtn){e.preventDefault();e.stopPropagation();const pairingId=hideBtn.getAttribute('data-hide-pairing')||'';const reportIso=hideBtn.getAttribute('data-report')||'';const details=hideBtn.closest('tr.details');const summary=details?.previousElementSibling;if(summary?.classList.contains('summary')){summary.remove();details.remove();applyHiddenCount(state.hiddenCount+1);}try{await fetch(`${apiBase}/api/hidden/hide`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({pairing_id:pairingId,report_local_iso:reportIso})});}catch(err){console.error('hide failed',err);}await renderOnce();return;}
  });

  // ===== render =====
  async function renderOnce(){
    const params=new URLSearchParams({is_24h:'0',only_reports:state.onlyReports?'1':'0'});
    let data;try{const res=await fetch(`${apiBase}/api/pairings?${params.toString()}`,{cache:'no-store'});data=await res.json();}catch(e){console.error('Failed to fetch /api/pairings',e);return;}

    state.lastPullIso=data.last_pull_local_iso||null;
    state.nextRefreshIso=data.next_pull_local_iso||null;

    const hintedHidden=(data&&(data.hidden_count??(data.hidden&&data.hidden.count)));applyHiddenCount(typeof hintedHidden==='number'?hintedHidden:state.hiddenCount);

    const label=(data.window&&data.window.label)||data.looking_through||'—';setText('#looking-through',label);

    setText('#last-pull',minutesOnlyAgo(state.lastPullIso));
    const base=(data.next_pull_local&&data.tz_label)?`${data.next_pull_local} (${data.tz_label})`:'—';
    const nextEl=qs('#next-refresh');if(nextEl)nextEl.innerHTML=`${esc(base)} <span id="next-refresh-eta"></span>`;

    const rows=data.rows||[];
    let firstOffIndex=-1;for(let i=0;i<rows.length;i++){if(rows[i]&&rows[i].kind==='off'){firstOffIndex=i;break;}}
    const tbody=qs('#pairings-body');
    tbody.innerHTML=rows.map((row,idx)=>renderRowHTML(row,HOME_BASE,idx===firstOffIndex)).join('');

    repaintTimesOnly();
  }

  function repaintTimesOnly(){
    const want24=state.clockMode===24;
    document.querySelectorAll('[data-dw="report"]').forEach(el=>{
      // Skip OFF rows - they don't need time conversion
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
    document.querySelectorAll('[data-dw="day-report"]').forEach(el=>{const raw=el.getAttribute('data-hhmm')||el.textContent;el.textContent=fmtHHMM(raw,want24,false);});
    document.querySelectorAll('[data-dw="day-release"]').forEach(el=>{const raw=el.getAttribute('data-hhmm')||el.textContent;el.textContent=fmtHHMM(raw,want24,false);});
    document.querySelectorAll('[data-dw="bt"]').forEach(el=>{const dep=el.getAttribute('data-dep')||'';const arr=el.getAttribute('data-arr')||'';const left=fmtHHMM(dep,want24,false);const right=fmtHHMM(arr,want24,false);el.textContent=arr?`${left} → ${right}`:left;});
  }

  function firstDepartureAirport(row){const days=row?.days||[];for(const d of days){const legs=d?.legs||[];if(legs.length&&legs[0].dep)return String(legs[0].dep).toUpperCase();}return null;}
  function wrapNotBoldBits(text,token,smallToken){if(!text)return'';const re=new RegExp(`\\s*\\(${token}\\)`,'i');if(re.test(text)){const base=text.replace(re,'').trim();const isSmall=window.matchMedia&&window.matchMedia('(max-width: 640px)').matches;const shown=isSmall&&smallToken?smallToken:token;return `${esc(base)} <span style="font-weight:400!important">(${shown})</span>`;}return esc(text);}
  function legsCount(row){let n=0;const days=row?.days||[];for(const d of days)n+=(d.legs||[]).length;return n;}
  function pairingNowTag(row){return row?.in_progress?' <span style="font-weight:400!important">(Now)</span>':'';}

  // ===== row renderer =====
  function renderRowHTML(row,homeBase,isFirstOff=false){
    if(row.kind==='off'){
      const rawLabel=(row.display&&row.display.off_label)?String(row.display.off_label):'OFF';
      const labelHTML=wrapNotBoldBits(rawLabel,'Now');

      let dur=String(row.display?.off_dur||'').trim();
      if(/\(.*remaining.*\)/i.test(dur)){
        dur = dur.replace(/\(\s*remaining\s*\)/i, ' <span style="font-weight:400!important">(Remaining)</span>');
      }else if(isFirstOff){
        dur = `${dur} <span style="font-weight:400!important">(Remaining)</span>`;
      }

      return `
        <tr class="off">
          <td class="ckcol"></td>
          <td class="sum-first"><span class="off-label">${labelHTML}</span></td>
          <td class="off-dur" data-dw="report">${dur}</td>
          <td data-dw="release"></td>
        </tr>`;
    }

    const totalLegs=legsCount(row);
    const hasLegs=totalLegs>0;
    const startDep=firstDepartureAirport(row);
    const showOOB=!!(startDep&&startDep!==homeBase);
    const oobPill=showOOB?`<span class="pill pill-red">${esc(startDep)}</span>`:'';

    const days=row.days||[];
    const detailsDays=hasLegs?days.map((day,i)=>renderDayHTML(row,day,i,days)).join(''):'';

    const noLegsBlock=!hasLegs?`<div data-stop-toggle style="display:flex;justify-content:center;align-items:center;gap:12px;padding:18px;text-align:center">
           <span class="muted">No legs found.</span>
           <button class="btn" data-hide-pairing="${esc(row.pairing_id||'')}" data-report="${esc(row.report_local_iso||'')}">
             Hide Event
           </button>
         </div>`:'';

    return `
      <tr class="summary" data-row-id="${esc(row.pairing_id||'')}">
        ${renderCheckCell(row)}
        <td class="sum-first">
          <strong>${esc(row.pairing_id||'')}</strong>${pairingNowTag(row)}
          ${hasLegs?`<span class="pill">${row.days?.length||1} day</span>`:``}
          ${oobPill}
        </td>
        <td data-dw="report">${esc(row.display?.report_str||'')}</td>
        <td data-dw="release">${esc(row.display?.release_str||'')}</td>
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
    const dayISO=dayDateFromRow(row,idx,day);
    const dow=weekdayFromISO(dayISO);
    const legs=(day.legs||[]).map(leg=>{
      const depHHMM=pickHHMM(leg.dep_hhmm,leg.dep_time);
      const arrHHMM=pickHHMM(leg.arr_hhmm,leg.arr_time);
      const left=fmtHHMM(depHHMM,want24,false);
      const right=fmtHHMM(arrHHMM,want24,false);
      let trackCell=''; if(dayISO){const d=new Date(dayISO);trackCell=`Avail. ${d.getMonth()+1}/${d.getDate()}`;}
      return `
        <tr class="leg-row ${leg.done?'leg-done':''}">
          <td>${esc(leg.flight||'')}</td>
          <td>${esc(leg.dep||'')}–${esc(leg.arr||'')}</td>
          <td class="bt" data-dw="bt" data-dep="${esc(depHHMM)}" data-arr="${esc(arrHHMM)}">${left} → ${right}</td>
          <td>${trackCell}</td>
        </tr>`;
    }).join('');
    const dayRepRaw=pickHHMM(day.report_hhmm,day.report);
    const dayRelRaw=pickHHMM(day.release_hhmm,day.release);
    const repDisp=fmtHHMM(dayRepRaw,want24,false);
    const relDisp=fmtHHMM(dayRelRaw,want24,false);
    return `
      <div class="day">
        <div class="dayhdr">
          <span class="dot"></span>
          <span class="daytitle">Day ${idx+1}</span>
          ${dayRepRaw?`· Report: ${esc(dow)} <span data-dw="day-report" data-hhmm="${esc(dayRepRaw)}">${esc(repDisp)}</span>`:''}
          ${dayRelRaw?` · Release: <span data-dw="day-release" data-hhmm="${esc(dayRelRaw)}">${esc(relDisp)}</span>`:''}
          ${day.hotel?` · Hotel: ${esc(day.hotel)}`:''}
        </div>
        ${legs?`
          <div class="legs-wrap">
            <table class="legs" style="display:none">
              <thead><tr><th>Flight</th><th>Route</th><th>Block Times</th><th>Flight Tracking</th></tr></thead>
              <tbody>${legs}</tbody>
            </table>
          </div>`:``}
      </div>`;
  }

  // ===== date utils =====
  function dayDateFromRow(row,dayIndex,dayObj){
    const keys=['date_local_iso','local_iso','start_local_iso','date_iso'];for(const k of keys){if(dayObj&&dayObj[k])return dayObj[k];}
    const leg=(dayObj&&dayObj.legs&&dayObj.legs[0])||null;
    if(leg){const legKeys=['dep_local_iso','dep_iso','dep_dt_iso','local_iso'];for(const k of legKeys){if(leg[k])return leg[k];}}
    if(row&&row.report_local_iso){const shifted=new Date(row.report_local_iso);if(!isNaN(shifted)){shifted.setDate(shifted.getDate()+dayIndex);return shifted.toISOString();}}
    return null;
  }
  function weekdayFromISO(iso){if(!iso)return'';const d=new Date(iso);if(isNaN(d))return'';return d.toLocaleDateString(undefined,{weekday:'short'});}

  function tickStatusLine(){if(state.lastPullIso)setText('#last-pull',minutesOnlyAgo(state.lastPullIso));const etaEl=qs('#next-refresh-eta');if(!etaEl||!state.nextRefreshIso)return;const leftSec=Math.max(0,Math.floor((new Date(state.nextRefreshIso).getTime()-Date.now())/1000));if(leftSec>0){const m=Math.ceil(leftSec/60);etaEl.textContent=`in ${m}m`;}else{etaEl.textContent='(refreshing…)';const now=Date.now();if(!state._zeroKick||now-state._zeroKick>4000){state._zeroKick=now;renderOnce();}}}

  function qs(sel){return document.querySelector(sel)}
  function setText(sel,v){const el=qs(sel); if(el) el.textContent=v;}
  function esc(s){return String(s).replace(/[&<>"'`=\/]/g,(ch)=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'}[ch]));}
  function minutesOnlyAgo(iso){if(!iso)return'—';const d=new Date(iso);if(isNaN(d))return'—';const sec=Math.max(0,Math.floor((Date.now()-d.getTime())/1000));const m=Math.max(0,Math.floor(sec/60));if(m<=0)return'just now';return `${m}m ago`;}
  function safeParseJSON(s){try{return JSON.parse(s||'{}')}catch{return null}}
})();