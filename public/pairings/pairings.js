const API = {
  pairings: (params={}) => {
    const q = new URLSearchParams(params).toString();
    return fetch(`/api/pairings?${q}`).then(r=>r.json());
  },
  ackPlan: (pairing_id, report_local_iso) =>
    fetch(`/api/ack/plan?pairing_id=${encodeURIComponent(pairing_id)}&report_local_iso=${encodeURIComponent(report_local_iso)}`).then(r=>r.json()),
  acknowledge: (pairing_id, report_local_iso) =>
    fetch(`/api/ack/acknowledge`, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({pairing_id, report_local_iso})}).then(r=>r.json()),
};

const els = {
  body: document.getElementById('pairingsBody'),
  looking: document.getElementById('lookingThrough'),
  clock: document.getElementById('clockMode'),
  refreshSel: document.getElementById('refreshMinutes'),
  manualRefresh: document.getElementById('manualRefresh'),
  status: document.getElementById('statusLine'),
  planModal: document.getElementById('planModal'),
  closePlanModal: document.getElementById('closePlanModal'),
  closePlanModal2: document.getElementById('closePlanModal2'),
  planRows: document.getElementById('planRows'),
  planMeta: document.getElementById('planMeta'),
};

let serverAckPolicy = null;
let is24h = false;

function fmtClock(hhmm) {
  if (!hhmm) return '';
  const h = parseInt(hhmm.slice(0,2),10);
  const m = hhmm.slice(2);
  if (is24h) return `${`${h}`.padStart(2,'0')}${m}`;
  const ampm = h < 12 ? 'AM' : 'PM';
  const h12 = (h % 12) || 12;
  return `${h12}:${m} ${ampm}`;
}

function setRefreshOptions(serverMinutes){
  els.refreshSel.innerHTML = '';
  const options = [5,10,15,30,60,120,240];
  for (const m of options){
    const opt = document.createElement('option');
    opt.value = m;
    opt.textContent = `${m} min`;
    if (m === serverMinutes) opt.selected = true;
    els.refreshSel.appendChild(opt);
  }
}

function render(rows, meta){
  els.body.innerHTML = '';
  els.looking.textContent = meta.looking_through;
  serverAckPolicy = meta.ack_policy || serverAckPolicy;

  let prevWasPairing = false;
  rows.forEach((r,i) => {
    if (r.kind === 'pairing'){
      const tr = document.createElement('tr');
      tr.className = 'pairing';
      // leading check-in
      const tdChk = document.createElement('td');
      const chk = document.createElement('span');
      chk.className = 'check-icon';
      chk.title = 'Check-in';
      const ack = r.ack || {};
      if (ack.acknowledged){
        chk.classList.add('done');
        chk.textContent = '✓';
        chk.addEventListener('click', (e)=> {
          e.stopPropagation();
          // already acked; show plan anyway for transparency
          openPlan(r);
        });
      }else if (ack.window_open){
        chk.classList.add('pending');
        chk.textContent = '•';
        chk.addEventListener('click', async (e) => {
          e.stopPropagation();
          // acknowledge now
          await API.acknowledge(r.pairing_id, r.report_local_iso || '');
          // refresh UI
          load();
        });
      }else{
        chk.classList.add('disabled');
        chk.textContent = '•';
        chk.addEventListener('click', (e)=> {
          e.stopPropagation();
          openPlan(r); // show the "what will happen" table
        });
      }
      tdChk.appendChild(chk);

      const tdMain = document.createElement('td');
      const left = document.createElement('div');
      left.className = 'checkin';
      const title = document.createElement('strong');
      title.textContent = r.pairing_id;
      title.style.marginRight = '8px';

      // pills
      const pillDays = document.createElement('span');
      pillDays.className = 'pill';
      const daysCount = (r.days || []).length || 1;
      pillDays.textContent = `${daysCount} day${daysCount>1?'s':''}`;
      left.appendChild(title);
      left.appendChild(pillDays);

      if (r.in_progress){
        const pillIn = document.createElement('span');
        pillIn.className = 'pill pill-inprog';
        pillIn.textContent = 'in progress';
        left.appendChild(pillIn);
      }

      const right = document.createElement('div');
      right.className = 'small';
      right.textContent = `${r.display.report_str} → ${r.display.release_str}`;

      tdMain.appendChild(left);
      tdMain.appendChild(right);

      tr.appendChild(tdChk);
      tr.appendChild(tdMain);
      els.body.appendChild(tr);

      // render day rows (always visible)
      (r.days || []).forEach((d, idx) => {
        const dtr = document.createElement('tr');
        dtr.className = 'day';
        const pad = document.createElement('td'); pad.textContent = '';
        const td = document.createElement('td');
        const legs = d.legs || [];
        const firstLeg = legs[0] || {};
        const hotel = d.hotel ? ` • ${d.hotel}` : '';
        td.textContent = [
          d.report ? `Report ${fmtClock(d.report)}` : '',
          firstLeg.dep && firstLeg.arr ? `${firstLeg.dep}→${firstLeg.arr}` : '',
          hotel
        ].filter(Boolean).join('  ');
        dtr.appendChild(pad); dtr.appendChild(td);
        els.body.appendChild(dtr);

        // flight rows (collapsed until click? spec says day rows are always shown, flights expand on click)
        legs.forEach((leg, ix) => {
          const ftr = document.createElement('tr');
          ftr.className = 'flight';
          const pad2 = document.createElement('td'); pad2.textContent = '';
          const tdf = document.createElement('td');
          tdf.textContent = `${leg.flight || ''}  ${leg.dep} ${leg.dep_time_str} → ${leg.arr} ${leg.arr_time_str}`;
          ftr.appendChild(pad2); ftr.appendChild(tdf);
          els.body.appendChild(ftr);
        });
      });

      // round out grouping spacing
      const spacer = document.createElement('tr');
      spacer.className = 'next';
      const s1 = document.createElement('td'); s1.textContent = '';
      const s2 = document.createElement('td'); s2.textContent = '';
      spacer.appendChild(s1); spacer.appendChild(s2);
      els.body.appendChild(spacer);

      prevWasPairing = true;
    } else if (r.kind === 'off'){
      const tr = document.createElement('tr');
      tr.className = 'off';
      const td1 = document.createElement('td'); td1.textContent = '';
      const td2 = document.createElement('td'); td2.textContent = `OFF • ${r.display.off_dur}`;
      tr.appendChild(td1); tr.appendChild(td2);
      els.body.appendChild(tr);
      prevWasPairing = false;
    }
  });
}

function openPlan(pairingRow){
  const pairing_id = pairingRow.pairing_id;
  const report_iso = pairingRow.report_local_iso || '';
  els.planRows.innerHTML = '';
  els.planMeta.textContent = 'Loading plan…';
  els.planModal.classList.remove('hidden');

  API.ackPlan(pairing_id, report_iso).then(data => {
    els.planMeta.textContent = `Pairing ${pairing_id} • Report ${new Date(report_iso).toLocaleString()}`;
    const tbody = els.planRows;
    tbody.innerHTML = '';
    (data.attempts || []).forEach(at => {
      const tr = document.createElement('tr');
      const when = new Date(at.at_iso);
      const tdWhen = document.createElement('td');
      tdWhen.textContent = when.toLocaleString();
      const tdType = document.createElement('td');
      tdType.textContent = at.kind === 'call' ? 'Call' : 'Push';
      const tdDet = document.createElement('td');
      tdDet.textContent = at.label;
      tr.appendChild(tdWhen); tr.appendChild(tdType); tr.appendChild(tdDet);
      tbody.appendChild(tr);
    });
  }).catch(() => {
    els.planMeta.textContent = 'Failed to load plan.';
  });
}

function closePlan(){ els.planModal.classList.add('hidden'); }

async function load(){
  const params = { is_24h: (is24h?1:0), only_reports: 1 };
  const data = await API.pairings(params);
  setRefreshOptions(data.refresh_minutes);
  render(data.rows || [], data);
  els.status.textContent = `Last pull ${data.last_pull_local} • Next @ ${data.next_pull_local}`;
}

function init(){
  els.clock.addEventListener('change', ()=>{ is24h = (els.clock.value === '24'); load(); });
  els.refreshSel.addEventListener('change', async ()=> {
    const minutes = parseInt(els.refreshSel.value, 10);
    await fetch('/api/settings/refresh-seconds', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({seconds: minutes*60})});
    load();
  });
  els.manualRefresh.addEventListener('click', async ()=> {
    await fetch('/api/refresh', {method:'POST'});
    load();
  });
  els.closePlanModal.addEventListener('click', closePlan);
  els.closePlanModal2.addEventListener('click', closePlan);

  // Initial
  load();

  // Optional SSE to auto-refresh on server change
  try{
    const es = new EventSource('/api/events');
    es.addEventListener('change', ()=> load());
    es.addEventListener('schedule_update', ()=> load());
  }catch(e){}
}

document.addEventListener('DOMContentLoaded', init);
