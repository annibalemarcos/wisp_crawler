const $=s=>document.querySelector(s); const $$=s=>Array.from(document.querySelectorAll(s));
const state={jobs:[],patterns:[],seeds:[],graphs:{},graph:{nodes:[],edges:[]},graphView:{nodes:[],edges:[]},results:[],externalReport:{candidates:[],resolved:[],failed:[],errors:[]},skippedRows:[],patternLab:{urls:[],results:[]},liveEvents:[],blacklist:[],configParsed:null,discoveryCandidates:[],rerunId:null,pan:{x:0,y:0,z:1},drag:false,last:null};
const esc=v=>String(v??'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
const trunc=(v,n=90)=>{v=String(v??'');return v.length>n?v.slice(0,n)+'…':v};

function valueOf(row, field){
  if(!field || field==='global') return JSON.stringify(row??{});
  const direct={
    id:row?.id, url:row?.url, status:row?.status||row?.error||'', source:row?.source, depth:row?.depth,
    job:row?.job_id||row?.id||liveJobId(row)||'', time:row?.ts||row?.ts_unix||'', event:row?.event,
    payload:row?.payload?JSON.stringify(row.payload):'', pattern:row?.pattern, domain:row?.domain,
    values:[...(row?.values||[]),...(row?.suggested_values||[]),...(row?.active_values||[])].join(' '),
    examples:(row?.examples||[]).join(' '), notes:row?.notes, priority:row?.priority, enabled:row?.enabled,
    timer:row?.elapsed_seconds, heartbeat:row?.seconds_since_heartbeat, pages:row?.pages_crawled??row?.progress?.pages_crawled??row?.progress?.pages,
    queue:row?.progress?.queued??row?.checkpoint?.queue??'', checkpoint:row?.checkpoint?.saved_at||row?.checkpoint?.pages||row?.checkpoint?.queue||'',
    updated:row?.updated_at||row?.finished_at||row?.started_at||row?.created_at, created:row?.created_at,
    external:row?.progress?.external_resolved??0,
    bucket:row?.bucket, from:row?.from||row?.from_url||row?.parent, target:row?.target,
    method:row?.method, reason:row?.reason||row?.error, title:row?.title
  };
  if(field==='masked' || field==='masked_url') return row?.masked_url||row?.url||'';
  return direct[field] ?? row?.[field] ?? '';
}
function matchesSearch(row, q, scope){
  q=String(q||'').trim().toLowerCase();
  if(!q) return true;
  return String(valueOf(row, scope||'global')??'').toLowerCase().includes(q);
}
function sortRows(rows, field, dir='asc'){
  rows=[...(rows||[])]; if(!field) return rows;
  const mul=dir==='desc'?-1:1;
  rows.sort((a,b)=>{
    let av=valueOf(a,field), bv=valueOf(b,field);
    const an=Number(av), bn=Number(bv);
    if(av!=='' && bv!=='' && !Number.isNaN(an) && !Number.isNaN(bn)) return (an-bn)*mul;
    av=String(av??'').toLowerCase(); bv=String(bv??'').toLowerCase();
    return av.localeCompare(bv, undefined, {numeric:true, sensitivity:'base'})*mul;
  });
  return rows;
}

function sortTh(table, field, label){
  const cfg={jobs:['jobSort','jobSortDir'],results:['resultsSort','resultsSortDir'],external:['externalSort','externalSortDir'],skipped:['skipSort','skipSortDir']}[table];
  let active=false, dir='asc';
  if(cfg){const s=$('#'+cfg[0]), d=$('#'+cfg[1]); active=s&&s.value===field; dir=d?.value||'asc';}
  const arrow=active?(dir==='asc'?' ▲':' ▼'):'';
  return `<th class="sortable ${active?'active':''}" onclick="setTableSort('${table}','${field}')" title="Sort by ${esc(label)}">${esc(label)}${arrow}</th>`;
}
function setTableSort(table, field){
  const cfg={jobs:['jobSort','jobSortDir',renderJobs],results:['resultsSort','resultsSortDir',renderResults],external:['externalSort','externalSortDir',renderExternalReport],skipped:['skipSort','skipSortDir',renderSkipped]}[table];
  if(!cfg)return;
  const [sortId,dirId,render]=cfg, sort=$('#'+sortId), dir=$('#'+dirId);
  if(sort&&dir){
    if(sort.value===field){dir.value=dir.value==='asc'?'desc':'asc';}
    else{sort.value=field; dir.value=['timer','pages','queue','depth'].includes(field)?'desc':'asc';}
  }
  render();
}
function addFilterListeners(ids, cb){ids.forEach(id=>{const el=$('#'+id); if(el) el.addEventListener(el.tagName==='INPUT'?'input':'change', cb)})}
async function refreshAfterJobCreate(){
  await new Promise(r=>setTimeout(r,250));
  await loadJobs();
  populateJobSelects(); populateExtraJobSelects(); loadStats();
}

async function api(url,opt={}){const r=await fetch(url,{headers:{'Content-Type':'application/json'},...opt}); if(!r.ok)throw new Error(await r.text()); return r.json()}
function toast(msg,type=''){const el=document.createElement('div');el.className='toast '+type;el.textContent=msg;$('#toast').appendChild(el);setTimeout(()=>el.remove(),4200)}
const titles={massive:['Massive Job','Create connected batches: roots + known directories + shared intelligence options.'],run:['Run Job','Start focused crawls, then expand intelligently.'],live:['Live Events','Dirty telemetry console: search, filter, pause, replay and export crawler logs.'],jobs:['Jobs','Track crawls, re-run discoveries and jump into results.'],graph:['Graph Explorer','Pick recent jobs, filter the map and inspect nodes without visual spaghetti.'],patterns:['Pattern Intelligence','Promote useful URL structures and kill noisy patterns.'],seeds:['Pattern Seeds','Manual hypotheses for smarter crawls.'],results:['Results','Search, filter and export discovered URLs.'],bulk:['Bulk Run','Launch multiple targets with controlled options.'],external:['External Resolver Report','Audit masked outbound redirects: candidates, resolved, failed and errors.'],skipped:['Why Not Crawled?','Explain skipped URLs instead of leaving a black box.'],patternlab:['Pattern Lab','Preview, probe and seed URL templates before a crawl.'],blacklist:['Blacklist Dirs','Edit skip rules for noisy paths. Slash optional, regex supported.'],configio:['Config Import/Export','Paste, parse, export and launch reusable WISP job configs.'],discovery:['Directory Discovery','Hybrid mode: known directory + section root + root + keyword siblings.']};
function show(view){$$('.view').forEach(v=>v.classList.remove('active'));$('#'+view).classList.add('active');$$('.sideNav button').forEach(b=>b.classList.toggle('active',b.dataset.view===view));$('#viewTitle').textContent=titles[view][0];$('#viewSub').textContent=titles[view][1];loadCurrent()}
$$('.sideNav button').forEach(b=>b.onclick=()=>show(b.dataset.view));$('#refreshBtn').onclick=()=>loadCurrent();
function loadCurrent(){const id=$$('.view').find(v=>v.classList.contains('active'))?.id; loadStats(); if(id==='live')loadLiveHistory(); if(id==='jobs')loadJobs(); if(id==='graph')loadGraph(); if(id==='patterns')loadPatterns(); if(id==='seeds')loadSeeds(); if(id==='results')loadResults(); if(id==='external')loadExternalReport(); if(id==='skipped')loadSkipped(); if(id==='patternlab')populateExtraJobSelects(); if(id==='blacklist')loadBlacklist(); if(id==='configio')populateConfigJobs(); if(id==='discovery')initDiscoveryDefaults(); if(id==='massive')renderMassivePreview();}
function clock(){const d=new Date();$('#clock').textContent=d.toLocaleString()} setInterval(clock,1000);clock();
async function loadStats(){try{const s=await api('/api/stats'); $('#stats').innerHTML=[['edges',s.edges],['jobs',s.jobs],['nodes',s.nodes],['patterns',s.patterns],['running',s.running],['services',s.services]].map(([k,v])=>`<div class="stat"><b>${v??0}</b><small>${k}</small></div>`).join('')}catch(e){}}
function preset(name){const p={safe:[2,100,false,false,true],balanced:[3,200,true,true,true],deep:[6,1000,true,true,true]};const v=p[name]; if(!v)return;$('#runDepth').value=v[0];$('#runPages').value=v[1];$('#optPattern').checked=v[2];$('#optRoutes').checked=v[3];$('#optProbe').checked=v[4]}

[['optMaskedAggressive','optMaskedOutbound'],['bulkMaskedAggressive','bulkMaskedOutbound'],['rrMaskedAggressive','rrMaskedOutbound'],['massiveMaskedAggressive','massiveMaskedOutbound']].forEach(([ag,base])=>{const a=$('#'+ag),b=$('#'+base); if(a&&b){a.addEventListener('change',()=>{if(a.checked)b.checked=true})}});
$('#runPreset').onchange=e=>preset(e.target.value); $$('.quickHints button').forEach(b=>b.onclick=()=>preset(b.dataset.preset)); $('#clearRunBtn').onclick=()=>{$('#runUrl').value=''};
$('#startBtn').onclick=async()=>{const url=$('#runUrl').value.trim(); if(!url)return toast('URL primeiro, comandante.','err'); const payload={url,depth:+$('#runDepth').value,max_depth:+$('#runDepth').value,max_pages:+$('#runPages').value,pattern_expansion:$('#optPattern').checked,route_inference:$('#optRoutes').checked,soft_probe:$('#optProbe').checked,use_pattern_seeds:$('#optSeeds').checked,follow_masked_outbound:$('#optMaskedOutbound').checked,masked_outbound_aggressive:$('#optMaskedAggressive').checked,masked_detail_boost:$('#optMaskedAggressive').checked,masked_outbound_limit:+$('#optMaskedLimit').value||500,auto_pagination:$('#optPagination').checked,pagination_limit:+$('#optPaginationLimit').value||25,use_blacklist_dirs:$('#optBlacklist')?$('#optBlacklist').checked:true}; try{const j=await api('/api/run',{method:'POST',body:JSON.stringify(payload)});toast('Crawl started: '+(j.id||j.job_id||''),'ok');$('#liveState').textContent='running';await refreshAfterJobCreate();show('jobs')}catch(e){toast('Failed to start: '+e.message,'err')}};
async function loadJobs(){try{state.jobs=await api('/api/jobs'); populateJobSelects(); populateExtraJobSelects(); autoResumeNotice(); renderJobs()}catch(e){toast('Failed to load jobs','err')}}
function fmtDuration(sec){sec=Number(sec||0);const h=Math.floor(sec/3600),m=Math.floor((sec%3600)/60),s=Math.floor(sec%60);return (h?String(h).padStart(2,'0')+':':'')+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0')}
function jobAge(j){if(['running','paused','pause_requested','restarting','cancel_requested'].includes(j.status))return fmtDuration(j.elapsed_seconds||0);return j.elapsed_seconds?fmtDuration(j.elapsed_seconds):'—'}
function heartbeatLabel(j){const s=j.seconds_since_heartbeat; if(s===undefined||s===null)return '—'; return s>90?`stale ${s}s`:`${s}s ago`}
async function jobAction(id,action){try{let opts={method:'POST'};let url=`/api/jobs/${encodeURIComponent(id)}/${action}`; if(action==='delete'){opts={method:'DELETE'};url=`/api/jobs/${encodeURIComponent(id)}`;} const r=await api(url,opts); toast(`${action}: ${id}`,'ok'); await loadJobs(); if(typeof loadLiveHistory==='function') await loadLiveHistory(); return r}catch(e){toast(`${action} failed: ${e.message}`,'err')}}
function confirmJobDelete(id){if(!confirm(`Delete job ${id}? This removes job, graph and checkpoint.`))return; jobAction(id,'delete')}
function renderJobControls(j){const id=esc(j.id);const st=j.status||'';let buttons=[]; if(['running','pause_requested','restarting'].includes(st))buttons.push(`<button onclick="jobAction('${id}','pause')">Pause</button>`); if(['paused','cancelled','failed'].includes(st))buttons.push(`<button onclick="jobAction('${id}','resume')">Resume</button>`); buttons.push(`<button onclick="jobAction('${id}','restart')" title="Restart from last checkpoint">Restart</button>`); if(['running','paused','pause_requested','restarting','queued'].includes(st))buttons.push(`<button class="warn" onclick="jobAction('${id}','cancel')">Stop</button>`); buttons.push(`<button class="danger" onclick="confirmJobDelete('${id}')">Delete</button>`); return buttons.join('')}
function renderRunningPanel(){const el=$('#runningJobsPanel'); if(!el)return; const active=state.jobs.filter(j=>['running','paused','pause_requested','restarting','cancel_requested','queued'].includes(j.status)); if(!active.length){el.innerHTML='<div class="empty">No running jobs. Paz no reino.</div>';return} el.innerHTML=`<table><thead><tr><th>Job</th><th>Status</th><th>Timer</th><th>Heartbeat</th><th>Pages</th><th>Queue</th><th>Checkpoint</th><th>Actions</th></tr></thead><tbody>${active.map(j=>`<tr><td class="codeLine">${esc(j.id)}<br><span class="urlCell">${esc(trunc(j.url,70))}</span></td><td><span class="pill status-${esc(j.status)}">${esc(j.status)}</span></td><td class="timerCell">${jobAge(j)}</td><td>${heartbeatLabel(j)}</td><td>${esc(j.progress?.pages??j.progress?.pages_crawled??0)}</td><td>${esc(j.progress?.queued??j.checkpoint?.queue??'—')}</td><td class="codeLine">${esc(j.checkpoint?.saved_at||'—')}<br>${j.checkpoint?.current_url?esc(trunc(j.checkpoint.current_url,60)):''}</td><td><div class="actions">${renderJobControls(j)}</div></td></tr>`).join('')}</tbody></table>`}
function filteredJobs(){const q=$('#jobSearch')?.value||'',scope=$('#jobSearchScope')?.value||'global',st=$('#jobStatus')?.value||'';let rows=state.jobs.filter(j=>(!st||j.status===st)&&matchesSearch(j,q,scope));return sortRows(rows,$('#jobSort')?.value||'updated',$('#jobSortDir')?.value||'desc')}
function renderJobs(){const jobs=filteredJobs(); renderRunningPanel(); if(!jobs.length){$('#jobsList').innerHTML='<div class="empty">No jobs found</div>';return} $('#jobsList').innerHTML=`<table><thead><tr>${sortTh('jobs','id','ID')}<th>URL</th>${sortTh('jobs','status','Status')}${sortTh('jobs','timer','Timer')}<th>Heartbeat</th><th>Progress</th>${sortTh('jobs','pages','Pages')}${sortTh('jobs','queue','Queue')}<th>External</th>${sortTh('jobs','checkpoint','Checkpoint')}<th>Actions</th></tr></thead><tbody>${jobs.map(j=>`<tr><td class="codeLine">${esc(j.id)}</td><td class="urlCell">${esc(j.url)}</td><td><span class="pill status-${esc(j.status||'unknown')}">${esc(j.status||'unknown')}</span></td><td class="timerCell">${jobAge(j)}</td><td>${heartbeatLabel(j)}</td><td><div class="progress"><span style="width:${progress(j)}%"></span></div></td><td>${esc(j.pages_crawled??j.progress?.pages_crawled??j.progress?.pages??0)}</td><td>${esc(j.progress?.queued??j.checkpoint?.queue??'—')}</td><td>${esc((j.progress?.external_resolved!==undefined||j.progress?.external_failed!==undefined)?`${j.progress?.external_resolved||0}/${j.progress?.external_failed||0}`:'—')}</td><td class="codeLine">${esc(j.checkpoint?.saved_at||'—')}<br>${j.checkpoint?.pages!==undefined?`pages: ${esc(j.checkpoint.pages)} · queue: ${esc(j.checkpoint.queue||0)}`:''}</td><td><div class="actions"><button onclick="goGraph('${esc(j.id)}')">Graph</button><button onclick="goResults('${esc(j.id)}')">Results</button><button onclick="openRerun('${esc(j.id)}','${esc(j.url)}')">Re-run</button>${renderJobControls(j)}</div></td></tr>`).join('')}</tbody></table>`}
function progress(j){if(j.status==='completed')return 100;if(j.status==='running')return 55;if(j.status==='failed')return 100;return 5} $('#jobSearch').oninput=renderJobs; $('#jobStatus').onchange=renderJobs;
addFilterListeners(['jobSearch','jobSearchScope','jobStatus','jobSort','jobSortDir'], renderJobs);
function populateJobSelects(){const opts=state.jobs.slice().reverse().map(j=>`<option value="${esc(j.id)}">[${esc(j.status)}] ${esc(trunc(j.url,54))}</option>`).join(''); ['graphJobSelect','resultsJobSelect'].forEach(id=>{const el=$('#'+id);if(el){const first=id==='graphJobSelect'?'<option value="">All jobs — merged graph</option>':'<option value="">All jobs</option>'; const old=el.value; el.innerHTML=first+opts; if([...el.options].some(o=>o.value===old))el.value=old;}})}
function goGraph(id){show('graph');setTimeout(()=>{$('#graphJobSelect').value=id;loadGraph()},50)} function goResults(id){show('results');setTimeout(()=>{$('#resultsJobSelect').value=id;loadResults()},50)}
function openRerun(id,url){state.rerunId=id;$('#rerunJob').textContent=url;$('#rerunModal').showModal()} $('#rerunGo').onclick=async()=>{try{await api(`/api/jobs/${state.rerunId}/rerun`,{method:'POST',body:JSON.stringify({depth:+$('#rerunDepth').value,max_depth:+$('#rerunDepth').value,max_pages:+$('#rerunPages').value,pattern_expansion:$('#rrPattern').checked,route_inference:$('#rrRoutes').checked,use_pattern_seeds:$('#rrSeeds').checked,soft_probe:$('#rrProbe').checked,follow_masked_outbound:$('#rrMaskedOutbound').checked,masked_outbound_aggressive:$('#rrMaskedAggressive').checked,masked_detail_boost:$('#rrMaskedAggressive').checked,masked_outbound_limit:+$('#rrMaskedLimit').value||500,auto_pagination:$('#rrPagination').checked,pagination_limit:+$('#rrPaginationLimit').value||25,use_blacklist_dirs:$('#rrBlacklist')?$('#rrBlacklist').checked:true})});$('#rerunModal').close();toast('Deep crawl started','ok');await refreshAfterJobCreate()}catch(e){toast('Rerun failed','err')}};
async function loadGraph(){try{if(!state.jobs.length)await loadJobs(); const jid=$('#graphJobSelect').value; state.graph=await api('/api/graph'+(jid?`?job_id=${encodeURIComponent(jid)}`:'')); state.pan={x:0,y:0,z:1}; drawGraph()}catch(e){toast('Failed to load graph','err')}}
['sourceFilter','depthFilter','graphSearch','graphLayout','graphLabels'].forEach(id=>$('#'+id).addEventListener(id==='graphLabels'?'change':'input',drawGraph)); $('#graphJobSelect').onchange=loadGraph; $('#fitGraphBtn').onclick=()=>{state.pan={x:0,y:0,z:1};drawGraph()};
function normEdge(e){return {from:e.from||e.source,to:e.to||e.target}};
function graphFiltered(){let nodes=(state.graph.nodes||[]).map((n,i)=>({...n,_i:i}));let edges=(state.graph.edges||[]).map(normEdge);const sf=$('#sourceFilter').value,df=$('#depthFilter').value,q=$('#graphSearch').value.toLowerCase();nodes=nodes.filter(n=>(!sf||n.source===sf)&&(!df||(+n.depth||0)<=+df)&&(!q||String(n.url||n.label||n.id).toLowerCase().includes(q)));const ids=new Set(nodes.map(n=>n.id));edges=edges.filter(e=>ids.has(e.from)&&ids.has(e.to));return {nodes,edges}}
function drawGraph(){const c=$('#graphCanvas'),ctx=c.getContext('2d'),w=c.width,h=c.height;ctx.clearRect(0,0,w,h);const {nodes,edges}=graphFiltered();state.graphView={nodes,edges};$('#graphStats').innerHTML=[['nodes',nodes.length],['edges',edges.length],['patterns',nodes.filter(n=>n.pattern_match||n.source==='pattern').length],['max depth',nodes.length?Math.max(...nodes.map(n=>+n.depth||0)):0],['job',$('#graphJobSelect').value?'selected':'merged']].map(([k,v])=>`<div class="graphMetric"><b>${esc(v)}</b><small>${k}</small></div>`).join('');if(!nodes.length){ctx.fillStyle='#55708f';ctx.font='18px sans-serif';ctx.fillText('No graph data for current filters',40,60);return} const layout=$('#graphLayout').value,cx=w/2,cy=h/2,R=Math.min(w,h)/2-70;const sourcePos={crawl:[cx-R*.35,cy],pattern:[cx+R*.35,cy],seed:[cx,cy-R*.35],inferred:[cx,cy+R*.35],api:[cx+R*.15,cy+R*.15],external:[cx+R*.55,cy-R*.2]};nodes.forEach((n,i)=>{let a=(i/nodes.length)*Math.PI*2,r=R*(.35+((i%11)/18)); if(layout==='depth'){r=80+(+n.depth||0)*75;a=(i/nodes.length)*Math.PI*2} else if(layout==='source'){const p=sourcePos[n.source]||[cx,cy];r=40+((i%8)*12);n.x=p[0]+Math.cos(a)*r;n.y=p[1]+Math.sin(a)*r;return} n.x=cx+Math.cos(a)*r;n.y=cy+Math.sin(a)*r});ctx.save();ctx.translate(state.pan.x,state.pan.y);ctx.scale(state.pan.z,state.pan.z);ctx.strokeStyle='#203453';ctx.lineWidth=1;edges.forEach(e=>{const a=nodes.find(n=>n.id===e.from),b=nodes.find(n=>n.id===e.to);if(a&&b){ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke()}});nodes.forEach(n=>{const color=n.source==='pattern'||n.pattern_match?'#ff9f43':n.source==='seed'?'#75f7a1':n.source==='inferred'?'#ffd166':n.source==='api'?'#7c6cff':n.source==='external'?'#ff4fd8':'#62f4ff';ctx.beginPath();ctx.fillStyle=color;ctx.arc(n.x,n.y,Math.max(4,11-(+n.depth||0)),0,Math.PI*2);ctx.fill();if($('#graphLabels').checked){ctx.fillStyle='#d8e6ff';ctx.font='10px sans-serif';ctx.fillText(trunc((new URL(n.url||'http://x/')).pathname||n.label||n.id,34),n.x+9,n.y-9)}});ctx.restore()}
const canvas=$('#graphCanvas');canvas.onwheel=e=>{e.preventDefault();state.pan.z*=e.deltaY<0?1.1:.9;state.pan.z=Math.max(.2,Math.min(5,state.pan.z));drawGraph()};canvas.onmousedown=e=>{state.drag=true;state.last=[e.offsetX,e.offsetY]};canvas.onmouseup=()=>state.drag=false;canvas.onmouseleave=()=>state.drag=false;canvas.onmousemove=e=>{if(state.drag){state.pan.x+=e.offsetX-state.last[0];state.pan.y+=e.offsetY-state.last[1];state.last=[e.offsetX,e.offsetY];drawGraph();return} const hit=hitNode(e.offsetX,e.offsetY);canvas.style.cursor=hit?'pointer':'grab'};canvas.onclick=e=>{const n=hitNode(e.offsetX,e.offsetY);if(n)showNode(n)};
function hitNode(x,y){const z=state.pan.z,px=(x-state.pan.x)/z,py=(y-state.pan.y)/z;return (state.graphView.nodes||[]).find(n=>Math.hypot(n.x-px,n.y-py)<12)} function showNode(n){$('#nodePanel').style.display='block';$('#nodePanel').innerHTML=`<h3>${esc(n.label||'Node')}</h3><p class="urlCell">${esc(n.url||n.id)}</p><p><span class="pill">${esc(n.source||'crawl')}</span> <span class="pill">depth ${esc(n.depth??0)}</span></p><pre>${esc(JSON.stringify(n,null,2))}</pre>`}
async function loadPatterns(){try{state.patterns=await api('/api/patterns'); renderPatterns()}catch(e){toast('Failed to load patterns','err')}} addFilterListeners(['patternSearch','patternSearchScope','patternMinConf','patternSort','patternSortDir'], renderPatterns);
function filteredPatterns(){const q=$('#patternSearch')?.value||'',scope=$('#patternSearchScope')?.value||'global',min=+($('#patternMinConf')?.value||0)/100;let rows=state.patterns.filter(p=>(!min||(p.confidence||0)>=min)&&matchesSearch(p,q,scope));return sortRows(rows,$('#patternSort')?.value||'confidence',$('#patternSortDir')?.value||'desc')}
function renderPatterns(){const ps=filteredPatterns(); if(!ps.length){$('#patternsList').innerHTML='<div class="empty">No patterns yet</div>';return} $('#patternsList').innerHTML=ps.map(p=>patternCard(p)).join('')}
function patternCard(p){const suggested=(p.suggested_values||[]).map(v=>`<span class="chip ${(p.active_values||[]).includes(v)?'on':''}" title="${esc(v)}" onclick="toggleVal('${esc(p.id)}','${encodeURIComponent(v)}',${(p.active_values||[]).includes(v)})">${(p.active_values||[]).includes(v)?'✓ ':''}${esc(trunc(v,44))}</span>`).join(''); const active=(p.active_values||[]).map(v=>`<span class="badge" title="${esc(v)}">${esc(trunc(v,48))}</span>`).join(''); const examples=(p.examples||[]).slice(0,8).map(v=>`<span class="badge" title="${esc(v)}">${esc(trunc(v,54))}</span>`).join(''); const conf=Math.round((p.confidence||0)*100);return `<div class="miniCard patternCard"><h3>${esc(p.pattern)}</h3><p class="muted codeLine">${esc(p.domain)} · ${esc(p.type||'path')} · confidence ${conf}%</p><div class="progress"><span style="width:${conf}%"></span></div><p><b>Examples</b></p><div class="chipRow">${examples||'<span class="muted">none</span>'}</div><p><b>Suggested Values</b></p><div class="chipRow">${suggested||'<span class="muted">none</span>'}</div><p><b>Active Values</b></p><div class="chipRow">${active||'<span class="muted">none</span>'}</div><div class="actions"><button onclick="editPattern('${esc(p.id)}')">Edit</button><button onclick="clonePattern('${esc(p.id)}')">Clone</button><button onclick="patternToSeed('${esc(p.id)}')">Seed</button><button onclick="openPatternRerun('${esc(p.job_id||'')}')">Re-run</button><button class="danger" onclick="confirmDelete('Pattern','${esc(p.pattern)}','/api/patterns/${esc(p.id)}',loadPatterns)">Delete</button><button onclick="explainPattern('${esc(p.pattern)}')">?</button></div></div>`}
async function toggleVal(id,val,on){val=decodeURIComponent(val);try{if(on){await api(`/api/patterns/${id}/values/${encodeURIComponent(val)}`,{method:'DELETE'});toast('Value removed')}else{await api(`/api/patterns/${id}/values`,{method:'POST',body:JSON.stringify({value:val})});toast('Value added','ok')}loadPatterns()}catch(e){toast('Could not toggle value','err')}}
function editPattern(id){const p=state.patterns.find(x=>x.id===id);if(!p)return;$('#editPatternId').value=id;$('#editPatternDomain').value=p.domain||'';$('#editPatternCode').value=p.pattern||'';$('#editPatternValues').value=(p.suggested_values||[]).join(', ');$('#editPatternConf').value=Math.round((p.confidence||0)*100);$('#editPatternModal').showModal()} $('#savePatternEdit').onclick=async()=>{const id=$('#editPatternId').value;await api('/api/patterns/'+id,{method:'PUT',body:JSON.stringify({domain:$('#editPatternDomain').value,pattern:$('#editPatternCode').value,suggested_values:$('#editPatternValues').value.split(',').map(x=>x.trim()).filter(Boolean),confidence:+$('#editPatternConf').value/100})});$('#editPatternModal').close();loadPatterns()};
async function clonePattern(id){const p={...state.patterns.find(x=>x.id===id)};delete p.id;p.pattern=(p.pattern||'')+'-clone';await api('/api/patterns',{method:'POST',body:JSON.stringify(p)});toast('Pattern cloned','ok');loadPatterns()} function openPatternRerun(id){id?openRerun(id,id):toast('No source job attached to this pattern')} function explainPattern(p){toast('Pattern = URL template reusable. Suggested values clicked become active values for expansion.')} async function patternToSeed(id){const p=state.patterns.find(x=>x.id===id);if(!p)return;show('seeds');setTimeout(()=>{$('#seedDomain').value=p.domain||'';$('#seedPattern').value=p.pattern||'';$('#seedValues').value=[...(p.active_values||[]),...(p.suggested_values||[])].slice(0,30).join(', ')},50)}
$('#suggestBtn').onclick=async()=>{const raw=prompt('Paste URLs, one per line');if(!raw)return;try{const urls=raw.split('\n').map(x=>x.trim()).filter(Boolean);const s=await api('/api/pattern-suggest',{method:'POST',body:JSON.stringify({urls})});const arr=Array.isArray(s)?s:(s.patterns||[]);for(const p of arr)await api('/api/patterns',{method:'POST',body:JSON.stringify(p)});toast(`${arr.length} suggestions created`,'ok');loadPatterns()}catch(e){toast('Suggest failed','err')}};
async function loadSeeds(){try{state.seeds=await api('/api/pattern-seeds');renderSeeds()}catch(e){toast('Failed to load seeds','err')}} addFilterListeners(['seedSearch','seedSearchScope','seedSort','seedSortDir'], renderSeeds);
function filteredSeeds(){const q=$('#seedSearch')?.value||'',scope=$('#seedSearchScope')?.value||'global';let rows=state.seeds.filter(s=>matchesSearch(s,q,scope));return sortRows(rows,$('#seedSort')?.value||'priority',$('#seedSortDir')?.value||'desc')}
function renderSeeds(){const ss=filteredSeeds(); if(!ss.length){$('#seedsList').innerHTML='<div class="empty">No seeds yet</div>';return} $('#seedsList').innerHTML=ss.map(s=>`<div class="miniCard"><h3>${esc(s.pattern)}</h3><p class="muted">${esc(s.domain)} · v${esc(s.version||1)} · ${s.enabled?'enabled':'disabled'}</p><div class="chipRow">${(s.values||[]).map(v=>`<span class="badge">${esc(trunc(v,38))}</span>`).join('')}</div><p class="muted">${esc(s.notes||'')}</p><div class="actions"><button onclick="toggleSeed('${esc(s.id)}',${!s.enabled})">${s.enabled?'Disable':'Enable'}</button><button onclick="seedToRun('${esc(s.domain)}')">Run Domain</button><button class="danger" onclick="confirmDelete('Pattern Seed','${esc(s.pattern)}','/api/pattern-seeds/${esc(s.id)}',loadSeeds)">Delete</button></div></div>`).join('')}
$('#createSeedBtn').onclick=async()=>{try{await api('/api/pattern-seeds',{method:'POST',body:JSON.stringify({domain:$('#seedDomain').value,pattern:$('#seedPattern').value,values:$('#seedValues').value.split(',').map(x=>x.trim()).filter(Boolean),language_variants:$('#seedLang').value.split(',').map(x=>x.trim()).filter(Boolean),priority:+$('#seedPriority').value||80,enabled:$('#seedEnabled').value==='true',notes:$('#seedNotes').value})});toast('Pattern Seed created','ok');loadSeeds()}catch(e){toast('Seed failed','err')}}; async function toggleSeed(id,en){await api('/api/pattern-seeds/'+id,{method:'PUT',body:JSON.stringify({enabled:en})});loadSeeds()} function seedToRun(domain){show('run');setTimeout(()=>{$('#runUrl').value=domain.startsWith('http')?domain:'https://'+domain;$('#optSeeds').checked=true},50)}
async function loadResults(){try{if(!state.jobs.length)await loadJobs(); const jid=$('#resultsJobSelect').value;const data=await api('/api/results'+(jid?`?job_id=${encodeURIComponent(jid)}`:'')); const arr=Array.isArray(data)?data:[data]; state.results=[];arr.forEach(x=>{const job=x.job||{};(x.graph?.nodes||[]).forEach(n=>state.results.push({job_id:job.id,url:n.url||n.id,source:n.source,depth:n.depth,status:n.status_code,title:n.title}))});renderResults()}catch(e){toast('Failed to load results','err')}} addFilterListeners(['resultsSearch','resultsSearchScope','resultsSource','resultsSort','resultsSortDir'], renderResults);$('#resultsJobSelect').onchange=loadResults;
function filteredResults(){const q=$('#resultsSearch')?.value||'',scope=$('#resultsSearchScope')?.value||'global',sf=$('#resultsSource')?.value||'';let rows=state.results.filter(r=>(!sf||r.source===sf)&&matchesSearch(r,q,scope));return sortRows(rows,$('#resultsSort')?.value||'url',$('#resultsSortDir')?.value||'asc')} function renderResults(){const rs=filteredResults(); if(!rs.length){$('#resultsList').innerHTML='<div class="empty">No results for current filters</div>';return} $('#resultsList').innerHTML=`<table><thead><tr><th>#</th><th>URL</th>${sortTh('results','source','Source')}${sortTh('results','depth','Depth')}${sortTh('results','status','Status')}<th>Job</th></tr></thead><tbody>${rs.slice(0,1200).map((r,i)=>`<tr><td>${i+1}</td><td class="urlCell">${esc(r.url)}</td><td><span class="pill">${esc(r.source||'crawl')}</span></td><td>${esc(r.depth??0)}</td><td>${esc(r.status??'—')}</td><td class="codeLine">${esc(r.job_id||'')}</td></tr>`).join('')}</tbody></table>`}
$('#bulkBtn').onclick=async()=>{const urls=$('#bulkUrls').value.split('\n').map(x=>x.trim()).filter(Boolean);if(!urls.length)return toast('Enter at least one URL','err');try{const r=await api('/api/bulk-run',{method:'POST',body:JSON.stringify({urls,depth:+$('#bulkDepth').value,max_depth:+$('#bulkDepth').value,max_pages:+$('#bulkPages').value,pattern_expansion:$('#bulkPattern').checked,soft_probe:$('#bulkProbe').checked,follow_masked_outbound:$('#bulkMaskedOutbound').checked,masked_outbound_aggressive:$('#bulkMaskedAggressive').checked,masked_detail_boost:$('#bulkMaskedAggressive').checked,masked_outbound_limit:+$('#bulkMaskedLimit').value||500,auto_pagination:$('#bulkPagination').checked,pagination_limit:+$('#bulkPaginationLimit').value||25,use_blacklist_dirs:$('#bulkBlacklist')?$('#bulkBlacklist').checked:true})});$('#bulkOut').innerHTML=(r.jobs||[]).map(j=>`<div class="miniCard"><h3>${esc(j.id||j.job_id)}</h3><p class="urlCell">${esc(j.url)}</p><span class="pill">${esc(j.status||'queued')}</span></div>`).join('');toast(`Bulk launched: ${r.count||urls.length} jobs`,'ok');await refreshAfterJobCreate()}catch(e){toast('Bulk failed','err')}};

function parseUrlLines(text){return String(text||'').split(/\r?\n/).map(x=>x.trim()).filter(x=>x&&!x.startsWith('#'))}
function parseMassiveEntries(){
  const rowText=$('#massiveRows')?.value||'';
  const rows=parseUrlLines(rowText);
  if(rows.length){
    return rows.map((line,i)=>{
      const parts=line.split('|').map(x=>x.trim());
      if(parts.length>=3)return {label:parts[0]||`row-${i+1}`,root_url:parts[1],seed_url:parts[2],enabled:true};
      if(parts.length===2)return {label:`row-${i+1}`,root_url:parts[0],seed_url:parts[1],enabled:true};
      return {label:`row-${i+1}`,seed_url:parts[0],enabled:true};
    });
  }
  const roots=parseUrlLines($('#massiveRoots')?.value||''), seeds=parseUrlLines($('#massiveSeeds')?.value||'');
  const n=Math.max(roots.length,seeds.length);
  const out=[];
  for(let i=0;i<n;i++)out.push({label:`node-${i+1}`,root_url:roots[i]||'',seed_url:seeds[i]||'',enabled:true});
  return out.filter(x=>x.root_url||x.seed_url);
}
function massivePayload(){
  return {
    entries:parseMassiveEntries(),
    launch_roots:$('#massiveLaunchRoots')?.checked,
    launch_seeds:$('#massiveLaunchSeeds')?.checked,
    depth:+($('#massiveDepth')?.value||2),
    max_depth:+($('#massiveDepth')?.value||2),
    max_pages:+($('#massivePages')?.value||100),
    pattern_expansion:$('#massivePattern')?.checked,
    route_inference:$('#massiveRoutes')?.checked,
    soft_probe:$('#massiveProbe')?.checked,
    use_pattern_seeds:$('#massiveSeedsOpt')?.checked,
    auto_pagination:$('#massivePagination')?.checked,
    pagination_limit:+($('#massivePaginationLimit')?.value||25),
    use_blacklist_dirs:$('#massiveBlacklist')?.checked,
    follow_masked_outbound:$('#massiveMaskedOutbound')?.checked,
    masked_outbound_aggressive:$('#massiveMaskedAggressive')?.checked,
    masked_detail_boost:$('#massiveMaskedAggressive')?.checked,
    masked_outbound_limit:+($('#massiveMaskedLimit')?.value||300)
  }
}
function renderMassivePreview(){
  const el=$('#massivePreview'); if(!el)return;
  const entries=parseMassiveEntries();
  if(!entries.length){el.innerHTML='<div class="empty">Paste roots/seeds to preview connections.</div>';return}
  el.innerHTML=entries.map((e,i)=>`<div class="miniCard"><h3>${esc(e.label||`node-${i+1}`)}</h3><p><b>root</b><br><span class="urlCell">${esc(e.root_url||'—')}</span></p><p><b>seed</b><br><span class="urlCell">${esc(e.seed_url||'—')}</span></p><small>${e.root_url&&e.seed_url?'root → seed connection':'single node'}</small></div>`).join('');
}
$('#massivePreviewBtn')?.addEventListener('click',renderMassivePreview);
['massiveRoots','massiveSeeds','massiveRows'].forEach(id=>$('#'+id)?.addEventListener('input',renderMassivePreview));
$('#massiveLaunchBtn')?.addEventListener('click',async()=>{
  const payload=massivePayload();
  if(!payload.entries.length)return toast('Adicione roots/seeds primeiro.','err');
  try{
    const r=await api('/api/massive-run',{method:'POST',body:JSON.stringify(payload)});
    $('#massivePreview').innerHTML=(r.jobs||[]).map(j=>`<div class="miniCard"><h3>${esc(j.id)}</h3><p class="urlCell">${esc(j.url)}</p><span class="pill">${esc(j.massive_node_type||'job')}</span> <span class="pill">${esc(j.status||'queued')}</span></div>`).join('');
    toast(`Massive Job launched: ${r.count} jobs · batch ${r.batch_id}`,'ok');
    await refreshAfterJobCreate(); show('jobs');
  }catch(e){toast('Massive Job failed: '+e.message,'err')}
});

async function loadBlacklist(){
  try{
    const d=await api('/api/blacklist-dirs');
    state.blacklist=d.rules||[];
    renderBlacklist();
  }catch(e){toast('Failed to load blacklist: '+e.message,'err')}
}
function renderBlacklist(){
  const el=$('#blacklistTable'); if(!el)return;
  $('#blackCount').textContent=`${state.blacklist.length} rules`;
  $('#blackTextList').value=state.blacklist.map(r=>(r.kind==='regex'?'re:':'')+r.pattern).join('\n');
  if(!state.blacklist.length){el.innerHTML='<div class="empty">No blacklist rules yet.</div>';return}
  el.innerHTML=`<table><thead><tr><th>On</th><th>Type</th><th>Pattern</th><th>Note</th><th>Actions</th></tr></thead><tbody>${state.blacklist.map(r=>`<tr><td><input type="checkbox" ${r.enabled?'checked':''} onchange="toggleBlacklistRule('${esc(r.id)}',this.checked)"></td><td><span class="pill">${esc(r.kind)}</span></td><td class="codeLine">${esc(r.pattern)}</td><td>${esc(r.note||'')}</td><td><div class="actions"><button onclick="editBlacklistRule('${esc(r.id)}')">Edit</button><button class="danger" onclick="deleteBlacklistRule('${esc(r.id)}')">Delete</button></div></td></tr>`).join('')}</tbody></table>`;
}
async function toggleBlacklistRule(id,on){try{await api('/api/blacklist-dirs/'+encodeURIComponent(id),{method:'PUT',body:JSON.stringify({enabled:on})});await loadBlacklist()}catch(e){toast('Update failed','err')}}
function editBlacklistRule(id){
  const r=state.blacklist.find(x=>x.id===id); if(!r)return;
  const p=prompt('Pattern:',r.pattern); if(p===null)return;
  const k=prompt('Type: contains or regex',r.kind||'contains')||'contains';
  const note=prompt('Note:',r.note||'')||'';
  api('/api/blacklist-dirs/'+encodeURIComponent(id),{method:'PUT',body:JSON.stringify({pattern:p,kind:k,note})}).then(loadBlacklist).catch(e=>toast('Edit failed: '+e.message,'err'));
}
async function deleteBlacklistRule(id){if(!confirm('Delete blacklist rule?'))return;try{await api('/api/blacklist-dirs/'+encodeURIComponent(id),{method:'DELETE'});await loadBlacklist()}catch(e){toast('Delete failed','err')}}
$('#blackAddBtn')?.addEventListener('click',async()=>{
  const pattern=$('#blackPattern').value.trim(); if(!pattern)return toast('Pattern first.','err');
  try{await api('/api/blacklist-dirs',{method:'POST',body:JSON.stringify({pattern,kind:$('#blackKind').value,note:$('#blackNote').value})});$('#blackPattern').value='';$('#blackNote').value='';await loadBlacklist();toast('Blacklist rule added','ok')}catch(e){toast('Add failed: '+e.message,'err')}
});
$('#blackExamplesBtn')?.addEventListener('click',async()=>{
  const examples=[
    {pattern:'partners-login',kind:'contains',note:'partner login, not partner directory'},
    {pattern:'auth',kind:'contains',note:'auth noise'},
    {pattern:'/(?:login|signin|sign-in|account)(?:/|$|\\?)',kind:'regex',note:'login/account regex'},
    {pattern:'/(?:wp-content|wp-admin|wp-includes)(?:/|$)',kind:'regex',note:'WordPress noise'}
  ];
  try{for(const e of examples)await api('/api/blacklist-dirs',{method:'POST',body:JSON.stringify(e)});await loadBlacklist();toast('Examples added','ok')}catch(e){toast('Examples failed: '+e.message,'err')}
});
async function saveBlacklistText(){
  const rows=parseUrlLines($('#blackTextList')?.value||'').map(line=>{
    const isRe=/^re:/i.test(line);
    return {pattern:isRe?line.replace(/^re:/i,'').trim():line,kind:isRe?'regex':'contains',enabled:true,note:'text list'};
  });
  try{await api('/api/blacklist-dirs',{method:'PUT',body:JSON.stringify({rules:rows})});await loadBlacklist();toast('Blacklist saved','ok')}catch(e){toast('Save failed: '+e.message,'err')}
}


function confirmDelete(type,name,url,cb){$('#confirmTitle').textContent='Delete '+type;$('#confirmBody').textContent='Delete permanently: '+name;$('#confirmYes').onclick=async()=>{try{await api(url,{method:'DELETE'});$('#confirmModal').close();toast(type+' deleted','ok');cb&&cb()}catch(e){toast('Delete failed','err')}};$('#confirmModal').showModal()}
function exportSection(title,content,type){const css='<style>body{font-family:Arial;background:#081323;color:#eaf4ff;padding:24px}table{width:100%;border-collapse:collapse}td,th{border:1px solid #203453;padding:8px;text-align:left}.url{word-break:break-all}</style>'; if(type==='pdf'){const w=window.open('','_blank');w.document.write(`<html><head><title>${esc(title)}</title>${css}</head><body><h1>${esc(title)}</h1>${content}</body></html>`);w.document.close();w.print();return} const blob=new Blob([type==='html'?`<html><head><meta charset="utf-8"><title>${title}</title>${css}</head><body><h1>${title}</h1>${content}</body></html>`:content],{type:type==='html'?'text/html':'text/plain'}); const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=`wisp_${title.toLowerCase().replace(/\s+/g,'_')}.${type}`;a.click();URL.revokeObjectURL(a.href)}
function renderJobsText(){return filteredJobs().map(j=>`${j.id}\t${j.status}\t${j.url}`).join('\n')} function renderJobsHTML(){return $('#jobsList').innerHTML} function renderPatternsText(){return filteredPatterns().map(p=>`${p.domain}\t${p.pattern}\t${Math.round((p.confidence||0)*100)}%`).join('\n')} function renderPatternsHTML(){return $('#patternsList').innerHTML} function renderSeedsText(){return filteredSeeds().map(s=>`${s.domain}\t${s.pattern}\t${(s.values||[]).join(',')}`).join('\n')} function renderSeedsHTML(){return $('#seedsList').innerHTML} function renderResultsText(){return filteredResults().map(r=>`${r.job_id}\t${r.source}\t${r.depth}\t${r.url}`).join('\n')} function renderResultsHTML(){return $('#resultsList').innerHTML} function renderGraphText(){return JSON.stringify(state.graphView,null,2)} function renderGraphHTML(){return `<pre>${esc(JSON.stringify(state.graphView,null,2))}</pre>`}
const guide={
Overview:`WISP is not just a crawler. Think of it as an intelligence workbench: it crawls visible links, learns structure, guesses likely routes, validates lightly, resolves masked external redirects, stores skipped reasons and builds a graph.<pre>Seed URL → Crawl → Pagination → Patterns → Route Inference → Soft Probe → External Resolver → Graph/Reports</pre><b>Rule of thumb:</b> start precise, expand later. Dirty link = precision. Root = discovery. Section root = the sweet spot.`,
RunJob:`Use Run Job for one target. Start with Depth 2 and 100–200 pages. Turn on only the features you are testing. If testing masked links, keep Pattern Expansion/Route Inference off at first and leave Follow Masked Outbound + Aggressive External Resolver on.`,
MassiveJob:`Massive Job is for connected batches. Use rows like <code>label | root_url | seed_url</code>. Roots discover siblings. Seeds extract known directories. Keep the shared options conservative, then re-run the best domains deeper.`,
ConfigImport:`Config Import/Export lets you save a whole job recipe as TXT/JSON/MD. It can include TYPE, URL, URL ROOT-SEED, CONFIG and BLACKLIST. Use it to repeat experiments without manually clicking dropdowns and checkboxes.`,
DirectoryDiscovery:`Directory Discovery Mode uses Known Directory URL + Section Root URL + Root URL. It previews candidate sibling routes from keywords, then launches controlled jobs. Example: from <code>/crypto/service-providers</code>, test <code>/crypto/exchanges</code>, <code>/crypto/wallets</code>, <code>/banks</code>, etc.`,
Blacklist:`Blacklist Dirs blocks noisy paths before crawl. Contains rules are simple words like <code>login</code>. Regex rules use <code>re:</code>, for example <code>re:/(login|signin|account)(/|$|\?)</code>. Slash is optional for contains rules.`,
Depth:`Depth controls distance from the start URL. Depth 2 = scout. Depth 4 = expedition. Depth 6+ = jungle with a machete. Combine high depth with page limits and blacklists or you will collect noise by the bucket.`,
Patterns:`Pattern Intelligence turns repeated URLs into templates like <code>/exchangers/{slug}</code>. Suggested values are hypotheses. Add good ones as seeds; delete noisy ones. Pattern Expansion is powerful after you understand the site shape.`,
Pagination:`Auto Pagination tries next buttons, page/offset links and DataTables-style tables. Pagination Limit means pagination steps, not total pages. Use it on directories that show only 25/50 rows at a time.`,
MaskedOutbound:`Follow Masked Outbound resolves links like <code>/away?sId=123</code> into the final external URL. Aggressive mode scans attributes/scripts and waits harder with browser fallback. It saves the external target; it does not crawl the external website.`,
LiveEvents:`Live Events is the dirty log. Use it to see why the crawler is moving, stuck, skipping, resolving, failing or completing. Filter by job/event/search. Export logs when debugging.`,
Jobs:`Jobs has timer, heartbeat, pause, resume, restart-from-checkpoint, stop and delete. Restart is not Re-run: it tries to continue from checkpoint. Re-run starts a new crawl based on the old config.`,
Reports:`External Report explains masked links: candidates, resolved, failed and errors. Why Not Crawled explains skipped URLs: duplicate, blacklisted, external-only, max depth, max pages, unresolved redirect, timeout, etc.`,
Graph:`For big graphs, keep labels off by default. Filter by source/depth/job and use Fit. A 1000-node graph with labels on is mosquito soup. Click a node to inspect it.`,
Exports:`Export TXT/HTML/PDF for human reports. Export JSON/CSV for reprocessing. Export GraphML/GEXF for Gephi/Cytoscape-style graph analysis.`
};
$('#helpBtn').onclick=()=>{$('#guideTabs').innerHTML=Object.keys(guide).map((k,i)=>`<button onclick="guideShow('${k}')">${k}</button>`).join('');guideShow('Overview');$('#helpModal').showModal()}; window.guideShow=k=>{$('#guideBody').innerHTML=guide[k]};
let liveES=null;
const LIVE_EVENTS=['job_queued','job_started','job_progress','job_paused','job_resumed','job_pause_requested','job_resume_requested','job_restart_requested','job_restarting','job_resumed_from_checkpoint','job_cancel_requested','job_cancelled','job_deleted','pagination_scan','pagination_step','pagination_links_collected','pagination_error','node_discovered','edge_created','url_skipped','pattern_detected','pattern_applied','pattern_value_added','pattern_value_removed','service_deleted','pattern_deleted','masked_outbound_resolved','masked_outbound_scan','masked_outbound_error','job_completed','job_failed'];
function livePayload(raw){try{return JSON.parse(raw)}catch(e){return {event:'message',payload:{raw},ts:new Date().toISOString(),ts_unix:Date.now()/1000}}}
function liveJobId(row){const p=row?.payload||{};return p.job_id||p.id||p.jobId||''}
function liveText(row, rawMode){const p=row?.payload??{}; if(rawMode) return JSON.stringify(p,null,0); return p.url||p.target||p.masked_url||p.pattern||p.error||p.reason||p.status||JSON.stringify(p)}
function rememberLive(row){
  if(!row || !row.event) return;
  if(!row.ts_unix) row.ts_unix=Date.now()/1000;
  row._local_id = row._local_id || `${row.ts_unix}_${row.event}_${Math.random().toString(16).slice(2)}`;
  state.liveEvents.push(row);
  if(state.liveEvents.length>5000) state.liveEvents.splice(0,state.liveEvents.length-5000);
  updateLiveFilters();
  renderLiveConsole();
}
function liveAppend(ev, raw){
  const parsed=livePayload(raw);
  const row={event:parsed.event||ev,payload:parsed.payload??parsed,ts:parsed.ts||new Date().toISOString(),ts_unix:parsed.ts_unix||Date.now()/1000,raw};
  rememberLive(row);
  let msg=liveText(row,true);
  const line=document.createElement('div');
  line.className='eventLine';
  line.textContent=`${new Date((row.ts_unix||Date.now()/1000)*1000).toLocaleTimeString()} ${row.event} ${trunc(msg,180)}`;
  const feed=$('#liveFeed');
  if(feed){
    if(feed.querySelector('.empty')) feed.innerHTML='';
    feed.appendChild(line);
    while(feed.children.length>420) feed.removeChild(feed.firstChild);
    feed.scrollTop=feed.scrollHeight;
  }
}
function updateLiveFilters(){
  const typeSel=$('#liveTypeFilter');
  if(typeSel){
    const old=typeSel.value;
    const types=[...new Set(state.liveEvents.map(e=>e.event).filter(Boolean))].sort();
    typeSel.innerHTML='<option value="">all event types</option>'+types.map(t=>`<option>${esc(t)}</option>`).join('');
    if(types.includes(old)) typeSel.value=old;
  }
  const stateEl=$('#liveConsoleState'); if(stateEl && liveES) stateEl.textContent='live';
}
function filteredLiveEvents(){
  const job=$('#liveJobSelect')?.value||'';
  const type=$('#liveTypeFilter')?.value||'';
  const q=$('#liveSearch')?.value||'';
  const scope=$('#liveSearchScope')?.value||'global';
  const limit=+($('#liveLimit')?.value||500);
  let rows=state.liveEvents.filter(r=>(!job||liveJobId(r)===job)&&(!type||r.event===type)&&matchesSearch(r,q,scope));
  rows=sortRows(rows,$('#liveSort')?.value||'time',$('#liveSortDir')?.value||'desc');
  return rows.slice(0,limit);
}
function renderLiveConsole(){
  const log=$('#liveLog'); if(!log) return;
  if($('#livePause')?.checked) return;
  const rows=filteredLiveEvents();
  const raw=$('#liveRawToggle')?.checked;
  const total=state.liveEvents.length;
  const resolved=state.liveEvents.filter(e=>e.event==='masked_outbound_resolved').length;
  const errors=state.liveEvents.filter(e=>String(e.event).includes('error')||e.event==='job_failed').length;
  const skipped=state.liveEvents.filter(e=>e.event==='url_skipped').length;
  const progress=state.liveEvents.filter(e=>e.event==='job_progress').length;
  const jobs=[...new Set(state.liveEvents.map(liveJobId).filter(Boolean))].length;
  const stats=$('#liveEventStats');
  if(stats) stats.innerHTML=[['showing',rows.length],['buffer',total],['jobs',jobs],['progress',progress],['resolved',resolved],['errors/skips',errors+skipped]].map(([k,v])=>`<div class="graphMetric"><b>${esc(v)}</b><small>${esc(k)}</small></div>`).join('');
  if(!rows.length){log.innerHTML='<div class="empty">No live events match current filters</div>';return}
  log.innerHTML=rows.map(r=>{
    const d=new Date((r.ts_unix||Date.now()/1000)*1000).toLocaleTimeString();
    const cls=[r.event, String(r.event).includes('error')?'err':'', r.event==='job_completed'||r.event==='masked_outbound_resolved'?'ok':'', r.event==='masked_outbound_scan'?'scan':''].join(' ');
    return `<div class="liveRow ${esc(cls)}"><div class="liveTs">${esc(d)}</div><div class="liveType">${esc(r.event)}</div><div class="liveJob">${esc(liveJobId(r)||'—')}</div><div class="liveMsg">${esc(liveText(r,raw))}</div></div>`
  }).join('');
  if($('#liveAutoscroll')?.checked) log.scrollTop=0;
}
async function loadLiveHistory(){
  try{
    if(!state.jobs.length) await loadJobs();
    const d=await api('/api/events?limit=1000');
    const seen=new Set(state.liveEvents.map(e=>e._local_id||`${e.ts_unix}_${e.event}_${JSON.stringify(e.payload)}`));
    (d.events||[]).forEach(e=>{const k=`${e.ts_unix}_${e.event}_${JSON.stringify(e.payload)}`; if(!seen.has(k)){e._local_id=k; state.liveEvents.push(e); seen.add(k)}});
    if(state.liveEvents.length>5000) state.liveEvents.splice(0,state.liveEvents.length-5000);
    updateLiveFilters(); renderLiveConsole();
  }catch(e){renderLiveConsole()}
}
function clearLiveConsole(){state.liveEvents=[]; renderLiveConsole(); toast('Live view cleared. Backend history is untouched.','ok')}
function renderLiveMarkdown(){return '# WISP Live Events\n\n'+filteredLiveEvents().map(r=>`- ${r.ts||''} [${r.event}] ${liveJobId(r)||'—'} ${liveText(r,true)}`).join('\n')}
function renderLiveHTML(){return `<table><thead><tr><th>Time</th><th>Event</th><th>Job</th><th>Payload</th></tr></thead><tbody>${filteredLiveEvents().map(r=>`<tr><td>${esc(r.ts||'')}</td><td>${esc(r.event)}</td><td>${esc(liveJobId(r)||'')}</td><td class="url">${esc(liveText(r,true))}</td></tr>`).join('')}</tbody></table>`}
function connectLive(force=false){
  const stateEl=$('#liveState'), consoleState=$('#liveConsoleState');
  if(liveES) try{liveES.close()}catch(e){}
  try{
    liveES=new EventSource('/api/stream?replay=1000');
    if(stateEl) stateEl.textContent='connecting'; if(consoleState) consoleState.textContent='connecting';
    liveES.onopen=()=>{ if(stateEl) stateEl.textContent='live'; if(consoleState) consoleState.textContent='live'; };
    liveES.onerror=()=>{ if(stateEl) stateEl.textContent='reconnecting'; if(consoleState) consoleState.textContent='reconnecting'; };
    liveES.onmessage=e=>liveAppend('message',e.data||'');
    LIVE_EVENTS.forEach(ev=>liveES.addEventListener(ev,e=>{
      liveAppend(ev,e.data||'');
      if(ev.includes('completed')||ev.includes('failed')) toast(ev.replace('_',' '));
      if(ev==='job_progress'||ev==='job_completed'||ev==='job_failed'||ev==='masked_outbound_scan'||ev==='masked_outbound_resolved') loadStats();
    }));
  }catch(e){ if(stateEl) stateEl.textContent='offline'; if(consoleState) consoleState.textContent='offline'; }
}
connectLive();
['liveSearch','liveSearchScope','liveTypeFilter','liveJobSelect','liveRawToggle','liveAutoscroll','liveLimit','liveSort','liveSortDir'].forEach(id=>{const el=$('#'+id); if(el) el.addEventListener(id==='liveSearch'?'input':'change', renderLiveConsole)}); const lp=$('#livePause'); if(lp) lp.addEventListener('change',()=>{if(!lp.checked) renderLiveConsole()});
loadStats();loadJobs();loadLiveHistory();setInterval(loadStats,5000);


function autoResumeNotice(){
  const running=state.jobs.filter(j=>j.status==='running');
  const feed=$('#liveFeed');
  if(running.length && feed && !$('#resumeNotice')){
    const box=document.createElement('div'); box.id='resumeNotice'; box.className='hintBox';
    box.innerHTML=`${running.length} job(s) running. <button onclick="goResults('${esc(running[0].id)}')">Open Results</button> <button onclick="goGraph('${esc(running[0].id)}')">Watch Graph</button>`;
    feed.prepend(box);
  }
}
function populateExtraJobSelects(){
  const opts=state.jobs.slice().reverse().map(j=>`<option value="${esc(j.id)}">[${esc(j.status)}] ${esc(trunc(j.url,54))}</option>`).join('');
  ['externalJobSelect','skippedJobSelect','liveJobSelect'].forEach(id=>{const el=$('#'+id); if(el){const old=el.value; el.innerHTML='<option value="">All jobs</option>'+opts; if([...el.options].some(o=>o.value===old)) el.value=old;}});
}
function rowsToCsv(rows){
  if(typeof rows==='string') return rows;
  rows=Array.isArray(rows)?rows:[]; const keys=[...new Set(rows.flatMap(r=>Object.keys(r||{})))];
  const cell=v=>'"'+String(v??'').replace(/"/g,'""')+'"';
  return [keys.map(cell).join(','),...rows.map(r=>keys.map(k=>cell(typeof r[k]==='object'?JSON.stringify(r[k]):r[k])).join(','))].join('\n');
}
function exportData(name,data,type){
  let body=data, mime='text/plain', ext=type;
  if(type==='json'){body=JSON.stringify(data,null,2); mime='application/json'}
  else if(type==='csv'){body=typeof data==='string'?data:rowsToCsv(data); mime='text/csv'}
  else if(type==='md'){body=String(data??''); mime='text/markdown'}
  else if(type==='graphml'){body=String(data??''); mime='application/xml'}
  else if(type==='gexf'){body=String(data??''); mime='application/xml'}
  const a=document.createElement('a'); a.href=URL.createObjectURL(new Blob([body],{type:mime})); a.download=`wisp_${name}.${ext}`; a.click(); URL.revokeObjectURL(a.href);
}
function renderResultsMarkdown(){return '# WISP Results\n\n'+filteredResults().map(r=>`- [${r.source||'crawl'}] depth ${r.depth??0} — ${r.url}`).join('\n')}
function renderGraphMarkdown(){return `# WISP Graph\n\nNodes: ${state.graphView.nodes.length}\nEdges: ${state.graphView.edges.length}\n\n## Nodes\n`+state.graphView.nodes.map(n=>`- ${n.url||n.id} (${n.source||'crawl'}, depth ${n.depth??0})`).join('\n')}
function renderGraphML(){const ns=state.graphView.nodes||[], es=state.graphView.edges||[];return `<?xml version="1.0" encoding="UTF-8"?><graphml xmlns="http://graphml.graphdrawing.org/xmlns"><graph edgedefault="directed">${ns.map(n=>`<node id="${esc(n.id)}"><data key="url">${esc(n.url||'')}</data><data key="source">${esc(n.source||'')}</data></node>`).join('')}${es.map(e=>`<edge id="${esc(e.id||e.from+'_'+e.to)}" source="${esc(e.from||e.source)}" target="${esc(e.to||e.target)}"/>`).join('')}</graph></graphml>`}
function renderGEXF(){const ns=state.graphView.nodes||[], es=state.graphView.edges||[];return `<?xml version="1.0" encoding="UTF-8"?><gexf version="1.2"><graph mode="static" defaultedgetype="directed"><nodes>${ns.map(n=>`<node id="${esc(n.id)}" label="${esc(n.url||n.id)}"/>`).join('')}</nodes><edges>${es.map((e,i)=>`<edge id="${esc(e.id||i)}" source="${esc(e.from||e.source)}" target="${esc(e.to||e.target)}"/>`).join('')}</edges></graph></gexf>`}
async function loadExternalReport(){
  try{if(!state.jobs.length) await loadJobs(); const jid=$('#externalJobSelect')?.value||''; state.externalReport=await api('/api/external-report'+(jid?`?job_id=${encodeURIComponent(jid)}`:'')); renderExternalReport();}catch(e){toast('External report failed','err')}
}
function flatExternalRows(){const r=state.externalReport||{}; return ['candidates','resolved','failed','errors'].flatMap(k=>(r[k]||[]).map(x=>({bucket:k,...x}))) }
function filteredExternalRows(){const q=$('#externalSearch')?.value||'',scope=$('#externalSearchScope')?.value||'global';let rows=flatExternalRows().filter(x=>matchesSearch(x,q,scope));return sortRows(rows,$('#externalSort')?.value||'bucket',$('#externalSortDir')?.value||'asc')}
function renderExternalReport(){const r=state.externalReport||{}; const rows=filteredExternalRows(); $('#externalStats').innerHTML=['candidates','resolved','failed','errors'].map(k=>`<div class="graphMetric"><b>${(r[k]||[]).length}</b><small>${k}</small></div>`).join(''); if(!rows.length){$('#externalReport').innerHTML='<div class="empty">No external resolver data yet</div>';return} $('#externalReport').innerHTML=`<table><thead><tr>${sortTh('external','bucket','Bucket')}<th>From</th><th>Masked / URL</th><th>Target</th><th>Method</th><th>Reason/Error</th></tr></thead><tbody>${rows.slice(0,1500).map(x=>`<tr><td><span class="pill">${esc(x.bucket)}</span></td><td class="urlCell">${esc(x.from||x.from_url||'')}</td><td class="urlCell">${esc(x.masked_url||x.url||'')}</td><td class="urlCell">${esc(x.target||'')}</td><td>${esc(x.method||'')}</td><td>${esc(x.reason||x.error||'')}</td></tr>`).join('')}</tbody></table>`}
function renderExternalHTML(){return $('#externalReport')?.innerHTML||''}
function renderExternalMarkdown(){return '# External Resolver Report\n\n'+filteredExternalRows().map(x=>`- ${x.bucket}: ${x.masked_url||x.url||''} → ${x.target||''} ${x.reason||x.error||x.method||''}`).join('\n')}
async function loadSkipped(){
  try{if(!state.jobs.length) await loadJobs(); const jid=$('#skippedJobSelect')?.value||''; const d=await api('/api/skipped'+(jid?`?job_id=${encodeURIComponent(jid)}`:'')); state.skippedRows=d.skipped||[]; const reasons=[...new Set(state.skippedRows.map(x=>x.reason).filter(Boolean))].sort(); const sel=$('#skipReasonFilter'); if(sel){const old=sel.value; sel.innerHTML='<option value="">all reasons</option>'+reasons.map(r=>`<option>${esc(r)}</option>`).join(''); if(reasons.includes(old)) sel.value=old;} renderSkipped();}catch(e){toast('Skipped report failed','err')}
}
function filteredSkipped(){const q=$('#skipSearch')?.value||'',scope=$('#skipSearchScope')?.value||'global', reason=$('#skipReasonFilter')?.value||''; let rows=state.skippedRows.filter(x=>(!reason||x.reason===reason)&&matchesSearch(x,q,scope));return sortRows(rows,$('#skipSort')?.value||'reason',$('#skipSortDir')?.value||'asc')}
function renderSkipped(){const rows=filteredSkipped(); if(!rows.length){$('#skippedList').innerHTML='<div class="empty">No skipped URLs for current filters</div>';return} $('#skippedList').innerHTML=`<table><thead><tr>${sortTh('skipped','reason','Reason')}<th>URL</th><th>From</th>${sortTh('skipped','depth','Depth')}${sortTh('skipped','status','Status/Error')}<th>Job</th></tr></thead><tbody>${rows.slice(0,2000).map(x=>`<tr><td><span class="pill">${esc(x.reason)}</span></td><td class="urlCell">${esc(x.url)}</td><td class="urlCell">${esc(x.from_url||x.parent||'')}</td><td>${esc(x.depth??'')}</td><td>${esc(x.status||x.error||'')}</td><td class="codeLine">${esc(x.job_id||'')}</td></tr>`).join('')}</tbody></table>`}
function renderSkippedHTML(){return $('#skippedList')?.innerHTML||''}
function renderSkippedMarkdown(){return '# Why Not Crawled\n\n'+filteredSkipped().map(x=>`- ${x.reason}: ${x.url}`).join('\n')}
addFilterListeners(['skipSearch','skipSearchScope','skipReasonFilter','skipSort','skipSortDir'], renderSkipped); addFilterListeners(['externalSearch','externalSearchScope','externalSort','externalSortDir'], renderExternalReport); $('#externalJobSelect')&&($('#externalJobSelect').onchange=loadExternalReport); $('#skippedJobSelect')&&($('#skippedJobSelect').onchange=loadSkipped);
async function patternLabPreview(){const payload={domain:$('#labDomain').value,pattern:$('#labPattern').value,values:$('#labValues').value}; const d=await api('/api/pattern-lab/preview',{method:'POST',body:JSON.stringify(payload)}); state.patternLab={urls:d.urls||[],results:[]}; renderPatternLab();}
async function patternLabProbe(){if(!state.patternLab.urls?.length) await patternLabPreview(); const d=await api('/api/pattern-lab/probe',{method:'POST',body:JSON.stringify({urls:state.patternLab.urls})}); state.patternLab.results=d.results||[]; renderPatternLab();}
async function patternLabSeed(){await api('/api/pattern-seeds',{method:'POST',body:JSON.stringify({domain:$('#labDomain').value,pattern:$('#labPattern').value,values:$('#labValues').value.split(/[\n,]+/).map(x=>x.trim()).filter(Boolean),priority:90,enabled:true,notes:'Created from Pattern Lab'})}); toast('Seed created from Pattern Lab','ok'); loadSeeds();}
function renderPatternLab(){const rows=(state.patternLab.results&&state.patternLab.results.length)?state.patternLab.results:state.patternLab.urls.map(u=>({url:u})); if(!rows.length){$('#patternLabOut').innerHTML='<div class="empty">No preview yet</div>';return} $('#patternLabOut').innerHTML=`<table><thead><tr><th>#</th><th>URL</th><th>Valid</th><th>Status</th><th>Final URL</th></tr></thead><tbody>${rows.map((r,i)=>`<tr><td>${i+1}</td><td class="urlCell">${esc(r.url)}</td><td>${r.valid===undefined?'—':esc(r.valid)}</td><td>${esc(r.status||r.status_code||'')}</td><td class="urlCell">${esc(r.final_url||r.url_final||'')}</td></tr>`).join('')}</tbody></table>`}
function renderPatternLabMarkdown(){return '# Pattern Lab\n\n'+(state.patternLab.urls||[]).map(u=>`- ${u}`).join('\n')}

setInterval(()=>{ if(state.jobs&&state.jobs.length){ state.jobs.forEach(j=>{ if(["running","paused","pause_requested","restarting","cancel_requested"].includes(j.status)){ j.elapsed_seconds=(Number(j.elapsed_seconds||0)+1); if(j.seconds_since_heartbeat!==undefined&&j.seconds_since_heartbeat!==null) j.seconds_since_heartbeat=Number(j.seconds_since_heartbeat||0)+1; }}); renderJobs(); renderRunningPanel(); } },1000);
setInterval(()=>{loadJobs()},8000);


// ── Config Import / Export ─────────────────────────────────────────────────
function populateConfigJobs(){ populateExtraJobSelects(); if(!$('#configText')?.value){ /* keep empty until user asks template */ } }
async function loadConfigTemplate(){
  try{ const d=await api('/api/config/export'); $('#configText').value=d.text||''; await parseConfigText(); toast('Template loaded','ok'); }catch(e){toast('Template failed: '+e.message,'err')}
}
async function parseConfigText(){
  const text=$('#configText').value||'';
  try{ const d=await api('/api/config/parse',{method:'POST',body:JSON.stringify({text})}); state.configParsed=d.config; renderConfigPreview(d.config,d.summary); return d.config; }catch(e){toast('Config parse failed: '+e.message,'err')}
}
function renderConfigPreview(cfg,summary={}){
  if(!cfg){$('#configPreview').innerHTML='<div class="empty">No config parsed yet.</div>';return}
  $('#configSummary').textContent=`${summary.type||cfg.type||'config'} · ${(cfg.urls||[]).length} urls · ${(cfg.roots||[]).length} roots · ${(cfg.seeds||[]).length} seeds · ${(cfg.blacklist||[]).length} blacklist`;
  const opts=[['Depth',cfg.depth],['Max Pages',cfg.max_pages],['Pattern Expansion',cfg.pattern_expansion],['Route Inference',cfg.route_inference],['Soft Probe',cfg.soft_probe],['Use Seeds',cfg.use_pattern_seeds],['Auto Pagination',cfg.auto_pagination],['Pagination Limit',cfg.pagination_limit],['Follow Masked Outbound',cfg.follow_masked_outbound],['Aggressive External Resolver',cfg.masked_outbound_aggressive],['External Limit',cfg.masked_outbound_limit]];
  const urls=[...(cfg.urls||[]).map(u=>({kind:'url',url:u})),...(cfg.roots||[]).map(u=>({kind:'root',url:u})),...(cfg.seeds||[]).map(u=>({kind:'seed',url:u}))];
  $('#configPreview').innerHTML=`<div class="configPreviewBox"><h3>Targets</h3><table><thead><tr><th>Kind</th><th>URL</th></tr></thead><tbody>${urls.map(x=>`<tr><td><span class="pill">${esc(x.kind)}</span></td><td class="urlCell">${esc(x.url)}</td></tr>`).join('')||'<tr><td colspan="2">No targets found</td></tr>'}</tbody></table><h3>Options</h3><table><tbody>${opts.map(([k,v])=>`<tr><th>${esc(k)}</th><td>${esc(v)}</td></tr>`).join('')}</tbody></table><h3>Blacklist</h3><pre>${esc((cfg.blacklist||[]).join('\n')||'—')}</pre></div>`;
}
async function launchParsedConfig(){
  const cfg=state.configParsed || await parseConfigText(); if(!cfg)return;
  try{const d=await api('/api/config/launch',{method:'POST',body:JSON.stringify({config:cfg,apply_blacklist:$('#configApplyBlacklist')?.checked!==false})}); toast(`Config launched: ${d.count} job(s)`,'ok'); await refreshAfterJobCreate(); show('jobs');}catch(e){toast('Launch from config failed: '+e.message,'err')}
}
function fillRunFromConfig(){
  const c=state.configParsed; if(!c)return toast('Parse config first','err');
  $('#runUrl').value=(c.urls?.[0]||c.seeds?.[0]||c.roots?.[0]||''); $('#runDepth').value=String(c.depth||2); $('#runPages').value=String(c.max_pages||100);
  $('#optPattern').checked=!!c.pattern_expansion; $('#optRoutes').checked=!!c.route_inference; $('#optProbe').checked=!!c.soft_probe; $('#optSeeds').checked=c.use_pattern_seeds!==false; $('#optPagination').checked=!!c.auto_pagination; $('#optPaginationLimit').value=c.pagination_limit||25; $('#optMaskedOutbound').checked=!!c.follow_masked_outbound; $('#optMaskedAggressive').checked=!!c.masked_outbound_aggressive; $('#optMaskedLimit').value=c.masked_outbound_limit||300; show('run');
}
function fillMassiveFromConfig(){
  const c=state.configParsed; if(!c)return toast('Parse config first','err');
  $('#massiveRoots').value=(c.roots||[]).join('\n'); $('#massiveSeeds').value=(c.seeds||c.urls||[]).join('\n'); $('#massiveDepth').value=String(c.depth||2); $('#massivePages').value=String(c.max_pages||100); $('#massivePattern').checked=!!c.pattern_expansion; $('#massiveRoutes').checked=!!c.route_inference; $('#massiveProbe').checked=!!c.soft_probe; $('#massiveSeedsOpt').checked=c.use_pattern_seeds!==false; $('#massivePagination').checked=!!c.auto_pagination; $('#massivePaginationLimit').value=c.pagination_limit||25; $('#massiveMaskedOutbound').checked=!!c.follow_masked_outbound; $('#massiveMaskedAggressive').checked=!!c.masked_outbound_aggressive; $('#massiveMaskedLimit').value=c.masked_outbound_limit||300; show('massive');
}
function exportParsedConfig(type='txt'){
  if(type==='json'){exportData('wisp_config',state.configParsed||{},'json');return}
  exportData('wisp_config',$('#configText').value||'',type)
}

// ── Directory Discovery Mode ────────────────────────────────────────────────
function initDiscoveryDefaults(){
  const el=$('#discKeywords'); if(el && !el.value.trim()) el.value=['directory','directories','companies','vendors','partners','ecosystem','marketplace','providers','service-providers','software','tools','apps','dapps','exchanges','brokers','wallets','custody','liquidity','banks','fintech','crypto','web3','infrastructure','infra','defi','dex'].join('\n');
}
function discoveryPayload(){
  return {known_directory_url:$('#discKnown').value.trim(),section_root_url:$('#discSection').value.trim(),root_url:$('#discRoot').value.trim(),directory_keywords:$('#discKeywords').value,depth:+$('#discDepth').value||2,max_pages:+$('#discPages').value||100,max_sibling_routes:+$('#discSibling').value||25,launch_limit:+$('#discLaunchLimit').value||40,pagination_limit:+$('#discPaginationLimit').value||25,external_limit:+$('#discExternalLimit').value||300,find_pagination:$('#discPagination').checked,find_listing_cards:$('#discListingCards').checked,find_outbound_company_links:$('#discOutbound').checked,find_profile_detail_pages:$('#discProfileDetail').checked,pattern_expansion:$('#discPattern').checked,route_inference:$('#discRoutes').checked,soft_probe:$('#discProbe').checked,aggressive_external_resolver:$('#discAggressive').checked};
}
async function previewDirectoryDiscovery(){
  try{const d=await api('/api/directory-discovery/preview',{method:'POST',body:JSON.stringify(discoveryPayload())}); state.discoveryCandidates=d.candidates||[]; renderDirectoryDiscovery(); toast(`${d.count} candidate route(s)`,'ok');}catch(e){toast('Discovery preview failed: '+e.message,'err')}
}
function renderDirectoryDiscovery(){
  const rows=state.discoveryCandidates||[]; if(!rows.length){$('#discOut').innerHTML='<div class="empty">No candidates yet.</div>';return}
  $('#discOut').innerHTML=`<table><thead><tr><th>#</th><th>Candidate URL</th><th>Reason</th></tr></thead><tbody>${rows.map((r,i)=>`<tr><td>${i+1}</td><td class="urlCell">${esc(r.url)}</td><td>${esc(r.reason||'')}</td></tr>`).join('')}</tbody></table>`;
}
async function runDirectoryDiscovery(){
  try{const d=await api('/api/directory-discovery/run',{method:'POST',body:JSON.stringify(discoveryPayload())}); toast(`Directory Discovery launched: ${d.count} job(s)`,'ok'); await refreshAfterJobCreate(); show('jobs');}catch(e){toast('Directory Discovery failed: '+e.message,'err')}
}
function exportDirectoryDiscovery(type){
  const rows=state.discoveryCandidates||[]; if(type==='json') exportData('directory_discovery',rows,'json'); else exportData('directory_discovery',rowsToCsv(rows),'csv')
}
