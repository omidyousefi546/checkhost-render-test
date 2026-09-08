import {setTimeout as sleep} from 'node:timers/promises';
export class CheckError extends Error {
  constructor(code,message,http_status){super(message);this.code=code;this.http_status=http_status;}
}
const fail=(code,message,status)=>{throw new CheckError(code,message,status);};
const object=x=>x && typeof x==='object'&&!Array.isArray(x);
export function createChecker({fetchFn=fetch,sleepFn=sleep,clock=Date.now,requestMs=10000,checkMs=180000,pollMs=4000}={}) {
  async function api(path,deadline) {
    for(let attempt=0;attempt<3;attempt++) {
      const remaining=deadline-clock();if(remaining<=0)fail('CHECK_TIMEOUT','Check deadline exceeded');
      let res;
      try{res=await fetchFn('https://check-host.net'+path,{headers:{Accept:'application/json','User-Agent':'TelegramProxyAdmin/1.1'},
        redirect:'manual',signal:AbortSignal.timeout(Math.max(1,Math.min(requestMs,remaining)))});}
      catch{
        if(attempt===2)fail('UPSTREAM_TIMEOUT','Check-Host request timed out or network failed');
        await sleepFn(1000*(attempt+1));continue;
      }
      if(!res.ok) {
        const retryAfter=Number(res.headers.get('retry-after'));
        const detail=`Check-Host HTTP ${res.status}; endpoint=${path.split('?')[0]}`;
        await res.body?.cancel();
        if((res.status>=500||res.status===429)&&attempt<2){
          await sleepFn(Math.min(10000,Math.max(1000*(attempt+1),Number.isFinite(retryAfter)?retryAfter*1000:0)));continue;
        }
        fail('UPSTREAM_HTTP',detail,res.status);
      }
      let data;try{data=await res.json();}catch(e){
        if(['AbortError','TimeoutError'].includes(e.name))fail('UPSTREAM_TIMEOUT','Check-Host response body timed out');
        fail('UPSTREAM_JSON','Check-Host returned invalid JSON');
      }
      if(!object(data))fail('UPSTREAM_JSON','Check-Host returned invalid object');
      return data;
    }
  }
  return async function execute(job) {
    const deadline=Math.min(job.expires_at-5000,clock()+checkMs);
    const discovered=await api('/nodes/hosts',deadline);
    if(!object(discovered.nodes))fail('UPSTREAM_JSON','Invalid nodes response');
    const nodes=Object.entries(discovered.nodes).filter(([,v])=>v?.location?.[0]?.toLowerCase()==='ir').map(([key])=>key);
    if(nodes.length<job.min_ir_nodes)fail('NO_IR_NODES','Not enough Iranian checking nodes');
    if(job.kind==='nodes')return {iran_nodes:nodes.length};
    const query=new URLSearchParams({host:job.target});for(const node of nodes)query.append('node',node);
    const started=await api('/check-ping?'+query,deadline);
    if(started.ok!==1 || !/^[a-zA-Z0-9_-]+$/.test(String(started.request_id||'')) || !object(started.nodes))fail('UPSTREAM_JSON','Invalid ping acceptance response');
    const selected=Object.keys(started.nodes).filter(n=>nodes.includes(n));
    if(selected.length<job.min_ir_nodes)fail('NO_IR_NODES','Not enough Iranian nodes accepted the check');
    while(clock()<deadline) {
      await sleepFn(Math.min(pollMs,Math.max(1,deadline-clock())));
      const data=await api('/check-result/'+encodeURIComponent(started.request_id),deadline);
      let successful=0,complete=true;
      for(const n of selected){
        const value=data[n];if(value==null){complete=false;continue;}
        if(!Array.isArray(value)||value.some(group=>!Array.isArray(group)))fail('UPSTREAM_JSON','Invalid ping results');
        if(value.some(group=>group.some(packet=>Array.isArray(packet)&&packet[0]==='OK')))successful++;
      }
      if(successful>=job.min_ir_nodes||complete)return {pass:successful>=job.min_ir_nodes,successful_nodes:successful,total_nodes:selected.length};
    }
    fail('CHECK_TIMEOUT','Check-Host did not complete the ping in time');
  };
}
