import express from 'express';
import {timingSafeEqual} from 'node:crypto';
import {isIP} from 'node:net';
import {pathToFileURL} from 'node:url';
import {setTimeout as sleep} from 'node:timers/promises';
import {createChecker} from './check-host.js';

export function configuration(env) {
  for(const key of ['RUNNER_SECRET','CALLBACK_SECRET'])if(!/^[A-Za-z0-9_-]{32,256}$/.test(env[key]||''))throw Error(`${key} must be 32–256 URL-safe characters`);
  if(env.RUNNER_SECRET===env.CALLBACK_SECRET)throw Error('Use two different secrets');
  const url=new URL(env.WORKER_CALLBACK_URL);
  if(url.protocol!=='https:'||url.username||url.password||url.search||url.hash||url.pathname!=='/internal/check-result')throw Error('Invalid WORKER_CALLBACK_URL');
  return {runnerSecret:env.RUNNER_SECRET,callbackSecret:env.CALLBACK_SECRET,callbackUrl:url.href};
}
export function validJob(job,time=Date.now()) {
  if(!job || typeof job!=='object' || Array.isArray(job))return false;
  if(Object.keys(job).some(k=>!['job_id','kind','target','min_ir_nodes','expires_at'].includes(k)))return false;
  if(!/^[a-zA-Z0-9_-]{16,80}$/.test(job.job_id||'')||!['nodes','ping'].includes(job.kind)||
    !Number.isInteger(job.min_ir_nodes)||job.min_ir_nodes<1||job.min_ir_nodes>50||
    !Number.isSafeInteger(job.expires_at)||job.expires_at>time+15*60000)return false;
  if(job.kind==='nodes')return job.target===undefined;
  const h=job.target;
  return typeof h==='string' && h.length<=253 && (isIP(h)!==0 ||
    /^(?=.{1,253}$)[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)+\.?$/.test(h));
}
const sameSecret=(header,secret)=>{
  const a=Buffer.from(header||''),b=Buffer.from('Bearer '+secret);
  return a.length===b.length&&timingSafeEqual(a,b);
};
export function createApp(config,{execute=createChecker(),fetchFn=fetch,sleepFn=sleep,clock=Date.now,log=console.log}={}) {
  const app=express();app.disable('x-powered-by');
  const jobs=new Map();let running=false;
  async function deliver(record) {
    if(record.delivering||record.delivered)return;
    record.delivering=true;
    try {
      for(let attempt=0;attempt<8 && clock()<record.job.expires_at;attempt++){
        let res;
        try {
          res=await fetchFn(config.callbackUrl,{method:'POST',redirect:'manual',signal:AbortSignal.timeout(10000),
            headers:{Authorization:'Bearer '+config.callbackSecret,'Content-Type':'application/json'},body:JSON.stringify(record.result)});
          if(res.ok){
            const ack=await res.json();if(ack.received===true){record.delivered=true;return;}
          }else{
            await res.body?.cancel();
            if(res.status===410){record.delivered=true;return;} // Worker no longer needs this job.
            if([400,401,403].includes(res.status)){log(JSON.stringify({event:'callback_rejected',job_id:record.job.job_id,status:res.status}));return;}
          }
        }catch{ /* Retry network failure, timeout, or invalid acknowledgement. */ }
        log(JSON.stringify({event:'callback_retry',job_id:record.job.job_id,attempt:attempt+1,status:res?.status||0}));
        if(attempt<7)await sleepFn(Math.min(30000,1000*2**attempt));
      }
      log(JSON.stringify({event:'callback_failed',job_id:record.job.job_id}));
    } finally {record.delivering=false;}
  }
  async function pump() {
    if(running)return;running=true;
    try {
      for(const record of jobs.values()) {
        if(record.result||record.processing)continue;
        record.processing=true;
        const job=record.job;
        try {
          if(clock()>=job.expires_at)throw Object.assign(Error('Job expired'),{code:'JOB_EXPIRED'});
          record.result={job_id:job.job_id,kind:job.kind,...(job.target?{target:job.target}:{}),status:'ok',result:await execute(job)};
        }catch(e){
          record.result={job_id:job.job_id,kind:job.kind,...(job.target?{target:job.target}:{}),status:'error',
            error:{code:e.code||'INTERNAL_ERROR',message:e.code?String(e.message).slice(0,400):'Runner internal error',
              ...(Number.isInteger(e.http_status)?{http_status:e.http_status}:{})}};
        }
        record.processing=false;
        // Callback retries must not block processing of the next queued job.
        void deliver(record).catch(()=>log(JSON.stringify({event:'callback_failed',job_id:job.job_id})));
      }
    }finally{running=false;}
  }
  app.get('/health',(_req,res)=>res.json({ok:true}));
  app.post('/check',(req,res,next)=>sameSecret(req.headers.authorization,config.runnerSecret)?next():res.status(401).json({error:'unauthorized'}),
    express.json({limit:'8kb'}),(req,res)=>{
      const time=clock(),job=req.body;
      if(!validJob(job,time))return res.status(400).json({error:'invalid_job'});
      if(job.expires_at<=time)return res.status(410).json({error:'expired_job'});
      for(const [id,r] of jobs)if(time>r.job.expires_at+600000&&!r.processing&&!r.delivering)jobs.delete(id);
      const old=jobs.get(job.job_id);
      if(old){
        if(['kind','target','min_ir_nodes','expires_at'].some(k=>old.job[k]!==job[k]))return res.status(409).json({error:'job_id_conflict'});
        res.status(202).json({accepted:true,job_id:job.job_id,duplicate:true});
        if(old.result&&!old.delivered)setImmediate(()=>void deliver(old).catch(()=>{}));
        return;
      }
      if(jobs.size>=500 || [...jobs.values()].filter(r=>!r.result).length>=20)return res.status(503).json({error:'queue_full'});
      jobs.set(job.job_id,{job});
      res.status(202).json({accepted:true,job_id:job.job_id});
      setImmediate(()=>void pump().catch(()=>log(JSON.stringify({event:'runner_failed'}))));
    });
  app.use((err,req,res,next)=>res.status(err.type==='entity.too.large'?413:400).json({error:'invalid_request'}));
  return app;
}
if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href){
  const config=configuration(process.env);
  const app=createApp(config);
  app.listen(Number(process.env.PORT||10000),'0.0.0.0',()=>console.log('Runner listening'));
}
