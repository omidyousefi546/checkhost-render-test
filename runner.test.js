import test from 'node:test';
import assert from 'node:assert/strict';
import {once} from 'node:events';
import {createApp,validJob,configuration} from '../server.js';
import {createChecker} from '../check-host.js';
const config={runnerSecret:'a'.repeat(32),callbackSecret:'b'.repeat(32),callbackUrl:'https://worker.example/internal/check-result'};
const job=(kind='ping')=>({job_id:crypto.randomUUID(),kind,min_ir_nodes:1,expires_at:Date.now()+600000,...(kind==='ping'?{target:'example.com'}:{})});
async function host(app){const server=app.listen(0,'127.0.0.1');await once(server,'listening');return {url:'http://127.0.0.1:'+server.address().port,close:()=>new Promise(r=>server.close(r))};}
const headers={Authorization:'Bearer '+config.runnerSecret,'Content-Type':'application/json'};
test('config validates separate secrets and fixed HTTPS callback destination',()=>{
 assert.throws(()=>configuration({RUNNER_SECRET:'a',CALLBACK_SECRET:'b'}));
 assert.throws(()=>configuration({RUNNER_SECRET:'a'.repeat(32),CALLBACK_SECRET:'a'.repeat(32),WORKER_CALLBACK_URL:config.callbackUrl}));
 assert.equal(validJob({...job(),target:'https://evil.example/path'}),false);
 assert.equal(validJob({...job(),callback_url:'https://evil.example'}),false);
});
test('POST accepts before execution completes; duplicate job is not executed twice and callback retries',async()=>{
 let resolveExecution,resolveCallback;const started=new Promise(r=>resolveExecution=r),delivered=new Promise(r=>resolveCallback=r);
 let calls=0,callbackCalls=0;const logs=[];
 const app=createApp(config,{execute:async()=>{calls++;return started;},sleepFn:async()=>{},log:x=>logs.push(x),fetchFn:async(url,init)=>{
  assert.equal(url,config.callbackUrl);assert.equal(init.headers.Authorization,'Bearer '+config.callbackSecret);
  callbackCalls++;if(callbackCalls===1)return Response.json({error:'busy'},{status:503});
  resolveCallback(JSON.parse(init.body));return Response.json({received:true});
 }});
 const h=await host(app);try{
  const j=job();const send=body=>fetch(h.url+'/check',{method:'POST',headers,body:JSON.stringify(body)});
  assert.equal((await send(j)).status,202);assert.equal((await send(j)).status,202);
  assert.equal((await send({...j,target:'other.example'})).status,409);
  resolveExecution({pass:true,successful_nodes:1,total_nodes:1});
  const result=await delivered;assert.equal(result.job_id,j.job_id);assert.equal(calls,1);assert.equal(callbackCalls,2);
 }finally{await h.close();}
});
test('API rejects wrong secret, invalid JSON, expired and invalid jobs',async()=>{
 const h=await host(createApp(config));try{
  assert.equal((await fetch(h.url+'/check',{method:'POST',headers:{...headers,Authorization:'Bearer wrong'},body:'{}'})).status,401);
  assert.equal((await fetch(h.url+'/check',{method:'POST',headers,body:'{'})).status,400);
  assert.equal((await fetch(h.url+'/check',{method:'POST',headers,body:JSON.stringify({...job(),expires_at:1})})).status,410);
  assert.equal((await fetch(h.url+'/check',{method:'POST',headers,body:JSON.stringify({...job(),target:'bad/path'})})).status,400);
 }finally{await h.close();}
});
test('Check-Host nodes selected dynamically; nested pending and successful ping results polled',async()=>{
 let polls=0;const execute=createChecker({sleepFn:async()=>{},fetchFn:async(url,init)=>{
  assert.deepEqual(init.headers,{Accept:'application/json','User-Agent':'TelegramProxyAdmin/1.1'});
  if(url.endsWith('/nodes/hosts'))return Response.json({nodes:{ir1:{location:['ir']},us1:{location:['us']}}});
  if(url.includes('/check-ping?')){assert.deepEqual(new URL(url).searchParams.getAll('node'),['ir1']);return Response.json({ok:1,request_id:'abc',nodes:{ir1:['ir']}});}
  polls++;return Response.json({ir1:polls===1?null:[[['TIMEOUT'],['OK',0.1]]]});
 }});
 assert.deepEqual(await execute(job()),{pass:true,successful_nodes:1,total_nodes:1});assert.equal(polls,2);
});
test('403 is not retried; 5xx retries bounded; invalid JSON and timeout return typed errors',async()=>{
 let calls=0;const options={sleepFn:async()=>{},fetchFn:async()=>{calls++;return new Response('Forbidden',{status:403});}};
 await assert.rejects(createChecker(options)(job()),e=>e.code==='UPSTREAM_HTTP'&&e.http_status===403);assert.equal(calls,1);
 calls=0;options.fetchFn=async()=>{calls++;return new Response('Unavailable',{status:503});};
 await assert.rejects(createChecker(options)(job()),e=>e.http_status===503);assert.equal(calls,3);
 options.fetchFn=async()=>new Response('<html>not json</html>');await assert.rejects(createChecker(options)(job()),e=>e.code==='UPSTREAM_JSON');
 options.fetchFn=async()=>{throw Error('network');};await assert.rejects(createChecker(options)(job()),e=>e.code==='UPSTREAM_TIMEOUT');
});
test('completed negative ping is a failure, incomplete ping ends in timeout',async()=>{
 let time=Date.now(),pending=false;const execute=createChecker({clock:()=>time,checkMs:100,sleepFn:async()=>{time+=30;},fetchFn:async url=>{
  if(url.endsWith('/nodes/hosts'))return Response.json({nodes:{ir1:{location:['ir']}}});
  if(url.includes('/check-ping?'))return Response.json({ok:1,request_id:'abc',nodes:{ir1:['ir']}});
  return Response.json({ir1:pending?null:[[['TIMEOUT']]]});
 }});
 assert.equal((await execute(job())).pass,false);pending=true;
 await assert.rejects(execute(job()),e=>e.code==='CHECK_TIMEOUT');
});
test('callback auth error is logged; duplicate submission retries cached result without new check',async()=>{
 let executions=0,deliveries=0,resolveRejected,resolveDelivered;
 const rejected=new Promise(r=>resolveRejected=r),delivered=new Promise(r=>resolveDelivered=r);
 const app=createApp(config,{execute:async()=>{executions++;return {iran_nodes:2};},log:line=>{if(line.includes('callback_rejected'))resolveRejected();},fetchFn:async()=>{
  deliveries++;if(deliveries===1)return Response.json({error:'unauthorized'},{status:401});
  resolveDelivered();return Response.json({received:true});
 }});
 const h=await host(app);try{
  const j=job('nodes'),send=()=>fetch(h.url+'/check',{method:'POST',headers,body:JSON.stringify(j)});
  await send();await rejected;await send();await delivered;assert.equal(executions,1);assert.equal(deliveries,2);
 }finally{await h.close();}
});
