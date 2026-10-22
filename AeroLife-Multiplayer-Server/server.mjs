import http from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import crypto from 'node:crypto';
import {promisify} from 'node:util';
import {readFileSync} from 'node:fs';
import {WebSocketServer,WebSocket} from 'ws';
const scrypt=promisify(crypto.scrypt);
const kinds=new Set(JSON.parse(readFileSync(new URL('./kinds.json',import.meta.url))));
const number=(v,min,max,fallback=0)=>typeof v==='number'&&Number.isFinite(v)?Math.min(max,Math.max(min,v)):fallback;
const text=(v,max)=>typeof v==='string'?v.replace(/[\x00-\x1f\x7f]/g,'').trim().slice(0,max):'';
const vec=(v,lo=-1180,hi=1180)=>Array.isArray(v)&&v.length===3&&v.every(n=>typeof n==='number'&&Number.isFinite(n))?v.map((n,i)=>number(n,i===1?-15:lo,i===1?600:hi)):null;
function config(v={}){
 if(!v||typeof v!=='object'||Array.isArray(v))v={};
 return {a:Math.floor(number(v.a,0,99)),b:Math.floor(number(v.b,0,99)),out:Math.floor(number(v.out,0,99)),interval:number(v.interval,.1,60,2),duration:number(v.duration,.1,60,.3),radius:number(v.radius,.1,30,4),enabled:v.enabled!==false,level:v.level===true};
}
function record(v){
 if(!v||!kinds.has(v.kind))return null;const p=vec(v.pos),r=vec(v.rot,-630,630);if(!p||!r)return null;
 return {kind:v.kind,pos:p,rot:r,scale:number(v.scale,.25,4,1),dynamic:v.dynamic===true,active:v.active!==false,signal:v.signal===true,config:config(v.config)};
}
export function createGameServer({port=Number(process.env.PORT||8787),host='0.0.0.0',maxRooms=8,maxPlayers=8}={}){
 const rooms=new Map(),clients=new Map(),ipBuckets=new Map();let serial=0,passwordJobs=0;
 const app=http.createServer((req,res)=>{res.setHeader('Content-Type','application/json');res.setHeader('Cache-Control','no-store');res.writeHead(req.url==='/health'||req.url==='/'?200:404);res.end(JSON.stringify({ok:req.url==='/health'||req.url==='/',service:'AeroLife room server',version:'0.7',rooms:rooms.size,players:clients.size,limits:{rooms:maxRooms,playersPerRoom:maxPlayers},persistentWorlds:false}));});
 const wss=new WebSocketServer({server:app,maxPayload:65536,perMessageDeflate:false});
 const send=(c,msg)=>{if(c.ws.readyState===WebSocket.OPEN&&c.ws.bufferedAmount<524288)c.ws.send(JSON.stringify(msg));};
 const error=(c,message)=>send(c,{t:'error',message});
 const members=r=>[...r.members].map(id=>{const c=clients.get(id);return {id,name:c.name,role:c.role,color:c.color};});
 const broadcast=(r,msg,except='')=>{for(const id of r.members)if(id!==except){const c=clients.get(id);if(c)send(c,msg);}};
 const roster=r=>broadcast(r,{t:'roster',players:members(r),owner:r.owner});
 const snapshot=(c,r)=>send(c,{t:'state',objects:[...r.objects.values()],elapsed:(Date.now()-r.created)/1000});
 function leave(c,reason='left'){
  const r=rooms.get(c.room);c.room='';if(!r)return;r.members.delete(c.id);
  if(c.id===r.owner){broadcast(r,{t:'closed',message:'Владелец закрыл сервер'});for(const id of r.members){const peer=clients.get(id);if(peer)peer.room='';}rooms.delete(r.id);}
  else {broadcast(r,{t:'left',id:c.id,reason});roster(r);}
 }
 function canBuild(c,obj){return ['owner','admin','editor'].includes(c.role)||(c.role==='builder'&&(!obj||obj.owner===c.id));}
 function admit(c,r){c.room=r.id;r.members.add(c.id);if(c.id!==r.owner)c.role='visitor';send(c,{t:'joined',room:{id:r.id,name:r.name,map:r.map,mode:r.mode,owner:r.owner},id:c.id,players:members(r),objects:[...r.objects.values()],elapsed:(Date.now()-r.created)/1000});roster(r);}
 function rate(c,key,count,ms){const now=Date.now();let b=c.rates[key];if(!b||now-b.at>=ms)b=c.rates[key]={at:now,n:0};return ++b.n<=count;}
 async function passwordHash(c,password,salt){
  if(passwordJobs>=4||c.authPending){error(c,'Сервер занят проверкой паролей. Повторите вход.');return null;}
  passwordJobs++;c.authPending=true;try{return await scrypt(password,salt,32);}finally{passwordJobs--;c.authPending=false;}
 }
 wss.on('connection',(ws,req)=>{
  const now=Date.now(),ip=req.socket.remoteAddress;let bucket=ipBuckets.get(ip);
  if(!bucket||now-bucket.at>10000){bucket={at:now,n:0};ipBuckets.set(ip,bucket);}
  if(++bucket.n>48){ws.close(1013,'Connection rate limit');return;}

  // No account/identity service: bans apply to this relay connection identity + opaque local ticket.
  // Do NOT claim that a guest identity prevents deliberate ban evasion.
  if(clients.size>=96){ws.close(1013,'Server full');return;}
  const c={id:String(++serial),ws,name:'Игрок',color:0,role:'visitor',room:'',ticket:'',rates:{},ip:req.socket.remoteAddress,alive:true};clients.set(c.id,c);
  send(c,{t:'hello',id:c.id,version:'0.7'});
  ws.on('pong',()=>c.alive=true);
  ws.on('message',async raw=>{
   try{
    if(!rate(c,'all',100,1000)){ws.close(1008,'Rate limit');return;}
    let m;try{m=JSON.parse(raw.toString());}catch{return error(c,'Некорректный JSON');}
    if(!m||typeof m!=='object'||Array.isArray(m)||typeof m.t!=='string')return error(c,'Некорректный пакет');
    if(m.t==='hello'){
     if(c.ticket)return error(c,'Идентификатор уже задан');
     c.name=text(m.name,24)||'Игрок';c.color=Math.floor(number(m.color,0,5));
     c.ticket=typeof m.ticket==='string'&&/^[a-f0-9]{48}$/.test(m.ticket)?m.ticket:crypto.randomBytes(24).toString('hex');send(c,{t:'identity',ticket:c.ticket});return;
    }
    if(!c.ticket)return error(c,'Сначала представьтесь');
    if(m.t==='list'){if(!rate(c,'list',3,1000))return;send(c,{t:'list',rooms:[...rooms.values()].map(r=>({id:r.id,name:r.name,map:r.map,mode:r.mode,locked:!!r.password,players:r.members.size,max:maxPlayers}))});return;}
    if(m.t==='create'){
     if(c.room)return error(c,'Сначала выйдите из комнаты');
     if(!rate(c,'create',2,60000)||rooms.size>=maxRooms)return error(c,'Лимит создания комнат');
     if(m.map!=='aero_world'||m.mode!=='sandbox')return error(c,'Пока доступны только Города и луга / Песочница');
     const password=text(m.password,64),salt=crypto.randomBytes(16),hash=password?await passwordHash(c,password,salt):null;
     if(password&&!hash)return;
     if(ws.readyState!==WebSocket.OPEN)return;
     if(c.room||rooms.size>=maxRooms)return error(c,'Лимит комнат');
     const id=crypto.randomBytes(4).toString('hex').toUpperCase();const r={id,name:text(m.name,48)||'AeroLife',map:m.map,mode:m.mode,owner:c.id,password:hash,salt,created:Date.now(),members:new Set(),bans:new Set(),objects:new Map(),nextId:1};
     if(Array.isArray(m.objects))for(const item of m.objects.slice(0,120)){const o=record(item);if(o){o.uid=r.nextId++;o.owner=c.id;r.objects.set(o.uid,o);}}
     rooms.set(id,r);c.role='owner';admit(c,r);return;
    }
    if(m.t==='join'){
     if(c.room)return error(c,'Сначала выйдите из комнаты');
     if(!rate(c,'join',5,15000))return error(c,'Слишком много попыток входа');
     const r=rooms.get(text(m.id,8).toUpperCase());if(!r)return error(c,'Комната не найдена');if(r.bans.has(c.ticket))return error(c,'Вход запрещён владельцем');
     if(r.password){const hash=await passwordHash(c,text(m.password,64),r.salt);if(!hash)return;if(!crypto.timingSafeEqual(hash,r.password))return error(c,'Неверный пароль');}
     // Recheck after async password hashing: room/ban/capacity can change concurrently.
     if(ws.readyState!==WebSocket.OPEN||c.room)return;if(rooms.get(r.id)!==r)return error(c,'Комната закрыта');
     if(r.bans.has(c.ticket)||r.members.size>=maxPlayers)return error(c,'Нет доступа или свободных мест');admit(c,r);return;
    }
    if(m.t==='leave'){leave(c);send(c,{t:'closed',message:'Вы вышли из сервера'});return;}
    const r=rooms.get(c.room);if(!r)return error(c,'Вы не в комнате');
    if(m.t==='pose'){
     if(!rate(c,'pose',25,1000))return;const p=vec(m.pos);if(!p)return;
     c.pose={id:c.id,pos:p,yaw:number(m.yaw,-630,630),car:m.car===true,color:c.color,clock:c.id===r.owner?number(m.clock,0,100000000):undefined};broadcast(r,{t:'pose',...c.pose},c.id);return;
    }
    if(m.t==='chat'){if(!rate(c,'chat',5,5000))return;const message=text(m.message,240);if(message)broadcast(r,{t:'chat',id:c.id,name:c.name,message});return;}
    if(m.t==='moderate'){
     if(!['owner','admin'].includes(c.role))return error(c,'Нет прав администратора');
     const target=clients.get(String(m.target));if(!target||target.room!==r.id||target.id===r.owner||target.id===c.id)return error(c,'Нельзя изменить этого игрока');
     if(c.role!=='owner'&&target.role==='admin')return error(c,'Только владелец управляет администраторами');
     if(m.action==='role'){
      if(c.role!=='owner'||!['visitor','builder','editor','admin'].includes(m.role))return error(c,'Только владелец назначает права');target.role=m.role;roster(r);return;
     }
     if(!['kick','ban'].includes(m.action))return error(c,'Неизвестное действие');
     if(m.action==='ban')r.bans.add(target.ticket);
     leave(target,m.action);send(target,{t:'closed',message:m.action==='ban'?'Вы заблокированы в этой комнате':'Вас исключили из комнаты'});return;
    }
    if(m.t==='sync'){
     if(c.id!==r.owner||!rate(c,'sync',32,1000)||!Array.isArray(m.objects))return;
     const changed=[];for(const item of m.objects.slice(0,20)){const old=r.objects.get(Number(item.uid));const p=vec(item.pos),rot=vec(item.rot,-630,630);if(old&&p&&rot){old.pos=p;old.rot=rot;old.active=item.active!==false;old.signal=item.signal===true;changed.push({uid:old.uid,pos:p,rot,active:old.active,signal:old.signal});}}
     if(changed.length)broadcast(r,{t:'sync',objects:changed},c.id);return;
    }
    if(!rate(c,'edit',12,1000))return error(c,'Слишком много изменений');
    if(m.t==='spawn'){
     if(!canBuild(c))return error(c,'Право строительства не выдано');if(r.objects.size>=120)return error(c,'Лимит 120 предметов');
     const o=record(m.record);if(!o)return error(c,'Неверный предмет');o.uid=r.nextId++;o.owner=c.id;r.objects.set(o.uid,o);broadcast(r,{t:'spawn',record:o});return;
    }
    const old=r.objects.get(Number(m.uid));if(!old)return error(c,'Предмет не найден');
    if(m.t==='control'){
     if(c.role==='visitor'&&m.configure)return error(c,'Нет прав настройки');
     if(m.configure&&!canBuild(c,old))return error(c,'Недостаточно прав');
     if(m.configure){old.config=config(m.config);broadcast(r,{t:'config',uid:old.uid,config:old.config});}
     else {
      if(old.kind==='switch')old.config.level=!old.config.level;
      else if(['repeater','trigger','and_gate','or_gate','not_gate','xor_gate','latch','delay'].includes(old.kind))old.config.enabled=!old.config.enabled;
      else if(old.kind!=='button'&&old.config.a===0)old.config.level=!old.config.level;
      broadcast(r,{t:'control',uid:old.uid});
     }return;
    }
    if(!canBuild(c,old))return error(c,'Нет прав изменять этот предмет');
    if(m.t==='delete'){r.objects.delete(old.uid);broadcast(r,{t:'delete',uid:old.uid});return;}
    if(m.t==='update'){
     const next=record(m.record);if(!next||next.kind!==old.kind)return error(c,'Неверное изменение');next.uid=old.uid;next.owner=old.owner;r.objects.set(old.uid,next);broadcast(r,{t:'update',record:next});return;
    }
   }catch(e){error(c,'Ошибка обработки запроса');}
  });
  ws.on('close',()=>{leave(c);clients.delete(c.id);});ws.on('error',()=>{});
 });
 const heartbeat=setInterval(()=>{for(const [ip,b] of ipBuckets)if(Date.now()-b.at>60000)ipBuckets.delete(ip);for(const c of clients.values()){if(!c.alive){c.ws.terminate();continue;}c.alive=false;c.ws.ping();}},20000);heartbeat.unref();
 const snapshots=setInterval(()=>{for(const r of rooms.values())for(const id of r.members){const c=clients.get(id);if(c)snapshot(c,r);}},5000);snapshots.unref();
 app.listen(port,host,()=>console.log(`AeroLife relay listening on ${host}:${app.address().port}`));
 return {app,wss,rooms,clients,close:async()=>{clearInterval(heartbeat);clearInterval(snapshots);for(const c of clients.values())c.ws.terminate();await new Promise(resolve=>wss.close(resolve));await new Promise(resolve=>app.close(resolve));}};
}
if(process.argv[1]&&path.resolve(fileURLToPath(import.meta.url))===path.resolve(process.argv[1]))createGameServer();
