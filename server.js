// 용사의 대륙 서버: 접속자 공유·채팅·계정저장 + 파티·거래
const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const DF=path.join(process.env.DATA_DIR||__dirname,'data.json');
let A={};try{A=JSON.parse(fs.readFileSync(DF,'utf8'))}catch(e){}
let dt=0;const persist=()=>{clearTimeout(dt);dt=setTimeout(()=>fs.writeFile(DF,JSON.stringify(A),()=>{}),2000)};
const srv=http.createServer((q,r)=>fs.readFile(path.join(__dirname,'index.html'),(e,b)=>{if(e){r.writeHead(200);return r.end('ok')}r.writeHead(200,{'content-type':'text/html; charset=utf-8'});r.end(b)}));
const wss=new WebSocketServer({server:srv,maxPayload:2e6}),C=new Set();let nid=0;
const out=(c,o)=>{if(c&&c.readyState===1)c.send(JSON.stringify(o))},tell=(c,d)=>out(c,{t:'g',d}),er=(c,r)=>tell(c,{a:'er',r});
const byName=n=>{for(const c of C)if(c.name===n)return c};
const nk=n=>'u'+Buffer.from(n.toLowerCase()).toString('hex');
const ver=c=>c&&c.name&&c.k===nk(c.name)&&A[c.k];
setInterval(()=>{const l=[...C].filter(c=>c.pres&&c.pres.n);for(const c of C)out(c,{t:'s',n:C.size,l:l.filter(x=>x!==c).map(x=>[x.id,x.pres])})},500);
wss.on('connection',c=>{c.id=++nid;C.add(c);
c.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(e){return}
if(m.t==='p'){if(m.d&&typeof m.d.n==='string'){c.pres=m.d;c.name=m.d.n.slice(0,40)}}
else if(m.t==='c'){const now=Date.now();if(now-(c.ct||0)<500)return;c.ct=now;for(const x of C)out(x,{t:'c',d:m.d,id:c.id,me:x===c})}
else if(m.t==='get'){const a=A[m.k],ok=!!a&&a.h===m.h;if(ok)c.k=m.k;out(c,{t:'r',r:m.r,ex:!!a,ok,p:ok?a.p:undefined,ts:a&&a.ts})}
else if(m.t==='set'){const a=A[m.k];if((a&&a.h!==m.h)||typeof m.p!=='string')return out(c,{t:'r',r:m.r,err:'auth'});A[m.k]={h:m.h,p:m.p,ts:m.ts};c.k=m.k;persist();out(c,{t:'r',r:m.r,ok:1})}
else if(m.t==='g'&&m.d)game(c,m.d)});
c.on('close',()=>{C.delete(c);if(c.name){leave(c.name);cancel(c.name,'상대가 접속을 종료했어요')}})});
// ===== 파티 =====
const inv=new Map(),PT=new Map(),TR=new Map(),fresh=k=>Date.now()-(inv.get(k)||0)<6e4;
const pushP=p=>p.m.forEach(n=>tell(byName(n),{a:'pt',m:p.m,l:p.l}));
function leave(n){const p=PT.get(n);if(!p)return;PT.delete(n);p.m=p.m.filter(x=>x!==n);tell(byName(n),{a:'pt',m:[],l:''});
if(p.m.length<2)p.m.forEach(x=>{PT.delete(x);tell(byName(x),{a:'pt',m:[],l:''})});else{if(p.l===n)p.l=p.m[0];pushP(p)}}
// ===== 거래 (저장된 계정 데이터로 골드·무기 보유를 검증하고 서버가 교환) =====
const view=(t,n)=>{const o=t.a===n?t.b:t.a;return{a:'tr',p:o,mine:t.o[n],theirs:t.o[o],ml:!!t.l[n],tl:!!t.l[o],ok:!!t.k[n]}};
const push=t=>[t.a,t.b].forEach(n=>tell(byName(n),view(t,n)));
function cancel(n,r){const t=TR.get(n);if(!t)return;TR.delete(t.a);TR.delete(t.b);[t.a,t.b].forEach(x=>tell(byName(x),{a:'tx',r}))}
const rec=n=>{try{return JSON.parse(A[nk(n)].p)}catch(e){return null}};
function chk(n,o,other){const p=rec(n),q=rec(other);if(!p||!q)return'저장 정보를 확인할 수 없어요';if(!(o.g>=0&&o.g<=p.g))return'골드가 부족해요';
for(const k of o.w){if(!p.ow||!p.ow[k]||p.w==k)return'없는 무기이거나 장착 중이에요';if(q.ow&&q.ow[k])return'상대가 이미 가진 무기예요'}return''}
function commit(t){const ca=byName(t.a),cb=byName(t.b);if(!ver(ca)||!ver(cb))return cancel(t.a,'계정 확인에 실패했어요');
const oa=t.o[t.a],ob=t.o[t.b],e=chk(t.a,oa,t.b)||chk(t.b,ob,t.a);if(e)return cancel(t.a,e);
const pa=rec(t.a),pb=rec(t.b);pa.g+=ob.g-oa.g;pb.g+=oa.g-ob.g;
oa.w.forEach(k=>{delete pa.ow[k];pb.ow[k]=1});ob.w.forEach(k=>{delete pb.ow[k];pa.ow[k]=1});
const ts=Date.now();[[t.a,pa],[t.b,pb]].forEach(([n,p])=>{A[nk(n)].p=JSON.stringify(p);A[nk(n)].ts=ts});persist();
TR.delete(t.a);TR.delete(t.b);tell(ca,{a:'td',give:oa,get:ob});tell(cb,{a:'td',give:ob,get:oa})}
function game(c,d){const n=c.name;if(!n)return;
if(['tq','ta','to','tl','tk'].includes(d.a)&&!ver(c))return er(c,'로그인 확인 중이에요. 잠시 후 다시 해 주세요');
if(d.a==='inv'||d.a==='tq'){const x=byName(d.to);if(!x||x===c)return er(c,'접속 중이 아니에요');
if(d.a==='tq'&&(!ver(x)||TR.has(n)||TR.has(d.to)))return er(c,'지금은 거래할 수 없어요');
inv.set(d.a+d.to+'|'+n,Date.now());tell(x,{a:d.a,from:n})}
else if(d.a==='acc'){const k='inv'+n+'|'+d.to;if(!fresh(k)||PT.has(n))return er(c,'초대가 만료됐거나 이미 파티 중이에요');inv.delete(k);
let p=PT.get(d.to);if(!p){p={m:[d.to],l:d.to};PT.set(d.to,p)}if(p.m.length>=4)return er(c,'파티가 가득 찼어요');p.m.push(n);PT.set(n,p);pushP(p)}
else if(d.a==='lv')leave(n);
else if(d.a==='kick'){const p=PT.get(n);if(p&&p.l===n&&d.n!==n&&p.m.includes(d.n))leave(d.n)}
else if(d.a==='sh'){const p=PT.get(n),now=Date.now();if(!p||!(d.v>0&&d.v<1e7)||now-(c.sh||0)<60)return;c.sh=now;p.m.forEach(x=>x!==n&&tell(byName(x),{a:'sh',from:n,v:d.v|0}))}
else if(d.a==='ta'){const k='tq'+n+'|'+d.to;if(!fresh(k)||!byName(d.to)||TR.has(n)||TR.has(d.to))return er(c,'거래를 시작할 수 없어요');inv.delete(k);
const t={a:d.to,b:n,o:{[d.to]:{g:0,w:[]},[n]:{g:0,w:[]}},l:{},k:{}};TR.set(d.to,t);TR.set(n,t);push(t)}
else{const t=TR.get(n);if(!t)return;const o=t.a===n?t.b:t.a;
if(d.a==='tc')cancel(n,'거래가 취소됐어요');
else if(d.a==='to'){const m=t.o[n],g=d.g===undefined?m.g:d.g|0,w=Array.isArray(d.w)?[...new Set(d.w.map(String))].slice(0,20):m.w,e=chk(n,{g,w},o);if(e)return er(c,e);m.g=g;m.w=w;t.l={};t.k={};push(t)}
else if(d.a==='tl'){const e=chk(n,t.o[n],o);if(e)return er(c,e);t.l[n]=1;push(t)}
else if(d.a==='tk'&&t.l[n]&&t.l[o]){t.k[n]=1;if(t.k[o])commit(t)}}}
srv.listen(process.env.PORT||3000);
