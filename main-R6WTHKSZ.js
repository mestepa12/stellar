var sb=Object.defineProperty,ob=Object.defineProperties;var ab=Object.getOwnPropertyDescriptors;var Ly=Object.getOwnPropertySymbols;var cb=Object.prototype.hasOwnProperty,lb=Object.prototype.propertyIsEnumerable;var Fy=(n,e,t)=>e in n?sb(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,Wt=(n,e)=>{for(var t in e||={})cb.call(e,t)&&Fy(n,t,e[t]);if(Ly)for(var t of Ly(e))lb.call(e,t)&&Fy(n,t,e[t]);return n},wn=(n,e)=>ob(n,ab(e));var nn=null,Pc=!1,Xf=1,ub=null,Gn=Symbol("SIGNAL");function Ie(n){let e=nn;return nn=n,e}function Lc(){return nn}var Ko={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Yf(n){if(Pc)throw new Error("");if(nn===null)return;nn.consumerOnSignalRead(n);let e=nn.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=nn.recomputing;if(i&&(t=e!==void 0?e.nextProducer:nn.producers,t!==void 0&&t.producer===n)){nn.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===nn&&(!i||fb(r,nn)))return;let s=Gs(nn),o={producer:n,consumer:nn,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};nn.producersTail=o,e!==void 0?e.nextProducer=o:nn.producers=o,s&&Vy(n,o)}function ky(){Xf++}function Zf(n){if(!(Gs(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Xf)){if(!n.producerMustRecompute(n)&&!eh(n)){qf(n);return}n.producerRecomputeValue(n),qf(n)}}function Jf(n){if(n.consumers===void 0)return;let e=Pc;Pc=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||db(i)}}finally{Pc=e}}function Kf(){return nn?.consumerAllowSignalWrites!==!1}function db(n){n.dirty=!0,Jf(n),n.consumerMarkedDirty?.(n)}function qf(n){n.dirty=!1,n.lastCleanEpoch=Xf}function Fc(n){return n&&Uy(n),Ie(n)}function Uy(n){n.producersTail=void 0,n.recomputing=!0}function Qf(n,e){Ie(e),n&&By(n)}function By(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(Gs(n))do t=th(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function eh(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(Zf(t),i!==t.version))return!0}return!1}function kc(n){if(Gs(n)){let e=n.producers;for(;e!==void 0;)e=th(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function Vy(n,e){let t=n.consumersTail,i=Gs(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)Vy(r.producer,r)}function th(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!Gs(e)){let s=e.producers;for(;s!==void 0;)s=th(s)}return t}function Gs(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function nh(n){ub?.(n)}function fb(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function ih(n,e){return Object.is(n,e)}function Uc(n,e){let t=Object.create(hb);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(Zf(t),Yf(t),t.value===Oc)throw t.error;return t.value};return i[Gn]=t,nh(t),i}var Wf=Symbol("UNSET"),$f=Symbol("COMPUTING"),Oc=Symbol("ERRORED"),hb=wn(Wt({},Ko),{value:Wf,dirty:!0,error:null,equal:ih,kind:"computed",producerMustRecompute(n){return n.value===Wf||n.value===$f},producerRecomputeValue(n){if(n.value===$f)throw new Error("");let e=n.value;n.value=$f;let t=Fc(n),i,r=!1;try{i=n.computation(),Ie(null),r=e!==Wf&&e!==Oc&&i!==Oc&&n.equal(e,i)}catch(s){i=Oc,n.error=s}finally{Qf(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function pb(){throw new Error}var Hy=pb;function zy(n){Hy(n)}function rh(n){Hy=n}var mb=null;function sh(n,e){let t=Object.create(Wy);t.value=n,e!==void 0&&(t.equal=e);let i=()=>Gy(t);return i[Gn]=t,nh(t),[i,o=>oh(t,o),o=>jy(t,o)]}function Gy(n){return Yf(n),n.value}function oh(n,e){Kf()||zy(n),n.equal(n.value,e)||(n.value=e,gb(n))}function jy(n,e){Kf()||zy(n),oh(n,e(n.value))}var Wy=wn(Wt({},Ko),{equal:ih,value:void 0,kind:"signal"});function gb(n){n.version++,ky(),Jf(n),mb?.(n)}function $e(n){return typeof n=="function"}function Bc(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Vc=Bc(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Qo(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var yn=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if($e(i))try{i()}catch(s){e=s instanceof Vc?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{$y(s)}catch(o){e=e??[],o instanceof Vc?e=[...e,...o.errors]:e.push(o)}}if(e)throw new Vc(e)}}add(e){var t;if(e&&e!==this)if(this.closed)$y(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Qo(t,e)}remove(e){let{_finalizers:t}=this;t&&Qo(t,e),e instanceof n&&e._removeParent(this)}};yn.EMPTY=(()=>{let n=new yn;return n.closed=!0,n})();var ah=yn.EMPTY;function Hc(n){return n instanceof yn||n&&"closed"in n&&$e(n.remove)&&$e(n.add)&&$e(n.unsubscribe)}function $y(n){$e(n)?n():n.unsubscribe()}var ti={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var js={setTimeout(n,e,...t){let{delegate:i}=js;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=js;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function zc(n){js.setTimeout(()=>{let{onUnhandledError:e}=ti;if(e)e(n);else throw n})}function ch(){}var qy=lh("C",void 0,void 0);function Xy(n){return lh("E",void 0,n)}function Yy(n){return lh("N",n,void 0)}function lh(n,e,t){return{kind:n,value:e,error:t}}var ts=null;function Ws(n){if(ti.useDeprecatedSynchronousErrorHandling){let e=!ts;if(e&&(ts={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=ts;if(ts=null,t)throw i}}else n()}function Zy(n){ti.useDeprecatedSynchronousErrorHandling&&ts&&(ts.errorThrown=!0,ts.error=n)}var ns=class extends yn{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Hc(e)&&e.add(this)):this.destination=_b}static create(e,t,i){return new $s(e,t,i)}next(e){this.isStopped?dh(Yy(e),this):this._next(e)}error(e){this.isStopped?dh(Xy(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?dh(qy,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},yb=Function.prototype.bind;function uh(n,e){return yb.call(n,e)}var fh=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Gc(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Gc(i)}else Gc(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Gc(t)}}},$s=class extends ns{constructor(e,t,i){super();let r;if($e(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&ti.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&uh(e.next,s),error:e.error&&uh(e.error,s),complete:e.complete&&uh(e.complete,s)}):r=e}this.destination=new fh(r)}};function Gc(n){ti.useDeprecatedSynchronousErrorHandling?Zy(n):zc(n)}function vb(n){throw n}function dh(n,e){let{onStoppedNotification:t}=ti;t&&js.setTimeout(()=>t(n,e))}var _b={closed:!0,next:ch,error:vb,complete:ch};var qs=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Jy(n){return n}function Ky(n){return n.length===0?Jy:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var bt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=Mb(t)?t:new $s(t,i,r);return Ws(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=Qy(i),new i((r,s)=>{let o=new $s({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[qs](){return this}pipe(...t){return Ky(t)(this)}toPromise(t){return t=Qy(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function Qy(n){var e;return(e=n??ti.Promise)!==null&&e!==void 0?e:Promise}function xb(n){return n&&$e(n.next)&&$e(n.error)&&$e(n.complete)}function Mb(n){return n&&n instanceof ns||xb(n)&&Hc(n)}function Eb(n){return $e(n?.lift)}function rn(n){return e=>{if(Eb(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function sn(n,e,t,i,r){return new hh(n,e,t,i,r)}var hh=class extends ns{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var ev=Bc(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Gi=(()=>{class n extends bt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new jc(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new ev}next(t){Ws(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Ws(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Ws(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?ah:(this.currentObservers=null,s.push(t),new yn(()=>{this.currentObservers=null,Qo(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new bt;return t.source=this,t}}return n.create=(e,t)=>new jc(e,t),n})(),jc=class extends Gi{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:ah}};var ea=class extends Gi{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function tv(n){return n&&$e(n.schedule)}function nv(n){return n[n.length-1]}function iv(n){return $e(nv(n))?n.pop():void 0}function rv(n){return tv(nv(n))?n.pop():void 0}function ov(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{l(i.next(u))}catch(f){o(f)}}function c(u){try{l(i.throw(u))}catch(f){o(f)}}function l(u){u.done?s(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function sv(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function is(n){return this instanceof is?(this.v=n,this):new is(n)}function av(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(h){return function(g){return Promise.resolve(g).then(h,f)}}function a(h,g){i[h]&&(r[h]=function(x){return new Promise(function(m,p){s.push([h,x,m,p])>1||c(h,x)})},g&&(r[h]=g(r[h])))}function c(h,g){try{l(i[h](g))}catch(x){d(s[0][3],x)}}function l(h){h.value instanceof is?Promise.resolve(h.value.v).then(u,f):d(s[0][2],h)}function u(h){c("next",h)}function f(h){c("throw",h)}function d(h,g){h(g),s.shift(),s.length&&c(s[0][0],s[0][1])}}function cv(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof sv=="function"?sv(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,c){o=n[s](o),r(a,c,o.done,o.value)})}}function r(s,o,a,c){Promise.resolve(c).then(function(l){s({value:l,done:a})},o)}}var Wc=n=>n&&typeof n.length=="number"&&typeof n!="function";function $c(n){return $e(n?.then)}function qc(n){return $e(n[qs])}function Xc(n){return Symbol.asyncIterator&&$e(n?.[Symbol.asyncIterator])}function Yc(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function bb(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Zc=bb();function Jc(n){return $e(n?.[Zc])}function Kc(n){return av(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield is(t.read());if(r)return yield is(void 0);yield yield is(i)}}finally{t.releaseLock()}})}function Qc(n){return $e(n?.getReader)}function on(n){if(n instanceof bt)return n;if(n!=null){if(qc(n))return Sb(n);if(Wc(n))return wb(n);if($c(n))return Tb(n);if(Xc(n))return lv(n);if(Jc(n))return Db(n);if(Qc(n))return Cb(n)}throw Yc(n)}function Sb(n){return new bt(e=>{let t=n[qs]();if($e(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function wb(n){return new bt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function Tb(n){return new bt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,zc)})}function Db(n){return new bt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function lv(n){return new bt(e=>{Ab(n,e).catch(t=>e.error(t))})}function Cb(n){return lv(Kc(n))}function Ab(n,e){var t,i,r,s;return ov(this,void 0,void 0,function*(){try{for(t=cv(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function jn(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function el(n,e=0){return rn((t,i)=>{t.subscribe(sn(i,r=>jn(i,n,()=>i.next(r),e),()=>jn(i,n,()=>i.complete(),e),r=>jn(i,n,()=>i.error(r),e)))})}function tl(n,e=0){return rn((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function uv(n,e){return on(n).pipe(tl(e),el(e))}function dv(n,e){return on(n).pipe(tl(e),el(e))}function fv(n,e){return new bt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function hv(n,e){return new bt(t=>{let i;return jn(t,e,()=>{i=n[Zc](),jn(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>$e(i?.return)&&i.return()})}function nl(n,e){if(!n)throw new Error("Iterable cannot be null");return new bt(t=>{jn(t,e,()=>{let i=n[Symbol.asyncIterator]();jn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function pv(n,e){return nl(Kc(n),e)}function mv(n,e){if(n!=null){if(qc(n))return uv(n,e);if(Wc(n))return fv(n,e);if($c(n))return dv(n,e);if(Xc(n))return nl(n,e);if(Jc(n))return hv(n,e);if(Qc(n))return pv(n,e)}throw Yc(n)}function gv(n,e){return e?mv(n,e):on(n)}function Xs(...n){let e=rv(n);return gv(n,e)}function ph(n,e){let t=$e(n)?n:()=>n,i=r=>r.error(t());return new bt(e?r=>e.schedule(i,0,r):i)}function vn(n,e){return rn((t,i)=>{let r=0;t.subscribe(sn(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:Ib}=Array;function Rb(n,e){return Ib(e)?n(...e):n(e)}function yv(n){return vn(e=>Rb(n,e))}var{isArray:Nb}=Array,{getPrototypeOf:Pb,prototype:Ob,keys:Lb}=Object;function vv(n){if(n.length===1){let e=n[0];if(Nb(e))return{args:e,keys:null};if(Fb(e)){let t=Lb(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function Fb(n){return n&&typeof n=="object"&&Pb(n)===Ob}function _v(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function xv(n,e,t,i,r,s,o,a){let c=[],l=0,u=0,f=!1,d=()=>{f&&!c.length&&!l&&e.complete()},h=x=>l<i?g(x):c.push(x),g=x=>{s&&e.next(x),l++;let m=!1;on(t(x,u++)).subscribe(sn(e,p=>{r?.(p),s?h(p):e.next(p)},()=>{m=!0},void 0,()=>{if(m)try{for(l--;c.length&&l<i;){let p=c.shift();o?jn(e,o,()=>g(p)):g(p)}d()}catch(p){e.error(p)}}))};return n.subscribe(sn(e,h,()=>{f=!0,d()})),()=>{a?.()}}function il(n,e,t=1/0){return $e(e)?il((i,r)=>vn((s,o)=>e(i,s,r,o))(on(n(i,r))),t):(typeof e=="number"&&(t=e),rn((i,r)=>xv(i,r,n,t)))}function mh(...n){let e=iv(n),{args:t,keys:i}=vv(n),r=new bt(s=>{let{length:o}=t;if(!o){s.complete();return}let a=new Array(o),c=o,l=o;for(let u=0;u<o;u++){let f=!1;on(t[u]).subscribe(sn(s,d=>{f||(f=!0,l--),a[u]=d},()=>c--,void 0,()=>{(!c||!f)&&(l||s.next(i?_v(i,a):a),s.complete())}))}});return e?r.pipe(yv(e)):r}function gh(n,e){return rn((t,i)=>{let r=0;t.subscribe(sn(i,s=>n.call(e,s,r++)&&i.next(s)))})}function ta(n){return rn((e,t)=>{let i=null,r=!1,s;i=e.subscribe(sn(t,void 0,void 0,o=>{s=on(n(o,ta(n)(e))),i?(i.unsubscribe(),i=null,s.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,s.subscribe(t))})}function yh(n,e){return $e(e)?il(n,e,1):il(n,1)}function vh(n){return rn((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function _h(n,e){return rn((t,i)=>{let r=null,s=0,o=!1,a=()=>o&&!r&&i.complete();t.subscribe(sn(i,c=>{r?.unsubscribe();let l=0,u=s++;on(n(c,u)).subscribe(r=sn(i,f=>i.next(e?e(c,f,u,l++):f),()=>{r=null,a()}))},()=>{o=!0,a()}))})}var xh;function rl(){return xh}function xi(n){let e=xh;return xh=n,e}var Mv=Symbol("NotFound");function Ys(n){return n===Mv||n?.name==="\u0275NotFound"}var Fh="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",we=class extends Error{code;constructor(e,t){super(ls(e,t)),this.code=e}};function Bb(n){return`NG0${Math.abs(n)}`}function ls(n,e){return`${Bb(n)}${e?": "+e:""}`}var us=globalThis;function dt(n){for(let e in n)if(n[e]===dt)return e;throw Error("")}function ul(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(ul).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function dl(n,e){return n?e?`${n} ${e}`:n:e||""}var Vb=dt({__forward_ref__:dt});function fl(n){return n.__forward_ref__=fl,n}function Nn(n){return Tv(n)?n():n}function Tv(n){return typeof n=="function"&&n.hasOwnProperty(Vb)&&n.__forward_ref__===fl}function Qe(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function hl(n){return Hb(n,pl)}function Hb(n,e){return n.hasOwnProperty(e)&&n[e]||null}function zb(n){let e=n?.[pl]??null;return e||null}function Eh(n){return n&&n.hasOwnProperty(ol)?n[ol]:null}var pl=dt({\u0275prov:dt}),ol=dt({\u0275inj:dt}),Pe=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Qe({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function kh(n){return n&&!!n.\u0275providers}var Uh=dt({\u0275cmp:dt}),Bh=dt({\u0275dir:dt}),Vh=dt({\u0275pipe:dt});var bh=dt({\u0275fac:dt}),ds=dt({__NG_ELEMENT_ID__:dt}),Ev=dt({__NG_ENV_ID__:dt});function fs(n){return zh(n,"@Component"),n[Uh]||null}function Hh(n){return zh(n,"@Directive"),n[Bh]||null}function Dv(n){return zh(n,"@Pipe"),n[Vh]||null}function zh(n,e){if(n==null)throw new we(-919,!1)}function ml(n){return typeof n=="string"?n:n==null?"":String(n)}var Cv=dt({ngErrorCode:dt}),Gb=dt({ngErrorMessage:dt}),jb=dt({ngTokenPath:dt});function Gh(n,e){return Av("",-200,e)}function gl(n,e){throw new we(-201,!1)}function Av(n,e,t){let i=new we(e,n);return i[Cv]=e,i[Gb]=n,t&&(i[jb]=t),i}function Wb(n){return n[Cv]}var Sh;function Iv(){return Sh}function _n(n){let e=Sh;return Sh=n,e}function jh(n,e,t){let i=hl(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;gl(n,"")}var $b={},rs=$b,qb="__NG_DI_FLAG__",wh=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=ss(t)||0;try{return this.injector.get(e,i&8?null:rs,i)}catch(r){if(Ys(r))return r;throw r}}};function Xb(n,e=0){let t=rl();if(t===void 0)throw new we(-203,!1);if(t===null)return jh(n,void 0,e);{let i=Yb(e),r=t.retrieve(n,i);if(Ys(r)){if(i.optional)return null;throw r}return r}}function Ye(n,e=0){return(Iv()||Xb)(Nn(n),e)}function Ee(n,e){return Ye(n,ss(e))}function ss(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function Yb(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function Th(n){let e=[];for(let t=0;t<n.length;t++){let i=Nn(n[t]);if(Array.isArray(i)){if(i.length===0)throw new we(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=Zb(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(Ye(r,s))}else e.push(Ye(i))}return e}function Zb(n){return n[qb]}function os(n,e){let t=n.hasOwnProperty(bh);return t?n[bh]:null}function Rv(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function Nv(n){return n.flat(Number.POSITIVE_INFINITY)}function yl(n,e){n.forEach(t=>Array.isArray(t)?yl(t,e):e(t))}function Wh(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function oa(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function Pv(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function $h(n,e,t){let i=Js(n,e);return i>=0?n[i|1]=t:(i=~i,Pv(n,i,e,t)),i}function vl(n,e){let t=Js(n,e);if(t>=0)return n[t|1]}function Js(n,e){return Jb(n,e,1)}function Jb(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var hs={},Mi=[],Ks=new Pe(""),qh=new Pe("",-1),Xh=new Pe(""),ia=class{get(e,t=rs){if(t===rs){let r=Av("",-201);throw r.name="\u0275NotFound",r}return t}};function Qs(n){return{\u0275providers:n}}function Ov(n){return Qs([{provide:Ks,multi:!0,useValue:n}])}function Lv(...n){return{\u0275providers:Yh(!0,n),\u0275fromNgModule:!0}}function Yh(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return yl(e,o=>{let a=o;al(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Fv(r,s),t}function Fv(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];Zh(r,s=>{e(s,i)})}}function al(n,e,t,i){if(n=Nn(n),!n)return!1;let r=null,s=Eh(n),o=!s&&fs(n);if(!s&&!o){let c=n.ngModule;if(s=Eh(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)al(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;yl(s.imports,u=>{al(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&Fv(l,e)}if(!a){let l=os(r)||(()=>new r);e({provide:r,useFactory:l,deps:Mi},r),e({provide:Xh,useValue:r,multi:!0},r),e({provide:Ks,useValue:()=>Ye(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;Zh(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function Zh(n,e){for(let t of n)kh(t)&&(t=t.\u0275providers),Array.isArray(t)?Zh(t,e):e(t)}var Kb=dt({provide:String,useValue:dt});function kv(n){return n!==null&&typeof n=="object"&&Kb in n}function Qb(n){return!!(n&&n.useExisting)}function eS(n){return!!(n&&n.useFactory)}function cl(n){return typeof n=="function"}var aa=new Pe(""),sl={},bv={},Mh;function ca(){return Mh===void 0&&(Mh=new ia),Mh}var xn=class{},as=class extends xn{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Ch(e,o=>this.processProvider(o)),this.records.set(qh,Zs(void 0,this)),r.has("environment")&&this.records.set(xn,Zs(void 0,this));let s=this.records.get(aa);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(Xh,Mi,{self:!0}))}retrieve(e,t){let i=ss(t)||0;try{return this.get(e,rs,i)}catch(r){if(Ys(r))return r;throw r}}destroy(){na(this),this._destroyed=!0;let e=Ie(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ie(e)}}onDestroy(e){return na(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){na(this);let t=xi(this),i=_n(void 0),r;try{return e()}finally{xi(t),_n(i)}}get(e,t=rs,i){if(na(this),e.hasOwnProperty(Ev))return e[Ev](this);let r=ss(i),s,o=xi(this),a=_n(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=sS(e)&&hl(e);u&&this.injectableDefInScope(u)?l=Zs(Dh(e),sl):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?ca():this.parent;return t=r&8&&t===rs?null:t,c.get(e,t)}catch(c){let l=Wb(c);throw l===-200||l===-201?new we(l,null):c}finally{_n(a),xi(o)}}resolveInjectorInitializers(){let e=Ie(null),t=xi(this),i=_n(void 0),r;try{let s=this.get(Ks,Mi,{self:!0});for(let o of s)o()}finally{xi(t),_n(i),Ie(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=Nn(e);let t=cl(e)?e:Nn(e&&e.provide),i=nS(e);if(!cl(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Zs(void 0,sl,!0),r.factory=()=>Th(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=Ie(null);try{if(t.value===bv)throw Gh("");return t.value===sl&&(t.value=bv,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&rS(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ie(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=Nn(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Dh(n){let e=hl(n),t=e!==null?e.factory:os(n);if(t!==null)return t;if(n instanceof Pe)throw new we(-204,!1);if(n instanceof Function)return tS(n);throw new we(-204,!1)}function tS(n){if(n.length>0)throw new we(-204,!1);let t=zb(n);return t!==null?()=>t.factory(n):()=>new n}function nS(n){if(kv(n))return Zs(void 0,n.useValue);{let e=Uv(n);return Zs(e,sl)}}function Uv(n,e,t){let i;if(cl(n)){let r=Nn(n);return os(r)||Dh(r)}else if(kv(n))i=()=>Nn(n.useValue);else if(eS(n))i=()=>n.useFactory(...Th(n.deps||[]));else if(Qb(n))i=(r,s)=>Ye(Nn(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=Nn(n&&(n.useClass||n.provide));if(iS(n))i=()=>new r(...Th(n.deps));else return os(r)||Dh(r)}return i}function na(n){if(n.destroyed)throw new we(-205,!1)}function Zs(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function iS(n){return!!n.deps}function rS(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function sS(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Ch(n,e){for(let t of n)Array.isArray(t)?Ch(t,e):t&&kh(t)?Ch(t.\u0275providers,e):e(t)}function eo(n,e){let t;n instanceof as?(na(n),t=n):t=new wh(n);let i,r=xi(t),s=_n(void 0);try{return e()}finally{xi(r),_n(s)}}function Bv(){return Iv()!==void 0||rl()!=null}var ii=0,De=1,Oe=2,$t=3,Wn=4,$n=5,to=6,no=7,Bt=8,qi=9,Ei=10,At=11,io=12,Jh=13,ps=14,qn=15,Mr=16,ms=17,bi=18,Xi=19,Kh=20,Wi=21,_l=22,la=23,Pn=24,xl=25,Er=26,qt=27,Vv=1,Qh=6,br=7,ua=8,gs=9,It=10;function Sr(n){return Array.isArray(n)&&typeof n[Vv]=="object"}function ri(n){return Array.isArray(n)&&n[Vv]===!0}function ep(n){return(n.flags&4)!==0}function wr(n){return n.componentOffset>-1}function Ml(n){return(n.flags&1)===1}function ys(n){return!!n.template}function ro(n){return(n[Oe]&512)!==0}function vs(n){return(n[Oe]&256)===256}var tp="svg",Hv="math";function Xn(n){for(;Array.isArray(n);)n=n[ii];return n}function np(n,e){return Xn(e[n])}function Si(n,e){return Xn(e[n.index])}function El(n,e){return n.data[e]}function zv(n,e){return n[e]}function ip(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function wi(n,e){let t=e[n];return Sr(t)?t:t[ii]}function Gv(n){return(n[Oe]&4)===4}function bl(n){return(n[Oe]&128)===128}function jv(n){return ri(n[$t])}function Ti(n,e){return e==null?null:n[e]}function rp(n){n[ms]=0}function sp(n){n[Oe]&1024||(n[Oe]|=1024,bl(n)&&fa(n))}function Wv(n,e){for(;n>0;)e=e[ps],n--;return e}function da(n){return!!(n[Oe]&9216||n[Pn]?.dirty)}function Sl(n){n[Ei].changeDetectionScheduler?.notify(8),n[Oe]&64&&(n[Oe]|=1024),da(n)&&fa(n)}function fa(n){n[Ei].changeDetectionScheduler?.notify(0);let e=xr(n);for(;e!==null&&!(e[Oe]&8192||(e[Oe]|=8192,!bl(e)));)e=xr(e)}function op(n,e){if(vs(n))throw new we(911,!1);n[Wi]===null&&(n[Wi]=[]),n[Wi].push(e)}function $v(n,e){if(n[Wi]===null)return;let t=n[Wi].indexOf(e);t!==-1&&n[Wi].splice(t,1)}function xr(n){let e=n[$t];return ri(e)?e[$t]:e}function ap(n){return n[no]??=[]}function cp(n){return n.cleanup??=[]}function qv(n,e,t,i){let r=ap(e);r.push(t),n.firstCreatePass&&cp(n).push(i,r.length-1)}var ze={lFrame:c_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Ah=!1;function Xv(){return ze.lFrame.elementDepthCount}function Yv(){ze.lFrame.elementDepthCount++}function lp(){ze.lFrame.elementDepthCount--}function Zv(){return ze.bindingsEnabled}function Jv(){return ze.skipHydrationRootTNode!==null}function up(n){return ze.skipHydrationRootTNode===n}function dp(){ze.skipHydrationRootTNode=null}function et(){return ze.lFrame.lView}function an(){return ze.lFrame.tView}function Di(n){return ze.lFrame.contextLView=n,n[Bt]}function Ci(n){return ze.lFrame.contextLView=null,n}function si(){let n=fp();for(;n!==null&&n.type===64;)n=n.parent;return n}function fp(){return ze.lFrame.currentTNode}function Kv(){let n=ze.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function so(n,e){let t=ze.lFrame;t.currentTNode=n,t.isParent=e}function hp(){return ze.lFrame.isParent}function Qv(){ze.lFrame.isParent=!1}function pp(){return Ah}function mp(n){let e=Ah;return Ah=n,e}function e_(){let n=ze.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function t_(){return ze.lFrame.bindingIndex}function n_(n){return ze.lFrame.bindingIndex=n}function oo(){return ze.lFrame.bindingIndex++}function wl(n){let e=ze.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function i_(){return ze.lFrame.inI18n}function r_(n,e){let t=ze.lFrame;t.bindingIndex=t.bindingRootIndex=n,Tl(e)}function s_(){return ze.lFrame.currentDirectiveIndex}function Tl(n){ze.lFrame.currentDirectiveIndex=n}function o_(n){let e=ze.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function gp(){return ze.lFrame.currentQueryIndex}function Dl(n){ze.lFrame.currentQueryIndex=n}function oS(n){let e=n[De];return e.type===2?e.declTNode:e.type===1?n[$n]:null}function yp(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=oS(s),r===null||(s=s[ps],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=ze.lFrame=a_();return i.currentTNode=e,i.lView=n,!0}function Cl(n){let e=a_(),t=n[De];ze.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function a_(){let n=ze.lFrame,e=n===null?null:n.child;return e===null?c_(n):e}function c_(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function l_(){let n=ze.lFrame;return ze.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var vp=l_;function Al(){let n=l_();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function u_(n){return(ze.lFrame.contextLView=Wv(n,ze.lFrame.contextLView))[Bt]}function Ai(){return ze.lFrame.selectedIndex}function Tr(n){ze.lFrame.selectedIndex=n}function _p(){let n=ze.lFrame;return El(n.tView,n.selectedIndex)}function Yi(){ze.lFrame.currentNamespace=tp}function Dr(){aS()}function aS(){ze.lFrame.currentNamespace=null}function d_(){return ze.lFrame.currentNamespace}var f_=!0;function Il(){return f_}function Rl(n){f_=n}function Ih(n,e=null,t=null,i){let r=h_(n,e,t,i);return r.resolveInjectorInitializers(),r}function h_(n,e=null,t=null,i,r=new Set){let s=[t||Mi,Lv(n)],o;return new as(s,e||ca(),o||null,r)}var $i=class n{static THROW_IF_NOT_FOUND=rs;static NULL=new ia;static create(e,t){if(Array.isArray(e))return Ih({name:""},t,e,"");{let i=e.name??"";return Ih({name:i},e.parent,e.providers,i)}}static \u0275prov=Qe({token:n,providedIn:"any",factory:()=>Ye(qh)});static __NG_ELEMENT_ID__=-1},cn=new Pe(""),Cr=(()=>{class n{static __NG_ELEMENT_ID__=cS;static __NG_ENV_ID__=t=>t}return n})(),Rh=class extends Cr{_lView;constructor(e){super(),this._lView=e}get destroyed(){return vs(this._lView)}onDestroy(e){let t=this._lView;return op(t,e),()=>$v(t,e)}};function cS(){return new Rh(et())}var p_=!1,m_=new Pe(""),_s=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new ea(!1);debugTaskTracker=Ee(m_,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new bt(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Qe({token:n,providedIn:"root",factory:()=>new n})}return n})(),Nh=class extends Gi{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,Bv()&&(this.destroyRef=Ee(Cr,{optional:!0})??void 0,this.pendingTasks=Ee(_s,{optional:!0})??void 0)}emit(e){let t=Ie(null);try{super.next(e)}finally{Ie(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof yn&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},ji=Nh;function ll(...n){}function xp(n){let e,t;function i(){n=ll;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function g_(n){return queueMicrotask(()=>n()),()=>{n=ll}}var Mp="isAngularZone",ra=Mp+"_ID",lS=0,Mn=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new ji(!1);onMicrotaskEmpty=new ji(!1);onStable=new ji(!1);onError=new ji(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=p_}=e;if(typeof Zone>"u")throw new we(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,fS(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Mp)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new we(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new we(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,uS,ll,ll);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},uS={};function Ep(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function dS(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){xp(()=>{n.callbackScheduled=!1,Ph(n),n.isCheckStableRunning=!0,Ep(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Ph(n)}function fS(n){let e=()=>{dS(n)},t=lS++;n._inner=n._inner.fork({name:"angular",properties:{[Mp]:!0,[ra]:t,[ra+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(hS(c))return i.invokeTask(s,o,a,c);try{return Sv(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),wv(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return Sv(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!pS(c)&&e(),wv(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Ph(n),Ep(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Ph(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function Sv(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function wv(n){n._nesting--,Ep(n)}var sa=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new ji;onMicrotaskEmpty=new ji;onStable=new ji;onError=new ji;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function hS(n){return y_(n,"__ignore_ng_zone__")}function pS(n){return y_(n,"__scheduler_tick__")}function y_(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var ni=class{_console=console;handleError(e){this._console.error("ERROR",e)}},Zi=new Pe("",{factory:()=>{let n=Ee(Mn),e=Ee(xn),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(ni),t.handleError(i))})}}}),v_={provide:Ks,useValue:()=>{let n=Ee(ni,{optional:!0})},multi:!0},mS=new Pe("",{factory:()=>{let n=Ee(cn).defaultView;if(!n)return;let e=Ee(Zi),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),Ee(Cr).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function bp(){return Qs([Ov(()=>{Ee(mS)})])}function ln(n,e){let[t,i,r]=sh(n,e?.equal),s=t,o=s[Gn];return s.set=i,s.update=r,s.asReadonly=Sp.bind(s),s}function Sp(){let n=this[Gn];if(n.readonlyFn===void 0){let e=()=>this();e[Gn]=n,n.readonlyFn=e}return n.readonlyFn}var cs=class{},ha=new Pe("",{factory:()=>!0});var wp=new Pe(""),pa=(()=>{class n{internalPendingTasks=Ee(_s);scheduler=Ee(cs);errorHandler=Ee(Zi);add(){let t=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(t)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(t))}}run(t){let i=this.add();t().catch(this.errorHandler).finally(i)}static \u0275prov=Qe({token:n,providedIn:"root",factory:()=>new n})}return n})(),Tp=(()=>{class n{static \u0275prov=Qe({token:n,providedIn:"root",factory:()=>new Oh})}return n})(),Oh=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},Lh=class{[Gn];constructor(e){this[Gn]=e}destroy(){this[Gn].destroy()}};function X_(n){return{toString:n}.toString()}function wS(n){return typeof n=="function"}function Y_(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var Ul=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function TS(n){return n.type.prototype.ngOnChanges&&(n.setInput=CS),DS}function DS(){let n=J_(this),e=n?.current;if(e){let t=n.previous;if(t===hs)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function CS(n,e,t,i,r){let s=this.declaredInputs[i],o=J_(n)||AS(n,{previous:hs,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new Ul(l&&l.currentValue,t,c===hs),Y_(n,e,r,t)}var Z_="__ngSimpleChanges__";function J_(n){return n[Z_]||null}function AS(n,e){return n[Z_]=e}var __=[];var ft=function(n,e=null,t){for(let i=0;i<__.length;i++){let r=__[i];r(n,e,t)}},rt=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(rt||{});function IS(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=TS(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function RS(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function Pl(n,e,t){K_(n,e,3,t)}function Ol(n,e,t,i){(n[Oe]&3)===t&&K_(n,e,t,i)}function Dp(n,e){let t=n[Oe];(t&3)===e&&(t&=16383,t+=1,n[Oe]=t)}function K_(n,e,t,i){let r=i!==void 0?n[ms]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[ms]+=65536),(a<s||s==-1)&&(NS(n,t,e,c),n[ms]=(n[ms]&4294901760)+c+2),c++}function x_(n,e){ft(rt.LifecycleHookStart,n,e);let t=Ie(null);try{e.call(n)}finally{Ie(t),ft(rt.LifecycleHookEnd,n,e)}}function NS(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Oe]>>14<n[ms]>>16&&(n[Oe]&3)===e&&(n[Oe]+=16384,x_(a,s)):x_(a,s)}var co=-1,_a=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function PS(n){return(n.flags&8)!==0}function OS(n){return(n.flags&16)!==0}function LS(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];kS(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function FS(n){return n===3||n===4||n===6}function kS(n){return n.charCodeAt(0)===64}function Ql(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?M_(n,t,r,null,e[++i]):M_(n,t,r,null,null))}}return n}function M_(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function Q_(n){return n!==co}function Bl(n){return n&32767}function US(n){return n>>16}function Vl(n,e){let t=US(n),i=e;for(;t>0;)i=i[ps],t--;return i}var Lp=!0;function Hl(n){let e=Lp;return Lp=n,e}var BS=256,e0=BS-1,t0=5,VS=0,Ii={};function HS(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(ds)&&(i=t[ds]),i==null&&(i=t[ds]=VS++);let r=i&e0,s=1<<r;e.data[n+(r>>t0)]|=s}function n0(n,e){let t=i0(n,e);if(t!==-1)return t;let i=e[De];i.firstCreatePass&&(n.injectorIndex=e.length,Cp(i.data,n),Cp(e,null),Cp(i.blueprint,null));let r=um(n,e),s=n.injectorIndex;if(Q_(r)){let o=Bl(r),a=Vl(r,e),c=a[De].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function Cp(n,e){n.push(0,0,0,0,0,0,0,0,e)}function i0(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function um(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=c0(r),i===null)return co;if(t++,r=r[ps],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return co}function zS(n,e,t){HS(n,e,t)}function r0(n,e,t){if(t&8||n!==void 0)return n;gl(e,"NodeInjector")}function s0(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[qi],s=_n(void 0);try{return r?r.get(e,i,t&8):jh(e,i,t&8)}finally{_n(s)}}return r0(i,e,t)}function o0(n,e,t,i=0,r){if(n!==null){if(e[Oe]&2048&&!(i&2)){let o=$S(n,e,t,i,Ii);if(o!==Ii)return o}let s=a0(n,e,t,i,Ii);if(s!==Ii)return s}return s0(e,t,i,r)}function a0(n,e,t,i,r){let s=jS(t);if(typeof s=="function"){if(!yp(e,n,i))return i&1?r0(r,t,i):s0(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))gl(t);else return o}finally{vp()}}else if(typeof s=="number"){let o=null,a=i0(n,e),c=co,l=i&1?e[qn][$n]:null;for((a===-1||i&4)&&(c=a===-1?um(n,e):e[a+8],c===co||!b_(i,!1)?a=-1:(o=e[De],a=Bl(c),e=Vl(c,e)));a!==-1;){let u=e[De];if(E_(s,a,u.data)){let f=GS(a,e,t,o,i,l);if(f!==Ii)return f}c=e[a+8],c!==co&&b_(i,e[De].data[a+8]===l)&&E_(s,a,e)?(o=u,a=Bl(c),e=Vl(c,e)):a=-1}}return r}function GS(n,e,t,i,r,s){let o=e[De],a=o.data[n+8],c=i==null?wr(a)&&Lp:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=Ll(a,o,t,c,l);return u!==null?zl(e,o,u,a,r):Ii}function Ll(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,f=i?a:a+u,d=r?a+u:l;for(let h=f;h<d;h++){let g=o[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=o[c];if(h&&ys(h)&&h.type===t)return c}return null}function zl(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof _a){let a=s;if(a.resolving)throw Gh("");let c=Hl(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,f=a.injectImpl?_n(a.injectImpl):null,d=yp(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&IS(t,o[t],e)}finally{f!==null&&_n(f),Hl(c),a.resolving=!1,vp()}}return s}function jS(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(ds)?n[ds]:void 0;return typeof e=="number"?e>=0?e&e0:WS:e}function E_(n,e,t){let i=1<<n;return!!(t[e+(n>>t0)]&i)}function b_(n,e){return!(n&2)&&!(n&1&&e)}var xs=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return o0(this._tNode,this._lView,e,ss(i),t)}};function WS(){return new xs(si(),et())}function $S(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Oe]&2048&&!ro(o);){let a=a0(s,o,t,i|2,Ii);if(a!==Ii)return a;let c=s.parent;if(!c){let l=o[Kh];if(l){let u=l.get(t,Ii,i&-5);if(u!==Ii)return u}c=c0(o),o=o[ps]}s=c}return r}function c0(n){let e=n[De],t=e.type;return t===2?e.declTNode:t===1?n[$n]:null}function qS(){return po(si(),et())}function po(n,e){return new mo(Si(n,e))}var mo=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=qS}return n})();function XS(n){return n instanceof mo?n.nativeElement:n}function YS(){return this._results[Symbol.iterator]()}var Gl=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Gi}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=Nv(e);(this._changesDetected=!Rv(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=YS};function l0(n){return(n.flags&128)===128}var dm=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(dm||{}),u0=new Map,ZS=0;function JS(){return ZS++}function KS(n){u0.set(n[Xi],n)}function Fp(n){u0.delete(n[Xi])}var S_="__ngContext__";function lo(n,e){Sr(e)?(n[S_]=e[Xi],KS(e)):n[S_]=e}function d0(n){return h0(n[io])}function f0(n){return h0(n[Wn])}function h0(n){for(;n!==null&&!ri(n);)n=n[Wn];return n}var QS;function fm(n){QS=n}var eu=new Pe("",{factory:()=>ew}),ew="ng";var tu=new Pe(""),Da=new Pe("",{providedIn:"platform",factory:()=>"unknown"});var Ca=new Pe("",{factory:()=>Ee(cn).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var p0="r";var m0="di";var g0=!1,y0=new Pe("",{factory:()=>g0});var w_=new WeakMap;function tw(n,e){if(n==null||typeof n!="object")return;let t=w_.get(n);t||(t=new WeakSet,w_.set(n,t)),t.add(e)}var nw=(n,e,t,i)=>{};function iw(n,e,t,i){nw(n,e,t,i)}function hm(n){return(n.flags&32)===32}var rw=()=>null;function v0(n,e,t=!1){return rw(n,e,t)}function _0(n,e){let t=n.contentQueries;if(t!==null){let i=Ie(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];Dl(s),a.contentQueries(2,e[o],o)}}}finally{Ie(i)}}}function kp(n,e,t){Dl(0);let i=Ie(null);try{e(n,t)}finally{Ie(i)}}function x0(n,e,t){if(ep(e)){let i=Ie(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{Ie(i)}}}var ai=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(ai||{});var Up=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Fh})`}};function nu(n){return n instanceof Up?n.changingThisBreaksApplicationSecurity:n}function sw(n,e){return n.createText(e)}function ow(n,e,t){n.setValue(e,t)}function M0(n,e,t){return n.createElement(e,t)}function jl(n,e,t,i,r){n.insertBefore(e,t,i,r)}function E0(n,e,t){n.appendChild(e,t)}function T_(n,e,t,i,r){i!==null?jl(n,e,t,i,r):E0(n,e,t)}function b0(n,e,t,i){n.removeChild(null,e,t,i)}function aw(n,e,t){n.setAttribute(e,"style",t)}function cw(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function S0(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&LS(n,e,i),r!==null&&cw(n,e,r),s!==null&&aw(n,e,s)}function lw(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var w0="ng-template";function uw(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&lw(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(pm(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function pm(n){return n.type===4&&n.value!==w0}function dw(n,e,t){let i=n.type===4&&!t?w0:n.value;return e===i}function fw(n,e,t){let i=4,r=n.attrs,s=r!==null?mw(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!oi(i)&&!oi(c))return!1;if(o&&oi(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!dw(n,c,t)||c===""&&e.length===1){if(oi(i))return!1;o=!0}}else if(i&8){if(r===null||!uw(n,r,c,t)){if(oi(i))return!1;o=!0}}else{let l=e[++a],u=hw(c,r,pm(n),t);if(u===-1){if(oi(i))return!1;o=!0;continue}if(l!==""){let f;if(u>s?f="":f=r[u+1].toLowerCase(),i&2&&l!==f){if(oi(i))return!1;o=!0}}}}return oi(i)||o}function oi(n){return(n&1)===0}function hw(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return gw(e,n)}function pw(n,e,t=!1){for(let i=0;i<e.length;i++)if(fw(n,e[i],t))return!0;return!1}function mw(n){for(let e=0;e<n.length;e++){let t=n[e];if(FS(t))return e}return n.length}function gw(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function D_(n,e){return n?":not("+e.trim()+")":e}function yw(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!oi(o)&&(e+=D_(s,r),r=""),i=o,s=s||!oi(i);t++}return r!==""&&(e+=D_(s,r)),e}function vw(n){return n.map(yw).join(",")}function _w(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!oi(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var Tn={};function mm(n,e,t,i,r,s,o,a,c,l,u){let f=qt+i,d=f+r,h=xw(f,d),g=typeof l=="function"?l():l;return h[De]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:d,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function xw(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:Tn);return t}function Mw(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=mm(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function gm(n,e,t,i,r,s,o,a,c,l,u){let f=e.blueprint.slice();return f[ii]=r,f[Oe]=i|4|128|8|64|1024,(l!==null||n&&n[Oe]&2048)&&(f[Oe]|=2048),rp(f),f[$t]=f[ps]=n,f[Bt]=t,f[Ei]=o||n&&n[Ei],f[At]=a||n&&n[At],f[qi]=c||n&&n[qi]||null,f[$n]=s,f[Xi]=JS(),f[to]=u,f[Kh]=l,f[qn]=e.type==2?n[qn]:f,f}function Ew(n,e,t){let i=Si(e,n),r=Mw(t),s=n[Ei].rendererFactory,o=ym(n,gm(n,r,null,T0(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function T0(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function D0(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function ym(n,e){return n[io]?n[Jh][Wn]=e:n[io]=e,n[Jh]=e,e}function yt(n=1){C0(an(),et(),Ai()+n,!1)}function C0(n,e,t,i){if(!i)if((e[Oe]&3)===3){let s=n.preOrderCheckHooks;s!==null&&Pl(e,s,t)}else{let s=n.preOrderHooks;s!==null&&Ol(e,s,0,t)}Tr(t)}var iu=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(iu||{});function Bp(n,e,t,i){let r=Ie(null);try{let[s,o,a]=n.inputs[t],c=null;(o&iu.SignalBased)!==0&&(c=e[s][Gn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):Y_(e,c,s,i)}finally{Ie(r)}}var Ri=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(Ri||{}),bw;function vm(n,e){return bw(n,e)}var sU=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Vp=new WeakMap,ga=new WeakSet;function Sw(n,e){let t=Vp.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let s=t.length-1;s>=0;s--){let o=t[s],a=o.parentNode;o===e?(t.splice(s,1),ga.add(o),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&o===r||a&&i&&a!==i)&&(t.splice(s,1),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),o.parentNode?.removeChild(o))}}function ww(n,e){let t=Vp.get(n);t?t.includes(e)||t.push(e):Vp.set(n,[e])}var Ms=new Set,_m=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(_m||{}),Ar=new Pe(""),C_=new Set;function go(n){C_.has(n)||(C_.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var A0=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=Qe({token:n,providedIn:"root",factory:()=>new n})}return n})();var I0=new Pe("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:Ee(xn)})});function R0(n,e,t){let i=n.get(I0);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function Tw(n,e){let t=n.get(I0);if(e.detachedLeaveAnimationFns){for(let i of e.detachedLeaveAnimationFns)t.queue.delete(i);e.detachedLeaveAnimationFns=void 0}}function Dw(n,e){for(let[t,i]of e)R0(n,i.animateFns)}function A_(n,e,t,i){let r=n?.[Er]?.enter;e!==null&&r&&r.has(t.index)&&Dw(i,r)}function ao(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;ri(r)?c=r:Sr(r)&&(l=!0,r=r[ii]);let u=Xn(r);n===0&&i!==null?(A_(a,i,s,t),o==null?E0(e,i,u):jl(e,i,u,o||null,!0)):n===1&&i!==null?(A_(a,i,s,t),jl(e,i,u,o||null,!0),Sw(s,u)):n===2?(a?.[Er]?.leave?.has(s.index)&&ww(s,u),ga.delete(u),I_(a,s,t,f=>{if(ga.has(u)){ga.delete(u);return}b0(e,u,l,f)})):n===3&&(ga.delete(u),I_(a,s,t,()=>{e.destroyNode(u)})),c!=null&&Vw(e,n,t,c,s,i,o)}}function Cw(n,e){N0(n,e),e[ii]=null,e[$n]=null}function Aw(n,e,t,i,r,s){i[ii]=r,i[$n]=e,su(n,i,t,1,r,s)}function N0(n,e){e[Ei].changeDetectionScheduler?.notify(9),su(n,e,e[At],2,null,null)}function Iw(n){let e=n[io];if(!e)return Ap(n[De],n);for(;e;){let t=null;if(Sr(e))t=e[io];else{let i=e[It];i&&(t=i)}if(!t){for(;e&&!e[Wn]&&e!==n;)Sr(e)&&Ap(e[De],e),e=e[$t];e===null&&(e=n),Sr(e)&&Ap(e[De],e),t=e&&e[Wn]}e=t}}function xm(n,e){let t=n[gs],i=t.indexOf(e);t.splice(i,1)}function ru(n,e){if(vs(e))return;let t=e[At];t.destroyNode&&su(n,e,t,3,null,null),Iw(e)}function Ap(n,e){if(vs(e))return;let t=Ie(null);try{e[Oe]&=-129,e[Oe]|=256,e[Pn]&&kc(e[Pn]),Pw(n,e),Nw(n,e),e[De].type===1&&e[At].destroy();let i=e[Mr];if(i!==null&&ri(e[$t])){i!==e[$t]&&xm(i,e);let r=e[bi];r!==null&&r.detachView(n)}Fp(e)}finally{Ie(t)}}function I_(n,e,t,i){let r=n?.[Er];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&Ms.add(n[Xi]),R0(t,()=>{if(r.leave&&r.leave.has(e.index)){let o=r.leave.get(e.index),a=[];if(o){for(let c=0;c<o.animateFns.length;c++){let l=o.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),Rw(n,i)}else n&&Ms.delete(n[Xi]),i(!1)},r)}function Rw(n,e){let t=n[Er]?.running;if(t){t.then(()=>{n[Er].running=void 0,Ms.delete(n[Xi]),e(!0)});return}e(!1)}function Nw(n,e){let t=n.cleanup,i=e[no];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[no]=null);let r=e[Wi];if(r!==null){e[Wi]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[la];if(s!==null){e[la]=null;for(let o of s)o.destroy()}}function Pw(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof _a)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];ft(rt.LifecycleHookStart,a,c);try{c.call(a)}finally{ft(rt.LifecycleHookEnd,a,c)}}else{ft(rt.LifecycleHookStart,r,s);try{s.call(r)}finally{ft(rt.LifecycleHookEnd,r,s)}}}}}function Ow(n,e,t){return Lw(n,e.parent,t)}function Lw(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[ii];if(wr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===ai.None||r===ai.Emulated)return null}return Si(i,t)}function Fw(n,e,t){return Uw(n,e,t)}function kw(n,e,t){return n.type&40?Si(n,t):null}var Uw=kw,R_;function Mm(n,e,t,i){let r=Ow(n,i,e),s=e[At],o=i.parent||e[$n],a=Fw(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)T_(s,r,t[c],a,!1);else T_(s,r,t,a,!1);R_!==void 0&&R_(s,i,e,t,r)}function ya(n,e){if(e!==null){let t=e.type;if(t&3)return Si(e,n);if(t&4)return Hp(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return ya(n,i);{let r=n[e.index];return ri(r)?Hp(-1,r):Xn(r)}}else{if(t&128)return ya(n,e.next);if(t&32)return vm(e,n)()||Xn(n[e.index]);{let i=P0(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=xr(n[qn]);return ya(r,i)}else return ya(n,e.next)}}}return null}function P0(n,e){if(e!==null){let i=n[qn][$n],r=e.projection;return i.projection[r]}return null}function Hp(n,e){let t=It+n+1;if(t<e.length){let i=e[t],r=i[De].firstChild;if(r!==null)return ya(i,r)}return e[br]}function Em(n,e,t,i,r,s,o){for(;t!=null;){let a=i[qi];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&lo(Xn(c),i),t.flags|=2),!hm(t))if(l&8)Em(n,e,t.child,i,r,s,!1),ao(e,n,a,r,c,t,s,i);else if(l&32){let u=vm(t,i),f;for(;f=u();)ao(e,n,a,r,f,t,s,i);ao(e,n,a,r,c,t,s,i)}else l&16?Bw(n,e,i,t,r,s):ao(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function su(n,e,t,i,r,s){Em(t,i,n.firstChild,e,r,s,!1)}function Bw(n,e,t,i,r,s){let o=t[qn],c=o[$n].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];ao(e,n,t[qi],r,u,i,s,t)}else{let l=c,u=o[$t];l0(i)&&(l.flags|=128),Em(n,e,l,u,r,s,!0)}}function Vw(n,e,t,i,r,s,o){let a=i[br],c=Xn(i);a!==c&&ao(e,n,t,s,a,r,o);for(let l=It;l<i.length;l++){let u=i[l];su(u[De],u,n,e,s,a)}}function Hw(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:Ri.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=Ri.Important),n.setStyle(t,i,r,s))}}function O0(n,e,t,i,r){let s=Ai(),o=i&2;try{Tr(-1),o&&e.length>qt&&C0(n,e,qt,!1);let a=o?rt.TemplateUpdateStart:rt.TemplateCreateStart;ft(a,r,t),t(i,r)}finally{Tr(s);let a=o?rt.TemplateUpdateEnd:rt.TemplateCreateEnd;ft(a,r,t)}}function L0(n,e,t){Xw(n,e,t),(t.flags&64)===64&&Yw(n,e,t)}function bm(n,e,t=Si){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function zw(n,e,t,i){let s=i.get(y0,g0)||t===ai.ShadowDom||t===ai.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);return Gw(o),o}function Gw(n){jw(n)}var jw=()=>null;function Ww(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function $w(n,e,t,i,r,s){let o=e[De];if(Sm(n,o,e,t,i)){wr(n)&&qw(e,n.index);return}n.type&3&&(t=Ww(t)),F0(n,e,t,i,r,s)}function F0(n,e,t,i,r,s){if(n.type&3){let o=Si(n,e);i=s!=null?s(i,n.value||"",t):i,r.setProperty(o,t,i)}else n.type&12}function qw(n,e){let t=wi(e,n);t[Oe]&16||(t[Oe]|=64)}function Xw(n,e,t){let i=t.directiveStart,r=t.directiveEnd;wr(t)&&Ew(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||n0(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=zl(e,n,o,t);if(lo(c,e),s!==null&&Kw(e,o-i,c,a,t,s),ys(a)){let l=wi(t.index,e);l[Bt]=zl(e,n,o,t)}}}function Yw(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=s_();try{Tr(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Tl(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&Zw(c,l)}}finally{Tr(-1),Tl(o)}}function Zw(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function Jw(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];pw(e,s.selectors,!1)&&(i??=[],ys(s)?i.unshift(s):i.push(s))}return i}function Kw(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];Bp(i,t,c,l)}}function k0(n,e,t,i,r){let s=qt+t,o=e[De],a=r(o,e,n,i,t);e[s]=a,so(n,!0);let c=n.type===2;return c?(S0(e[At],a,n),(Xv()===0||Ml(n))&&lo(a,e),Yv()):lo(a,e),Il()&&(!c||!hm(n))&&Mm(o,e,a,n),n}function U0(n){let e=n;return hp()?Qv():(e=e.parent,so(e,!1)),e}function Qw(n,e){let t=n[qi];if(!t)return;let i;try{i=t.get(Zi,null)}catch{i=null}i?.(e)}function Sm(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],f=e.data[l];Bp(f,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];Bp(u,l,i,r),a=!0}return a}function eT(n,e){let t=wi(e,n),i=t[De];tT(i,t);let r=t[ii];r!==null&&t[to]===null&&(t[to]=v0(r,t[qi])),ft(rt.ComponentStart);try{wm(i,t,t[Bt])}finally{ft(rt.ComponentEnd,t[Bt])}}function tT(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function wm(n,e,t){Cl(e);try{let i=n.viewQuery;i!==null&&kp(1,i,t);let r=n.template;r!==null&&O0(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[bi]?.finishViewCreation(n),n.staticContentQueries&&_0(n,e),n.staticViewQueries&&kp(2,n.viewQuery,t);let s=n.components;s!==null&&nT(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Oe]&=-5,Al()}}function nT(n,e){for(let t=0;t<e.length;t++)eT(n,e[t])}function ou(n,e,t,i){let r=Ie(null);try{let s=e.tView,a=n[Oe]&4096?4096:16,c=gm(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[Mr]=l;let u=n[bi];return u!==null&&(c[bi]=u.createEmbeddedView(s)),wm(s,c,t),c}finally{Ie(r)}}function xa(n,e){return!e||e.firstChild===null||l0(n)}function Ma(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(Xn(s)),ri(s)&&B0(s,i);let o=t.type;if(o&8)Ma(n,e,t.child,i);else if(o&32){let a=vm(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=P0(e,t);if(Array.isArray(a))i.push(...a);else{let c=xr(e[qn]);Ma(c[De],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function B0(n,e){for(let t=It;t<n.length;t++){let i=n[t],r=i[De].firstChild;r!==null&&Ma(i[De],i,r,e)}n[br]!==n[ii]&&e.push(n[br])}function V0(n){if(n[xl]!==null){for(let e of n[xl])e.impl.addSequence(e);n[xl].length=0}}var H0=[];function iT(n){return n[Pn]??rT(n)}function rT(n){let e=H0.pop()??Object.create(oT);return e.lView=n,e}function sT(n){n.lView[Pn]!==n&&(n.lView=null,H0.push(n))}var oT=wn(Wt({},Ko),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{fa(n.lView)},consumerOnSignalRead(){this.lView[Pn]=this}});function aT(n){let e=n[Pn]??Object.create(cT);return e.lView=n,e}var cT=wn(Wt({},Ko),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=xr(n.lView);for(;e&&!z0(e[De]);)e=xr(e);e&&sp(e)},consumerOnSignalRead(){this.lView[Pn]=this}});function z0(n){return n.type!==2}function G0(n){if(n[la]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[la])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Oe]&8192)}}var lT=100;function j0(n,e=0){let i=n[Ei].rendererFactory,r=!1;r||i.begin?.();try{uT(n,e)}finally{r||i.end?.()}}function uT(n,e){let t=pp();try{mp(!0),zp(n,e);let i=0;for(;da(n);){if(i===lT)throw new we(103,!1);i++,zp(n,1)}}finally{mp(t)}}function dT(n,e,t,i){if(vs(e))return;let r=e[Oe],s=!1,o=!1;Cl(e);let a=!0,c=null,l=null;s||(z0(n)?(l=iT(e),c=Fc(l)):Lc()===null?(a=!1,l=aT(e),c=Fc(l)):e[Pn]&&(kc(e[Pn]),e[Pn]=null));try{rp(e),n_(n.bindingStartIndex),t!==null&&O0(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&Pl(e,h,null)}else{let h=n.preOrderHooks;h!==null&&Ol(e,h,0,null),Dp(e,0)}if(o||fT(e),G0(e),W0(e,0),n.contentQueries!==null&&_0(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&Pl(e,h)}else{let h=n.contentHooks;h!==null&&Ol(e,h,1),Dp(e,1)}pT(n,e);let f=n.components;f!==null&&q0(e,f,0);let d=n.viewQuery;if(d!==null&&kp(2,d,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&Pl(e,h)}else{let h=n.viewHooks;h!==null&&Ol(e,h,2),Dp(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[_l]){for(let h of e[_l])h();e[_l]=null}s||(V0(e),e[Oe]&=-73)}catch(u){throw s||fa(e),u}finally{l!==null&&(Qf(l,c),a&&sT(l)),Al()}}function W0(n,e){for(let t=d0(n);t!==null;t=f0(t))for(let i=It;i<t.length;i++){let r=t[i];$0(r,e)}}function fT(n){for(let e=d0(n);e!==null;e=f0(e)){if(!(e[Oe]&2))continue;let t=e[gs];for(let i=0;i<t.length;i++){let r=t[i];sp(r)}}}function hT(n,e,t){ft(rt.ComponentStart);let i=wi(e,n);try{$0(i,t)}finally{ft(rt.ComponentEnd,i[Bt])}}function $0(n,e){bl(n)&&zp(n,e)}function zp(n,e){let i=n[De],r=n[Oe],s=n[Pn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&eh(s)),o||=!1,s&&(s.dirty=!1),n[Oe]&=-9217,o)dT(i,n,i.template,n[Bt]);else if(r&8192){let a=Ie(null);try{G0(n),W0(n,1);let c=i.components;c!==null&&q0(n,c,1),V0(n)}finally{Ie(a)}}}function q0(n,e,t){for(let i=0;i<e.length;i++)hT(n,e[i],t)}function pT(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)Tr(~r);else{let s=r,o=t[++i],a=t[++i];r_(o,s);let c=e[s];ft(rt.HostBindingsUpdateStart,c);try{a(2,c)}finally{ft(rt.HostBindingsUpdateEnd,c)}}}}finally{Tr(-1)}}function Tm(n,e){let t=pp()?64:1088;for(n[Ei].changeDetectionScheduler?.notify(e);n;){n[Oe]|=t;let i=xr(n);if(ro(n)&&!i)return n;n=i}return null}function X0(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function Y0(n,e){let t=It+e;if(t<n.length)return n[t]}function au(n,e,t,i=!0){let r=e[De];if(mT(r,e,n,t),i){let o=Hp(t,n),a=e[At],c=a.parentNode(n[br]);c!==null&&Aw(r,n[$n],a,e,c,o)}let s=e[to];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function Z0(n,e){let t=Ea(n,e);return t!==void 0&&ru(t[De],t),t}function Ea(n,e){if(n.length<=It)return;let t=It+e,i=n[t];if(i){let r=i[Mr];r!==null&&r!==n&&xm(r,i),e>0&&(n[t-1][Wn]=i[Wn]);let s=oa(n,It+e);Cw(i[De],i);let o=s[bi];o!==null&&o.detachView(s[De]),i[$t]=null,i[Wn]=null,i[Oe]&=-129}return i}function mT(n,e,t,i){let r=It+i,s=t.length;i>0&&(t[r-1][Wn]=e),i<s-It?(e[Wn]=t[r],Wh(t,It+i,e)):(t.push(e),e[Wn]=null),e[$t]=t;let o=e[Mr];o!==null&&t!==o&&J0(o,e);let a=e[bi];a!==null&&a.insertView(n),Sl(e),e[Oe]|=128}function J0(n,e){let t=n[gs],i=e[$t];if(Sr(i))n[Oe]|=2;else{let r=i[$t][qn];e[qn]!==r&&(n[Oe]|=2)}t===null?n[gs]=[e]:t.push(e)}var uo=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[De];return Ma(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[Bt]}set context(e){this._lView[Bt]=e}get destroyed(){return vs(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[$t];if(ri(e)){let t=e[ua],i=t?t.indexOf(this):-1;i>-1&&(Ea(e,i),oa(t,i))}this._attachedToViewContainer=!1}ru(this._lView[De],this._lView)}onDestroy(e){op(this._lView,e)}markForCheck(){Tm(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Oe]&=-129}reattach(){Sl(this._lView),this._lView[Oe]|=128}detectChanges(){this._lView[Oe]|=1024,j0(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new we(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=ro(this._lView),t=this._lView[Mr];t!==null&&!e&&xm(t,this._lView),N0(this._lView[De],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new we(902,!1);this._appRef=e;let t=ro(this._lView),i=this._lView[Mr];i!==null&&!t&&J0(i,this._lView),Sl(this._lView)}};var fo=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=gT;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=ou(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new uo(s)}}return n})();function gT(){return Dm(si(),et())}function Dm(n,e){return n.type&4?new fo(e,n,po(n,e)):null}function cu(n,e,t,i,r){let s=n.data[e];if(s===null)s=yT(n,e,t,i,r),i_()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=Kv();s.injectorIndex=o===null?-1:o.injectorIndex}return so(s,!0),s}function yT(n,e,t,i,r){let s=fp(),o=hp(),a=o?s:s&&s.parent,c=n.data[e]=_T(n,a,t,e,i,r);return vT(n,c,s,o),c}function vT(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function _T(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return Jv()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function xT(n){let e=n[Qh]??[],i=n[$t][At],r=[];for(let s of e)s.data[m0]!==void 0?r.push(s):MT(s,i);n[Qh]=r}function MT(n,e){let t=0,i=n.firstChild;if(i){let r=n.data[p0];for(;t<r;){let s=i.nextSibling;b0(e,i,!1),i=s,t++}}}var ET=()=>null,bT=()=>null;function Gp(n,e){return ET(n,e)}function K0(n,e,t){return bT(n,e,t)}var Q0=class{},lu=class{},jp=class{resolveComponentFactory(e){throw new we(917,!1)}},uu=class{static NULL=new jp},Es=class{};var ex=(()=>{class n{static \u0275prov=Qe({token:n,providedIn:"root",factory:()=>null})}return n})();var Fl={},Wp=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,Fl,i);return r!==Fl||t===Fl?r:this.parentInjector.get(e,t,i)}};function Wl(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=dl(r,a);else if(s==2){let c=a,l=e[++o];i=dl(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Ki(n,e=0){let t=et();if(t===null)return Ye(n,e);let i=si();return o0(i,t,Nn(n),e)}function ST(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}DT(n,e,t,a,s,c,l)}s!==null&&i!==null&&wT(t,i,s)}function wT(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new we(-301,!1);i.push(e[r],s)}}function TT(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function DT(n,e,t,i,r,s,o){let a=i.length,c=null;for(let d=0;d<a;d++){let h=i[d];c===null&&ys(h)&&(c=h,TT(n,t,d)),zS(n0(t,e),n,h.type)}PT(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let d=0;d<a;d++){let h=i[d];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,f=D0(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let d=0;d<a;d++){let h=i[d];if(t.mergedAttrs=Ql(t.mergedAttrs,h.hostAttrs),AT(n,t,e,f,h),NT(f,h,r),o!==null&&o.has(h)){let[x,m]=o.get(h);t.directiveToIndex.set(h.type,[f,x+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,f);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),f++}CT(n,t,s)}function CT(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))N_(0,e,r,i),N_(1,e,r,i),O_(e,i,!1);else{let s=t.get(r);P_(0,e,s,i),P_(1,e,s,i),O_(e,i,!0)}}}function N_(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),tx(e,s)}}function P_(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),tx(e,o)}}function tx(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function O_(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||pm(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function AT(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=os(r.type,!0)),o=new _a(s,ys(r),Ki,null);n.blueprint[i]=o,t[i]=o,IT(n,e,i,D0(n,t,r.hostVars,Tn),r)}function IT(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;RT(o)!=a&&o.push(a),o.push(t,i,s)}}function RT(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function NT(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;ys(e)&&(t[""]=n)}}function PT(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function nx(n,e,t,i,r,s,o,a){let c=e[De],l=c.consts,u=Ti(l,o),f=cu(c,n,t,i,u);return s&&ST(c,e,f,Ti(l,a),r),f.mergedAttrs=Ql(f.mergedAttrs,f.attrs),f.attrs!==null&&Wl(f,f.attrs,!1),f.mergedAttrs!==null&&Wl(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function ix(n,e){RS(n,e),ep(e)&&n.queries.elementEnd(e)}function OT(n,e,t,i,r,s){let o=e.consts,a=Ti(o,r),c=cu(e,n,t,i,a);if(c.mergedAttrs=Ql(c.mergedAttrs,c.attrs),s!=null){let l=Ti(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&Wl(c,c.attrs,!1),c.mergedAttrs!==null&&Wl(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function LT(n,e,t){return n[e]=t}function Ji(n,e,t){if(t===Tn)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function rx(n,e,t,i){let r=Ji(n,e,t);return Ji(n,e+1,i)||r}function kl(n,e,t){return function i(r){let s=i.__ngNativeEl__;s!==void 0&&tw(r,s);let o=wr(n)?wi(n.index,e):e;Tm(o,5);let a=e[Bt],c=L_(e,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=L_(e,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function L_(n,e,t,i){let r=Ie(null);try{return ft(rt.OutputStart,e,t),t(i)!==!1}catch(s){return Qw(n,s),!1}finally{ft(rt.OutputEnd,e,t),Ie(r)}}function sx(n,e,t,i,r,s,o,a){let c=Ml(n),l=!1,u=null;if(!i&&c&&(u=kT(e,t,s,n.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let f=Si(n,t),d=i?i(f):f;iw(t,d,s,a),i||(a.__ngNativeEl__=f);let h=r.listen(d,s,a);if(!FT(s)){let g=i?x=>i(Xn(x[n.index])):n.index;ox(g,e,t,s,a,h,!1)}}return l}function FT(n){return n.startsWith("animation")||n.startsWith("transition")}function kT(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[no],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function ox(n,e,t,i,r,s,o){let a=e.firstCreatePass?cp(e):null,c=ap(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}function F_(n,e,t,i,r,s){let o=e[t],a=e[De],l=a.data[t].outputs[i],f=o[l].subscribe(s);ox(n.index,a,e,r,s,f,!0)}var $p=Symbol("BINDING");function ax(n){return n.debugInfo?.className||n.type.name||null}var qp=class extends uu{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=fs(e);return new ba(t,this.ngModule)}};function UT(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&iu.SignalBased)!==0};return r&&(s.transform=r),s})}function BT(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function VT(n,e,t){let i=e instanceof xn?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Wp(t,i):t}function HT(n){let e=n.get(Es,null);if(e===null)throw new we(407,!1);let t=n.get(ex,null),i=n.get(cs,null),r=n.get(Ar,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function zT(n,e){let t=cx(n);return M0(e,t,t==="svg"?tp:t==="math"?Hv:null)}function cx(n){return(n.selectors[0][0]||"div").toLowerCase()}var ba=class extends lu{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=UT(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=BT(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=vw(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){ft(rt.DynamicComponentStart);let a=Ie(null);try{let c=this.componentDef,l=VT(c,r||this.ngModule,e),u=HT(l),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(ax(c),()=>this.createComponentRef(u,l,t,i,s,o)):this.createComponentRef(u,l,t,i,s,o)}finally{Ie(a)}}createComponentRef(e,t,i,r,s,o){let a=this.componentDef,c=GT(r,a,o,s),l=e.rendererFactory.createRenderer(null,a),u=r?zw(l,r,a.encapsulation,t):zT(a,l),f=o?.some(k_)||s?.some(g=>typeof g!="function"&&g.bindings.some(k_)),d=gm(null,c,null,512|T0(a),null,null,e,l,t,null,v0(u,t,!0));d[qt]=u,Cl(d);let h=null;try{let g=nx(qt,d,2,"#host",()=>c.directiveRegistry,!0,0);S0(l,u,g),lo(u,d),L0(c,d,g),x0(c,g,d),ix(c,g),i!==void 0&&WT(g,this.ngContentSelectors,i),h=wi(g.index,d),d[Bt]=h[Bt],wm(c,d,null)}catch(g){throw h!==null&&Fp(h),Fp(d),g}finally{ft(rt.DynamicComponentEnd),Al()}return new $l(this.componentType,d,!!f)}};function GT(n,e,t,i){let r=n?["ng-version","21.2.12"]:_w(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[$p].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let d of f.bindings){a+=d[$p].requiredVars;let h=u+1;d.create&&(d.targetIdx=h,(s??=[]).push(d)),d.update&&(d.targetIdx=h,(o??=[]).push(d))}}let c=[e];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,d=Hh(f);c.push(d)}return mm(0,null,jT(s,o),1,a,c,null,null,null,[r],null)}function jT(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function k_(n){let e=n[$p].kind;return e==="input"||e==="twoWay"}var $l=class extends Q0{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=El(t[De],qt),this.location=po(this._tNode,t),this.instance=wi(this._tNode.index,t)[Bt],this.hostView=this.changeDetectorRef=new uo(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=Sm(i,r[De],r,e,t);this.previousInputValues.set(e,t);let o=wi(i.index,r);Tm(o,1)}get injector(){return new xs(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function WT(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var Aa=(()=>{class n{static __NG_ELEMENT_ID__=$T}return n})();function $T(){let n=si();return lx(n,et())}var Xp=class n extends Aa{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return po(this._hostTNode,this._hostLView)}get injector(){return new xs(this._hostTNode,this._hostLView)}get parentInjector(){let e=um(this._hostTNode,this._hostLView);if(Q_(e)){let t=Vl(e,this._hostLView),i=Bl(e),r=t[De].data[i+8];return new xs(r,t)}else return new xs(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=U_(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-It}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=Gp(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,xa(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c=e&&!wS(e),l;if(c)l=t;else{let m=t||{};l=m.index,i=m.injector,r=m.projectableNodes,s=m.environmentInjector||m.ngModuleRef,o=m.directives,a=m.bindings}let u=c?e:new ba(fs(e)),f=i||this.parentInjector;if(!s&&u.ngModule==null){let p=(c?f:this.parentInjector).get(xn,null);p&&(s=p)}let d=fs(u.componentType??{}),h=Gp(this._lContainer,d?.id??null),g=h?.firstChild??null,x=u.create(f,r,g,s,o,a);return this.insertImpl(x.hostView,l,xa(this._hostTNode,h)),x}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(jv(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[$t],l=new n(c,c[$n],c[$t]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return au(o,r,s,i),e.attachToViewContainerRef(),Wh(Ip(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=U_(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=Ea(this._lContainer,t);i&&(oa(Ip(this._lContainer),t),ru(i[De],i))}detach(e){let t=this._adjustIndex(e,-1),i=Ea(this._lContainer,t);return i&&oa(Ip(this._lContainer),t)!=null?new uo(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function U_(n){return n[ua]}function Ip(n){return n[ua]||(n[ua]=[])}function lx(n,e){let t,i=e[n.index];return ri(i)?t=i:(t=X0(i,e,null,n),e[n.index]=t,ym(e,t)),XT(t,e,n,i),new Xp(t,n,e)}function qT(n,e){let t=n[At],i=t.createComment(""),r=Si(e,n),s=t.parentNode(r);return jl(t,s,i,t.nextSibling(r),!1),i}var XT=JT,YT=()=>!1;function ZT(n,e,t){return YT(n,e,t)}function JT(n,e,t,i){if(n[br])return;let r;t.type&8?r=Xn(i):r=qT(e,t),n[br]=r}var Yp=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Zp=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Cm(e,t).matches!==null&&this.queries[t].setDirty()}},Jp=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=sD(e):this.predicate=e}},Kp=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},Qp=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,KT(t,s)),this.matchTNodeWithReadOption(e,t,Ll(t,e,s,!1,!1))}else i===fo?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,Ll(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===mo||r===Aa||r===fo&&t.type&4)this.addMatch(t.index,-2);else{let s=Ll(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function KT(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function QT(n,e){return n.type&11?po(n,e):n.type&4?Dm(n,e):null}function eD(n,e,t,i){return t===-1?QT(e,n):t===-2?tD(n,e,i):zl(n,n[De],t,e)}function tD(n,e,t){if(t===mo)return po(e,n);if(t===fo)return Dm(e,n);if(t===Aa)return lx(e,n)}function ux(n,e,t,i){let r=e[bi].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(eD(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function em(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=ux(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let f=It;f<u.length;f++){let d=u[f];d[Mr]===d[$t]&&em(d[De],d,l,i)}if(u[gs]!==null){let f=u[gs];for(let d=0;d<f.length;d++){let h=f[d];em(h[De],h,l,i)}}}}}return i}function nD(n,e){return n[bi].queries[e].queryList}function iD(n,e,t){let i=new Gl((t&4)===4);return qv(n,e,i,i.destroy),(e[bi]??=new Zp).queries.push(new Yp(i))-1}function rD(n,e,t){let i=an();return i.firstCreatePass&&(oD(i,new Jp(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),iD(i,et(),e)}function sD(n){return n.split(",").map(e=>e.trim())}function oD(n,e,t){n.queries===null&&(n.queries=new Kp),n.queries.track(new Qp(e,t))}function Cm(n,e){return n.queries.getByIndex(e)}function aD(n,e){let t=n[De],i=Cm(t,e);return i.crossesNgTemplate?em(t,n,e,[]):ux(t,n,i,e)}var Sa=class{};var wa=class extends Sa{injector;componentFactoryResolver=new qp(this);instance=null;constructor(e){super();let t=new as([...e.providers,{provide:Sa,useValue:this},{provide:uu,useValue:this.componentFactoryResolver}],e.parent||ca(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function dx(n,e,t=null){return new wa({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var cD=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Yh(!1,t.type),r=i.length>0?dx([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Qe({token:n,providedIn:"environment",factory:()=>new n(Ye(xn))})}return n})();function Ss(n){return X_(()=>{let e=fD(n),t=wn(Wt({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===dm.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(cD).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||ai.Emulated,styles:n.styles||Mi,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&go("NgStandalone"),hD(t);let i=n.dependencies;return t.directiveDefs=B_(i,lD),t.pipeDefs=B_(i,Dv),t.id=pD(t),t})}function lD(n){return fs(n)||Hh(n)}function uD(n,e){if(n==null)return hs;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=iu.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function dD(n){if(n==null)return hs;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function Am(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function fD(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||hs,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Mi,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:uD(n.inputs,e),outputs:dD(n.outputs),debugInfo:null}}function hD(n){n.features?.forEach(e=>e(n))}function B_(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function pD(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function mD(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=Ql(n.mergedAttrs,n.attrs);let u=n.tView=mm(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),so(n,!1);let c=gD(t,e,n,i);Il()&&Mm(t,e,c,n),lo(c,e);let l=X0(c,e,c,n);e[i+qt]=l,ym(e,l),ZT(l,n,e)}function ql(n,e,t,i,r,s,o,a,c,l,u){let f=t+qt,d;if(e.firstCreatePass){if(d=cu(e,f,4,o||null,a||null),l!=null){let h=Ti(e.consts,l);d.localNames=[];for(let g=0;g<h.length;g+=2)d.localNames.push(h[g],-1)}}else d=e.data[f];return mD(d,n,e,t,i,r,s,c),l!=null&&bm(n,d,u),d}var gD=yD;function yD(n,e,t,i){return Rl(!0),e[At].createComment("")}var Im=new Pe("");function du(n){return!!n&&typeof n.then=="function"}function Rm(n){return!!n&&typeof n.subscribe=="function"}var fx=new Pe("");var Nm=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=Ee(fx,{optional:!0})??[];injector=Ee($i);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=eo(this.injector,r);if(du(s))t.push(s);else if(Rm(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),hx=new Pe("");function px(){rh(()=>{let n="";throw new we(600,n)})}function mx(n){return n.isBoundToModule}var vD=10;var fu=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=Ee(Zi);afterRenderManager=Ee(A0);zonelessEnabled=Ee(ha);rootEffectScheduler=Ee(Tp);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new Gi;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=Ee(_s);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(vn(t=>!t))}constructor(){Ee(Ar,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=Ee(xn);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=$i.NULL){return this._injector.get(Mn).run(()=>{ft(rt.BootstrapComponentStart);let o=t instanceof lu;if(!this._injector.get(Nm).done){let g="";throw new we(405,g)}let c;o?c=t:c=this._injector.get(uu).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=mx(c)?void 0:this._injector.get(Sa),u=i||c.selector,f=c.create(r,[],u,l),d=f.location.nativeElement,h=f.injector.get(Im,null);return h?.registerApplication(d),f.onDestroy(()=>{this.detachView(f.hostView),va(this.components,f),h?.unregisterApplication(d)}),this._loadComponent(f),ft(rt.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ft(rt.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(_m.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ft(rt.ChangeDetectionEnd),new we(101,!1);let t=Ie(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,Ie(t),this.afterTick.next(),ft(rt.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Es,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<vD;){ft(rt.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ft(rt.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!da(r))continue;let s=i&&!this.zonelessEnabled?0:1;j0(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>da(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;va(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(hx,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>va(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new we(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function va(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}var tm=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function Rp(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function _D(n,e,t,i){let r,s,o=0,a=n.length-1,c=void 0;if(Array.isArray(e)){Ie(i);let l=e.length-1;for(Ie(null);o<=a&&o<=l;){let u=n.at(o),f=e[o],d=Rp(o,u,o,f,t);if(d!==0){d<0&&n.updateValue(o,f),o++;continue}let h=n.at(a),g=e[l],x=Rp(a,h,l,g,t);if(x!==0){x<0&&n.updateValue(a,g),a--,l--;continue}let m=t(o,u),p=t(a,h),E=t(o,f);if(Object.is(E,p)){let S=t(l,g);Object.is(S,m)?(n.swap(o,a),n.updateValue(a,g),l--,a--):n.move(a,o),n.updateValue(o,f),o++;continue}if(r??=new Xl,s??=H_(n,o,a,t),nm(n,r,o,E))n.updateValue(o,f),o++,a++;else if(s.has(E))r.set(m,n.detach(o)),a--;else{let S=n.create(o,e[o]);n.attach(o,S),o++,a++}}for(;o<=l;)V_(n,r,t,o,e[o]),o++}else if(e!=null){Ie(i);let l=e[Symbol.iterator]();Ie(null);let u=l.next();for(;!u.done&&o<=a;){let f=n.at(o),d=u.value,h=Rp(o,f,o,d,t);if(h!==0)h<0&&n.updateValue(o,d),o++,u=l.next();else{r??=new Xl,s??=H_(n,o,a,t);let g=t(o,d);if(nm(n,r,o,g))n.updateValue(o,d),o++,a++,u=l.next();else if(!s.has(g))n.attach(o,n.create(o,d)),o++,a++,u=l.next();else{let x=t(o,f);r.set(x,n.detach(o)),a--}}}for(;!u.done;)V_(n,r,t,n.length,u.value),u=l.next()}for(;o<=a;)n.destroy(n.detach(a--));r?.forEach(l=>{n.destroy(l)})}function nm(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function V_(n,e,t,i,r){if(nm(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function H_(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var Xl=class{kvMap=new Map;_vMap=void 0;has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function Qi(n,e,t,i,r,s,o,a){go("NgControlFlow");let c=et(),l=an(),u=Ti(l.consts,s);return ql(c,l,n,e,t,i,r,u,256,o,a),Pm}function Pm(n,e,t,i,r,s,o,a){go("NgControlFlow");let c=et(),l=an(),u=Ti(l.consts,s);return ql(c,l,n,e,t,i,r,u,512,o,a),Pm}function er(n,e){go("NgControlFlow");let t=et(),i=oo(),r=t[i]!==Tn?t[i]:-1,s=r!==-1?Yl(t,qt+r):void 0,o=0;if(Ji(t,i,n)){let a=Ie(null);try{if(s!==void 0&&Z0(s,o),n!==-1){let c=qt+n,l=Yl(t,c),u=om(t[De],c),f=K0(l,u,t),d=ou(t,u,e,{dehydratedView:f});au(l,d,o,xa(u,f))}}finally{Ie(a)}}else if(s!==void 0){let a=Y0(s,o);a!==void 0&&(a[Bt]=e)}}var im=class{lContainer;$implicit;$index;constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-It}};function Om(n,e){return e}var rm=class{hasEmptyBlock;trackByFn;liveCollection;constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function hu(n,e,t,i,r,s,o,a,c,l,u,f,d){go("NgControlFlow");let h=et(),g=an(),x=c!==void 0,m=et(),p=a?o.bind(m[qn][Bt]):o,E=new rm(x,p);m[qt+n]=E,ql(h,g,n+1,e,t,i,r,Ti(g.consts,s),256),x&&ql(h,g,n+2,c,l,u,f,Ti(g.consts,d),512)}var sm=class extends tm{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-It}at(e){return this.getLView(e)[Bt].$implicit}attach(e,t){let i=t[to];this.needsIndexUpdate||=e!==this.length,au(this.lContainer,t,e,xa(this.templateTNode,i)),xD(this.lContainer,e)}detach(e){return this.needsIndexUpdate||=e!==this.length-1,MD(this.lContainer,e),ED(this.lContainer,e)}create(e,t){let i=Gp(this.lContainer,this.templateTNode.tView.ssrId);return ou(this.hostLView,this.templateTNode,new im(this.lContainer,t,e),{dehydratedView:i})}destroy(e){ru(e[De],e)}updateValue(e,t){this.getLView(e)[Bt].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[Bt].$index=e}getLView(e){return bD(this.lContainer,e)}};function pu(n){let e=Ie(null),t=Ai();try{let i=et(),r=i[De],s=i[t],o=t+1,a=Yl(i,o);if(s.liveCollection===void 0){let l=om(r,o);s.liveCollection=new sm(a,i,l)}else s.liveCollection.reset();let c=s.liveCollection;if(_D(c,n,s.trackByFn,e),c.updateIndexes(),s.hasEmptyBlock){let l=oo(),u=c.length===0;if(Ji(i,l,u)){let f=t+2,d=Yl(i,f);if(u){let h=om(r,f),g=K0(d,h,i),x=ou(i,h,void 0,{dehydratedView:g});au(d,x,0,xa(h,g))}else r.firstUpdatePass&&xT(d),Z0(d,0)}}}finally{Ie(e)}}function Yl(n,e){return n[e]}function xD(n,e){if(n.length<=It)return;let t=It+e,i=n[t],r=i?i[Er]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let s=i[qi];Tw(s,r),Ms.delete(i[Xi]),r.detachedLeaveAnimationFns=void 0}}function MD(n,e){if(n.length<=It)return;let t=It+e,i=n[t],r=i?i[Er]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function ED(n,e){return Ea(n,e)}function bD(n,e){return Y0(n,e)}function om(n,e){return El(n,e)}function mu(n,e,t){let i=et(),r=oo();if(Ji(i,r,e)){let s=an(),o=_p();$w(o,i,n,e,i[At],t)}return mu}function am(n,e,t,i,r){Sm(e,n,t,r?"class":"style",i)}function xt(n,e,t,i){let r=et(),s=r[De],o=n+qt,a=s.firstCreatePass?nx(o,r,2,e,Jw,Zv(),t,i):s.data[o];if(wr(a)){let c=r[Ei].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(ax(l),()=>(z_(n,e,r,a,i),xt))}}return z_(n,e,r,a,i),xt}function z_(n,e,t,i,r){if(k0(i,t,n,e,gx),Ml(i)){let s=t[De];L0(s,t,i),x0(s,i,t)}r!=null&&bm(t,i)}function Rt(){let n=an(),e=si(),t=U0(e);return n.firstCreatePass&&ix(n,t),up(t)&&dp(),lp(),t.classesWithoutHost!=null&&PS(t)&&am(n,t,et(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&OS(t)&&am(n,t,et(),t.stylesWithoutHost,!1),Rt}function tr(n,e,t,i){return xt(n,e,t,i),Rt(),tr}function St(n,e,t,i){let r=et(),s=r[De],o=n+qt,a=s.firstCreatePass?OT(o,s,2,e,t,i):s.data[o];return k0(a,r,n,e,gx),i!=null&&bm(r,a),St}function Nt(){let n=si(),e=U0(n);return up(e)&&dp(),lp(),Nt}function Ni(n,e,t,i){return St(n,e,t,i),Nt(),Ni}var gx=(n,e,t,i,r)=>(Rl(!0),M0(e[At],i,d_()));function Ir(){return et()}function Ia(n,e,t){let i=et(),r=oo();if(Ji(i,r,e)){let s=an(),o=_p();F0(o,i,n,e,i[At],t)}return Ia}var ma=void 0;function SD(n){let e=Math.floor(Math.abs(n)),t=n.toString().replace(/^[^.]*\.?/,"").length;return e===1&&t===0?1:5}var wD=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],ma,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],ma,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",ma,ma,ma],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",SD],Np={};function On(n){let e=TD(n),t=G_(e);if(t)return t;let i=e.split("-")[0];if(t=G_(i),t)return t;if(i==="en")return wD;throw new we(701,!1)}function G_(n){return n in Np||(Np[n]=us.ng&&us.ng.common&&us.ng.common.locales&&us.ng.common.locales[n]),Np[n]}var Vt=(function(n){return n[n.LocaleId=0]="LocaleId",n[n.DayPeriodsFormat=1]="DayPeriodsFormat",n[n.DayPeriodsStandalone=2]="DayPeriodsStandalone",n[n.DaysFormat=3]="DaysFormat",n[n.DaysStandalone=4]="DaysStandalone",n[n.MonthsFormat=5]="MonthsFormat",n[n.MonthsStandalone=6]="MonthsStandalone",n[n.Eras=7]="Eras",n[n.FirstDayOfWeek=8]="FirstDayOfWeek",n[n.WeekendRange=9]="WeekendRange",n[n.DateFormat=10]="DateFormat",n[n.TimeFormat=11]="TimeFormat",n[n.DateTimeFormat=12]="DateTimeFormat",n[n.NumberSymbols=13]="NumberSymbols",n[n.NumberFormats=14]="NumberFormats",n[n.CurrencyCode=15]="CurrencyCode",n[n.CurrencySymbol=16]="CurrencySymbol",n[n.CurrencyName=17]="CurrencyName",n[n.Currencies=18]="Currencies",n[n.Directionality=19]="Directionality",n[n.PluralCase=20]="PluralCase",n[n.ExtraData=21]="ExtraData",n})(Vt||{});function TD(n){return n.toLowerCase().replace(/_/g,"-")}var Ra="en-US";var DD=Ra;function yx(n){typeof n=="string"&&(DD=n.toLowerCase().replace(/_/g,"-"))}function ws(n,e,t){let i=et(),r=an(),s=si();return CD(r,i,i[At],s,n,e,t),ws}function Ts(n,e,t){let i=et(),r=an(),s=si();return(s.type&3||t)&&sx(s,r,i,t,i[At],n,e,kl(s,i,e)),Ts}function CD(n,e,t,i,r,s,o){let a=!0,c=null;if((i.type&3||o)&&(c??=kl(i,e,s),sx(i,n,e,o,t,r,s,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let d=u[f],h=u[f+1];c??=kl(i,e,s),F_(i,e,d,h,r,c)}if(l&&l.length)for(let f of l)c??=kl(i,e,s),F_(i,e,f,r,r,c)}}function un(n=1){return u_(n)}function gu(n,e,t){return rD(n,e,t),gu}function Lm(n){let e=et(),t=an(),i=gp();Dl(i+1);let r=Cm(t,i);if(n.dirty&&Gv(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=aD(e,i);n.reset(s,XS),n.notifyOnChanges()}return!0}return!1}function Fm(){return nD(et(),gp())}function Nl(n,e){return n<<17|e<<2}function bs(n){return n>>17&32767}function AD(n){return(n&2)==2}function ID(n,e){return n&131071|e<<17}function cm(n){return n|2}function ho(n){return(n&131068)>>2}function Pp(n,e){return n&-131069|e<<2}function RD(n){return(n&1)===1}function lm(n){return n|1}function ND(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=bs(o),c=ho(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let f=t;u=f[1],(u===null||Js(f,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let d=bs(n[a+1]);n[i+1]=Nl(d,a),d!==0&&(n[d+1]=Pp(n[d+1],i)),n[a+1]=ID(n[a+1],i)}else n[i+1]=Nl(a,0),a!==0&&(n[a+1]=Pp(n[a+1],i)),a=i;else n[i+1]=Nl(c,0),a===0?a=i:n[c+1]=Pp(n[c+1],i),c=i;l&&(n[i+1]=cm(n[i+1])),j_(n,u,i,!0),j_(n,u,i,!1),PD(e,u,n,i,s),o=Nl(a,c),s?e.classBindings=o:e.styleBindings=o}function PD(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Js(s,e)>=0&&(t[i+1]=lm(t[i+1]))}function j_(n,e,t,i){let r=n[t+1],s=e===null,o=i?bs(r):ho(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];OD(c,e)&&(a=!0,n[o+1]=i?lm(l):cm(l)),o=i?bs(l):ho(l)}a&&(n[t+1]=i?cm(r):lm(r))}function OD(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Js(n,e)>=0:!1}var En={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function LD(n){return n.substring(En.key,En.keyEnd)}function FD(n){return n.substring(En.value,En.valueEnd)}function kD(n){return UD(n),vx(n,Zl(n,0,En.textEnd))}function vx(n,e){let t=En.textEnd,i=En.key=Zl(n,e,t);return t===i?-1:(i=En.keyEnd=BD(n,i,t),i=W_(n,i,t,58),i=En.value=Zl(n,i,t),i=En.valueEnd=VD(n,i,t),W_(n,i,t,59))}function UD(n){En.key=0,En.keyEnd=0,En.value=0,En.valueEnd=0,En.textEnd=n.length}function Zl(n,e,t){for(;e<t&&n.charCodeAt(e)<=32;)e++;return e}function BD(n,e,t){let i;for(;e<t&&((i=n.charCodeAt(e))===45||i===95||(i&-33)>=65&&(i&-33)<=90||i>=48&&i<=57);)e++;return e}function W_(n,e,t,i){return e=Zl(n,e,t),e<t&&e++,e}function VD(n,e,t){let i=-1,r=-1,s=-1,o=e,a=o;for(;o<t;){let c=n.charCodeAt(o++);if(c===59)return a;c===34||c===39?a=o=$_(n,c,o,t):e===o-4&&s===85&&r===82&&i===76&&c===40?a=o=$_(n,41,o,t):c>32&&(a=o),s=r,r=i,i=c&-33}return a}function $_(n,e,t,i){let r=-1,s=t;for(;s<i;){let o=n.charCodeAt(s++);if(o==e&&r!==92)return s;o==92&&r===92?r=0:r=o}throw new Error}function yu(n,e,t){return _x(n,e,t,!1),yu}function yo(n,e){return _x(n,e,null,!0),yo}function km(n){zD(Ex,HD,n,!1)}function HD(n,e){for(let t=kD(e);t>=0;t=vx(e,t))Ex(n,LD(e),FD(e))}function _x(n,e,t,i){let r=et(),s=an(),o=wl(2);if(s.firstUpdatePass&&Mx(s,n,o,i),e!==Tn&&Ji(r,o,e)){let a=s.data[Ai()];bx(s,a,r,r[At],n,r[o+1]=YD(e,t),i,o)}}function zD(n,e,t,i){let r=an(),s=wl(2);r.firstUpdatePass&&Mx(r,null,s,i);let o=et();if(t!==Tn&&Ji(o,s,t)){let a=r.data[Ai()];if(Sx(a,i)&&!xx(r,s)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=dl(c,t||"")),am(r,a,o,t,i)}else XD(r,a,o,o[At],o[s+1],o[s+1]=qD(n,e,t),i,s)}}function xx(n,e){return e>=n.expandoStartIndex}function Mx(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Ai()],o=xx(n,t);Sx(s,i)&&e===null&&!o&&(e=!1),e=GD(r,s,e,i),ND(r,s,e,t,o,i)}}function GD(n,e,t,i){let r=o_(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=Op(null,n,e,t,i),t=Ta(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=Op(r,n,e,t,i),s===null){let c=jD(n,e,i);c!==void 0&&Array.isArray(c)&&(c=Op(null,n,e,c[1],i),c=Ta(c,e.attrs,i),WD(n,e,i,c))}else s=$D(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function jD(n,e,t){let i=t?e.classBindings:e.styleBindings;if(ho(i)!==0)return n[bs(i)]}function WD(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[bs(r)]=i}function $D(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=Ta(i,o,t)}return Ta(i,e.attrs,t)}function Op(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=Ta(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function Ta(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),$h(n,o,t?!0:e[++s]))}return n===void 0?null:n}function qD(n,e,t){if(t==null||t==="")return Mi;let i=[],r=nu(t);if(Array.isArray(r))for(let s=0;s<r.length;s++)n(i,r[s],!0);else if(r instanceof Set)for(let s of r)n(i,s,!0);else if(typeof r=="object")for(let s in r)r.hasOwnProperty(s)&&n(i,s,r[s]);else typeof r=="string"&&e(i,r);return i}function Ex(n,e,t){$h(n,e,nu(t))}function XD(n,e,t,i,r,s,o,a){r===Tn&&(r=Mi);let c=0,l=0,u=0<r.length?r[0]:null,f=0<s.length?s[0]:null;for(;u!==null||f!==null;){let d=c<r.length?r[c+1]:void 0,h=l<s.length?s[l+1]:void 0,g=null,x;u===f?(c+=2,l+=2,d!==h&&(g=f,x=h)):f===null||u!==null&&u<f?(c+=2,g=u):(l+=2,g=f,x=h),g!==null&&bx(n,e,t,i,g,x,o,a),u=c<r.length?r[c]:null,f=l<s.length?s[l]:null}}function bx(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=RD(l)?q_(c,e,t,r,ho(l),o):void 0;if(!Jl(u)){Jl(s)||AD(l)&&(s=q_(c,null,t,r,a,o));let f=np(Ai(),t);Hw(i,o,f,r,s)}}function q_(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,f=u===null,d=t[r+1];d===Tn&&(d=f?Mi:void 0);let h=f?vl(d,i):u===i?d:void 0;if(l&&!Jl(h)&&(h=vl(c,i)),Jl(h)&&(a=h,o))return a;let g=n[r+1];r=o?bs(g):ho(g)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=vl(c,i))}return a}function Jl(n){return n!==void 0}function YD(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=ul(nu(n)))),n}function Sx(n,e){return(n.flags&(e?8:16))!==0}function ot(n,e=""){let t=et(),i=an(),r=n+qt,s=i.firstCreatePass?cu(i,r,1,e,null):i.data[r],o=ZD(i,t,s,e);t[r]=o,Il()&&Mm(i,t,o,s),so(s,!1)}var ZD=(n,e,t,i)=>(Rl(!0),sw(e[At],i));function JD(n,e,t,i=""){return Ji(n,oo(),t)?e+ml(t)+i:Tn}function KD(n,e,t,i,r,s=""){let o=t_(),a=rx(n,o,t,r);return wl(2),a?e+ml(t)+i+ml(r)+s:Tn}function ci(n){return nr("",n),ci}function nr(n,e,t){let i=et(),r=JD(i,n,e,t);return r!==Tn&&wx(i,Ai(),r),nr}function vu(n,e,t,i,r){let s=et(),o=KD(s,n,e,t,i,r);return o!==Tn&&wx(s,Ai(),o),vu}function wx(n,e,t){let i=np(e,n);ow(n[At],i,t)}function QD(n,e){let t=n[e];return t===Tn?void 0:t}function eC(n,e,t,i,r,s,o){let a=e+t;return rx(n,a,r,s)?LT(n,a+2,o?i.call(o,r,s):i(r,s)):QD(n,a+2)}function vo(n,e){let t=an(),i,r=n+qt;t.firstCreatePass?(i=tC(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let s=i.factory||(i.factory=os(i.type,!0)),o,a=_n(Ki);try{let c=Hl(!1),l=s();return Hl(c),ip(t,et(),r,l),l}finally{_n(a)}}function tC(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function _o(n,e,t,i){let r=n+qt,s=et(),o=zv(s,r);return nC(s,r)?eC(s,e_(),e,o.transform,t,i,o):o.transform(t,i)}function nC(n,e){return n[De].data[e].pure}var Tx=(()=>{class n{applicationErrorHandler=Ee(Zi);appRef=Ee(fu);taskService=Ee(_s);ngZone=Ee(Mn);zonelessEnabled=Ee(ha);tracing=Ee(Ar,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new yn;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(ra):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(Ee(wp,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?g_:xp;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(ra+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Dx(){return[{provide:cs,useExisting:Tx},{provide:Mn,useClass:sa},{provide:ha,useValue:!0}]}function iC(){return typeof $localize<"u"&&$localize.locale||Ra}var Na=new Pe("",{factory:()=>Ee(Na,{optional:!0,skipSelf:!0})||iC()});var _u=class{destroyed=!1;listeners=null;errorHandler=Ee(ni,{optional:!0});destroyRef=Ee(Cr);constructor(){this.destroyRef.onDestroy(()=>{this.destroyed=!0,this.listeners=null})}subscribe(e){if(this.destroyed)throw new we(953,!1);return(this.listeners??=[]).push(e),{unsubscribe:()=>{let t=this.listeners?.indexOf(e);t!==void 0&&t!==-1&&this.listeners?.splice(t,1)}}}emit(e){if(this.destroyed){console.warn(ls(953,!1));return}if(this.listeners===null)return;let t=Ie(null);try{for(let i of this.listeners)try{i(e)}catch(r){this.errorHandler?.handleError(r)}}finally{Ie(t)}}};function xu(n,e){return Uc(n,e?.equal)}function Eu(n){return new _u}var Um=new Pe(""),vC=new Pe("");function Pa(n){return!n.moduleRef}function _C(n){let e=Pa(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Mn);return t.run(()=>{Pa(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(Zi),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),Pa(n)){let s=()=>e.destroy(),o=n.platformInjector.get(Um);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(Um);o.add(s),n.moduleRef.onDestroy(()=>{va(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return MC(i,t,()=>{let s=e.get(_s),o=s.add(),a=e.get(Nm);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(Na,Ra);if(yx(c||Ra),!e.get(vC,!0))return Pa(n)?e.get(fu):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Pa(n)){let u=e.get(fu);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return xC?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var xC;function MC(n,e,t){try{let i=t();return du(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var Mu=null;function EC(n=[],e){return $i.create({name:e,providers:[{provide:aa,useValue:"platform"},{provide:Um,useValue:new Set([()=>Mu=null])},...n]})}function bC(n=[]){if(Mu)return Mu;let e=EC(n);return Mu=e,px(),SC(e),e}function SC(n){let e=n.get(tu,null);eo(n,()=>{e?.forEach(t=>t())})}var wC=1e4;var ZG=wC-1e3;function Cx(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;ft(rt.BootstrapApplicationStart);try{let s=r?.injector??bC(i),o=[Dx(),v_,...t||[]],a=new wa({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return _C({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{ft(rt.BootstrapApplicationEnd)}}var Ax=null;function Rr(){return Ax}function Hm(n){Ax??=n}var Oa=class{},bu=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:()=>Ee(Ix),providedIn:"platform"})}return n})();var Ix=(()=>{class n extends bu{_location;_history;_doc=Ee(cn);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Rr().getBaseHref(this._doc)}onPopState(t){let i=Rr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=Rr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:()=>new n,providedIn:"platform"})}return n})();var dn=(function(n){return n[n.Format=0]="Format",n[n.Standalone=1]="Standalone",n})(dn||{}),ht=(function(n){return n[n.Narrow=0]="Narrow",n[n.Abbreviated=1]="Abbreviated",n[n.Wide=2]="Wide",n[n.Short=3]="Short",n})(ht||{}),Dn=(function(n){return n[n.Short=0]="Short",n[n.Medium=1]="Medium",n[n.Long=2]="Long",n[n.Full=3]="Full",n})(Dn||{}),sr={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function Nx(n){return On(n)[Vt.LocaleId]}function Px(n,e,t){let i=On(n),r=[i[Vt.DayPeriodsFormat],i[Vt.DayPeriodsStandalone]],s=Yn(r,e);return Yn(s,t)}function Ox(n,e,t){let i=On(n),r=[i[Vt.DaysFormat],i[Vt.DaysStandalone]],s=Yn(r,e);return Yn(s,t)}function Lx(n,e,t){let i=On(n),r=[i[Vt.MonthsFormat],i[Vt.MonthsStandalone]],s=Yn(r,e);return Yn(s,t)}function Fx(n,e){let i=On(n)[Vt.Eras];return Yn(i,e)}function La(n,e){let t=On(n);return Yn(t[Vt.DateFormat],e)}function Fa(n,e){let t=On(n);return Yn(t[Vt.TimeFormat],e)}function ka(n,e){let i=On(n)[Vt.DateTimeFormat];return Yn(i,e)}function Ua(n,e){let t=On(n),i=t[Vt.NumberSymbols][e];if(typeof i>"u"){if(e===sr.CurrencyDecimal)return t[Vt.NumberSymbols][sr.Decimal];if(e===sr.CurrencyGroup)return t[Vt.NumberSymbols][sr.Group]}return i}function kx(n){if(!n[Vt.ExtraData])throw new we(2303,!1)}function Ux(n){let e=On(n);return kx(e),(e[Vt.ExtraData][2]||[]).map(i=>typeof i=="string"?zm(i):[zm(i[0]),zm(i[1])])}function Bx(n,e,t){let i=On(n);kx(i);let r=[i[Vt.ExtraData][0],i[Vt.ExtraData][1]],s=Yn(r,e)||[];return Yn(s,t)||[]}function Yn(n,e){for(let t=e;t>-1;t--)if(typeof n[t]<"u")return n[t];throw new we(2304,!1)}function zm(n){let[e,t]=n.split(":");return{hours:+e,minutes:+t}}var TC=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,Su={},DC=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/;function Vx(n,e,t,i){let r=FC(n);e=rr(t,e)||e;let o=[],a;for(;e;)if(a=DC.exec(e),a){o=o.concat(a.slice(1));let u=o.pop();if(!u)break;e=u}else{o.push(e);break}let c=r.getTimezoneOffset();i&&(c=zx(i,c),r=LC(r,i));let l="";return o.forEach(u=>{let f=PC(u);l+=f?f(r,t,c):u==="''"?"'":u.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),l}function Au(n,e,t){let i=new Date(0);return i.setFullYear(n,e,t),i.setHours(0,0,0),i}function rr(n,e){let t=Nx(n);if(Su[t]??={},Su[t][e])return Su[t][e];let i="";switch(e){case"shortDate":i=La(n,Dn.Short);break;case"mediumDate":i=La(n,Dn.Medium);break;case"longDate":i=La(n,Dn.Long);break;case"fullDate":i=La(n,Dn.Full);break;case"shortTime":i=Fa(n,Dn.Short);break;case"mediumTime":i=Fa(n,Dn.Medium);break;case"longTime":i=Fa(n,Dn.Long);break;case"fullTime":i=Fa(n,Dn.Full);break;case"short":let r=rr(n,"shortTime"),s=rr(n,"shortDate");i=wu(ka(n,Dn.Short),[r,s]);break;case"medium":let o=rr(n,"mediumTime"),a=rr(n,"mediumDate");i=wu(ka(n,Dn.Medium),[o,a]);break;case"long":let c=rr(n,"longTime"),l=rr(n,"longDate");i=wu(ka(n,Dn.Long),[c,l]);break;case"full":let u=rr(n,"fullTime"),f=rr(n,"fullDate");i=wu(ka(n,Dn.Full),[u,f]);break}return i&&(Su[t][e]=i),i}function wu(n,e){return e&&(n=n.replace(/\{([^}]+)}/g,function(t,i){return e!=null&&i in e?e[i]:t})),n}function li(n,e,t="-",i,r){let s="";(n<0||r&&n<=0)&&(r?n=-n+1:(n=-n,s=t));let o=String(n);for(;o.length<e;)o="0"+o;return i&&(o=o.slice(o.length-e)),s+o}function CC(n,e){return li(n,3).substring(0,e)}function Ht(n,e,t=0,i=!1,r=!1){return function(s,o){let a=AC(n,s);if((t>0||a>-t)&&(a+=t),n===3)a===0&&t===-12&&(a=12);else if(n===6)return CC(a,e);let c=Ua(o,sr.MinusSign);return li(a,e,c,i,r)}}function AC(n,e){switch(n){case 0:return e.getFullYear();case 1:return e.getMonth();case 2:return e.getDate();case 3:return e.getHours();case 4:return e.getMinutes();case 5:return e.getSeconds();case 6:return e.getMilliseconds();case 7:return e.getDay();default:throw new we(2301,!1)}}function Mt(n,e,t=dn.Format,i=!1){return function(r,s){return IC(r,s,n,e,t,i)}}function IC(n,e,t,i,r,s){switch(t){case 2:return Lx(e,r,i)[n.getMonth()];case 1:return Ox(e,r,i)[n.getDay()];case 0:let o=n.getHours(),a=n.getMinutes();if(s){let l=Ux(e),u=Bx(e,r,i),f=l.findIndex(d=>{if(Array.isArray(d)){let[h,g]=d,x=o>=h.hours&&a>=h.minutes,m=o<g.hours||o===g.hours&&a<g.minutes;if(h.hours<g.hours){if(x&&m)return!0}else if(x||m)return!0}else if(d.hours===o&&d.minutes===a)return!0;return!1});if(f!==-1)return u[f]}return Px(e,r,i)[o<12?0:1];case 3:return Fx(e,i)[n.getFullYear()<=0?0:1];default:let c=t;throw new we(2302,!1)}}function Tu(n){return function(e,t,i){let r=-1*i,s=Ua(t,sr.MinusSign),o=r>0?Math.floor(r/60):Math.ceil(r/60);switch(n){case 0:return(r>=0?"+":"")+li(o,2,s)+li(Math.abs(r%60),2,s);case 1:return"GMT"+(r>=0?"+":"")+li(o,1,s);case 2:return"GMT"+(r>=0?"+":"")+li(o,2,s)+":"+li(Math.abs(r%60),2,s);case 3:return i===0?"Z":(r>=0?"+":"")+li(o,2,s)+":"+li(Math.abs(r%60),2,s);default:throw new we(2310,!1)}}}var RC=0,Cu=4;function NC(n){let e=Au(n,RC,1).getDay();return Au(n,0,1+(e<=Cu?Cu:Cu+7)-e)}function Hx(n){let e=n.getDay(),t=e===0?-3:Cu-e;return Au(n.getFullYear(),n.getMonth(),n.getDate()+t)}function Gm(n,e=!1){return function(t,i){let r;if(e){let s=new Date(t.getFullYear(),t.getMonth(),1).getDay()-1,o=t.getDate();r=1+Math.floor((o+s)/7)}else{let s=Hx(t),o=NC(s.getFullYear()),a=s.getTime()-o.getTime();r=1+Math.round(a/6048e5)}return li(r,n,Ua(i,sr.MinusSign))}}function Du(n,e=!1){return function(t,i){let s=Hx(t).getFullYear();return li(s,n,Ua(i,sr.MinusSign),e)}}var jm={};function PC(n){if(jm[n])return jm[n];let e;switch(n){case"G":case"GG":case"GGG":e=Mt(3,ht.Abbreviated);break;case"GGGG":e=Mt(3,ht.Wide);break;case"GGGGG":e=Mt(3,ht.Narrow);break;case"y":e=Ht(0,1,0,!1,!0);break;case"yy":e=Ht(0,2,0,!0,!0);break;case"yyy":e=Ht(0,3,0,!1,!0);break;case"yyyy":e=Ht(0,4,0,!1,!0);break;case"Y":e=Du(1);break;case"YY":e=Du(2,!0);break;case"YYY":e=Du(3);break;case"YYYY":e=Du(4);break;case"M":case"L":e=Ht(1,1,1);break;case"MM":case"LL":e=Ht(1,2,1);break;case"MMM":e=Mt(2,ht.Abbreviated);break;case"MMMM":e=Mt(2,ht.Wide);break;case"MMMMM":e=Mt(2,ht.Narrow);break;case"LLL":e=Mt(2,ht.Abbreviated,dn.Standalone);break;case"LLLL":e=Mt(2,ht.Wide,dn.Standalone);break;case"LLLLL":e=Mt(2,ht.Narrow,dn.Standalone);break;case"w":e=Gm(1);break;case"ww":e=Gm(2);break;case"W":e=Gm(1,!0);break;case"d":e=Ht(2,1);break;case"dd":e=Ht(2,2);break;case"c":case"cc":e=Ht(7,1);break;case"ccc":e=Mt(1,ht.Abbreviated,dn.Standalone);break;case"cccc":e=Mt(1,ht.Wide,dn.Standalone);break;case"ccccc":e=Mt(1,ht.Narrow,dn.Standalone);break;case"cccccc":e=Mt(1,ht.Short,dn.Standalone);break;case"E":case"EE":case"EEE":e=Mt(1,ht.Abbreviated);break;case"EEEE":e=Mt(1,ht.Wide);break;case"EEEEE":e=Mt(1,ht.Narrow);break;case"EEEEEE":e=Mt(1,ht.Short);break;case"a":case"aa":case"aaa":e=Mt(0,ht.Abbreviated);break;case"aaaa":e=Mt(0,ht.Wide);break;case"aaaaa":e=Mt(0,ht.Narrow);break;case"b":case"bb":case"bbb":e=Mt(0,ht.Abbreviated,dn.Standalone,!0);break;case"bbbb":e=Mt(0,ht.Wide,dn.Standalone,!0);break;case"bbbbb":e=Mt(0,ht.Narrow,dn.Standalone,!0);break;case"B":case"BB":case"BBB":e=Mt(0,ht.Abbreviated,dn.Format,!0);break;case"BBBB":e=Mt(0,ht.Wide,dn.Format,!0);break;case"BBBBB":e=Mt(0,ht.Narrow,dn.Format,!0);break;case"h":e=Ht(3,1,-12);break;case"hh":e=Ht(3,2,-12);break;case"H":e=Ht(3,1);break;case"HH":e=Ht(3,2);break;case"m":e=Ht(4,1);break;case"mm":e=Ht(4,2);break;case"s":e=Ht(5,1);break;case"ss":e=Ht(5,2);break;case"S":e=Ht(6,1);break;case"SS":e=Ht(6,2);break;case"SSS":e=Ht(6,3);break;case"Z":case"ZZ":case"ZZZ":e=Tu(0);break;case"ZZZZZ":e=Tu(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":e=Tu(1);break;case"OOOO":case"ZZZZ":case"zzzz":e=Tu(2);break;default:return null}return jm[n]=e,e}function zx(n,e){n=n.replace(/:/g,"");let t=Date.parse("Jan 01, 1970 00:00:00 "+n)/6e4;return isNaN(t)?e:t}function OC(n,e){return n=new Date(n.getTime()),n.setMinutes(n.getMinutes()+e),n}function LC(n,e,t){let r=n.getTimezoneOffset(),s=zx(e,r);return OC(n,-1*(s-r))}function FC(n){if(Rx(n))return n;if(typeof n=="number"&&!isNaN(n))return new Date(n);if(typeof n=="string"){if(n=n.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(n)){let[r,s=1,o=1]=n.split("-").map(a=>+a);return Au(r,s-1,o)}let t=parseFloat(n);if(!isNaN(n-t))return new Date(t);let i;if(i=n.match(TC))return kC(i)}let e=new Date(n);if(!Rx(e))throw new we(2311,!1);return e}function kC(n){let e=new Date(0),t=0,i=0,r=n[8]?e.setUTCFullYear:e.setFullYear,s=n[8]?e.setUTCHours:e.setHours;n[9]&&(t=Number(n[9]+n[10]),i=Number(n[9]+n[11])),r.call(e,Number(n[1]),Number(n[2])-1,Number(n[3]));let o=Number(n[4]||0)-t,a=Number(n[5]||0)-i,c=Number(n[6]||0),l=Math.floor(parseFloat("0."+(n[7]||0))*1e3);return s.call(e,o,a,c,l),e}function Rx(n){return n instanceof Date&&!isNaN(n.valueOf())}function UC(n,e){return new we(2100,!1)}var BC="mediumDate",Gx=new Pe(""),jx=new Pe(""),Ba=(()=>{class n{locale;defaultTimezone;defaultOptions;constructor(t,i,r){this.locale=t,this.defaultTimezone=i,this.defaultOptions=r}transform(t,i,r,s){if(t==null||t===""||t!==t)return null;try{let o=i??this.defaultOptions?.dateFormat??BC,a=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return Vx(t,o,s||this.locale,a)}catch(o){throw UC(n,o.message)}}static \u0275fac=function(i){return new(i||n)(Ki(Na,16),Ki(Gx,24),Ki(jx,24))};static \u0275pipe=Am({name:"date",type:n,pure:!0})}return n})();function Va(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var Ds=class{};var Wx="browser";var Ha=class{_doc;constructor(e){this._doc=e}manager},Iu=(()=>{class n extends Ha{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(Ye(cn))};static \u0275prov=Qe({token:n,factory:n.\u0275fac})}return n})(),Pu=new Pe(""),Xm=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof Iu));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof Iu);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new we(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(Ye(Pu),Ye(Mn))};static \u0275prov=Qe({token:n,factory:n.\u0275fac})}return n})(),Wm="ng-app-id";function $x(n){for(let e of n)e.remove()}function qx(n,e){let t=e.createElement("style");return t.textContent=n,t}function zC(n,e,t,i){let r=n.head?.querySelectorAll(`style[${Wm}="${e}"],link[${Wm}="${e}"]`);if(r)for(let s of r)s.removeAttribute(Wm),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function qm(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var Ym=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,zC(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,qx);i?.forEach(r=>this.addUsage(r,this.external,qm))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&($x(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])$x(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,qx(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,qm(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(Ye(cn),Ye(eu),Ye(Ca,8),Ye(Da))};static \u0275prov=Qe({token:n,factory:n.\u0275fac})}return n})(),$m={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Zm=/%COMP%/g;var Yx="%COMP%",GC=`_nghost-${Yx}`,jC=`_ngcontent-${Yx}`,WC=!0,$C=new Pe("",{factory:()=>WC});function qC(n){return jC.replace(Zm,n)}function XC(n){return GC.replace(Zm,n)}function Zx(n,e){return e.map(t=>t.replace(Zm,n))}var Jm=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new za(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Nu?r.applyToHost(t):r instanceof Ga&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case ai.Emulated:s=new Nu(c,l,i,this.appId,u,o,a,f);break;case ai.ShadowDom:return new Ru(c,t,i,o,a,this.nonce,f,l);case ai.ExperimentalIsolatedShadowDom:return new Ru(c,t,i,o,a,this.nonce,f);default:s=new Ga(c,l,i,u,o,a,f);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(Ye(Xm),Ye(Ym),Ye(eu),Ye($C),Ye(cn),Ye(Mn),Ye(Ca),Ye(Ar,8))};static \u0275prov=Qe({token:n,factory:n.\u0275fac})}return n})(),za=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS($m[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(Xx(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(Xx(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new we(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=$m[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=$m[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(Ri.DashCase|Ri.Important)?e.style.setProperty(t,i,r&Ri.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&Ri.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=Rr().getGlobalEventTarget(this.doc,e),!e))throw new we(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function Xx(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Ru=class extends za{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=Zx(i.id,l);for(let f of l){let d=document.createElement("style");o&&d.setAttribute("nonce",o),d.textContent=f,this.shadowRoot.appendChild(d)}let u=i.getExternalStyles?.();if(u)for(let f of u){let d=qm(f,r);o&&d.setAttribute("nonce",o),this.shadowRoot.appendChild(d)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Ga=class extends za{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?Zx(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Ms.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Nu=class extends Ga{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=qC(l),this.hostAttr=XC(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var Ou=class n extends Oa{supportsDOMEvents=!0;static makeCurrent(){Hm(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=YC();return t==null?null:ZC(t)}resetBaseElement(){ja=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return Va(document.cookie,e)}},ja=null;function YC(){return ja=ja||document.head.querySelector("base"),ja?ja.getAttribute("href"):null}function ZC(n){return new URL(n,document.baseURI).pathname}var JC=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:n.\u0275fac})}return n})(),Jx=["alt","control","meta","shift"],KC={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},QC={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},Kx=(()=>{class n extends Ha{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Rr().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),Jx.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=KC[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Jx.forEach(o=>{if(o!==r){let a=QC[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(Ye(cn))};static \u0275prov=Qe({token:n,factory:n.\u0275fac})}return n})();async function Km(n,e,t){let i=Wt({rootComponent:n},eA(e,t));return Cx(i)}function eA(n,e){return{platformRef:e?.platformRef,appProviders:[...sA,...n?.providers??[]],platformProviders:rA}}function tA(){Ou.makeCurrent()}function nA(){return new ni}function iA(){return fm(document),document}var rA=[{provide:Da,useValue:Wx},{provide:tu,useValue:tA,multi:!0},{provide:cn,useFactory:iA}];var sA=[{provide:aa,useValue:"root"},{provide:ni,useFactory:nA},{provide:Pu,useClass:Iu,multi:!0},{provide:Pu,useClass:Kx,multi:!0},Jm,Ym,Xm,{provide:Es,useExisting:Jm},{provide:Ds,useClass:JC},[]];var Pi=class n{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(e){e?typeof e=="string"?this.lazyInit=()=>{this.headers=new Map,e.split(`
`).forEach(t=>{let i=t.indexOf(":");if(i>0){let r=t.slice(0,i),s=t.slice(i+1).trim();this.addHeaderEntry(r,s)}})}:typeof Headers<"u"&&e instanceof Headers?(this.headers=new Map,e.forEach((t,i)=>{this.addHeaderEntry(i,t)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(e).forEach(([t,i])=>{this.setHeaderEntries(t,i)})}:this.headers=new Map}has(e){return this.init(),this.headers.has(e.toLowerCase())}get(e){this.init();let t=this.headers.get(e.toLowerCase());return t&&t.length>0?t[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(e){return this.init(),this.headers.get(e.toLowerCase())||null}append(e,t){return this.clone({name:e,value:t,op:"a"})}set(e,t){return this.clone({name:e,value:t,op:"s"})}delete(e,t){return this.clone({name:e,value:t,op:"d"})}maybeSetNormalizedName(e,t){this.normalizedNames.has(t)||this.normalizedNames.set(t,e)}init(){this.lazyInit&&(this.lazyInit instanceof n?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(e=>this.applyUpdate(e)),this.lazyUpdate=null))}copyFrom(e){e.init(),Array.from(e.headers.keys()).forEach(t=>{this.headers.set(t,e.headers.get(t)),this.normalizedNames.set(t,e.normalizedNames.get(t))})}clone(e){let t=new n;return t.lazyInit=this.lazyInit&&this.lazyInit instanceof n?this.lazyInit:this,t.lazyUpdate=(this.lazyUpdate||[]).concat([e]),t}applyUpdate(e){let t=e.name.toLowerCase();switch(e.op){case"a":case"s":let i=e.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(e.name,t);let r=(e.op==="a"?this.headers.get(t):void 0)||[];r.push(...i),this.headers.set(t,r);break;case"d":let s=e.value;if(!s)this.headers.delete(t),this.normalizedNames.delete(t);else{let o=this.headers.get(t);if(!o)return;o=o.filter(a=>s.indexOf(a)===-1),o.length===0?(this.headers.delete(t),this.normalizedNames.delete(t)):this.headers.set(t,o)}break}}addHeaderEntry(e,t){let i=e.toLowerCase();this.maybeSetNormalizedName(e,i),this.headers.has(i)?this.headers.get(i).push(t):this.headers.set(i,[t])}setHeaderEntries(e,t){let i=(Array.isArray(t)?t:[t]).map(s=>s.toString()),r=e.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(e,r)}forEach(e){this.init(),Array.from(this.normalizedNames.keys()).forEach(t=>e(this.normalizedNames.get(t),this.headers.get(t)))}};var Fu=class{map=new Map;set(e,t){return this.map.set(e,t),this}get(e){return this.map.has(e)||this.map.set(e,e.defaultValue()),this.map.get(e)}delete(e){return this.map.delete(e),this}has(e){return this.map.has(e)}keys(){return this.map.keys()}},ku=class{encodeKey(e){return Qx(e)}encodeValue(e){return Qx(e)}decodeKey(e){return decodeURIComponent(e)}decodeValue(e){return decodeURIComponent(e)}};function oA(n,e){let t=new Map;return n.length>0&&n.replace(/^\?/,"").split("&").forEach(r=>{let s=r.indexOf("="),[o,a]=s==-1?[e.decodeKey(r),""]:[e.decodeKey(r.slice(0,s)),e.decodeValue(r.slice(s+1))],c=t.get(o)||[];c.push(a),t.set(o,c)}),t}var aA=/%(\d[a-f0-9])/gi,cA={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function Qx(n){return encodeURIComponent(n).replace(aA,(e,t)=>cA[t]??e)}function Lu(n){return`${n}`}var or=class n{map;encoder;updates=null;cloneFrom=null;constructor(e={}){if(this.encoder=e.encoder||new ku,e.fromString){if(e.fromObject)throw new we(2805,!1);this.map=oA(e.fromString,this.encoder)}else e.fromObject?(this.map=new Map,Object.keys(e.fromObject).forEach(t=>{let i=e.fromObject[t],r=Array.isArray(i)?i.map(Lu):[Lu(i)];this.map.set(t,r)})):this.map=null}has(e){return this.init(),this.map.has(e)}get(e){this.init();let t=this.map.get(e);return t?t[0]:null}getAll(e){return this.init(),this.map.get(e)||null}keys(){return this.init(),Array.from(this.map.keys())}append(e,t){return this.clone({param:e,value:t,op:"a"})}appendAll(e){let t=[];return Object.keys(e).forEach(i=>{let r=e[i];Array.isArray(r)?r.forEach(s=>{t.push({param:i,value:s,op:"a"})}):t.push({param:i,value:r,op:"a"})}),this.clone(t)}set(e,t){return this.clone({param:e,value:t,op:"s"})}delete(e,t){return this.clone({param:e,value:t,op:"d"})}toString(){return this.init(),this.keys().map(e=>{let t=this.encoder.encodeKey(e);return this.map.get(e).map(i=>t+"="+this.encoder.encodeValue(i)).join("&")}).filter(e=>e!=="").join("&")}clone(e){let t=new n({encoder:this.encoder});return t.cloneFrom=this.cloneFrom||this,t.updates=(this.updates||[]).concat(e),t}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(e=>this.map.set(e,this.cloneFrom.map.get(e))),this.updates.forEach(e=>{switch(e.op){case"a":case"s":let t=(e.op==="a"?this.map.get(e.param):void 0)||[];t.push(Lu(e.value)),this.map.set(e.param,t);break;case"d":if(e.value!==void 0){let i=this.map.get(e.param)||[],r=i.indexOf(Lu(e.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(e.param,i):this.map.delete(e.param)}else{this.map.delete(e.param);break}}}),this.cloneFrom=this.updates=null)}};function lA(n){switch(n){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function eM(n){return typeof ArrayBuffer<"u"&&n instanceof ArrayBuffer}function tM(n){return typeof Blob<"u"&&n instanceof Blob}function nM(n){return typeof FormData<"u"&&n instanceof FormData}function uA(n){return typeof URLSearchParams<"u"&&n instanceof URLSearchParams}var iM="Content-Type",rM="Accept",sM="text/plain",oM="application/json",dA=`${oM}, ${sM}, */*`,xo=class n{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(e,t,i,r){this.url=t,this.method=e.toUpperCase();let s;if(lA(this.method)||r?(this.body=i!==void 0?i:null,s=r):s=i,s){if(this.reportProgress=!!s.reportProgress,this.withCredentials=!!s.withCredentials,this.keepalive=!!s.keepalive,s.responseType&&(this.responseType=s.responseType),s.headers&&(this.headers=s.headers),s.context&&(this.context=s.context),s.params&&(this.params=s.params),s.priority&&(this.priority=s.priority),s.cache&&(this.cache=s.cache),s.credentials&&(this.credentials=s.credentials),typeof s.timeout=="number"){if(s.timeout<1||!Number.isInteger(s.timeout))throw new we(2822,"");this.timeout=s.timeout}s.mode&&(this.mode=s.mode),s.redirect&&(this.redirect=s.redirect),s.integrity&&(this.integrity=s.integrity),s.referrer&&(this.referrer=s.referrer),s.referrerPolicy&&(this.referrerPolicy=s.referrerPolicy),this.transferCache=s.transferCache}if(this.headers??=new Pi,this.context??=new Fu,!this.params)this.params=new or,this.urlWithParams=t;else{let o=this.params.toString();if(o.length===0)this.urlWithParams=t;else{let a=t.indexOf("?"),c=a===-1?"?":a<t.length-1?"&":"";this.urlWithParams=t+c+o}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||eM(this.body)||tM(this.body)||nM(this.body)||uA(this.body)?this.body:this.body instanceof or?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||nM(this.body)?null:tM(this.body)?this.body.type||null:eM(this.body)?null:typeof this.body=="string"?sM:this.body instanceof or?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?oM:null}clone(e={}){let t=e.method||this.method,i=e.url||this.url,r=e.responseType||this.responseType,s=e.keepalive??this.keepalive,o=e.priority||this.priority,a=e.cache||this.cache,c=e.mode||this.mode,l=e.redirect||this.redirect,u=e.credentials||this.credentials,f=e.referrer||this.referrer,d=e.integrity||this.integrity,h=e.referrerPolicy||this.referrerPolicy,g=e.transferCache??this.transferCache,x=e.timeout??this.timeout,m=e.body!==void 0?e.body:this.body,p=e.withCredentials??this.withCredentials,E=e.reportProgress??this.reportProgress,S=e.headers||this.headers,b=e.params||this.params,I=e.context??this.context;return e.setHeaders!==void 0&&(S=Object.keys(e.setHeaders).reduce((w,C)=>w.set(C,e.setHeaders[C]),S)),e.setParams&&(b=Object.keys(e.setParams).reduce((w,C)=>w.set(C,e.setParams[C]),b)),new n(t,i,m,{params:b,headers:S,context:I,reportProgress:E,responseType:r,withCredentials:p,transferCache:g,keepalive:s,cache:a,priority:o,timeout:x,mode:c,redirect:l,credentials:u,referrer:f,integrity:d,referrerPolicy:h})}},Cs=(function(n){return n[n.Sent=0]="Sent",n[n.UploadProgress=1]="UploadProgress",n[n.ResponseHeader=2]="ResponseHeader",n[n.DownloadProgress=3]="DownloadProgress",n[n.Response=4]="Response",n[n.User=5]="User",n})(Cs||{}),Eo=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(e,t=200,i="OK"){this.headers=e.headers||new Pi,this.status=e.status!==void 0?e.status:t,this.statusText=e.statusText||i,this.url=e.url||null,this.redirected=e.redirected,this.responseType=e.responseType,this.ok=this.status>=200&&this.status<300}},Uu=class n extends Eo{constructor(e={}){super(e)}type=Cs.ResponseHeader;clone(e={}){return new n({headers:e.headers||this.headers,status:e.status!==void 0?e.status:this.status,statusText:e.statusText||this.statusText,url:e.url||this.url||void 0})}},Wa=class n extends Eo{body;constructor(e={}){super(e),this.body=e.body!==void 0?e.body:null}type=Cs.Response;clone(e={}){return new n({body:e.body!==void 0?e.body:this.body,headers:e.headers||this.headers,status:e.status!==void 0?e.status:this.status,statusText:e.statusText||this.statusText,url:e.url||this.url||void 0,redirected:e.redirected??this.redirected,responseType:e.responseType??this.responseType})}},Mo=class extends Eo{name="HttpErrorResponse";message;error;ok=!1;constructor(e){super(e,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${e.url||"(unknown url)"}`:this.message=`Http failure response for ${e.url||"(unknown url)"}: ${e.status} ${e.statusText}`,this.error=e.error||null}},fA=200,hA=204;var pA=new Pe("");var mA=/^\)\]\}',?\n/;var eg=(()=>{class n{xhrFactory;tracingService=Ee(Ar,{optional:!0});constructor(t){this.xhrFactory=t}maybePropagateTrace(t){return this.tracingService?.propagate?this.tracingService.propagate(t):t}handle(t){if(t.method==="JSONP")throw new we(-2800,!1);let i=this.xhrFactory;return Xs(null).pipe(_h(()=>new bt(s=>{let o=i.build();if(o.open(t.method,t.urlWithParams),t.withCredentials&&(o.withCredentials=!0),t.headers.forEach((m,p)=>o.setRequestHeader(m,p.join(","))),t.headers.has(rM)||o.setRequestHeader(rM,dA),!t.headers.has(iM)){let m=t.detectContentTypeHeader();m!==null&&o.setRequestHeader(iM,m)}if(t.timeout&&(o.timeout=t.timeout),t.responseType){let m=t.responseType.toLowerCase();o.responseType=m!=="json"?m:"text"}let a=t.serializeBody(),c=null,l=()=>{if(c!==null)return c;let m=o.statusText||"OK",p=new Pi(o.getAllResponseHeaders()),E=o.responseURL||t.url;return c=new Uu({headers:p,status:o.status,statusText:m,url:E}),c},u=this.maybePropagateTrace(()=>{let{headers:m,status:p,statusText:E,url:S}=l(),b=null;p!==hA&&(b=typeof o.response>"u"?o.responseText:o.response),p===0&&(p=b?fA:0);let I=p>=200&&p<300;if(t.responseType==="json"&&typeof b=="string"){let w=b;b=b.replace(mA,"");try{b=b!==""?JSON.parse(b):null}catch(C){b=w,I&&(I=!1,b={error:C,text:b})}}I?(s.next(new Wa({body:b,headers:m,status:p,statusText:E,url:S||void 0})),s.complete()):s.error(new Mo({error:b,headers:m,status:p,statusText:E,url:S||void 0}))}),f=this.maybePropagateTrace(m=>{let{url:p}=l(),E=new Mo({error:m,status:o.status||0,statusText:o.statusText||"Unknown Error",url:p||void 0});s.error(E)}),d=f;t.timeout&&(d=this.maybePropagateTrace(m=>{let{url:p}=l(),E=new Mo({error:new DOMException("Request timed out","TimeoutError"),status:o.status||0,statusText:o.statusText||"Request timeout",url:p||void 0});s.error(E)}));let h=!1,g=this.maybePropagateTrace(m=>{h||(s.next(l()),h=!0);let p={type:Cs.DownloadProgress,loaded:m.loaded};m.lengthComputable&&(p.total=m.total),t.responseType==="text"&&o.responseText&&(p.partialText=o.responseText),s.next(p)}),x=this.maybePropagateTrace(m=>{let p={type:Cs.UploadProgress,loaded:m.loaded};m.lengthComputable&&(p.total=m.total),s.next(p)});return o.addEventListener("load",u),o.addEventListener("error",f),o.addEventListener("timeout",d),o.addEventListener("abort",f),t.reportProgress&&(o.addEventListener("progress",g),a!==null&&o.upload&&o.upload.addEventListener("progress",x)),o.send(a),s.next({type:Cs.Sent}),()=>{o.removeEventListener("error",f),o.removeEventListener("abort",f),o.removeEventListener("load",u),o.removeEventListener("timeout",d),t.reportProgress&&(o.removeEventListener("progress",g),a!==null&&o.upload&&o.upload.removeEventListener("progress",x)),o.readyState!==o.DONE&&o.abort()}})))}static \u0275fac=function(i){return new(i||n)(Ye(Ds))};static \u0275prov=Qe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function gA(n,e){return e(n)}function yA(n,e,t){return(i,r)=>eo(t,()=>e(i,s=>n(s,r)))}var aM=new Pe("",{factory:()=>[]}),cM=new Pe(""),lM=new Pe("",{factory:()=>!0});var tg=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=Ye(eg),r},providedIn:"root"})}return n})();var Bu=(()=>{class n{backend;injector;chain=null;pendingTasks=Ee(pa);contributeToStability=Ee(lM);constructor(t,i){this.backend=t,this.injector=i}handle(t){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(aM),...this.injector.get(cM,[])]));this.chain=i.reduceRight((r,s)=>yA(r,s,this.injector),gA)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(t,r=>this.backend.handle(r)).pipe(vh(i))}else return this.chain(t,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||n)(Ye(tg),Ye(xn))};static \u0275prov=Qe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),ng=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=Ye(Bu),r},providedIn:"root"})}return n})();function Qm(n,e){return{body:e,headers:n.headers,context:n.context,observe:n.observe,params:n.params,reportProgress:n.reportProgress,responseType:n.responseType,withCredentials:n.withCredentials,credentials:n.credentials,transferCache:n.transferCache,timeout:n.timeout,keepalive:n.keepalive,priority:n.priority,cache:n.cache,mode:n.mode,redirect:n.redirect,integrity:n.integrity,referrer:n.referrer,referrerPolicy:n.referrerPolicy}}var Vu=(()=>{class n{handler;constructor(t){this.handler=t}request(t,i,r={}){let s;if(t instanceof xo)s=t;else{let c;r.headers instanceof Pi?c=r.headers:c=new Pi(r.headers);let l;r.params&&(r.params instanceof or?l=r.params:l=new or({fromObject:r.params})),s=new xo(t,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let o=Xs(s).pipe(yh(c=>this.handler.handle(c)));if(t instanceof xo||r.observe==="events")return o;let a=o.pipe(gh(c=>c instanceof Wa));switch(r.observe||"body"){case"body":switch(s.responseType){case"arraybuffer":return a.pipe(vn(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new we(2806,!1);return c.body}));case"blob":return a.pipe(vn(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new we(2807,!1);return c.body}));case"text":return a.pipe(vn(c=>{if(c.body!==null&&typeof c.body!="string")throw new we(2808,!1);return c.body}));default:return a.pipe(vn(c=>c.body))}case"response":return a;default:throw new we(2809,!1)}}delete(t,i={}){return this.request("DELETE",t,i)}get(t,i={}){return this.request("GET",t,i)}head(t,i={}){return this.request("HEAD",t,i)}jsonp(t,i){return this.request("JSONP",t,{params:new or().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(t,i={}){return this.request("OPTIONS",t,i)}patch(t,i,r={}){return this.request("PATCH",t,Qm(r,i))}post(t,i,r={}){return this.request("POST",t,Qm(r,i))}put(t,i,r={}){return this.request("PUT",t,Qm(r,i))}static \u0275fac=function(i){return new(i||n)(Ye(ng))};static \u0275prov=Qe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var vA=new Pe("",{factory:()=>!0}),_A="XSRF-TOKEN",xA=new Pe("",{factory:()=>_A}),MA="X-XSRF-TOKEN",EA=new Pe("",{factory:()=>MA}),bA=(()=>{class n{cookieName=Ee(xA);doc=Ee(cn);lastCookieString="";lastToken=null;parseCount=0;getToken(){let t=this.doc.cookie||"";return t!==this.lastCookieString&&(this.parseCount++,this.lastToken=Va(t,this.cookieName),this.lastCookieString=t),this.lastToken}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),uM=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Qe({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=Ye(bA),r},providedIn:"root"})}return n})();function SA(n,e){if(!Ee(vA)||n.method==="GET"||n.method==="HEAD")return e(n);try{let r=Ee(bu).href,{origin:s}=new URL(r),{origin:o}=new URL(n.url,s);if(s!==o)return e(n)}catch{return e(n)}let t=Ee(uM).getToken(),i=Ee(EA);return t!=null&&!n.headers.has(i)&&(n=n.clone({headers:n.headers.set(i,t)})),e(n)}function ig(...n){let e=[Vu,Bu,{provide:ng,useExisting:Bu},{provide:tg,useFactory:()=>Ee(pA,{optional:!0})??Ee(eg)},{provide:aM,useValue:SA,multi:!0}];for(let t of n)e.push(...t.\u0275providers);return Qs(e)}var dM={providers:[bp(),ig()]};var Hu=class n{http=Ee(Vu);api="https://api.github.com";headers=new Pi({Accept:"application/vnd.github.v3+json"});parseRepo(e){let t=e.trim().replace(/\/$/,"").replace(/\.git$/,""),i=t.match(/github\.com\/([^/\s]+)\/([^/\s]+)/);if(i)return{owner:i[1],name:i[2]};let r=t.match(/^([^/\s]+)\/([^/\s]+)$/);return r?{owner:r[1],name:r[2]}:null}getCommits(e,t){let i={headers:this.headers},r=a=>`${this.api}/repos/${e}/${t}/commits?per_page=100&page=${a}`,s=this.http.get(r(1),i),o=this.http.get(r(2),i).pipe(ta(()=>Xs([])));return mh([s,o]).pipe(vn(([a,c])=>[...a,...c].map(l=>this.toCommit(l))),ta(a=>ph(()=>a)))}toCommit(e){return{sha:e.sha,shortSha:e.sha.slice(0,7),message:e.commit.message.split(`
`)[0].slice(0,80),author:e.commit.author?.name??"Unknown",authorLogin:e.author?.login??null,avatarUrl:e.author?.avatar_url??null,date:new Date(e.commit.author?.date??e.commit.committer?.date),url:e.html_url}}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Qe({token:n,factory:n.\u0275fac,providedIn:"root"})};var wA=(n,e)=>e.id;function TA(n,e){if(n&1&&Ni(0,"span",24),n&2){let t=e.$implicit;km(t.style)}}function DA(n,e){if(n&1){let t=Ir();St(0,"button",25),Ts("click",function(){Di(t);let r=un();return Ci(r.clear())}),Yi(),St(1,"svg",26),Ni(2,"path",27),Nt()()}}function CA(n,e){if(n&1&&(St(0,"p",18),ot(1),Nt()),n&2){let t=un();yt(),ci(t.errorMsg())}}function AA(n,e){if(n&1){let t=Ir();St(0,"button",28),Ts("click",function(){let r=Di(t).$implicit,s=un();return Ci(s.useExample(r))}),ot(1),Nt()}if(n&2){let t=e.$implicit;yt(),ci(t)}}var zu=class n{search=Eu();inputValue=ln("");errorMsg=ln("");examples=["torvalds/linux","angular/angular","microsoft/vscode"];decorativeStars=Array.from({length:60},(e,t)=>({id:t,style:this.randomStarStyle()}));randomStarStyle(){let e=Math.random()*2+1,t=Math.random()*100,i=Math.random()*100,r=Math.random()*4,s=Math.random()*3+2,o=Math.random()*.6+.1;return`left:${t}%;top:${i}%;width:${e}px;height:${e}px;animation-delay:${r}s;animation-duration:${s}s;opacity:${o}`}onInput(e){this.inputValue.set(e.target.value),this.errorMsg()&&this.errorMsg.set("")}useExample(e){this.inputValue.set(e),this.errorMsg.set("")}clear(){this.inputValue.set(""),this.errorMsg.set("")}onSubmit(e){e.preventDefault();let t=this.inputValue().trim();if(!t)return;if(!(/^[^/\s]+\/[^/\s]+$/.test(t)||t.includes("github.com"))){this.errorMsg.set("Escribe algo como facebook/react o pega la URL del repo.");return}this.search.emit(t)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Ss({type:n,selectors:[["app-search"]],outputs:{search:"search"},decls:35,vars:6,consts:[[1,"search"],["aria-hidden","true",1,"stars"],[1,"star",3,"style"],[1,"search__inner"],[1,"search__header"],[1,"search__logo"],["width","28","height","28","viewBox","0 0 28 28","fill","none","aria-hidden","true"],["cx","14","cy","14","r","2.5","fill","white"],["cx","5","cy","6","r","1.2","fill","white","opacity","0.6"],["cx","23","cy","8","r","1.5","fill","white","opacity","0.8"],["cx","22","cy","22","r","1","fill","white","opacity","0.5"],["cx","7","cy","21","r","1.8","fill","white","opacity","0.7"],["cx","14","cy","3","r","0.8","fill","white","opacity","0.4"],[1,"search__tagline"],[1,"search__form",3,"submit"],[1,"search__field"],["type","text","placeholder","facebook/react","autocomplete","off","autocorrect","off","spellcheck","false","aria-label","Repositorio de GitHub",1,"search__input",3,"input","value"],["type","button","aria-label","Limpiar",1,"search__clear"],["role","alert",1,"search__error"],["type","submit",1,"search__submit",3,"disabled"],["width","16","height","16","viewBox","0 0 16 16","fill","none","aria-hidden","true"],["d","M3 8h10M9 4l4 4-4 4","stroke","currentColor","stroke-width","1.5","stroke-linecap","round","stroke-linejoin","round"],[1,"search__examples"],[1,"search__example"],[1,"star"],["type","button","aria-label","Limpiar",1,"search__clear",3,"click"],["width","12","height","12","viewBox","0 0 12 12","fill","none"],["d","M1 1l10 10M11 1L1 11","stroke","currentColor","stroke-width","1.5","stroke-linecap","round"],[1,"search__example",3,"click"]],template:function(t,i){t&1&&(St(0,"div",0)(1,"div",1),hu(2,TA,1,2,"span",2,wA),Nt(),St(4,"div",3)(5,"header",4)(6,"div",5),Yi(),St(7,"svg",6),Ni(8,"circle",7)(9,"circle",8)(10,"circle",9)(11,"circle",10)(12,"circle",11)(13,"circle",12),Nt(),Dr(),St(14,"span"),ot(15,"Stellar"),Nt()(),St(16,"p",13),ot(17," Cada commit, una estrella."),Ni(18,"br"),St(19,"em"),ot(20,"Cada repositorio, un universo."),Nt()()(),St(21,"form",14),Ts("submit",function(s){return i.onSubmit(s)}),St(22,"div",15)(23,"input",16),Ts("input",function(s){return i.onInput(s)}),Nt(),Qi(24,DA,3,0,"button",17),Nt(),Qi(25,CA,2,1,"p",18),St(26,"button",19),ot(27," Explorar universo "),Yi(),St(28,"svg",20),Ni(29,"path",21),Nt()()(),Dr(),St(30,"div",22)(31,"span"),ot(32,"Prueba con"),Nt(),hu(33,AA,2,1,"button",23,Om),Nt()()()),t&2&&(yt(2),pu(i.decorativeStars),yt(20),yo("search__field--error",i.errorMsg()),yt(),Ia("value",i.inputValue()),yt(),er(i.inputValue().length>0?24:-1),yt(),er(i.errorMsg()?25:-1),yt(),Ia("disabled",!i.inputValue().trim()),yt(7),pu(i.examples))},styles:[".search[_ngcontent-%COMP%]{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;overflow:hidden}.stars[_ngcontent-%COMP%]{position:absolute;inset:0;pointer-events:none}.star[_ngcontent-%COMP%]{position:absolute;border-radius:50%;background:#fff;animation:_ngcontent-%COMP%_twinkle linear infinite}@keyframes _ngcontent-%COMP%_twinkle{0%,to{opacity:var(--base-opacity, .3);transform:scale(1)}50%{opacity:.05;transform:scale(.5)}}.search__inner[_ngcontent-%COMP%]{position:relative;z-index:1;width:100%;max-width:420px;padding:0 24px;display:flex;flex-direction:column;gap:40px}.search__header[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.search__logo[_ngcontent-%COMP%]{display:flex;align-items:center;gap:10px;font-family:var(--font-display);font-size:28px;letter-spacing:-.5px}.search__tagline[_ngcontent-%COMP%]{font-size:15px;line-height:1.6;color:var(--muted)}.search__tagline[_ngcontent-%COMP%]   em[_ngcontent-%COMP%]{font-family:var(--font-display);font-style:italic;color:#fff9}.search__form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:12px}.search__field[_ngcontent-%COMP%]{position:relative;display:flex;align-items:center;border:1px solid var(--stroke);border-radius:10px;background:var(--surface);transition:border-color .2s}.search__field[_ngcontent-%COMP%]:focus-within{border-color:#a78bfa80;box-shadow:0 0 0 3px #a78bfa14}.search__field--error[_ngcontent-%COMP%]{border-color:#f8717180}.search__field--error[_ngcontent-%COMP%]:focus-within{border-color:#f8717180;box-shadow:0 0 0 3px #f8717114}.search__input[_ngcontent-%COMP%]{width:100%;padding:14px 16px;background:none;border:none;outline:none;color:var(--white);font-size:14px;letter-spacing:.01em}.search__input[_ngcontent-%COMP%]::placeholder{color:var(--muted);opacity:.6}.search__clear[_ngcontent-%COMP%]{padding:0 14px;color:var(--muted);display:flex;align-items:center;flex-shrink:0;transition:color .15s}.search__clear[_ngcontent-%COMP%]:hover{color:var(--white)}.search__error[_ngcontent-%COMP%]{font-size:12px;color:#f87171;padding-left:2px}.search__submit[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:center;gap:8px;padding:14px;border-radius:10px;background:var(--white);color:var(--bg);font-size:14px;font-weight:500;letter-spacing:.01em;transition:opacity .15s,transform .15s}.search__submit[_ngcontent-%COMP%]:hover:not(:disabled){opacity:.9;transform:translateY(-1px)}.search__submit[_ngcontent-%COMP%]:disabled{opacity:.25;cursor:not-allowed}.search__examples[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.search__examples[_ngcontent-%COMP%] > span[_ngcontent-%COMP%]{font-size:12px;color:var(--muted)}.search__example[_ngcontent-%COMP%]{font-size:12px;color:var(--muted);padding:4px 10px;border:1px solid var(--stroke);border-radius:20px;transition:color .15s,border-color .15s}.search__example[_ngcontent-%COMP%]:hover{color:var(--white);border-color:#fff3}"]})};var $r={LEFT:0,MIDDLE:1,RIGHT:2,ROTATE:0,DOLLY:1,PAN:2},qr={ROTATE:0,PAN:1,DOLLY_PAN:2,DOLLY_ROTATE:3},LM=0,Fg=1,FM=2;var _c=1,kM=2,Wo=3,hr=0,Sn=1,ki=2,Ui=0,Ls=1,kg=2,Ug=3,Bg=4,UM=5;var Br=100,BM=101,VM=102,HM=103,zM=104,GM=200,jM=201,WM=202,$M=203,fd=204,hd=205,qM=206,XM=207,YM=208,ZM=209,JM=210,KM=211,QM=212,eE=213,tE=214,pd=0,md=1,gd=2,Fs=3,yd=4,vd=5,_d=6,xd=7,Vg=0,nE=1,iE=2,mi=0,Hg=1,zg=2,Gg=3,jg=4,Wg=5,$g=6,qg=7;var wg=300,Xr=301,Us=302,jd=303,Wd=304,xc=306,Md=1e3,Li=1001,Ed=1002,Qt=1003,rE=1004;var Mc=1005;var en=1006,$d=1007;var Yr=1008;var Hn=1009,Xg=1010,Yg=1011,$o=1012,qd=1013,gi=1014,yi=1015,Bi=1016,Xd=1017,Yd=1018,qo=1020,Zg=35902,Jg=35899,Kg=1021,Qg=1022,ei=1023,Fi=1026,Zr=1027,ey=1028,Zd=1029,Jr=1030,Jd=1031;var Kd=1033,Ec=33776,bc=33777,Sc=33778,wc=33779,Qd=35840,ef=35841,tf=35842,nf=35843,rf=36196,sf=37492,of=37496,af=37488,cf=37489,Tc=37490,lf=37491,uf=37808,df=37809,ff=37810,hf=37811,pf=37812,mf=37813,gf=37814,yf=37815,vf=37816,_f=37817,xf=37818,Mf=37819,Ef=37820,bf=37821,Sf=36492,wf=36494,Tf=36495,Df=36283,Cf=36284,Dc=36285,Af=36286;var Ka=2300,bd=2301,dd=2302,Tg=2303,Dg=2400,Cg=2401,Ag=2402;var sE=3200;var ty=0,oE=1,mr="",kn="srgb",Qa="srgb-linear",ec="linear",st="srgb";var Ps=7680;var Ig=519,aE=512,cE=513,lE=514,If=515,uE=516,dE=517,Rf=518,fE=519,Rg=35044;var ny="300 es",hi=2e3,tc=2001;function IA(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function RA(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function nc(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function hE(){let n=nc("canvas");return n.style.display="block",n}var fM={},ko=null;function iy(...n){let e="THREE."+n.shift();ko?ko("log",e,...n):console.log(e,...n)}function pE(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Te(...n){n=pE(n);let e="THREE."+n.shift();if(ko)ko("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Ce(...n){n=pE(n);let e="THREE."+n.shift();if(ko)ko("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function Sd(...n){let e=n.join(" ");e in fM||(fM[e]=!0,Te(...n))}function mE(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var gE={[pd]:md,[gd]:_d,[yd]:xd,[Fs]:vd,[md]:pd,[_d]:gd,[xd]:yd,[vd]:Fs},pi=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},fn=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"],hM=1234567,Za=Math.PI/180,Uo=180/Math.PI;function Xo(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(fn[n&255]+fn[n>>8&255]+fn[n>>16&255]+fn[n>>24&255]+"-"+fn[e&255]+fn[e>>8&255]+"-"+fn[e>>16&15|64]+fn[e>>24&255]+"-"+fn[t&63|128]+fn[t>>8&255]+"-"+fn[t>>16&255]+fn[t>>24&255]+fn[i&255]+fn[i>>8&255]+fn[i>>16&255]+fn[i>>24&255]).toLowerCase()}function qe(n,e,t){return Math.max(e,Math.min(t,n))}function ry(n,e){return(n%e+e)%e}function NA(n,e,t,i,r){return i+(n-e)*(r-i)/(t-e)}function PA(n,e,t){return n!==e?(t-n)/(e-n):0}function Ja(n,e,t){return(1-t)*n+t*e}function OA(n,e,t,i){return Ja(n,e,1-Math.exp(-t*i))}function LA(n,e=1){return e-Math.abs(ry(n,e*2)-e)}function FA(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*(3-2*n))}function kA(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*n*(n*(n*6-15)+10))}function UA(n,e){return n+Math.floor(Math.random()*(e-n+1))}function BA(n,e){return n+Math.random()*(e-n)}function VA(n){return n*(.5-Math.random())}function HA(n){n!==void 0&&(hM=n);let e=hM+=1831565813;return e=Math.imul(e^e>>>15,e|1),e^=e+Math.imul(e^e>>>7,e|61),((e^e>>>14)>>>0)/4294967296}function zA(n){return n*Za}function GA(n){return n*Uo}function jA(n){return(n&n-1)===0&&n!==0}function WA(n){return Math.pow(2,Math.ceil(Math.log(n)/Math.LN2))}function $A(n){return Math.pow(2,Math.floor(Math.log(n)/Math.LN2))}function qA(n,e,t,i,r){let s=Math.cos,o=Math.sin,a=s(t/2),c=o(t/2),l=s((e+i)/2),u=o((e+i)/2),f=s((e-i)/2),d=o((e-i)/2),h=s((i-e)/2),g=o((i-e)/2);switch(r){case"XYX":n.set(a*u,c*f,c*d,a*l);break;case"YZY":n.set(c*d,a*u,c*f,a*l);break;case"ZXZ":n.set(c*f,c*d,a*u,a*l);break;case"XZX":n.set(a*u,c*g,c*h,a*l);break;case"YXY":n.set(c*h,a*u,c*g,a*l);break;case"ZYZ":n.set(c*g,c*h,a*u,a*l);break;default:Te("MathUtils: .setQuaternionFromProperEuler() encountered an unknown order: "+r)}}function Lo(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function bn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var sy={DEG2RAD:Za,RAD2DEG:Uo,generateUUID:Xo,clamp:qe,euclideanModulo:ry,mapLinear:NA,inverseLerp:PA,lerp:Ja,damp:OA,pingpong:LA,smoothstep:FA,smootherstep:kA,randInt:UA,randFloat:BA,randFloatSpread:VA,seededRandom:HA,degToRad:zA,radToDeg:GA,isPowerOfTwo:jA,ceilPowerOfTwo:WA,floorPowerOfTwo:$A,setQuaternionFromProperEuler:qA,normalize:bn,denormalize:Lo},ke=class n{static{n.prototype.isVector2=!0}constructor(e=0,t=0){this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=qe(this.x,e.x,t.x),this.y=qe(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=qe(this.x,e,t),this.y=qe(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(qe(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(qe(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Un=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],f=i[r+3],d=s[o+0],h=s[o+1],g=s[o+2],x=s[o+3];if(f!==x||c!==d||l!==h||u!==g){let m=c*d+l*h+u*g+f*x;m<0&&(d=-d,h=-h,g=-g,x=-x,m=-m);let p=1-a;if(m<.9995){let E=Math.acos(m),S=Math.sin(E);p=Math.sin(p*E)/S,a=Math.sin(a*E)/S,c=c*p+d*a,l=l*p+h*a,u=u*p+g*a,f=f*p+x*a}else{c=c*p+d*a,l=l*p+h*a,u=u*p+g*a,f=f*p+x*a;let E=1/Math.sqrt(c*c+l*l+u*u+f*f);c*=E,l*=E,u*=E,f*=E}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=f}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],f=s[o],d=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*f+c*h-l*d,e[t+1]=c*g+u*d+l*f-a*h,e[t+2]=l*g+u*h+a*d-c*f,e[t+3]=u*g-a*f-c*d-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),f=a(s/2),d=c(i/2),h=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=d*u*f+l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f-d*h*g;break;case"YXZ":this._x=d*u*f+l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f+d*h*g;break;case"ZXY":this._x=d*u*f-l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f-d*h*g;break;case"ZYX":this._x=d*u*f-l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f+d*h*g;break;case"YZX":this._x=d*u*f+l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f-d*h*g;break;case"XZY":this._x=d*u*f-l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f+d*h*g;break;default:Te("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],f=t[10],d=i+a+f;if(d>0){let h=.5/Math.sqrt(d+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>f){let h=2*Math.sqrt(1+i-a-f);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>f){let h=2*Math.sqrt(1+a-i-f);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+f-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(qe(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},F=class n{static{n.prototype.isVector3=!0}constructor(e=0,t=0,i=0){this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(pM.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(pM.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),f=2*(s*i-o*t);return this.x=t+c*l+o*f-a*u,this.y=i+c*u+a*l-s*f,this.z=r+c*f+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=qe(this.x,e.x,t.x),this.y=qe(this.y,e.y,t.y),this.z=qe(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=qe(this.x,e,t),this.y=qe(this.y,e,t),this.z=qe(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(qe(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return rg.copy(this).projectOnVector(e),this.sub(rg)}reflect(e){return this.sub(rg.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(qe(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},rg=new F,pM=new Un,Le=class n{static{n.prototype.isMatrix3=!0}constructor(e,t,i,r,s,o,a,c,l){this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],f=i[7],d=i[2],h=i[5],g=i[8],x=r[0],m=r[3],p=r[6],E=r[1],S=r[4],b=r[7],I=r[2],w=r[5],C=r[8];return s[0]=o*x+a*E+c*I,s[3]=o*m+a*S+c*w,s[6]=o*p+a*b+c*C,s[1]=l*x+u*E+f*I,s[4]=l*m+u*S+f*w,s[7]=l*p+u*b+f*C,s[2]=d*x+h*E+g*I,s[5]=d*m+h*S+g*w,s[8]=d*p+h*b+g*C,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=u*o-a*l,d=a*c-u*s,h=l*s-o*c,g=t*f+i*d+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let x=1/g;return e[0]=f*x,e[1]=(r*l-u*i)*x,e[2]=(a*i-r*o)*x,e[3]=d*x,e[4]=(u*t-r*c)*x,e[5]=(r*s-a*t)*x,e[6]=h*x,e[7]=(i*c-l*t)*x,e[8]=(o*t-i*s)*x,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(sg.makeScale(e,t)),this}rotate(e){return this.premultiply(sg.makeRotation(-e)),this}translate(e,t){return this.premultiply(sg.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},sg=new Le,mM=new Le().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),gM=new Le().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function XA(){let n={enabled:!0,workingColorSpace:Qa,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===st&&(r.r=fr(r.r),r.g=fr(r.g),r.b=fr(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===st&&(r.r=Fo(r.r),r.g=Fo(r.g),r.b=Fo(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===mr?ec:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return Sd("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return Sd("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[Qa]:{primaries:e,whitePoint:i,transfer:ec,toXYZ:mM,fromXYZ:gM,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:kn},outputColorSpaceConfig:{drawingBufferColorSpace:kn}},[kn]:{primaries:e,whitePoint:i,transfer:st,toXYZ:mM,fromXYZ:gM,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:kn}}}),n}var Ke=XA();function fr(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function Fo(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var bo,wd=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{bo===void 0&&(bo=nc("canvas")),bo.width=e.width,bo.height=e.height;let r=bo.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=bo}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=nc("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=fr(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(fr(t[i]/255)*255):t[i]=fr(t[i]);return{data:t,width:e.width,height:e.height}}else return Te("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},YA=0,Bo=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:YA++}),this.uuid=Xo(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayWidth,t.displayHeight,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(og(r[o].image)):s.push(og(r[o]))}else s=og(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function og(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?wd.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Te("Texture: Unable to serialize Texture."),{})}var ZA=0,ag=new F,gr=(()=>{class n extends pi{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Li,s=Li,o=en,a=Yr,c=ei,l=Hn,u=n.DEFAULT_ANISOTROPY,f=mr){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:ZA++}),this.uuid=Xo(),this.name="",this.source=new Bo(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new ke(0,0),this.repeat=new ke(1,1),this.center=new ke(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Le,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=f,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0,this.normalized=!1}get width(){return this.source.getSize(ag).x}get height(){return this.source.getSize(ag).y}get depth(){return this.source.getSize(ag).z}get image(){return this.source.data}set image(t){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.normalized=t.normalized,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){Te(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){Te(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,normalized:this.normalized,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==wg)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case Md:t.x=t.x-Math.floor(t.x);break;case Li:t.x=t.x<0?0:1;break;case Ed:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case Md:t.y=t.y-Math.floor(t.y);break;case Li:t.y=t.y<0?0:1;break;case Ed:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=wg,n.DEFAULT_ANISOTROPY=1,n})(),Lt=class n{static{n.prototype.isVector4=!0}constructor(e=0,t=0,i=0,r=1){this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],f=c[8],d=c[1],h=c[5],g=c[9],x=c[2],m=c[6],p=c[10];if(Math.abs(u-d)<.01&&Math.abs(f-x)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+d)<.1&&Math.abs(f+x)<.1&&Math.abs(g+m)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let S=(l+1)/2,b=(h+1)/2,I=(p+1)/2,w=(u+d)/4,C=(f+x)/4,v=(g+m)/4;return S>b&&S>I?S<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(S),r=w/i,s=C/i):b>I?b<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(b),i=w/r,s=v/r):I<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(I),i=C/s,r=v/s),this.set(i,r,s,t),this}let E=Math.sqrt((m-g)*(m-g)+(f-x)*(f-x)+(d-u)*(d-u));return Math.abs(E)<.001&&(E=1),this.x=(m-g)/E,this.y=(f-x)/E,this.z=(d-u)/E,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=qe(this.x,e.x,t.x),this.y=qe(this.y,e.y,t.y),this.z=qe(this.z,e.z,t.z),this.w=qe(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=qe(this.x,e,t),this.y=qe(this.y,e,t),this.z=qe(this.z,e,t),this.w=qe(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(qe(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},Td=class extends pi{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:en,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new Lt(0,0,e,t),this.scissorTest=!1,this.viewport=new Lt(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new gr(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:en,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new Bo(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this.multiview=e.multiview,this}dispose(){this.dispatchEvent({type:"dispose"})}},Bn=class extends Td{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},ic=class extends gr{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Qt,this.minFilter=Qt,this.wrapR=Li,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Dd=class extends gr{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Qt,this.minFilter=Qt,this.wrapR=Li,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Ot=class n{static{n.prototype.isMatrix4=!0}constructor(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m){this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m)}set(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=f,p[14]=d,p[3]=h,p[7]=g,p[11]=x,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/So.setFromMatrixColumn(e,0).length(),s=1/So.setFromMatrixColumn(e,1).length(),o=1/So.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),f=Math.sin(s);if(e.order==="XYZ"){let d=o*u,h=o*f,g=a*u,x=a*f;t[0]=c*u,t[4]=-c*f,t[8]=l,t[1]=h+g*l,t[5]=d-x*l,t[9]=-a*c,t[2]=x-d*l,t[6]=g+h*l,t[10]=o*c}else if(e.order==="YXZ"){let d=c*u,h=c*f,g=l*u,x=l*f;t[0]=d+x*a,t[4]=g*a-h,t[8]=o*l,t[1]=o*f,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=x+d*a,t[10]=o*c}else if(e.order==="ZXY"){let d=c*u,h=c*f,g=l*u,x=l*f;t[0]=d-x*a,t[4]=-o*f,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=x-d*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let d=o*u,h=o*f,g=a*u,x=a*f;t[0]=c*u,t[4]=g*l-h,t[8]=d*l+x,t[1]=c*f,t[5]=x*l+d,t[9]=h*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let d=o*c,h=o*l,g=a*c,x=a*l;t[0]=c*u,t[4]=x-d*f,t[8]=g*f+h,t[1]=f,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*f+g,t[10]=d-x*f}else if(e.order==="XZY"){let d=o*c,h=o*l,g=a*c,x=a*l;t[0]=c*u,t[4]=-f,t[8]=l*u,t[1]=d*f+x,t[5]=o*u,t[9]=h*f-g,t[2]=g*f-h,t[6]=a*u,t[10]=x*f+d}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(JA,e,KA)}lookAt(e,t,i){let r=this.elements;return Ln.subVectors(e,t),Ln.lengthSq()===0&&(Ln.z=1),Ln.normalize(),Nr.crossVectors(i,Ln),Nr.lengthSq()===0&&(Math.abs(i.z)===1?Ln.x+=1e-4:Ln.z+=1e-4,Ln.normalize(),Nr.crossVectors(i,Ln)),Nr.normalize(),Gu.crossVectors(Ln,Nr),r[0]=Nr.x,r[4]=Gu.x,r[8]=Ln.x,r[1]=Nr.y,r[5]=Gu.y,r[9]=Ln.y,r[2]=Nr.z,r[6]=Gu.z,r[10]=Ln.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],f=i[5],d=i[9],h=i[13],g=i[2],x=i[6],m=i[10],p=i[14],E=i[3],S=i[7],b=i[11],I=i[15],w=r[0],C=r[4],v=r[8],T=r[12],O=r[1],D=r[5],U=r[9],j=r[13],X=r[2],P=r[6],z=r[10],k=r[14],K=r[3],Q=r[7],le=r[11],ve=r[15];return s[0]=o*w+a*O+c*X+l*K,s[4]=o*C+a*D+c*P+l*Q,s[8]=o*v+a*U+c*z+l*le,s[12]=o*T+a*j+c*k+l*ve,s[1]=u*w+f*O+d*X+h*K,s[5]=u*C+f*D+d*P+h*Q,s[9]=u*v+f*U+d*z+h*le,s[13]=u*T+f*j+d*k+h*ve,s[2]=g*w+x*O+m*X+p*K,s[6]=g*C+x*D+m*P+p*Q,s[10]=g*v+x*U+m*z+p*le,s[14]=g*T+x*j+m*k+p*ve,s[3]=E*w+S*O+b*X+I*K,s[7]=E*C+S*D+b*P+I*Q,s[11]=E*v+S*U+b*z+I*le,s[15]=E*T+S*j+b*k+I*ve,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],f=e[6],d=e[10],h=e[14],g=e[3],x=e[7],m=e[11],p=e[15],E=c*h-l*d,S=a*h-l*f,b=a*d-c*f,I=o*h-l*u,w=o*d-c*u,C=o*f-a*u;return t*(x*E-m*S+p*b)-i*(g*E-m*I+p*w)+r*(g*S-x*I+p*C)-s*(g*b-x*w+m*C)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=e[9],d=e[10],h=e[11],g=e[12],x=e[13],m=e[14],p=e[15],E=t*a-i*o,S=t*c-r*o,b=t*l-s*o,I=i*c-r*a,w=i*l-s*a,C=r*l-s*c,v=u*x-f*g,T=u*m-d*g,O=u*p-h*g,D=f*m-d*x,U=f*p-h*x,j=d*p-h*m,X=E*j-S*U+b*D+I*O-w*T+C*v;if(X===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let P=1/X;return e[0]=(a*j-c*U+l*D)*P,e[1]=(r*U-i*j-s*D)*P,e[2]=(x*C-m*w+p*I)*P,e[3]=(d*w-f*C-h*I)*P,e[4]=(c*O-o*j-l*T)*P,e[5]=(t*j-r*O+s*T)*P,e[6]=(m*b-g*C-p*S)*P,e[7]=(u*C-d*b+h*S)*P,e[8]=(o*U-a*O+l*v)*P,e[9]=(i*O-t*U-s*v)*P,e[10]=(g*w-x*b+p*E)*P,e[11]=(f*b-u*w-h*E)*P,e[12]=(a*T-o*D-c*v)*P,e[13]=(t*D-i*T+r*v)*P,e[14]=(x*S-g*I-m*E)*P,e[15]=(u*I-f*S+d*E)*P,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,f=a+a,d=s*l,h=s*u,g=s*f,x=o*u,m=o*f,p=a*f,E=c*l,S=c*u,b=c*f,I=i.x,w=i.y,C=i.z;return r[0]=(1-(x+p))*I,r[1]=(h+b)*I,r[2]=(g-S)*I,r[3]=0,r[4]=(h-b)*w,r[5]=(1-(d+p))*w,r[6]=(m+E)*w,r[7]=0,r[8]=(g+S)*C,r[9]=(m-E)*C,r[10]=(1-(d+x))*C,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinant();if(s===0)return i.set(1,1,1),t.identity(),this;let o=So.set(r[0],r[1],r[2]).length(),a=So.set(r[4],r[5],r[6]).length(),c=So.set(r[8],r[9],r[10]).length();s<0&&(o=-o),ui.copy(this);let l=1/o,u=1/a,f=1/c;return ui.elements[0]*=l,ui.elements[1]*=l,ui.elements[2]*=l,ui.elements[4]*=u,ui.elements[5]*=u,ui.elements[6]*=u,ui.elements[8]*=f,ui.elements[9]*=f,ui.elements[10]*=f,t.setFromRotationMatrix(ui),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=hi,c=!1){let l=this.elements,u=2*s/(t-e),f=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),g,x;if(c)g=s/(o-s),x=o*s/(o-s);else if(a===hi)g=-(o+s)/(o-s),x=-2*o*s/(o-s);else if(a===tc)g=-o/(o-s),x=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=f,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=g,l[14]=x,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=hi,c=!1){let l=this.elements,u=2/(t-e),f=2/(i-r),d=-(t+e)/(t-e),h=-(i+r)/(i-r),g,x;if(c)g=1/(o-s),x=o/(o-s);else if(a===hi)g=-2/(o-s),x=-(o+s)/(o-s);else if(a===tc)g=-1/(o-s),x=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=d,l[1]=0,l[5]=f,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=g,l[14]=x,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},So=new F,ui=new Ot,JA=new F(0,0,0),KA=new F(1,1,1),Nr=new F,Gu=new F,Ln=new F,yM=new Ot,vM=new Un,rc=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],f=s[9],d=s[2],h=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(qe(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-f,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-qe(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-d,o),this._z=0);break;case"ZXY":this._x=Math.asin(qe(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-d,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-qe(d,-1,1)),Math.abs(d)<.9999999?(this._x=Math.atan2(h,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(qe(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-f,u),this._y=Math.atan2(-d,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-qe(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-f,g),this._y=0);break;default:Te("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return yM.makeRotationFromQuaternion(t),this.setFromRotationMatrix(yM,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return vM.setFromEuler(this),this.setFromQuaternion(vM,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Vo=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},QA=0,_M=new F,wo=new Un,ar=new Ot,ju=new F,$a=new F,eI=new F,tI=new Un,xM=new F(1,0,0),MM=new F(0,1,0),EM=new F(0,0,1),bM={type:"added"},nI={type:"removed"},To={type:"childadded",child:null},cg={type:"childremoved",child:null},Kr=(()=>{class n extends pi{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:QA++}),this.uuid=Xo(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new F,i=new rc,r=new Un,s=new F(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Ot},normalMatrix:{value:new Le}}),this.matrix=new Ot,this.matrixWorld=new Ot,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Vo,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return wo.setFromAxisAngle(t,i),this.quaternion.multiply(wo),this}rotateOnWorldAxis(t,i){return wo.setFromAxisAngle(t,i),this.quaternion.premultiply(wo),this}rotateX(t){return this.rotateOnAxis(xM,t)}rotateY(t){return this.rotateOnAxis(MM,t)}rotateZ(t){return this.rotateOnAxis(EM,t)}translateOnAxis(t,i){return _M.copy(t).applyQuaternion(this.quaternion),this.position.add(_M.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(xM,t)}translateY(t){return this.translateOnAxis(MM,t)}translateZ(t){return this.translateOnAxis(EM,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(ar.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?ju.copy(t):ju.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),$a.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?ar.lookAt($a,ju,this.up):ar.lookAt(ju,$a,this.up),this.quaternion.setFromRotationMatrix(ar),s&&(ar.extractRotation(s.matrixWorld),wo.setFromRotationMatrix(ar),this.quaternion.premultiply(wo.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Ce("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(bM),To.child=t,this.dispatchEvent(To),To.child=null):Ce("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(nI),cg.child=t,this.dispatchEvent(cg),cg.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),ar.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),ar.multiply(t.parent.matrixWorld)),t.applyMatrix4(ar),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(bM),To.child=t,this.dispatchEvent(To),To.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose($a,t,eI),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose($a,tI,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),this.static!==!1&&(s.static=this.static),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>wn(Wt({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>Wt({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,f=l.length;u<f;u++){let d=l[u];o(t.shapes,d)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),f=a(t.images),d=a(t.shapes),h=a(t.skeletons),g=a(t.animations),x=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),f.length>0&&(r.images=f),d.length>0&&(r.shapes=d),h.length>0&&(r.skeletons=h),g.length>0&&(r.animations=g),x.length>0&&(r.nodes=x)}return r.object=s,r;function a(c){let l=[];for(let u in c){let f=c[u];delete f.metadata,l.push(f)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.pivot=t.pivot!==null?t.pivot.clone():null,this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new F(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Os=class extends Kr{constructor(){super(),this.isGroup=!0,this.type="Group"}},iI={type:"move"},Ho=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new Os,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new Os,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new F,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new F),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new Os,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new F,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new F,this._grip.eventsEnabled=!1),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let x of e.hand.values()){let m=t.getJointPose(x,i),p=this._getHandJoint(l,x);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],f=l.joints["thumb-tip"],d=u.position.distanceTo(f.position),h=.02,g=.005;l.inputState.pinching&&d>h+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&d<=h-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1,c.eventsEnabled&&c.dispatchEvent({type:"gripUpdated",data:e,target:this})));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(iI)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new Os;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},yE={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},Pr={h:0,s:0,l:0},Wu={h:0,s:0,l:0};function lg(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var Ge=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=kn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,Ke.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=Ke.workingColorSpace){return this.r=e,this.g=t,this.b=i,Ke.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=Ke.workingColorSpace){if(e=ry(e,1),t=qe(t,0,1),i=qe(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=lg(o,s,e+1/3),this.g=lg(o,s,e),this.b=lg(o,s,e-1/3)}return Ke.colorSpaceToWorking(this,r),this}setStyle(e,t=kn){function i(s){s!==void 0&&parseFloat(s)<1&&Te("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:Te("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);Te("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=kn){let i=yE[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Te("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=fr(e.r),this.g=fr(e.g),this.b=fr(e.b),this}copyLinearToSRGB(e){return this.r=Fo(e.r),this.g=Fo(e.g),this.b=Fo(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=kn){return Ke.workingToColorSpace(hn.copy(this),e),Math.round(qe(hn.r*255,0,255))*65536+Math.round(qe(hn.g*255,0,255))*256+Math.round(qe(hn.b*255,0,255))}getHexString(e=kn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=Ke.workingColorSpace){Ke.workingToColorSpace(hn.copy(this),t);let i=hn.r,r=hn.g,s=hn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let f=o-a;switch(l=u<=.5?f/(o+a):f/(2-o-a),o){case i:c=(r-s)/f+(r<s?6:0);break;case r:c=(s-i)/f+2;break;case s:c=(i-r)/f+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=Ke.workingColorSpace){return Ke.workingToColorSpace(hn.copy(this),t),e.r=hn.r,e.g=hn.g,e.b=hn.b,e}getStyle(e=kn){Ke.workingToColorSpace(hn.copy(this),e);let t=hn.r,i=hn.g,r=hn.b;return e!==kn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(Pr),this.setHSL(Pr.h+e,Pr.s+t,Pr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(Pr),e.getHSL(Wu);let i=Ja(Pr.h,Wu.h,t),r=Ja(Pr.s,Wu.s,t),s=Ja(Pr.l,Wu.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},hn=new Ge;Ge.NAMES=yE;var sc=class n{constructor(e,t=25e-5){this.isFogExp2=!0,this.name="",this.color=new Ge(e),this.density=t}clone(){return new n(this.color,this.density)}toJSON(){return{type:"FogExp2",name:this.name,color:this.color.getHex(),density:this.density}}};var oc=class extends Kr{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new rc,this.environmentIntensity=1,this.environmentRotation=new rc,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},di=new F,cr=new F,ug=new F,lr=new F,Do=new F,Co=new F,SM=new F,dg=new F,fg=new F,hg=new F,pg=new Lt,mg=new Lt,gg=new Lt,Ur=class n{constructor(e=new F,t=new F,i=new F){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),di.subVectors(e,t),r.cross(di);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){di.subVectors(r,t),cr.subVectors(i,t),ug.subVectors(e,t);let o=di.dot(di),a=di.dot(cr),c=di.dot(ug),l=cr.dot(cr),u=cr.dot(ug),f=o*l-a*a;if(f===0)return s.set(0,0,0),null;let d=1/f,h=(l*c-a*u)*d,g=(o*u-a*c)*d;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,lr)===null?!1:lr.x>=0&&lr.y>=0&&lr.x+lr.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,lr)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,lr.x),c.addScaledVector(o,lr.y),c.addScaledVector(a,lr.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return pg.setScalar(0),mg.setScalar(0),gg.setScalar(0),pg.fromBufferAttribute(e,t),mg.fromBufferAttribute(e,i),gg.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(pg,s.x),o.addScaledVector(mg,s.y),o.addScaledVector(gg,s.z),o}static isFrontFacing(e,t,i,r){return di.subVectors(i,t),cr.subVectors(e,t),di.cross(cr).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return di.subVectors(this.c,this.b),cr.subVectors(this.a,this.b),di.cross(cr).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;Do.subVectors(r,i),Co.subVectors(s,i),dg.subVectors(e,i);let c=Do.dot(dg),l=Co.dot(dg);if(c<=0&&l<=0)return t.copy(i);fg.subVectors(e,r);let u=Do.dot(fg),f=Co.dot(fg);if(u>=0&&f<=u)return t.copy(r);let d=c*f-u*l;if(d<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(Do,o);hg.subVectors(e,s);let h=Do.dot(hg),g=Co.dot(hg);if(g>=0&&h<=g)return t.copy(s);let x=h*l-c*g;if(x<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(Co,a);let m=u*g-h*f;if(m<=0&&f-u>=0&&h-g>=0)return SM.subVectors(s,r),a=(f-u)/(f-u+(h-g)),t.copy(r).addScaledVector(SM,a);let p=1/(m+x+d);return o=x*p,a=d*p,t.copy(i).addScaledVector(Do,o).addScaledVector(Co,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},Vr=class{constructor(e=new F(1/0,1/0,1/0),t=new F(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(fi.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(fi.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=fi.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,fi):fi.fromBufferAttribute(s,o),fi.applyMatrix4(e.matrixWorld),this.expandByPoint(fi);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),$u.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),$u.copy(i.boundingBox)),$u.applyMatrix4(e.matrixWorld),this.union($u)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,fi),fi.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(qa),qu.subVectors(this.max,qa),Ao.subVectors(e.a,qa),Io.subVectors(e.b,qa),Ro.subVectors(e.c,qa),Or.subVectors(Io,Ao),Lr.subVectors(Ro,Io),As.subVectors(Ao,Ro);let t=[0,-Or.z,Or.y,0,-Lr.z,Lr.y,0,-As.z,As.y,Or.z,0,-Or.x,Lr.z,0,-Lr.x,As.z,0,-As.x,-Or.y,Or.x,0,-Lr.y,Lr.x,0,-As.y,As.x,0];return!yg(t,Ao,Io,Ro,qu)||(t=[1,0,0,0,1,0,0,0,1],!yg(t,Ao,Io,Ro,qu))?!1:(Xu.crossVectors(Or,Lr),t=[Xu.x,Xu.y,Xu.z],yg(t,Ao,Io,Ro,qu))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,fi).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(fi).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(ur[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),ur[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),ur[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),ur[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),ur[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),ur[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),ur[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),ur[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(ur),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},ur=[new F,new F,new F,new F,new F,new F,new F,new F],fi=new F,$u=new Vr,Ao=new F,Io=new F,Ro=new F,Or=new F,Lr=new F,As=new F,qa=new F,qu=new F,Xu=new F,Is=new F;function yg(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){Is.fromArray(n,s);let a=r.x*Math.abs(Is.x)+r.y*Math.abs(Is.y)+r.z*Math.abs(Is.z),c=e.dot(Is),l=t.dot(Is),u=i.dot(Is);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var zt=new F,Yu=new ke,rI=0,Xt=class extends pi{constructor(e,t,i=!1){if(super(),Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:rI++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=Rg,this.updateRanges=[],this.gpuType=yi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Yu.fromBufferAttribute(this,t),Yu.applyMatrix3(e),this.setXY(t,Yu.x,Yu.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)zt.fromBufferAttribute(this,t),zt.applyMatrix3(e),this.setXYZ(t,zt.x,zt.y,zt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)zt.fromBufferAttribute(this,t),zt.applyMatrix4(e),this.setXYZ(t,zt.x,zt.y,zt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)zt.fromBufferAttribute(this,t),zt.applyNormalMatrix(e),this.setXYZ(t,zt.x,zt.y,zt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)zt.fromBufferAttribute(this,t),zt.transformDirection(e),this.setXYZ(t,zt.x,zt.y,zt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=Lo(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=bn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=Lo(t,this.array)),t}setX(e,t){return this.normalized&&(t=bn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=Lo(t,this.array)),t}setY(e,t){return this.normalized&&(t=bn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=Lo(t,this.array)),t}setZ(e,t){return this.normalized&&(t=bn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=Lo(t,this.array)),t}setW(e,t){return this.normalized&&(t=bn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=bn(t,this.array),i=bn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=bn(t,this.array),i=bn(i,this.array),r=bn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=bn(t,this.array),i=bn(i,this.array),r=bn(r,this.array),s=bn(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==Rg&&(e.usage=this.usage),e}dispose(){this.dispatchEvent({type:"dispose"})}};var ac=class extends Xt{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var cc=class extends Xt{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var Kn=class extends Xt{constructor(e,t,i){super(new Float32Array(e),t,i)}},sI=new Vr,Xa=new F,vg=new F,ks=class{constructor(e=new F,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):sI.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;Xa.subVectors(e,this.center);let t=Xa.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(Xa,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(vg.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(Xa.copy(e.center).add(vg)),this.expandByPoint(Xa.copy(e.center).sub(vg))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},oI=0,Zn=new Ot,_g=new Kr,No=new F,Fn=new Vr,Ya=new Vr,Kt=new F,Cn=class n extends pi{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:oI++}),this.uuid=Xo(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(IA(e)?cc:ac)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Le().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return Zn.makeRotationFromQuaternion(e),this.applyMatrix4(Zn),this}rotateX(e){return Zn.makeRotationX(e),this.applyMatrix4(Zn),this}rotateY(e){return Zn.makeRotationY(e),this.applyMatrix4(Zn),this}rotateZ(e){return Zn.makeRotationZ(e),this.applyMatrix4(Zn),this}translate(e,t,i){return Zn.makeTranslation(e,t,i),this.applyMatrix4(Zn),this}scale(e,t,i){return Zn.makeScale(e,t,i),this.applyMatrix4(Zn),this}lookAt(e){return _g.lookAt(e),_g.updateMatrix(),this.applyMatrix4(_g.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(No).negate(),this.translate(No.x,No.y,No.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new Kn(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&Te("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Vr);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ce("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new F(-1/0,-1/0,-1/0),new F(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];Fn.setFromBufferAttribute(s),this.morphTargetsRelative?(Kt.addVectors(this.boundingBox.min,Fn.min),this.boundingBox.expandByPoint(Kt),Kt.addVectors(this.boundingBox.max,Fn.max),this.boundingBox.expandByPoint(Kt)):(this.boundingBox.expandByPoint(Fn.min),this.boundingBox.expandByPoint(Fn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Ce('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new ks);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ce("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new F,1/0);return}if(e){let i=this.boundingSphere.center;if(Fn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];Ya.setFromBufferAttribute(a),this.morphTargetsRelative?(Kt.addVectors(Fn.min,Ya.min),Fn.expandByPoint(Kt),Kt.addVectors(Fn.max,Ya.max),Fn.expandByPoint(Kt)):(Fn.expandByPoint(Ya.min),Fn.expandByPoint(Ya.max))}Fn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Kt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Kt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)Kt.fromBufferAttribute(a,l),c&&(No.fromBufferAttribute(e,l),Kt.add(No)),r=Math.max(r,i.distanceToSquared(Kt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Ce('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Ce("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new Xt(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let v=0;v<i.count;v++)a[v]=new F,c[v]=new F;let l=new F,u=new F,f=new F,d=new ke,h=new ke,g=new ke,x=new F,m=new F;function p(v,T,O){l.fromBufferAttribute(i,v),u.fromBufferAttribute(i,T),f.fromBufferAttribute(i,O),d.fromBufferAttribute(s,v),h.fromBufferAttribute(s,T),g.fromBufferAttribute(s,O),u.sub(l),f.sub(l),h.sub(d),g.sub(d);let D=1/(h.x*g.y-g.x*h.y);isFinite(D)&&(x.copy(u).multiplyScalar(g.y).addScaledVector(f,-h.y).multiplyScalar(D),m.copy(f).multiplyScalar(h.x).addScaledVector(u,-g.x).multiplyScalar(D),a[v].add(x),a[T].add(x),a[O].add(x),c[v].add(m),c[T].add(m),c[O].add(m))}let E=this.groups;E.length===0&&(E=[{start:0,count:e.count}]);for(let v=0,T=E.length;v<T;++v){let O=E[v],D=O.start,U=O.count;for(let j=D,X=D+U;j<X;j+=3)p(e.getX(j+0),e.getX(j+1),e.getX(j+2))}let S=new F,b=new F,I=new F,w=new F;function C(v){I.fromBufferAttribute(r,v),w.copy(I);let T=a[v];S.copy(T),S.sub(I.multiplyScalar(I.dot(T))).normalize(),b.crossVectors(w,T);let D=b.dot(c[v])<0?-1:1;o.setXYZW(v,S.x,S.y,S.z,D)}for(let v=0,T=E.length;v<T;++v){let O=E[v],D=O.start,U=O.count;for(let j=D,X=D+U;j<X;j+=3)C(e.getX(j+0)),C(e.getX(j+1)),C(e.getX(j+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new Xt(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let d=0,h=i.count;d<h;d++)i.setXYZ(d,0,0,0);let r=new F,s=new F,o=new F,a=new F,c=new F,l=new F,u=new F,f=new F;if(e)for(let d=0,h=e.count;d<h;d+=3){let g=e.getX(d+0),x=e.getX(d+1),m=e.getX(d+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,x),o.fromBufferAttribute(t,m),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,x),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(x,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let d=0,h=t.count;d<h;d+=3)r.fromBufferAttribute(t,d+0),s.fromBufferAttribute(t,d+1),o.fromBufferAttribute(t,d+2),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),i.setXYZ(d+0,u.x,u.y,u.z),i.setXYZ(d+1,u.x,u.y,u.z),i.setXYZ(d+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Kt.fromBufferAttribute(e,t),Kt.normalize(),e.setXYZ(t,Kt.x,Kt.y,Kt.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,f=a.normalized,d=new l.constructor(c.length*u),h=0,g=0;for(let x=0,m=c.length;x<m;x++){a.isInterleavedBufferAttribute?h=c[x]*a.data.stride+a.offset:h=c[x]*u;for(let p=0;p<u;p++)d[g++]=l[h++]}return new Xt(d,u,f)}if(this.index===null)return Te("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,f=l.length;u<f;u++){let d=l[u],h=e(d,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let f=0,d=l.length;f<d;f++){let h=l[f];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],f=s[l];for(let d=0,h=f.length;d<h;d++)u.push(f[d].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let f=o[l];this.addGroup(f.start,f.count,f.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}};var aI=0,Hr=class extends pi{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:aI++}),this.uuid=Xo(),this.name="",this.type="Material",this.blending=Ls,this.side=hr,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=fd,this.blendDst=hd,this.blendEquation=Br,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new Ge(0,0,0),this.blendAlpha=0,this.depthFunc=Fs,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=Ig,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Ps,this.stencilZFail=Ps,this.stencilZPass=Ps,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Te(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Te(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==Ls&&(i.blending=this.blending),this.side!==hr&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==fd&&(i.blendSrc=this.blendSrc),this.blendDst!==hd&&(i.blendDst=this.blendDst),this.blendEquation!==Br&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==Fs&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==Ig&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Ps&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Ps&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Ps&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var dr=new F,xg=new F,Zu=new F,Fr=new F,Mg=new F,Ju=new F,Eg=new F,zr=class{constructor(e=new F,t=new F(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,dr)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=dr.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(dr.copy(this.origin).addScaledVector(this.direction,t),dr.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){xg.copy(e).add(t).multiplyScalar(.5),Zu.copy(t).sub(e).normalize(),Fr.copy(this.origin).sub(xg);let s=e.distanceTo(t)*.5,o=-this.direction.dot(Zu),a=Fr.dot(this.direction),c=-Fr.dot(Zu),l=Fr.lengthSq(),u=Math.abs(1-o*o),f,d,h,g;if(u>0)if(f=o*c-a,d=o*a-c,g=s*u,f>=0)if(d>=-g)if(d<=g){let x=1/u;f*=x,d*=x,h=f*(f+o*d+2*a)+d*(o*f+d+2*c)+l}else d=s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d=-s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d<=-g?(f=Math.max(0,-(-o*s+a)),d=f>0?-s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l):d<=g?(f=0,d=Math.min(Math.max(-s,-c),s),h=d*(d+2*c)+l):(f=Math.max(0,-(o*s+a)),d=f>0?s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l);else d=o>0?-s:s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,f),r&&r.copy(xg).addScaledVector(Zu,d),h}intersectSphere(e,t){dr.subVectors(e.center,this.origin);let i=dr.dot(this.direction),r=dr.dot(dr)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,f=1/this.direction.z,d=this.origin;return l>=0?(i=(e.min.x-d.x)*l,r=(e.max.x-d.x)*l):(i=(e.max.x-d.x)*l,r=(e.min.x-d.x)*l),u>=0?(s=(e.min.y-d.y)*u,o=(e.max.y-d.y)*u):(s=(e.max.y-d.y)*u,o=(e.min.y-d.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),f>=0?(a=(e.min.z-d.z)*f,c=(e.max.z-d.z)*f):(a=(e.max.z-d.z)*f,c=(e.min.z-d.z)*f),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,dr)!==null}intersectTriangle(e,t,i,r,s){Mg.subVectors(t,e),Ju.subVectors(i,e),Eg.crossVectors(Mg,Ju);let o=this.direction.dot(Eg),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;Fr.subVectors(this.origin,e);let c=a*this.direction.dot(Ju.crossVectors(Fr,Ju));if(c<0)return null;let l=a*this.direction.dot(Mg.cross(Fr));if(l<0||c+l>o)return null;let u=-a*Fr.dot(Eg);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},lc=class extends Hr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new Ge(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new rc,this.combine=Vg,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},wM=new Ot,Rs=new zr,Ku=new ks,TM=new F,Qu=new F,ed=new F,td=new F,bg=new F,nd=new F,DM=new F,id=new F,Qn=class extends Kr{constructor(e=new Cn,t=new lc){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){nd.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],f=s[c];u!==0&&(bg.fromBufferAttribute(f,e),o?nd.addScaledVector(bg,u):nd.addScaledVector(bg.sub(t),u))}t.add(nd)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Ku.copy(i.boundingSphere),Ku.applyMatrix4(s),Rs.copy(e.ray).recast(e.near),!(Ku.containsPoint(Rs.origin)===!1&&(Rs.intersectSphere(Ku,TM)===null||Rs.origin.distanceToSquared(TM)>(e.far-e.near)**2))&&(wM.copy(s).invert(),Rs.copy(e.ray).applyMatrix4(wM),!(i.boundingBox!==null&&Rs.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,Rs)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,f=s.attributes.normal,d=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,x=d.length;g<x;g++){let m=d[g],p=o[m.materialIndex],E=Math.max(m.start,h.start),S=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let b=E,I=S;b<I;b+=3){let w=a.getX(b),C=a.getX(b+1),v=a.getX(b+2);r=rd(this,p,e,i,l,u,f,w,C,v),r&&(r.faceIndex=Math.floor(b/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(a.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let E=a.getX(m),S=a.getX(m+1),b=a.getX(m+2);r=rd(this,o,e,i,l,u,f,E,S,b),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,x=d.length;g<x;g++){let m=d[g],p=o[m.materialIndex],E=Math.max(m.start,h.start),S=Math.min(c.count,Math.min(m.start+m.count,h.start+h.count));for(let b=E,I=S;b<I;b+=3){let w=b,C=b+1,v=b+2;r=rd(this,p,e,i,l,u,f,w,C,v),r&&(r.faceIndex=Math.floor(b/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(c.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let E=m,S=m+1,b=m+2;r=rd(this,o,e,i,l,u,f,E,S,b),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function cI(n,e,t,i,r,s,o,a){let c;if(e.side===Sn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===hr,a),c===null)return null;id.copy(a),id.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(id);return l<t.near||l>t.far?null:{distance:l,point:id.clone(),object:n}}function rd(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,Qu),n.getVertexPosition(c,ed),n.getVertexPosition(l,td);let u=cI(n,e,t,i,Qu,ed,td,DM);if(u){let f=new F;Ur.getBarycoord(DM,Qu,ed,td,f),r&&(u.uv=Ur.getInterpolatedAttribute(r,a,c,l,f,new ke)),s&&(u.uv1=Ur.getInterpolatedAttribute(s,a,c,l,f,new ke)),o&&(u.normal=Ur.getInterpolatedAttribute(o,a,c,l,f,new F),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let d={a,b:c,c:l,normal:new F,materialIndex:0};Ur.getNormal(Qu,ed,td,d.normal),u.face=d,u.barycoord=f}return u}var Cd=class extends gr{constructor(e=null,t=1,i=1,r,s,o,a,c,l=Qt,u=Qt,f,d){super(null,o,a,c,l,u,r,s,f,d),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Sg=new F,lI=new F,uI=new Le,Jn=class{constructor(e=new F(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=Sg.subVectors(i,t).cross(lI.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t,i=!0){let r=e.delta(Sg),s=this.normal.dot(r);if(s===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/s;return i===!0&&(o<0||o>1)?null:t.copy(e.start).addScaledVector(r,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||uI.getNormalMatrix(e),r=this.coplanarPoint(Sg).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},Ns=new ks,dI=new ke(.5,.5),sd=new F,uc=class{constructor(e=new Jn,t=new Jn,i=new Jn,r=new Jn,s=new Jn,o=new Jn){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=hi,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],f=s[5],d=s[6],h=s[7],g=s[8],x=s[9],m=s[10],p=s[11],E=s[12],S=s[13],b=s[14],I=s[15];if(r[0].setComponents(l-o,h-u,p-g,I-E).normalize(),r[1].setComponents(l+o,h+u,p+g,I+E).normalize(),r[2].setComponents(l+a,h+f,p+x,I+S).normalize(),r[3].setComponents(l-a,h-f,p-x,I-S).normalize(),i)r[4].setComponents(c,d,m,b).normalize(),r[5].setComponents(l-c,h-d,p-m,I-b).normalize();else if(r[4].setComponents(l-c,h-d,p-m,I-b).normalize(),t===hi)r[5].setComponents(l+c,h+d,p+m,I+b).normalize();else if(t===tc)r[5].setComponents(c,d,m,b).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),Ns.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),Ns.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(Ns)}intersectsSprite(e){Ns.center.set(0,0,0);let t=dI.distanceTo(e.center);return Ns.radius=.7071067811865476+t,Ns.applyMatrix4(e.matrixWorld),this.intersectsSphere(Ns)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(sd.x=r.normal.x>0?e.max.x:e.min.x,sd.y=r.normal.y>0?e.max.y:e.min.y,sd.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(sd)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var Ad=class extends Hr{constructor(e){super(),this.isPointsMaterial=!0,this.type="PointsMaterial",this.color=new Ge(16777215),this.map=null,this.alphaMap=null,this.size=1,this.sizeAttenuation=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.size=e.size,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},CM=new Ot,Ng=new zr,od=new ks,ad=new F,zo=class extends Kr{constructor(e=new Cn,t=new Ad){super(),this.isPoints=!0,this.type="Points",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Points.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),od.copy(i.boundingSphere),od.applyMatrix4(r),od.radius+=s,e.ray.intersectsSphere(od)===!1)return;CM.copy(r).invert(),Ng.copy(e.ray).applyMatrix4(CM);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=i.index,f=i.attributes.position;if(l!==null){let d=Math.max(0,o.start),h=Math.min(l.count,o.start+o.count);for(let g=d,x=h;g<x;g++){let m=l.getX(g);ad.fromBufferAttribute(f,m),AM(ad,m,c,r,e,t,this)}}else{let d=Math.max(0,o.start),h=Math.min(f.count,o.start+o.count);for(let g=d,x=h;g<x;g++)ad.fromBufferAttribute(f,g),AM(ad,g,c,r,e,t,this)}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function AM(n,e,t,i,r,s,o){let a=Ng.distanceSqToPoint(n);if(a<t){let c=new F;Ng.closestPointToPoint(n,c),c.applyMatrix4(i);let l=r.ray.origin.distanceTo(c);if(l<r.near||l>r.far)return;s.push({distance:l,distanceToRay:Math.sqrt(a),point:c,index:e,face:null,faceIndex:null,barycoord:null,object:o})}}var dc=class extends gr{constructor(e=[],t=Xr,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}};var pr=class extends gr{constructor(e,t,i=gi,r,s,o,a=Qt,c=Qt,l,u=Fi,f=1){if(u!==Fi&&u!==Zr)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let d={width:e,height:t,depth:f};super(d,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new Bo(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},Id=class extends pr{constructor(e,t=gi,i=Xr,r,s,o=Qt,a=Qt,c,l=Fi){let u={width:e,height:e,depth:1},f=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=f,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},fc=class extends gr{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},Go=class n extends Cn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],f=[],d=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new Kn(l,3)),this.setAttribute("normal",new Kn(u,3)),this.setAttribute("uv",new Kn(f,2));function g(x,m,p,E,S,b,I,w,C,v,T){let O=b/C,D=I/v,U=b/2,j=I/2,X=w/2,P=C+1,z=v+1,k=0,K=0,Q=new F;for(let le=0;le<z;le++){let ve=le*D-j;for(let be=0;be<P;be++){let tt=be*O-U;Q[x]=tt*E,Q[m]=ve*S,Q[p]=X,l.push(Q.x,Q.y,Q.z),Q[x]=0,Q[m]=0,Q[p]=w>0?1:-1,u.push(Q.x,Q.y,Q.z),f.push(be/C),f.push(1-le/v),k+=1}}for(let le=0;le<v;le++)for(let ve=0;ve<C;ve++){let be=d+ve+P*le,tt=d+ve+P*(le+1),at=d+(ve+1)+P*(le+1),Ve=d+(ve+1)+P*le;c.push(be,tt,Ve),c.push(tt,at,Ve),K+=6}a.addGroup(h,K,T),h+=K,d+=k}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var hc=class n extends Cn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,f=e/a,d=t/c,h=[],g=[],x=[],m=[];for(let p=0;p<u;p++){let E=p*d-o;for(let S=0;S<l;S++){let b=S*f-s;g.push(b,-E,0),x.push(0,0,1),m.push(S/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let E=0;E<a;E++){let S=E+l*p,b=E+l*(p+1),I=E+1+l*(p+1),w=E+1+l*p;h.push(S,b,w),h.push(b,I,w)}this.setIndex(h),this.setAttribute("position",new Kn(g,3)),this.setAttribute("normal",new Kn(x,3)),this.setAttribute("uv",new Kn(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};function Bs(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];if(IM(r))r.isRenderTargetTexture?(Te("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone();else if(Array.isArray(r))if(IM(r[0])){let s=[];for(let o=0,a=r.length;o<a;o++)s[o]=r[o].clone();e[t][i]=s}else e[t][i]=r.slice();else e[t][i]=r}}return e}function gn(n){let e={};for(let t=0;t<n.length;t++){let i=Bs(n[t]);for(let r in i)e[r]=i[r]}return e}function IM(n){return n&&(n.isColor||n.isMatrix3||n.isMatrix4||n.isVector2||n.isVector3||n.isVector4||n.isTexture||n.isQuaternion)}function fI(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function oy(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:Ke.workingColorSpace}var vE={clone:Bs,merge:gn},hI=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,pI=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,mn=class extends Hr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=hI,this.fragmentShader=pI,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=Bs(e.uniforms),this.uniformsGroups=fI(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Rd=class extends mn{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}};var Nd=class extends Hr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=sE,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Pd=class extends Hr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function cd(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var Gr=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},Od=class extends Gr{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:Dg,endingEnd:Dg}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case Cg:s=e,a=2*t-i;break;case Ag:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case Cg:o=e,c=2*i-t;break;case Ag:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,f=this._offsetNext,d=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),x=g*g,m=x*g,p=-d*m+2*d*x-d*g,E=(1+d)*m+(-1.5-2*d)*x+(-.5+d)*g+1,S=(-1-h)*m+(1.5+h)*x+.5*g,b=h*m-h*x;for(let I=0;I!==a;++I)s[I]=p*o[u+I]+E*o[l+I]+S*o[c+I]+b*o[f+I];return s}},Ld=class extends Gr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),f=1-u;for(let d=0;d!==a;++d)s[d]=o[l+d]*f+o[c+d]*u;return s}},Fd=class extends Gr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},kd=class extends Gr{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,f=u.inTangents,d=u.outTangents;if(!f||!d){let x=(i-t)/(r-t),m=1-x;for(let p=0;p!==a;++p)s[p]=o[l+p]*m+o[c+p]*x;return s}let h=a*2,g=e-1;for(let x=0;x!==a;++x){let m=o[l+x],p=o[c+x],E=g*h+x*2,S=d[E],b=d[E+1],I=e*h+x*2,w=f[I],C=f[I+1],v=(i-t)/(r-t),T,O,D,U,j;for(let X=0;X<8;X++){T=v*v,O=T*v,D=1-v,U=D*D,j=U*D;let z=j*t+3*U*v*S+3*D*T*w+O*r-i;if(Math.abs(z)<1e-10)break;let k=3*U*(S-t)+6*D*v*(w-S)+3*T*(r-w);if(Math.abs(k)<1e-10)break;v=v-z/k,v=Math.max(0,Math.min(1,v))}s[x]=j*m+3*U*v*b+3*D*T*C+O*p}return s}},Vn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=cd(t,this.TimeBufferType),this.values=cd(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:cd(e.times,Array),values:cd(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new Fd(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new Ld(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Od(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new kd(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case Ka:t=this.InterpolantFactoryMethodDiscrete;break;case bd:t=this.InterpolantFactoryMethodLinear;break;case dd:t=this.InterpolantFactoryMethodSmooth;break;case Tg:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Te("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Ka;case this.InterpolantFactoryMethodLinear:return bd;case this.InterpolantFactoryMethodSmooth:return dd;case this.InterpolantFactoryMethodBezier:return Tg}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Ce("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Ce("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Ce("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Ce("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&RA(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Ce("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===dd,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let f=a*i,d=f-i,h=f+i;for(let g=0;g!==i;++g){let x=t[f+g];if(x!==t[d+g]||x!==t[h+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let f=a*i,d=o*i;for(let h=0;h!==i;++h)t[d+h]=t[f+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};Vn.prototype.ValueTypeName="";Vn.prototype.TimeBufferType=Float32Array;Vn.prototype.ValueBufferType=Float32Array;Vn.prototype.DefaultInterpolation=bd;var jr=class extends Vn{constructor(e,t,i){super(e,t,i)}};jr.prototype.ValueTypeName="bool";jr.prototype.ValueBufferType=Array;jr.prototype.DefaultInterpolation=Ka;jr.prototype.InterpolantFactoryMethodLinear=void 0;jr.prototype.InterpolantFactoryMethodSmooth=void 0;var Ud=class extends Vn{constructor(e,t,i,r){super(e,t,i,r)}};Ud.prototype.ValueTypeName="color";var Bd=class extends Vn{constructor(e,t,i,r){super(e,t,i,r)}};Bd.prototype.ValueTypeName="number";var Vd=class extends Gr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)Un.slerpFlat(s,0,o,l-a,o,l,c);return s}},pc=class extends Vn{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new Vd(this.times,this.values,this.getValueSize(),e)}};pc.prototype.ValueTypeName="quaternion";pc.prototype.InterpolantFactoryMethodSmooth=void 0;var Wr=class extends Vn{constructor(e,t,i){super(e,t,i)}};Wr.prototype.ValueTypeName="string";Wr.prototype.ValueBufferType=Array;Wr.prototype.DefaultInterpolation=Ka;Wr.prototype.InterpolantFactoryMethodLinear=void 0;Wr.prototype.InterpolantFactoryMethodSmooth=void 0;var Hd=class extends Vn{constructor(e,t,i,r){super(e,t,i,r)}};Hd.prototype.ValueTypeName="vector";var ld=new F,ud=new Un,Oi=new F,mc=class extends Kr{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Ot,this.projectionMatrix=new Ot,this.projectionMatrixInverse=new Ot,this.coordinateSystem=hi,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(ld,ud,Oi),Oi.x===1&&Oi.y===1&&Oi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(ld,ud,Oi.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(ld,ud,Oi),Oi.x===1&&Oi.y===1&&Oi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(ld,ud,Oi.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},kr=new F,RM=new ke,NM=new ke,pn=class extends mc{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Uo*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(Za*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Uo*2*Math.atan(Math.tan(Za*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){kr.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(kr.x,kr.y).multiplyScalar(-e/kr.z),kr.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(kr.x,kr.y).multiplyScalar(-e/kr.z)}getViewSize(e,t){return this.getViewBounds(e,RM,NM),t.subVectors(NM,RM)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(Za*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var gc=class extends mc{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}};var Po=-90,Oo=1,zd=class extends Kr{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new pn(Po,Oo,e,t);r.layers=this.layers,this.add(r);let s=new pn(Po,Oo,e,t);s.layers=this.layers,this.add(s);let o=new pn(Po,Oo,e,t);o.layers=this.layers,this.add(o);let a=new pn(Po,Oo,e,t);a.layers=this.layers,this.add(a);let c=new pn(Po,Oo,e,t);c.layers=this.layers,this.add(c);let l=new pn(Po,Oo,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===hi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===tc)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,f=e.getRenderTarget(),d=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let x=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let m=!1;e.isWebGLRenderer===!0?m=e.state.buffers.depth.getReversed():m=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=x,e.setRenderTarget(i,5,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(f,d,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},Gd=class extends pn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var ay="\\[\\]\\.:\\/",mI=new RegExp("["+ay+"]","g"),cy="[^"+ay+"]",gI="[^"+ay.replace("\\.","")+"]",yI=/((?:WC+[\/:])*)/.source.replace("WC",cy),vI=/(WCOD+)?/.source.replace("WCOD",gI),_I=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",cy),xI=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",cy),MI=new RegExp("^"+yI+vI+_I+xI+"$"),EI=["material","materials","bones","map"],Pg=class{constructor(e,t,i){let r=i||Pt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Pt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(mI,"")}static parseTrackName(t){let i=MI.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);EI.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){Te("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Ce("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Ce("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Ce("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let f=0;f<t.length;f++)if(t[f].name===u){u=f;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Ce("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Ce("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Ce("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Ce("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;Ce("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){Ce("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Ce("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=Pg,n})();Pt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Pt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Pt.prototype.GetterByBindingType=[Pt.prototype._getValue_direct,Pt.prototype._getValue_array,Pt.prototype._getValue_arrayElement,Pt.prototype._getValue_toArray];Pt.prototype.SetterByBindingTypeAndVersioning=[[Pt.prototype._setValue_direct,Pt.prototype._setValue_direct_setNeedsUpdate,Pt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Pt.prototype._setValue_array,Pt.prototype._setValue_array_setNeedsUpdate,Pt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Pt.prototype._setValue_arrayElement,Pt.prototype._setValue_arrayElement_setNeedsUpdate,Pt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Pt.prototype._setValue_fromArray,Pt.prototype._setValue_fromArray_setNeedsUpdate,Pt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var Lj=new Float32Array(1);var PM=new Ot,yc=class{constructor(e,t,i=0,r=1/0){this.ray=new zr(e,t),this.near=i,this.far=r,this.camera=null,this.layers=new Vo,this.params={Mesh:{},Line:{threshold:1},LOD:{},Points:{threshold:1},Sprite:{}}}set(e,t){this.ray.set(e,t)}setFromCamera(e,t){t.isPerspectiveCamera?(this.ray.origin.setFromMatrixPosition(t.matrixWorld),this.ray.direction.set(e.x,e.y,.5).unproject(t).sub(this.ray.origin).normalize(),this.camera=t):t.isOrthographicCamera?(this.ray.origin.set(e.x,e.y,(t.near+t.far)/(t.near-t.far)).unproject(t),this.ray.direction.set(0,0,-1).transformDirection(t.matrixWorld),this.camera=t):Ce("Raycaster: Unsupported camera type: "+t.type)}setFromXRController(e){return PM.identity().extractRotation(e.matrixWorld),this.ray.origin.setFromMatrixPosition(e.matrixWorld),this.ray.direction.set(0,0,-1).applyMatrix4(PM),this}intersectObject(e,t=!0,i=[]){return Og(e,this,i,t),i.sort(OM),i}intersectObjects(e,t=!0,i=[]){for(let r=0,s=e.length;r<s;r++)Og(e[r],this,i,t);return i.sort(OM),i}};function OM(n,e){return n.distance-e.distance}function Og(n,e,t,i){let r=!0;if(n.layers.test(e.layers)&&n.raycast(e,t)===!1&&(r=!1),r===!0&&i===!0){let s=n.children;for(let o=0,a=s.length;o<a;o++)Og(s[o],e,t,!0)}}var jo=class{constructor(e=1,t=0,i=0){this.radius=e,this.phi=t,this.theta=i}set(e,t,i){return this.radius=e,this.phi=t,this.theta=i,this}copy(e){return this.radius=e.radius,this.phi=e.phi,this.theta=e.theta,this}makeSafe(){return this.phi=qe(this.phi,1e-6,Math.PI-1e-6),this}setFromVector3(e){return this.setFromCartesianCoords(e.x,e.y,e.z)}setFromCartesianCoords(e,t,i){return this.radius=Math.sqrt(e*e+t*t+i*i),this.radius===0?(this.theta=0,this.phi=0):(this.theta=Math.atan2(e,i),this.phi=Math.acos(qe(t/this.radius,-1,1))),this}clone(){return new this.constructor().copy(this)}};var Lg=class n{static{n.prototype.isMatrix2=!0}constructor(e,t,i,r){this.elements=[1,0,0,1],e!==void 0&&this.set(e,t,i,r)}identity(){return this.set(1,0,0,1),this}fromArray(e,t=0){for(let i=0;i<4;i++)this.elements[i]=e[i+t];return this}set(e,t,i,r){let s=this.elements;return s[0]=e,s[2]=t,s[1]=i,s[3]=r,this}};var vc=class extends pi{constructor(e,t=null){super(),this.object=e,this.domElement=t,this.enabled=!0,this.state=-1,this.keys={},this.mouseButtons={LEFT:null,MIDDLE:null,RIGHT:null},this.touches={ONE:null,TWO:null}}connect(e){if(e===void 0){Te("Controls: connect() now requires an element.");return}this.domElement!==null&&this.disconnect(),this.domElement=e}disconnect(){}dispose(){}update(){}};function ly(n,e,t,i){let r=bI(i);switch(t){case Kg:return n*e;case ey:return n*e/r.components*r.byteLength;case Zd:return n*e/r.components*r.byteLength;case Jr:return n*e*2/r.components*r.byteLength;case Jd:return n*e*2/r.components*r.byteLength;case Qg:return n*e*3/r.components*r.byteLength;case ei:return n*e*4/r.components*r.byteLength;case Kd:return n*e*4/r.components*r.byteLength;case Ec:case bc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Sc:case wc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case ef:case nf:return Math.max(n,16)*Math.max(e,8)/4;case Qd:case tf:return Math.max(n,8)*Math.max(e,8)/2;case rf:case sf:case af:case cf:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case of:case Tc:case lf:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case uf:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case df:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case ff:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case hf:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case pf:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case mf:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case gf:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case yf:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case vf:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case _f:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case xf:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Mf:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case Ef:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case bf:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Sf:case wf:case Tf:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Df:case Cf:return Math.ceil(n/4)*Math.ceil(e/4)*8;case Dc:case Af:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function bI(n){switch(n){case Hn:case Xg:return{byteLength:1,components:1};case $o:case Yg:case Bi:return{byteLength:2,components:1};case Xd:case Yd:return{byteLength:2,components:4};case gi:case qd:case yi:return{byteLength:4,components:1};case Zg:case Jg:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"184"}}));typeof window<"u"&&(window.__THREE__?Te("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="184");function HE(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&n!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n!==null&&n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function wI(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,f=l.byteLength,d=n.createBuffer();n.bindBuffer(c,d),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:d,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:f}}function i(a,c,l){let u=c.array,f=c.updateRanges;if(n.bindBuffer(l,a),f.length===0)n.bufferSubData(l,0,u);else{f.sort((h,g)=>h.start-g.start);let d=0;for(let h=1;h<f.length;h++){let g=f[d],x=f[h];x.start<=g.start+g.count+1?g.count=Math.max(g.count,x.start+x.count-g.start):(++d,f[d]=x)}f.length=d+1;for(let h=0,g=f.length;h<g;h++){let x=f[h];n.bufferSubData(l,x.start*u.BYTES_PER_ELEMENT,u,x.start,x.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var TI=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,DI=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,CI=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,AI=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,II=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,RI=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,NI=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,PI=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,OI=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,LI=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,FI=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,kI=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,UI=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,BI=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,VI=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,HI=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,zI=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,GI=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,jI=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,WI=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,$I=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,qI=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,XI=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,YI=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,ZI=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,JI=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,KI=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,QI=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,e1=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,t1=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,n1="gl_FragColor = linearToOutputTexel( gl_FragColor );",i1=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,r1=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * reflectVec );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,s1=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,o1=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,a1=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,c1=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,l1=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,u1=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,d1=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,f1=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,h1=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,p1=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,m1=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,g1=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,y1=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif
#include <lightprobes_pars_fragment>`,v1=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,_1=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,x1=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,M1=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,E1=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,b1=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,S1=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
		vec3 iridescenceFresnelDielectric;
		vec3 iridescenceFresnelMetallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		return 0.5 / max( gv + gl, EPSILON );
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
vec3 BRDF_GGX_Multiscatter( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 singleScatter = BRDF_GGX( lightDir, viewDir, normal, material );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 dfgV = texture2D( dfgLUT, vec2( material.roughness, dotNV ) ).rg;
	vec2 dfgL = texture2D( dfgLUT, vec2( material.roughness, dotNL ) ).rg;
	vec3 FssEss_V = material.specularColorBlended * dfgV.x + material.specularF90 * dfgV.y;
	vec3 FssEss_L = material.specularColorBlended * dfgL.x + material.specularF90 * dfgL.y;
	float Ess_V = dfgV.x + dfgV.y;
	float Ess_L = dfgL.x + dfgL.y;
	float Ems_V = 1.0 - Ess_V;
	float Ems_L = 1.0 - Ess_L;
	vec3 Favg = material.specularColorBlended + ( 1.0 - material.specularColorBlended ) * 0.047619;
	vec3 Fms = FssEss_V * FssEss_L * Favg / ( 1.0 - Ems_V * Ems_L * Favg + EPSILON );
	float compensationFactor = Ems_V * Ems_L;
	vec3 multiScatter = Fms * compensationFactor;
	return singleScatter + multiScatter;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX_Multiscatter( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnelDielectric, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceFresnelMetallic, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,w1=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( material.iridescenceFresnelDielectric, material.iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
	#ifdef USE_LIGHT_PROBES_GRID
		vec3 probeWorldPos = ( ( vec4( geometryPosition, 1.0 ) - viewMatrix[ 3 ] ) * viewMatrix ).xyz;
		vec3 probeWorldNormal = inverseTransformDirection( geometryNormal, viewMatrix );
		irradiance += getLightProbeGridIrradiance( probeWorldPos, probeWorldNormal );
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,T1=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,D1=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,C1=`#ifdef USE_LIGHT_PROBES_GRID
uniform highp sampler3D probesSH;
uniform vec3 probesMin;
uniform vec3 probesMax;
uniform vec3 probesResolution;
vec3 getLightProbeGridIrradiance( vec3 worldPos, vec3 worldNormal ) {
	vec3 res = probesResolution;
	vec3 gridRange = probesMax - probesMin;
	vec3 resMinusOne = res - 1.0;
	vec3 probeSpacing = gridRange / resMinusOne;
	vec3 samplePos = worldPos + worldNormal * probeSpacing * 0.5;
	vec3 uvw = clamp( ( samplePos - probesMin ) / gridRange, 0.0, 1.0 );
	uvw = uvw * resMinusOne / res + 0.5 / res;
	float nz          = res.z;
	float paddedSlices = nz + 2.0;
	float atlasDepth  = 7.0 * paddedSlices;
	float uvZBase     = uvw.z * nz + 1.0;
	vec4 s0 = texture( probesSH, vec3( uvw.xy, ( uvZBase                       ) / atlasDepth ) );
	vec4 s1 = texture( probesSH, vec3( uvw.xy, ( uvZBase +       paddedSlices   ) / atlasDepth ) );
	vec4 s2 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 2.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s3 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 3.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s4 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 4.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s5 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 5.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s6 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 6.0 * paddedSlices   ) / atlasDepth ) );
	vec3 c0 = s0.xyz;
	vec3 c1 = vec3( s0.w, s1.xy );
	vec3 c2 = vec3( s1.zw, s2.x );
	vec3 c3 = s2.yzw;
	vec3 c4 = s3.xyz;
	vec3 c5 = vec3( s3.w, s4.xy );
	vec3 c6 = vec3( s4.zw, s5.x );
	vec3 c7 = s5.yzw;
	vec3 c8 = s6.xyz;
	float x = worldNormal.x, y = worldNormal.y, z = worldNormal.z;
	vec3 result = c0 * 0.886227;
	result += c1 * 2.0 * 0.511664 * y;
	result += c2 * 2.0 * 0.511664 * z;
	result += c3 * 2.0 * 0.511664 * x;
	result += c4 * 2.0 * 0.429043 * x * y;
	result += c5 * 2.0 * 0.429043 * y * z;
	result += c6 * ( 0.743125 * z * z - 0.247708 );
	result += c7 * 2.0 * 0.429043 * x * z;
	result += c8 * 0.429043 * ( x * x - y * y );
	return max( result, vec3( 0.0 ) );
}
#endif`,A1=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,I1=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,R1=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,N1=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,P1=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,O1=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,L1=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,F1=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,k1=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,U1=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,B1=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,V1=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,H1=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,z1=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,G1=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,j1=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,W1=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#if defined( USE_PACKED_NORMALMAP )
		mapN = vec3( mapN.xy, sqrt( saturate( 1.0 - dot( mapN.xy, mapN.xy ) ) ) );
	#endif
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,$1=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,q1=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,X1=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,Y1=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,Z1=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,J1=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,K1=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,Q1=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,eR=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,tR=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,nR=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,iR=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,rR=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,sR=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,oR=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,aR=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,cR=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,lR=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,uR=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	#ifdef HAS_NORMAL
		vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	#else
		vec3 shadowWorldNormal = vec3( 0.0 );
	#endif
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,dR=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,fR=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,hR=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,pR=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,mR=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,gR=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,yR=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,vR=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,_R=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,xR=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,MR=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,ER=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,bR=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,SR=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,wR=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,TR=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,DR=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,CR=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,AR=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vWorldDirection );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,IR=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,RR=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,NR=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,PR=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,OR=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,LR=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,FR=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,kR=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,UR=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,BR=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,VR=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,HR=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,zR=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,GR=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,jR=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,WR=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,$R=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,qR=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,XR=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,YR=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,ZR=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,JR=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,KR=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,QR=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,eN=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,tN=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,nN=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,iN=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,rN=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,sN=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,je={alphahash_fragment:TI,alphahash_pars_fragment:DI,alphamap_fragment:CI,alphamap_pars_fragment:AI,alphatest_fragment:II,alphatest_pars_fragment:RI,aomap_fragment:NI,aomap_pars_fragment:PI,batching_pars_vertex:OI,batching_vertex:LI,begin_vertex:FI,beginnormal_vertex:kI,bsdfs:UI,iridescence_fragment:BI,bumpmap_pars_fragment:VI,clipping_planes_fragment:HI,clipping_planes_pars_fragment:zI,clipping_planes_pars_vertex:GI,clipping_planes_vertex:jI,color_fragment:WI,color_pars_fragment:$I,color_pars_vertex:qI,color_vertex:XI,common:YI,cube_uv_reflection_fragment:ZI,defaultnormal_vertex:JI,displacementmap_pars_vertex:KI,displacementmap_vertex:QI,emissivemap_fragment:e1,emissivemap_pars_fragment:t1,colorspace_fragment:n1,colorspace_pars_fragment:i1,envmap_fragment:r1,envmap_common_pars_fragment:s1,envmap_pars_fragment:o1,envmap_pars_vertex:a1,envmap_physical_pars_fragment:v1,envmap_vertex:c1,fog_vertex:l1,fog_pars_vertex:u1,fog_fragment:d1,fog_pars_fragment:f1,gradientmap_pars_fragment:h1,lightmap_pars_fragment:p1,lights_lambert_fragment:m1,lights_lambert_pars_fragment:g1,lights_pars_begin:y1,lights_toon_fragment:_1,lights_toon_pars_fragment:x1,lights_phong_fragment:M1,lights_phong_pars_fragment:E1,lights_physical_fragment:b1,lights_physical_pars_fragment:S1,lights_fragment_begin:w1,lights_fragment_maps:T1,lights_fragment_end:D1,lightprobes_pars_fragment:C1,logdepthbuf_fragment:A1,logdepthbuf_pars_fragment:I1,logdepthbuf_pars_vertex:R1,logdepthbuf_vertex:N1,map_fragment:P1,map_pars_fragment:O1,map_particle_fragment:L1,map_particle_pars_fragment:F1,metalnessmap_fragment:k1,metalnessmap_pars_fragment:U1,morphinstance_vertex:B1,morphcolor_vertex:V1,morphnormal_vertex:H1,morphtarget_pars_vertex:z1,morphtarget_vertex:G1,normal_fragment_begin:j1,normal_fragment_maps:W1,normal_pars_fragment:$1,normal_pars_vertex:q1,normal_vertex:X1,normalmap_pars_fragment:Y1,clearcoat_normal_fragment_begin:Z1,clearcoat_normal_fragment_maps:J1,clearcoat_pars_fragment:K1,iridescence_pars_fragment:Q1,opaque_fragment:eR,packing:tR,premultiplied_alpha_fragment:nR,project_vertex:iR,dithering_fragment:rR,dithering_pars_fragment:sR,roughnessmap_fragment:oR,roughnessmap_pars_fragment:aR,shadowmap_pars_fragment:cR,shadowmap_pars_vertex:lR,shadowmap_vertex:uR,shadowmask_pars_fragment:dR,skinbase_vertex:fR,skinning_pars_vertex:hR,skinning_vertex:pR,skinnormal_vertex:mR,specularmap_fragment:gR,specularmap_pars_fragment:yR,tonemapping_fragment:vR,tonemapping_pars_fragment:_R,transmission_fragment:xR,transmission_pars_fragment:MR,uv_pars_fragment:ER,uv_pars_vertex:bR,uv_vertex:SR,worldpos_vertex:wR,background_vert:TR,background_frag:DR,backgroundCube_vert:CR,backgroundCube_frag:AR,cube_vert:IR,cube_frag:RR,depth_vert:NR,depth_frag:PR,distance_vert:OR,distance_frag:LR,equirect_vert:FR,equirect_frag:kR,linedashed_vert:UR,linedashed_frag:BR,meshbasic_vert:VR,meshbasic_frag:HR,meshlambert_vert:zR,meshlambert_frag:GR,meshmatcap_vert:jR,meshmatcap_frag:WR,meshnormal_vert:$R,meshnormal_frag:qR,meshphong_vert:XR,meshphong_frag:YR,meshphysical_vert:ZR,meshphysical_frag:JR,meshtoon_vert:KR,meshtoon_frag:QR,points_vert:eN,points_frag:tN,shadow_vert:nN,shadow_frag:iN,sprite_vert:rN,sprite_frag:sN},ce={common:{diffuse:{value:new Ge(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Le},alphaMap:{value:null},alphaMapTransform:{value:new Le},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Le}},envmap:{envMap:{value:null},envMapRotation:{value:new Le},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Le}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Le}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Le},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Le},normalScale:{value:new ke(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Le},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Le}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Le}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Le}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new Ge(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null},probesSH:{value:null},probesMin:{value:new F},probesMax:{value:new F},probesResolution:{value:new F}},points:{diffuse:{value:new Ge(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Le},alphaTest:{value:0},uvTransform:{value:new Le}},sprite:{diffuse:{value:new Ge(16777215)},opacity:{value:1},center:{value:new ke(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Le},alphaMap:{value:null},alphaMapTransform:{value:new Le},alphaTest:{value:0}}},Hi={basic:{uniforms:gn([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.fog]),vertexShader:je.meshbasic_vert,fragmentShader:je.meshbasic_frag},lambert:{uniforms:gn([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,ce.lights,{emissive:{value:new Ge(0)},envMapIntensity:{value:1}}]),vertexShader:je.meshlambert_vert,fragmentShader:je.meshlambert_frag},phong:{uniforms:gn([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,ce.lights,{emissive:{value:new Ge(0)},specular:{value:new Ge(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:je.meshphong_vert,fragmentShader:je.meshphong_frag},standard:{uniforms:gn([ce.common,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.roughnessmap,ce.metalnessmap,ce.fog,ce.lights,{emissive:{value:new Ge(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:je.meshphysical_vert,fragmentShader:je.meshphysical_frag},toon:{uniforms:gn([ce.common,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.gradientmap,ce.fog,ce.lights,{emissive:{value:new Ge(0)}}]),vertexShader:je.meshtoon_vert,fragmentShader:je.meshtoon_frag},matcap:{uniforms:gn([ce.common,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,{matcap:{value:null}}]),vertexShader:je.meshmatcap_vert,fragmentShader:je.meshmatcap_frag},points:{uniforms:gn([ce.points,ce.fog]),vertexShader:je.points_vert,fragmentShader:je.points_frag},dashed:{uniforms:gn([ce.common,ce.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:je.linedashed_vert,fragmentShader:je.linedashed_frag},depth:{uniforms:gn([ce.common,ce.displacementmap]),vertexShader:je.depth_vert,fragmentShader:je.depth_frag},normal:{uniforms:gn([ce.common,ce.bumpmap,ce.normalmap,ce.displacementmap,{opacity:{value:1}}]),vertexShader:je.meshnormal_vert,fragmentShader:je.meshnormal_frag},sprite:{uniforms:gn([ce.sprite,ce.fog]),vertexShader:je.sprite_vert,fragmentShader:je.sprite_frag},background:{uniforms:{uvTransform:{value:new Le},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:je.background_vert,fragmentShader:je.background_frag},backgroundCube:{uniforms:{envMap:{value:null},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Le}},vertexShader:je.backgroundCube_vert,fragmentShader:je.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:je.cube_vert,fragmentShader:je.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:je.equirect_vert,fragmentShader:je.equirect_frag},distance:{uniforms:gn([ce.common,ce.displacementmap,{referencePosition:{value:new F},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:je.distance_vert,fragmentShader:je.distance_frag},shadow:{uniforms:gn([ce.lights,ce.fog,{color:{value:new Ge(0)},opacity:{value:1}}]),vertexShader:je.shadow_vert,fragmentShader:je.shadow_frag}};Hi.physical={uniforms:gn([Hi.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Le},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Le},clearcoatNormalScale:{value:new ke(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Le},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Le},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Le},sheen:{value:0},sheenColor:{value:new Ge(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Le},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Le},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Le},transmissionSamplerSize:{value:new ke},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Le},attenuationDistance:{value:0},attenuationColor:{value:new Ge(0)},specularColor:{value:new Ge(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Le},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Le},anisotropyVector:{value:new ke},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Le}}]),vertexShader:je.meshphysical_vert,fragmentShader:je.meshphysical_frag};var Nf={r:0,b:0,g:0},oN=new Ot,zE=new Le;zE.set(-1,0,0,0,1,0,0,0,1);function aN(n,e,t,i,r,s){let o=new Ge(0),a=r===!0?0:1,c,l,u=null,f=0,d=null;function h(E){let S=E.isScene===!0?E.background:null;if(S&&S.isTexture){let b=E.backgroundBlurriness>0;S=e.get(S,b)}return S}function g(E){let S=!1,b=h(E);b===null?m(o,a):b&&b.isColor&&(m(b,1),S=!0);let I=n.xr.getEnvironmentBlendMode();I==="additive"?t.buffers.color.setClear(0,0,0,1,s):I==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||S)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function x(E,S){let b=h(S);b&&(b.isCubeTexture||b.mapping===xc)?(l===void 0&&(l=new Qn(new Go(1,1,1),new mn({name:"BackgroundCubeMaterial",uniforms:Bs(Hi.backgroundCube.uniforms),vertexShader:Hi.backgroundCube.vertexShader,fragmentShader:Hi.backgroundCube.fragmentShader,side:Sn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(I,w,C){this.matrixWorld.copyPosition(C.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),l.material.uniforms.envMap.value=b,l.material.uniforms.backgroundBlurriness.value=S.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=S.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(oN.makeRotationFromEuler(S.backgroundRotation)).transpose(),b.isCubeTexture&&b.isRenderTargetTexture===!1&&l.material.uniforms.backgroundRotation.value.premultiply(zE),l.material.toneMapped=Ke.getTransfer(b.colorSpace)!==st,(u!==b||f!==b.version||d!==n.toneMapping)&&(l.material.needsUpdate=!0,u=b,f=b.version,d=n.toneMapping),l.layers.enableAll(),E.unshift(l,l.geometry,l.material,0,0,null)):b&&b.isTexture&&(c===void 0&&(c=new Qn(new hc(2,2),new mn({name:"BackgroundMaterial",uniforms:Bs(Hi.background.uniforms),vertexShader:Hi.background.vertexShader,fragmentShader:Hi.background.fragmentShader,side:hr,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=b,c.material.uniforms.backgroundIntensity.value=S.backgroundIntensity,c.material.toneMapped=Ke.getTransfer(b.colorSpace)!==st,b.matrixAutoUpdate===!0&&b.updateMatrix(),c.material.uniforms.uvTransform.value.copy(b.matrix),(u!==b||f!==b.version||d!==n.toneMapping)&&(c.material.needsUpdate=!0,u=b,f=b.version,d=n.toneMapping),c.layers.enableAll(),E.unshift(c,c.geometry,c.material,0,0,null))}function m(E,S){E.getRGB(Nf,oy(n)),t.buffers.color.setClear(Nf.r,Nf.g,Nf.b,S,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(E,S=1){o.set(E),a=S,m(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(E){a=E,m(o,a)},render:g,addToRenderList:x,dispose:p}}function cN(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=d(null),s=r,o=!1;function a(D,U,j,X,P){let z=!1,k=f(D,X,j,U);s!==k&&(s=k,l(s.object)),z=h(D,X,j,P),z&&g(D,X,j,P),P!==null&&e.update(P,n.ELEMENT_ARRAY_BUFFER),(z||o)&&(o=!1,b(D,U,j,X),P!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(P).buffer))}function c(){return n.createVertexArray()}function l(D){return n.bindVertexArray(D)}function u(D){return n.deleteVertexArray(D)}function f(D,U,j,X){let P=X.wireframe===!0,z=i[U.id];z===void 0&&(z={},i[U.id]=z);let k=D.isInstancedMesh===!0?D.id:0,K=z[k];K===void 0&&(K={},z[k]=K);let Q=K[j.id];Q===void 0&&(Q={},K[j.id]=Q);let le=Q[P];return le===void 0&&(le=d(c()),Q[P]=le),le}function d(D){let U=[],j=[],X=[];for(let P=0;P<t;P++)U[P]=0,j[P]=0,X[P]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:U,enabledAttributes:j,attributeDivisors:X,object:D,attributes:{},index:null}}function h(D,U,j,X){let P=s.attributes,z=U.attributes,k=0,K=j.getAttributes();for(let Q in K)if(K[Q].location>=0){let ve=P[Q],be=z[Q];if(be===void 0&&(Q==="instanceMatrix"&&D.instanceMatrix&&(be=D.instanceMatrix),Q==="instanceColor"&&D.instanceColor&&(be=D.instanceColor)),ve===void 0||ve.attribute!==be||be&&ve.data!==be.data)return!0;k++}return s.attributesNum!==k||s.index!==X}function g(D,U,j,X){let P={},z=U.attributes,k=0,K=j.getAttributes();for(let Q in K)if(K[Q].location>=0){let ve=z[Q];ve===void 0&&(Q==="instanceMatrix"&&D.instanceMatrix&&(ve=D.instanceMatrix),Q==="instanceColor"&&D.instanceColor&&(ve=D.instanceColor));let be={};be.attribute=ve,ve&&ve.data&&(be.data=ve.data),P[Q]=be,k++}s.attributes=P,s.attributesNum=k,s.index=X}function x(){let D=s.newAttributes;for(let U=0,j=D.length;U<j;U++)D[U]=0}function m(D){p(D,0)}function p(D,U){let j=s.newAttributes,X=s.enabledAttributes,P=s.attributeDivisors;j[D]=1,X[D]===0&&(n.enableVertexAttribArray(D),X[D]=1),P[D]!==U&&(n.vertexAttribDivisor(D,U),P[D]=U)}function E(){let D=s.newAttributes,U=s.enabledAttributes;for(let j=0,X=U.length;j<X;j++)U[j]!==D[j]&&(n.disableVertexAttribArray(j),U[j]=0)}function S(D,U,j,X,P,z,k){k===!0?n.vertexAttribIPointer(D,U,j,P,z):n.vertexAttribPointer(D,U,j,X,P,z)}function b(D,U,j,X){x();let P=X.attributes,z=j.getAttributes(),k=U.defaultAttributeValues;for(let K in z){let Q=z[K];if(Q.location>=0){let le=P[K];if(le===void 0&&(K==="instanceMatrix"&&D.instanceMatrix&&(le=D.instanceMatrix),K==="instanceColor"&&D.instanceColor&&(le=D.instanceColor)),le!==void 0){let ve=le.normalized,be=le.itemSize,tt=e.get(le);if(tt===void 0)continue;let at=tt.buffer,Ve=tt.type,Y=tt.bytesPerElement,fe=Ve===n.INT||Ve===n.UNSIGNED_INT||le.gpuType===qd;if(le.isInterleavedBufferAttribute){let ie=le.data,Ae=ie.stride,Fe=le.offset;if(ie.isInstancedInterleavedBuffer){for(let Re=0;Re<Q.locationSize;Re++)p(Q.location+Re,ie.meshPerAttribute);D.isInstancedMesh!==!0&&X._maxInstanceCount===void 0&&(X._maxInstanceCount=ie.meshPerAttribute*ie.count)}else for(let Re=0;Re<Q.locationSize;Re++)m(Q.location+Re);n.bindBuffer(n.ARRAY_BUFFER,at);for(let Re=0;Re<Q.locationSize;Re++)S(Q.location+Re,be/Q.locationSize,Ve,ve,Ae*Y,(Fe+be/Q.locationSize*Re)*Y,fe)}else{if(le.isInstancedBufferAttribute){for(let ie=0;ie<Q.locationSize;ie++)p(Q.location+ie,le.meshPerAttribute);D.isInstancedMesh!==!0&&X._maxInstanceCount===void 0&&(X._maxInstanceCount=le.meshPerAttribute*le.count)}else for(let ie=0;ie<Q.locationSize;ie++)m(Q.location+ie);n.bindBuffer(n.ARRAY_BUFFER,at);for(let ie=0;ie<Q.locationSize;ie++)S(Q.location+ie,be/Q.locationSize,Ve,ve,be*Y,be/Q.locationSize*ie*Y,fe)}}else if(k!==void 0){let ve=k[K];if(ve!==void 0)switch(ve.length){case 2:n.vertexAttrib2fv(Q.location,ve);break;case 3:n.vertexAttrib3fv(Q.location,ve);break;case 4:n.vertexAttrib4fv(Q.location,ve);break;default:n.vertexAttrib1fv(Q.location,ve)}}}}E()}function I(){T();for(let D in i){let U=i[D];for(let j in U){let X=U[j];for(let P in X){let z=X[P];for(let k in z)u(z[k].object),delete z[k];delete X[P]}}delete i[D]}}function w(D){if(i[D.id]===void 0)return;let U=i[D.id];for(let j in U){let X=U[j];for(let P in X){let z=X[P];for(let k in z)u(z[k].object),delete z[k];delete X[P]}}delete i[D.id]}function C(D){for(let U in i){let j=i[U];for(let X in j){let P=j[X];if(P[D.id]===void 0)continue;let z=P[D.id];for(let k in z)u(z[k].object),delete z[k];delete P[D.id]}}}function v(D){for(let U in i){let j=i[U],X=D.isInstancedMesh===!0?D.id:0,P=j[X];if(P!==void 0){for(let z in P){let k=P[z];for(let K in k)u(k[K].object),delete k[K];delete P[z]}delete j[X],Object.keys(j).length===0&&delete i[U]}}}function T(){O(),o=!0,s!==r&&(s=r,l(s.object))}function O(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:T,resetDefaultState:O,dispose:I,releaseStatesOfGeometry:w,releaseStatesOfObject:v,releaseStatesOfProgram:C,initAttributes:x,enableAttribute:m,disableUnusedAttributes:E}}function lN(n,e,t){let i;function r(c){i=c}function s(c,l){n.drawArrays(i,c,l),t.update(l,i,1)}function o(c,l,u){u!==0&&(n.drawArraysInstanced(i,c,l,u),t.update(l,i,u))}function a(c,l,u){if(u===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,l,0,u);let d=0;for(let h=0;h<u;h++)d+=l[h];t.update(d,i,1)}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a}function uN(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let C=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(C.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(C){return!(C!==ei&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(C){let v=C===Bi&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(C!==Hn&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&C!==yi&&!v)}function c(C){if(C==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";C="mediump"}return C==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Te("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let f=t.logarithmicDepthBuffer===!0,d=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control");t.reversedDepthBuffer===!0&&d===!1&&Te("WebGLRenderer: Unable to use reversed depth buffer due to missing EXT_clip_control extension. Fallback to default depth buffer.");let h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),x=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),E=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),S=n.getParameter(n.MAX_VARYING_VECTORS),b=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),I=n.getParameter(n.MAX_SAMPLES),w=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:f,reversedDepthBuffer:d,maxTextures:h,maxVertexTextures:g,maxTextureSize:x,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:E,maxVaryings:S,maxFragmentUniforms:b,maxSamples:I,samples:w}}function dN(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Jn,a=new Le,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(f,d){let h=f.length!==0||d||i!==0||r;return r=d,i=f.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(f,d){t=u(f,d,0)},this.setState=function(f,d,h){let g=f.clippingPlanes,x=f.clipIntersection,m=f.clipShadows,p=n.get(f);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let E=s?0:i,S=E*4,b=p.clippingState||null;c.value=b,b=u(g,d,S,h);for(let I=0;I!==S;++I)b[I]=t[I];p.clippingState=b,this.numIntersection=x?this.numPlanes:0,this.numPlanes+=E}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(f,d,h,g){let x=f!==null?f.length:0,m=null;if(x!==0){if(m=c.value,g!==!0||m===null){let p=h+x*4,E=d.matrixWorldInverse;a.getNormalMatrix(E),(m===null||m.length<p)&&(m=new Float32Array(p));for(let S=0,b=h;S!==x;++S,b+=4)o.copy(f[S]).applyMatrix4(E,a),o.normal.toArray(m,b),m[b+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=x,e.numIntersection=0,m}}var Qr=4,_E=[.125,.215,.35,.446,.526,.582],Vs=20,fN=256,Cc=new gc,xE=new Ge,uy=null,dy=0,fy=0,hy=!1,hN=new F,Of=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=hN}=s;uy=this._renderer.getRenderTarget(),dy=this._renderer.getActiveCubeFace(),fy=this._renderer.getActiveMipmapLevel(),hy=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=bE(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=EE(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(uy,dy,fy),this._renderer.xr.enabled=hy,e.scissorTest=!1,Yo(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===Xr||e.mapping===Us?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),uy=this._renderer.getRenderTarget(),dy=this._renderer.getActiveCubeFace(),fy=this._renderer.getActiveMipmapLevel(),hy=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:en,minFilter:en,generateMipmaps:!1,type:Bi,format:ei,colorSpace:Qa,depthBuffer:!1},r=ME(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=ME(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=pN(s)),this._blurMaterial=gN(s,e,t),this._ggxMaterial=mN(s,e,t)}return r}_compileMaterial(e){let t=new Qn(new Cn,e);this._renderer.compile(t,Cc)}_sceneToCubeUV(e,t,i,r,s){let c=new pn(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],f=this._renderer,d=f.autoClear,h=f.toneMapping;f.getClearColor(xE),f.toneMapping=mi,f.autoClear=!1,f.state.buffers.depth.getReversed()&&(f.setRenderTarget(r),f.clearDepth(),f.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new Qn(new Go,new lc({name:"PMREM.Background",side:Sn,depthWrite:!1,depthTest:!1})));let x=this._backgroundBox,m=x.material,p=!1,E=e.background;E?E.isColor&&(m.color.copy(E),e.background=null,p=!0):(m.color.copy(xE),p=!0);for(let S=0;S<6;S++){let b=S%3;b===0?(c.up.set(0,l[S],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[S],s.y,s.z)):b===1?(c.up.set(0,0,l[S]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[S],s.z)):(c.up.set(0,l[S],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[S]));let I=this._cubeSize;Yo(r,b*I,S>2?I:0,I,I),f.setRenderTarget(r),p&&f.render(x,c),f.render(e,c)}f.toneMapping=h,f.autoClear=d,e.background=E}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===Xr||e.mapping===Us;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=bE()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=EE());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;Yo(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,Cc)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),f=Math.sqrt(l*l-u*u),d=0+l*1.25,h=f*d,{_lodMax:g}=this,x=this._sizeLods[i],m=3*x*(i>g-Qr?i-g+Qr:0),p=4*(this._cubeSize-x);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=g-t,Yo(s,m,p,3*x,2*x),r.setRenderTarget(s),r.render(a,Cc),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=g-i,Yo(e,m,p,3*x,2*x),r.setRenderTarget(e),r.render(a,Cc)}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&Ce("blur direction must be either latitudinal or longitudinal!");let u=3,f=this._lodMeshes[r];f.material=l;let d=l.uniforms,h=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*Vs-1),x=s/g,m=isFinite(s)?1+Math.floor(u*x):Vs;m>Vs&&Te(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${Vs}`);let p=[],E=0;for(let C=0;C<Vs;++C){let v=C/x,T=Math.exp(-v*v/2);p.push(T),C===0?E+=T:C<m&&(E+=2*T)}for(let C=0;C<p.length;C++)p[C]=p[C]/E;d.envMap.value=e.texture,d.samples.value=m,d.weights.value=p,d.latitudinal.value=o==="latitudinal",a&&(d.poleAxis.value=a);let{_lodMax:S}=this;d.dTheta.value=g,d.mipInt.value=S-i;let b=this._sizeLods[r],I=3*b*(r>S-Qr?r-S+Qr:0),w=4*(this._cubeSize-b);Yo(t,I,w,3*b,2*b),c.setRenderTarget(t),c.render(f,Cc)}};function pN(n){let e=[],t=[],i=[],r=n,s=n-Qr+1+_E.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);e.push(a);let c=1/a;o>n-Qr?c=_E[o-n+Qr-1]:o===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,f=1+l,d=[u,u,f,u,f,f,u,u,f,f,u,f],h=6,g=6,x=3,m=2,p=1,E=new Float32Array(x*g*h),S=new Float32Array(m*g*h),b=new Float32Array(p*g*h);for(let w=0;w<h;w++){let C=w%3*2/3-1,v=w>2?0:-1,T=[C,v,0,C+2/3,v,0,C+2/3,v+1,0,C,v,0,C+2/3,v+1,0,C,v+1,0];E.set(T,x*g*w),S.set(d,m*g*w);let O=[w,w,w,w,w,w];b.set(O,p*g*w)}let I=new Cn;I.setAttribute("position",new Xt(E,x)),I.setAttribute("uv",new Xt(S,m)),I.setAttribute("faceIndex",new Xt(b,p)),i.push(new Qn(I,null)),r>Qr&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function ME(n,e,t){let i=new Bn(n,e,t);return i.texture.mapping=xc,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Yo(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function mN(n,e,t){return new mn({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:fN,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:kf(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:Ui,depthTest:!1,depthWrite:!1})}function gN(n,e,t){let i=new Float32Array(Vs),r=new F(0,1,0);return new mn({name:"SphericalGaussianBlur",defines:{n:Vs,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:kf(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:Ui,depthTest:!1,depthWrite:!1})}function EE(){return new mn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:kf(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:Ui,depthTest:!1,depthWrite:!1})}function bE(){return new mn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:kf(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:Ui,depthTest:!1,depthWrite:!1})}function kf(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}var Lf=class extends Bn{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new dc(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new Go(5,5,5),s=new mn({name:"CubemapFromEquirect",uniforms:Bs(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:Sn,blending:Ui});s.uniforms.tEquirect.value=t;let o=new Qn(r,s),a=t.minFilter;return t.minFilter===Yr&&(t.minFilter=en),new zd(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function yN(n){let e=new WeakMap,t=new WeakMap,i=null;function r(d,h=!1){return d==null?null:h?o(d):s(d)}function s(d){if(d&&d.isTexture){let h=d.mapping;if(h===jd||h===Wd)if(e.has(d)){let g=e.get(d).texture;return a(g,d.mapping)}else{let g=d.image;if(g&&g.height>0){let x=new Lf(g.height);return x.fromEquirectangularTexture(n,d),e.set(d,x),d.addEventListener("dispose",l),a(x.texture,d.mapping)}else return null}}return d}function o(d){if(d&&d.isTexture){let h=d.mapping,g=h===jd||h===Wd,x=h===Xr||h===Us;if(g||x){let m=t.get(d),p=m!==void 0?m.texture.pmremVersion:0;if(d.isRenderTargetTexture&&d.pmremVersion!==p)return i===null&&(i=new Of(n)),m=g?i.fromEquirectangular(d,m):i.fromCubemap(d,m),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),m.texture;if(m!==void 0)return m.texture;{let E=d.image;return g&&E&&E.height>0||x&&E&&c(E)?(i===null&&(i=new Of(n)),m=g?i.fromEquirectangular(d):i.fromCubemap(d),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),d.addEventListener("dispose",u),m.texture):null}}}return d}function a(d,h){return h===jd?d.mapping=Xr:h===Wd&&(d.mapping=Us),d}function c(d){let h=0,g=6;for(let x=0;x<g;x++)d[x]!==void 0&&h++;return h===g}function l(d){let h=d.target;h.removeEventListener("dispose",l);let g=e.get(h);g!==void 0&&(e.delete(h),g.dispose())}function u(d){let h=d.target;h.removeEventListener("dispose",u);let g=t.get(h);g!==void 0&&(t.delete(h),g.dispose())}function f(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:f}}function vN(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Sd("WebGLRenderer: "+i+" extension not supported."),r}}}function _N(n,e,t,i){let r={},s=new WeakMap;function o(f){let d=f.target;d.index!==null&&e.remove(d.index);for(let g in d.attributes)e.remove(d.attributes[g]);d.removeEventListener("dispose",o),delete r[d.id];let h=s.get(d);h&&(e.remove(h),s.delete(d)),i.releaseStatesOfGeometry(d),d.isInstancedBufferGeometry===!0&&delete d._maxInstanceCount,t.memory.geometries--}function a(f,d){return r[d.id]===!0||(d.addEventListener("dispose",o),r[d.id]=!0,t.memory.geometries++),d}function c(f){let d=f.attributes;for(let h in d)e.update(d[h],n.ARRAY_BUFFER)}function l(f){let d=[],h=f.index,g=f.attributes.position,x=0;if(g===void 0)return;if(h!==null){let E=h.array;x=h.version;for(let S=0,b=E.length;S<b;S+=3){let I=E[S+0],w=E[S+1],C=E[S+2];d.push(I,w,w,C,C,I)}}else{let E=g.array;x=g.version;for(let S=0,b=E.length/3-1;S<b;S+=3){let I=S+0,w=S+1,C=S+2;d.push(I,w,w,C,C,I)}}let m=new(g.count>=65535?cc:ac)(d,1);m.version=x;let p=s.get(f);p&&e.remove(p),s.set(f,m)}function u(f){let d=s.get(f);if(d){let h=f.index;h!==null&&d.version<h.version&&l(f)}else l(f);return s.get(f)}return{get:a,update:c,getWireframeAttribute:u}}function xN(n,e,t){let i;function r(f){i=f}let s,o;function a(f){s=f.type,o=f.bytesPerElement}function c(f,d){n.drawElements(i,d,s,f*o),t.update(d,i,1)}function l(f,d,h){h!==0&&(n.drawElementsInstanced(i,d,s,f*o,h),t.update(d,i,h))}function u(f,d,h){if(h===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,d,0,s,f,0,h);let x=0;for(let m=0;m<h;m++)x+=d[m];t.update(x,i,1)}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u}function MN(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Ce("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function EN(n,e,t){let i=new WeakMap,r=new Lt;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,f=u!==void 0?u.length:0,d=i.get(a);if(d===void 0||d.count!==f){let O=function(){v.dispose(),i.delete(a),a.removeEventListener("dispose",O)};var h=O;d!==void 0&&d.texture.dispose();let g=a.morphAttributes.position!==void 0,x=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],E=a.morphAttributes.normal||[],S=a.morphAttributes.color||[],b=0;g===!0&&(b=1),x===!0&&(b=2),m===!0&&(b=3);let I=a.attributes.position.count*b,w=1;I>e.maxTextureSize&&(w=Math.ceil(I/e.maxTextureSize),I=e.maxTextureSize);let C=new Float32Array(I*w*4*f),v=new ic(C,I,w,f);v.type=yi,v.needsUpdate=!0;let T=b*4;for(let D=0;D<f;D++){let U=p[D],j=E[D],X=S[D],P=I*w*4*D;for(let z=0;z<U.count;z++){let k=z*T;g===!0&&(r.fromBufferAttribute(U,z),C[P+k+0]=r.x,C[P+k+1]=r.y,C[P+k+2]=r.z,C[P+k+3]=0),x===!0&&(r.fromBufferAttribute(j,z),C[P+k+4]=r.x,C[P+k+5]=r.y,C[P+k+6]=r.z,C[P+k+7]=0),m===!0&&(r.fromBufferAttribute(X,z),C[P+k+8]=r.x,C[P+k+9]=r.y,C[P+k+10]=r.z,C[P+k+11]=X.itemSize===4?r.w:1)}}d={count:f,texture:v,size:new ke(I,w)},i.set(a,d),a.addEventListener("dispose",O)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<l.length;m++)g+=l[m];let x=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",x),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",d.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",d.size)}return{update:s}}function bN(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,f=l.geometry,d=e.get(l,f);if(s.get(d)!==u&&(e.update(d),s.set(d,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return d}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var SN={[Hg]:"LINEAR_TONE_MAPPING",[zg]:"REINHARD_TONE_MAPPING",[Gg]:"CINEON_TONE_MAPPING",[jg]:"ACES_FILMIC_TONE_MAPPING",[$g]:"AGX_TONE_MAPPING",[qg]:"NEUTRAL_TONE_MAPPING",[Wg]:"CUSTOM_TONE_MAPPING"};function wN(n,e,t,i,r){let s=new Bn(e,t,{type:n,depthBuffer:i,stencilBuffer:r,depthTexture:i?new pr(e,t):void 0}),o=new Bn(e,t,{type:Bi,depthBuffer:!1,stencilBuffer:!1}),a=new Cn;a.setAttribute("position",new Kn([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new Kn([0,2,0,0,2,0],2));let c=new Rd({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),l=new Qn(a,c),u=new gc(-1,1,1,-1,0,1),f=null,d=null,h=!1,g,x=null,m=[],p=!1;this.setSize=function(E,S){s.setSize(E,S),o.setSize(E,S);for(let b=0;b<m.length;b++){let I=m[b];I.setSize&&I.setSize(E,S)}},this.setEffects=function(E){m=E,p=m.length>0&&m[0].isRenderPass===!0;let S=s.width,b=s.height;for(let I=0;I<m.length;I++){let w=m[I];w.setSize&&w.setSize(S,b)}},this.begin=function(E,S){if(h||E.toneMapping===mi&&m.length===0)return!1;if(x=S,S!==null){let b=S.width,I=S.height;(s.width!==b||s.height!==I)&&this.setSize(b,I)}return p===!1&&E.setRenderTarget(s),g=E.toneMapping,E.toneMapping=mi,!0},this.hasRenderPass=function(){return p},this.end=function(E,S){E.toneMapping=g,h=!0;let b=s,I=o;for(let w=0;w<m.length;w++){let C=m[w];if(C.enabled!==!1&&(C.render(E,I,b,S),C.needsSwap!==!1)){let v=b;b=I,I=v}}if(f!==E.outputColorSpace||d!==E.toneMapping){f=E.outputColorSpace,d=E.toneMapping,c.defines={},Ke.getTransfer(f)===st&&(c.defines.SRGB_TRANSFER="");let w=SN[d];w&&(c.defines[w]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=b.texture,E.setRenderTarget(x),E.render(l,u),x=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){s.depthTexture&&s.depthTexture.dispose(),s.dispose(),o.dispose(),a.dispose(),c.dispose()}}var GE=new gr,gy=new pr(1,1),jE=new ic,WE=new Dd,$E=new dc,SE=[],wE=[],TE=new Float32Array(16),DE=new Float32Array(9),CE=new Float32Array(4);function Jo(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=SE[r];if(s===void 0&&(s=new Float32Array(r),SE[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Yt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Zt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function Uf(n,e){let t=wE[e];t===void 0&&(t=new Int32Array(e),wE[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function TN(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function DN(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Yt(t,e))return;n.uniform2fv(this.addr,e),Zt(t,e)}}function CN(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Yt(t,e))return;n.uniform3fv(this.addr,e),Zt(t,e)}}function AN(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Yt(t,e))return;n.uniform4fv(this.addr,e),Zt(t,e)}}function IN(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Yt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Zt(t,e)}else{if(Yt(t,i))return;CE.set(i),n.uniformMatrix2fv(this.addr,!1,CE),Zt(t,i)}}function RN(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Yt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Zt(t,e)}else{if(Yt(t,i))return;DE.set(i),n.uniformMatrix3fv(this.addr,!1,DE),Zt(t,i)}}function NN(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Yt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Zt(t,e)}else{if(Yt(t,i))return;TE.set(i),n.uniformMatrix4fv(this.addr,!1,TE),Zt(t,i)}}function PN(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function ON(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Yt(t,e))return;n.uniform2iv(this.addr,e),Zt(t,e)}}function LN(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Yt(t,e))return;n.uniform3iv(this.addr,e),Zt(t,e)}}function FN(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Yt(t,e))return;n.uniform4iv(this.addr,e),Zt(t,e)}}function kN(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function UN(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Yt(t,e))return;n.uniform2uiv(this.addr,e),Zt(t,e)}}function BN(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Yt(t,e))return;n.uniform3uiv(this.addr,e),Zt(t,e)}}function VN(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Yt(t,e))return;n.uniform4uiv(this.addr,e),Zt(t,e)}}function HN(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(gy.compareFunction=t.isReversedDepthBuffer()?Rf:If,s=gy):s=GE,t.setTexture2D(e||s,r)}function zN(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||WE,r)}function GN(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||$E,r)}function jN(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||jE,r)}function WN(n){switch(n){case 5126:return TN;case 35664:return DN;case 35665:return CN;case 35666:return AN;case 35674:return IN;case 35675:return RN;case 35676:return NN;case 5124:case 35670:return PN;case 35667:case 35671:return ON;case 35668:case 35672:return LN;case 35669:case 35673:return FN;case 5125:return kN;case 36294:return UN;case 36295:return BN;case 36296:return VN;case 35678:case 36198:case 36298:case 36306:case 35682:return HN;case 35679:case 36299:case 36307:return zN;case 35680:case 36300:case 36308:case 36293:return GN;case 36289:case 36303:case 36311:case 36292:return jN}}function $N(n,e){n.uniform1fv(this.addr,e)}function qN(n,e){let t=Jo(e,this.size,2);n.uniform2fv(this.addr,t)}function XN(n,e){let t=Jo(e,this.size,3);n.uniform3fv(this.addr,t)}function YN(n,e){let t=Jo(e,this.size,4);n.uniform4fv(this.addr,t)}function ZN(n,e){let t=Jo(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function JN(n,e){let t=Jo(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function KN(n,e){let t=Jo(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function QN(n,e){n.uniform1iv(this.addr,e)}function eP(n,e){n.uniform2iv(this.addr,e)}function tP(n,e){n.uniform3iv(this.addr,e)}function nP(n,e){n.uniform4iv(this.addr,e)}function iP(n,e){n.uniform1uiv(this.addr,e)}function rP(n,e){n.uniform2uiv(this.addr,e)}function sP(n,e){n.uniform3uiv(this.addr,e)}function oP(n,e){n.uniform4uiv(this.addr,e)}function aP(n,e,t){let i=this.cache,r=e.length,s=Uf(t,r);Yt(i,s)||(n.uniform1iv(this.addr,s),Zt(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=gy:o=GE;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function cP(n,e,t){let i=this.cache,r=e.length,s=Uf(t,r);Yt(i,s)||(n.uniform1iv(this.addr,s),Zt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||WE,s[o])}function lP(n,e,t){let i=this.cache,r=e.length,s=Uf(t,r);Yt(i,s)||(n.uniform1iv(this.addr,s),Zt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||$E,s[o])}function uP(n,e,t){let i=this.cache,r=e.length,s=Uf(t,r);Yt(i,s)||(n.uniform1iv(this.addr,s),Zt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||jE,s[o])}function dP(n){switch(n){case 5126:return $N;case 35664:return qN;case 35665:return XN;case 35666:return YN;case 35674:return ZN;case 35675:return JN;case 35676:return KN;case 5124:case 35670:return QN;case 35667:case 35671:return eP;case 35668:case 35672:return tP;case 35669:case 35673:return nP;case 5125:return iP;case 36294:return rP;case 36295:return sP;case 36296:return oP;case 35678:case 36198:case 36298:case 36306:case 35682:return aP;case 35679:case 36299:case 36307:return cP;case 35680:case 36300:case 36308:case 36293:return lP;case 36289:case 36303:case 36311:case 36292:return uP}}var yy=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=WN(t.type)}},vy=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=dP(t.type)}},_y=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},py=/(\w+)(\])?(\[|\.)?/g;function AE(n,e){n.seq.push(e),n.map[e.id]=e}function fP(n,e,t){let i=n.name,r=i.length;for(py.lastIndex=0;;){let s=py.exec(i),o=py.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){AE(t,l===void 0?new yy(a,n,e):new vy(a,n,e));break}else{let f=t.map[a];f===void 0&&(f=new _y(a),AE(t,f)),t=f}}}var Zo=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);fP(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function IE(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var hP=37297,pP=0;function mP(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var RE=new Le;function gP(n){Ke._getMatrix(RE,Ke.workingColorSpace,n);let e=`mat3( ${RE.elements.map(t=>t.toFixed(4))} )`;switch(Ke.getTransfer(n)){case ec:return[e,"LinearTransferOETF"];case st:return[e,"sRGBTransferOETF"];default:return Te("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function NE(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+mP(n.getShaderSource(e),a)}else return s}function yP(n,e){let t=gP(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var vP={[Hg]:"Linear",[zg]:"Reinhard",[Gg]:"Cineon",[jg]:"ACESFilmic",[$g]:"AgX",[qg]:"Neutral",[Wg]:"Custom"};function _P(n,e){let t=vP[e];return t===void 0?(Te("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Pf=new F;function xP(){Ke.getLuminanceCoefficients(Pf);let n=Pf.x.toFixed(4),e=Pf.y.toFixed(4),t=Pf.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function MP(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Ic).join(`
`)}function EP(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function bP(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function Ic(n){return n!==""}function PE(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function OE(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var SP=/^[ \t]*#include +<([\w\d./]+)>/gm;function xy(n){return n.replace(SP,TP)}var wP=new Map;function TP(n,e){let t=je[e];if(t===void 0){let i=wP.get(e);if(i!==void 0)t=je[i],Te('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return xy(t)}var DP=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function LE(n){return n.replace(DP,CP)}function CP(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function FE(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var AP={[_c]:"SHADOWMAP_TYPE_PCF",[Wo]:"SHADOWMAP_TYPE_VSM"};function IP(n){return AP[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var RP={[Xr]:"ENVMAP_TYPE_CUBE",[Us]:"ENVMAP_TYPE_CUBE",[xc]:"ENVMAP_TYPE_CUBE_UV"};function NP(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":RP[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var PP={[Us]:"ENVMAP_MODE_REFRACTION"};function OP(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":PP[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var LP={[Vg]:"ENVMAP_BLENDING_MULTIPLY",[nE]:"ENVMAP_BLENDING_MIX",[iE]:"ENVMAP_BLENDING_ADD"};function FP(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":LP[n.combine]||"ENVMAP_BLENDING_NONE"}function kP(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function UP(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=IP(t),l=NP(t),u=OP(t),f=FP(t),d=kP(t),h=MP(t),g=EP(s),x=r.createProgram(),m,p,E=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Ic).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Ic).join(`
`),p.length>0&&(p+=`
`)):(m=[FE(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexNormals?"#define HAS_NORMAL":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Ic).join(`
`),p=[FE(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+f:"",d?"#define CUBEUV_TEXEL_WIDTH "+d.texelWidth:"",d?"#define CUBEUV_TEXEL_HEIGHT "+d.texelHeight:"",d?"#define CUBEUV_MAX_MIP "+d.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.packedNormalMap?"#define USE_PACKED_NORMALMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.numLightProbeGrids>0?"#define USE_LIGHT_PROBES_GRID":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==mi?"#define TONE_MAPPING":"",t.toneMapping!==mi?je.tonemapping_pars_fragment:"",t.toneMapping!==mi?_P("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",je.colorspace_pars_fragment,yP("linearToOutputTexel",t.outputColorSpace),xP(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Ic).join(`
`)),o=xy(o),o=PE(o,t),o=OE(o,t),a=xy(a),a=PE(a,t),a=OE(a,t),o=LE(o),a=LE(a),t.isRawShaderMaterial!==!0&&(E=`#version 300 es
`,m=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===ny?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===ny?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let S=E+m+o,b=E+p+a,I=IE(r,r.VERTEX_SHADER,S),w=IE(r,r.FRAGMENT_SHADER,b);r.attachShader(x,I),r.attachShader(x,w),t.index0AttributeName!==void 0?r.bindAttribLocation(x,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(x,0,"position"),r.linkProgram(x);function C(D){if(n.debug.checkShaderErrors){let U=r.getProgramInfoLog(x)||"",j=r.getShaderInfoLog(I)||"",X=r.getShaderInfoLog(w)||"",P=U.trim(),z=j.trim(),k=X.trim(),K=!0,Q=!0;if(r.getProgramParameter(x,r.LINK_STATUS)===!1)if(K=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,x,I,w);else{let le=NE(r,I,"vertex"),ve=NE(r,w,"fragment");Ce("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(x,r.VALIDATE_STATUS)+`

Material Name: `+D.name+`
Material Type: `+D.type+`

Program Info Log: `+P+`
`+le+`
`+ve)}else P!==""?Te("WebGLProgram: Program Info Log:",P):(z===""||k==="")&&(Q=!1);Q&&(D.diagnostics={runnable:K,programLog:P,vertexShader:{log:z,prefix:m},fragmentShader:{log:k,prefix:p}})}r.deleteShader(I),r.deleteShader(w),v=new Zo(r,x),T=bP(r,x)}let v;this.getUniforms=function(){return v===void 0&&C(this),v};let T;this.getAttributes=function(){return T===void 0&&C(this),T};let O=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return O===!1&&(O=r.getProgramParameter(x,hP)),O},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(x),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=pP++,this.cacheKey=e,this.usedTimes=1,this.program=x,this.vertexShader=I,this.fragmentShader=w,this}var BP=0,My=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Ey(e),t.set(e,i)),i}},Ey=class{constructor(e){this.id=BP++,this.code=e,this.usedTimes=0}};function VP(n){return n===Jr||n===Tc||n===Dc}function HP(n,e,t,i,r,s){let o=new Vo,a=new My,c=new Set,l=[],u=new Map,f=i.logarithmicDepthBuffer,d=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function g(v){return c.add(v),v===0?"uv":`uv${v}`}function x(v,T,O,D,U,j){let X=D.fog,P=U.geometry,z=v.isMeshStandardMaterial||v.isMeshLambertMaterial||v.isMeshPhongMaterial?D.environment:null,k=v.isMeshStandardMaterial||v.isMeshLambertMaterial&&!v.envMap||v.isMeshPhongMaterial&&!v.envMap,K=e.get(v.envMap||z,k),Q=K&&K.mapping===xc?K.image.height:null,le=h[v.type];v.precision!==null&&(d=i.getMaxPrecision(v.precision),d!==v.precision&&Te("WebGLProgram.getParameters:",v.precision,"not supported, using",d,"instead."));let ve=P.morphAttributes.position||P.morphAttributes.normal||P.morphAttributes.color,be=ve!==void 0?ve.length:0,tt=0;P.morphAttributes.position!==void 0&&(tt=1),P.morphAttributes.normal!==void 0&&(tt=2),P.morphAttributes.color!==void 0&&(tt=3);let at,Ve,Y,fe;if(le){let Ue=Hi[le];at=Ue.vertexShader,Ve=Ue.fragmentShader}else at=v.vertexShader,Ve=v.fragmentShader,a.update(v),Y=a.getVertexShaderID(v),fe=a.getFragmentShaderID(v);let ie=n.getRenderTarget(),Ae=n.state.buffers.depth.getReversed(),Fe=U.isInstancedMesh===!0,Re=U.isBatchedMesh===!0,wt=!!v.map,Ze=!!v.matcap,ct=!!K,Et=!!v.aoMap,Xe=!!v.lightMap,Gt=!!v.bumpMap,Tt=!!v.normalMap,In=!!v.displacementMap,R=!!v.emissiveMap,jt=!!v.metalnessMap,Je=!!v.roughnessMap,vt=v.anisotropy>0,ae=v.clearcoat>0,Dt=v.dispersion>0,M=v.iridescence>0,y=v.sheen>0,L=v.transmission>0,$=vt&&!!v.anisotropyMap,J=ae&&!!v.clearcoatMap,ee=ae&&!!v.clearcoatNormalMap,oe=ae&&!!v.clearcoatRoughnessMap,G=M&&!!v.iridescenceMap,q=M&&!!v.iridescenceThicknessMap,he=y&&!!v.sheenColorMap,ge=y&&!!v.sheenRoughnessMap,re=!!v.specularMap,te=!!v.specularColorMap,Ne=!!v.specularIntensityMap,He=L&&!!v.transmissionMap,it=L&&!!v.thicknessMap,A=!!v.gradientMap,ne=!!v.alphaMap,W=v.alphaTest>0,pe=!!v.alphaHash,se=!!v.extensions,Z=mi;v.toneMapped&&(ie===null||ie.isXRRenderTarget===!0)&&(Z=n.toneMapping);let xe={shaderID:le,shaderType:v.type,shaderName:v.name,vertexShader:at,fragmentShader:Ve,defines:v.defines,customVertexShaderID:Y,customFragmentShaderID:fe,isRawShaderMaterial:v.isRawShaderMaterial===!0,glslVersion:v.glslVersion,precision:d,batching:Re,batchingColor:Re&&U._colorsTexture!==null,instancing:Fe,instancingColor:Fe&&U.instanceColor!==null,instancingMorph:Fe&&U.morphTexture!==null,outputColorSpace:ie===null?n.outputColorSpace:ie.isXRRenderTarget===!0?ie.texture.colorSpace:Ke.workingColorSpace,alphaToCoverage:!!v.alphaToCoverage,map:wt,matcap:Ze,envMap:ct,envMapMode:ct&&K.mapping,envMapCubeUVHeight:Q,aoMap:Et,lightMap:Xe,bumpMap:Gt,normalMap:Tt,displacementMap:In,emissiveMap:R,normalMapObjectSpace:Tt&&v.normalMapType===oE,normalMapTangentSpace:Tt&&v.normalMapType===ty,packedNormalMap:Tt&&v.normalMapType===ty&&VP(v.normalMap.format),metalnessMap:jt,roughnessMap:Je,anisotropy:vt,anisotropyMap:$,clearcoat:ae,clearcoatMap:J,clearcoatNormalMap:ee,clearcoatRoughnessMap:oe,dispersion:Dt,iridescence:M,iridescenceMap:G,iridescenceThicknessMap:q,sheen:y,sheenColorMap:he,sheenRoughnessMap:ge,specularMap:re,specularColorMap:te,specularIntensityMap:Ne,transmission:L,transmissionMap:He,thicknessMap:it,gradientMap:A,opaque:v.transparent===!1&&v.blending===Ls&&v.alphaToCoverage===!1,alphaMap:ne,alphaTest:W,alphaHash:pe,combine:v.combine,mapUv:wt&&g(v.map.channel),aoMapUv:Et&&g(v.aoMap.channel),lightMapUv:Xe&&g(v.lightMap.channel),bumpMapUv:Gt&&g(v.bumpMap.channel),normalMapUv:Tt&&g(v.normalMap.channel),displacementMapUv:In&&g(v.displacementMap.channel),emissiveMapUv:R&&g(v.emissiveMap.channel),metalnessMapUv:jt&&g(v.metalnessMap.channel),roughnessMapUv:Je&&g(v.roughnessMap.channel),anisotropyMapUv:$&&g(v.anisotropyMap.channel),clearcoatMapUv:J&&g(v.clearcoatMap.channel),clearcoatNormalMapUv:ee&&g(v.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:oe&&g(v.clearcoatRoughnessMap.channel),iridescenceMapUv:G&&g(v.iridescenceMap.channel),iridescenceThicknessMapUv:q&&g(v.iridescenceThicknessMap.channel),sheenColorMapUv:he&&g(v.sheenColorMap.channel),sheenRoughnessMapUv:ge&&g(v.sheenRoughnessMap.channel),specularMapUv:re&&g(v.specularMap.channel),specularColorMapUv:te&&g(v.specularColorMap.channel),specularIntensityMapUv:Ne&&g(v.specularIntensityMap.channel),transmissionMapUv:He&&g(v.transmissionMap.channel),thicknessMapUv:it&&g(v.thicknessMap.channel),alphaMapUv:ne&&g(v.alphaMap.channel),vertexTangents:!!P.attributes.tangent&&(Tt||vt),vertexNormals:!!P.attributes.normal,vertexColors:v.vertexColors,vertexAlphas:v.vertexColors===!0&&!!P.attributes.color&&P.attributes.color.itemSize===4,pointsUvs:U.isPoints===!0&&!!P.attributes.uv&&(wt||ne),fog:!!X,useFog:v.fog===!0,fogExp2:!!X&&X.isFogExp2,flatShading:v.wireframe===!1&&(v.flatShading===!0||P.attributes.normal===void 0&&Tt===!1&&(v.isMeshLambertMaterial||v.isMeshPhongMaterial||v.isMeshStandardMaterial||v.isMeshPhysicalMaterial)),sizeAttenuation:v.sizeAttenuation===!0,logarithmicDepthBuffer:f,reversedDepthBuffer:Ae,skinning:U.isSkinnedMesh===!0,morphTargets:P.morphAttributes.position!==void 0,morphNormals:P.morphAttributes.normal!==void 0,morphColors:P.morphAttributes.color!==void 0,morphTargetsCount:be,morphTextureStride:tt,numDirLights:T.directional.length,numPointLights:T.point.length,numSpotLights:T.spot.length,numSpotLightMaps:T.spotLightMap.length,numRectAreaLights:T.rectArea.length,numHemiLights:T.hemi.length,numDirLightShadows:T.directionalShadowMap.length,numPointLightShadows:T.pointShadowMap.length,numSpotLightShadows:T.spotShadowMap.length,numSpotLightShadowsWithMaps:T.numSpotLightShadowsWithMaps,numLightProbes:T.numLightProbes,numLightProbeGrids:j.length,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:v.dithering,shadowMapEnabled:n.shadowMap.enabled&&O.length>0,shadowMapType:n.shadowMap.type,toneMapping:Z,decodeVideoTexture:wt&&v.map.isVideoTexture===!0&&Ke.getTransfer(v.map.colorSpace)===st,decodeVideoTextureEmissive:R&&v.emissiveMap.isVideoTexture===!0&&Ke.getTransfer(v.emissiveMap.colorSpace)===st,premultipliedAlpha:v.premultipliedAlpha,doubleSided:v.side===ki,flipSided:v.side===Sn,useDepthPacking:v.depthPacking>=0,depthPacking:v.depthPacking||0,index0AttributeName:v.index0AttributeName,extensionClipCullDistance:se&&v.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(se&&v.extensions.multiDraw===!0||Re)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:v.customProgramCacheKey()};return xe.vertexUv1s=c.has(1),xe.vertexUv2s=c.has(2),xe.vertexUv3s=c.has(3),c.clear(),xe}function m(v){let T=[];if(v.shaderID?T.push(v.shaderID):(T.push(v.customVertexShaderID),T.push(v.customFragmentShaderID)),v.defines!==void 0)for(let O in v.defines)T.push(O),T.push(v.defines[O]);return v.isRawShaderMaterial===!1&&(p(T,v),E(T,v),T.push(n.outputColorSpace)),T.push(v.customProgramCacheKey),T.join()}function p(v,T){v.push(T.precision),v.push(T.outputColorSpace),v.push(T.envMapMode),v.push(T.envMapCubeUVHeight),v.push(T.mapUv),v.push(T.alphaMapUv),v.push(T.lightMapUv),v.push(T.aoMapUv),v.push(T.bumpMapUv),v.push(T.normalMapUv),v.push(T.displacementMapUv),v.push(T.emissiveMapUv),v.push(T.metalnessMapUv),v.push(T.roughnessMapUv),v.push(T.anisotropyMapUv),v.push(T.clearcoatMapUv),v.push(T.clearcoatNormalMapUv),v.push(T.clearcoatRoughnessMapUv),v.push(T.iridescenceMapUv),v.push(T.iridescenceThicknessMapUv),v.push(T.sheenColorMapUv),v.push(T.sheenRoughnessMapUv),v.push(T.specularMapUv),v.push(T.specularColorMapUv),v.push(T.specularIntensityMapUv),v.push(T.transmissionMapUv),v.push(T.thicknessMapUv),v.push(T.combine),v.push(T.fogExp2),v.push(T.sizeAttenuation),v.push(T.morphTargetsCount),v.push(T.morphAttributeCount),v.push(T.numDirLights),v.push(T.numPointLights),v.push(T.numSpotLights),v.push(T.numSpotLightMaps),v.push(T.numHemiLights),v.push(T.numRectAreaLights),v.push(T.numDirLightShadows),v.push(T.numPointLightShadows),v.push(T.numSpotLightShadows),v.push(T.numSpotLightShadowsWithMaps),v.push(T.numLightProbes),v.push(T.shadowMapType),v.push(T.toneMapping),v.push(T.numClippingPlanes),v.push(T.numClipIntersection),v.push(T.depthPacking)}function E(v,T){o.disableAll(),T.instancing&&o.enable(0),T.instancingColor&&o.enable(1),T.instancingMorph&&o.enable(2),T.matcap&&o.enable(3),T.envMap&&o.enable(4),T.normalMapObjectSpace&&o.enable(5),T.normalMapTangentSpace&&o.enable(6),T.clearcoat&&o.enable(7),T.iridescence&&o.enable(8),T.alphaTest&&o.enable(9),T.vertexColors&&o.enable(10),T.vertexAlphas&&o.enable(11),T.vertexUv1s&&o.enable(12),T.vertexUv2s&&o.enable(13),T.vertexUv3s&&o.enable(14),T.vertexTangents&&o.enable(15),T.anisotropy&&o.enable(16),T.alphaHash&&o.enable(17),T.batching&&o.enable(18),T.dispersion&&o.enable(19),T.batchingColor&&o.enable(20),T.gradientMap&&o.enable(21),T.packedNormalMap&&o.enable(22),T.vertexNormals&&o.enable(23),v.push(o.mask),o.disableAll(),T.fog&&o.enable(0),T.useFog&&o.enable(1),T.flatShading&&o.enable(2),T.logarithmicDepthBuffer&&o.enable(3),T.reversedDepthBuffer&&o.enable(4),T.skinning&&o.enable(5),T.morphTargets&&o.enable(6),T.morphNormals&&o.enable(7),T.morphColors&&o.enable(8),T.premultipliedAlpha&&o.enable(9),T.shadowMapEnabled&&o.enable(10),T.doubleSided&&o.enable(11),T.flipSided&&o.enable(12),T.useDepthPacking&&o.enable(13),T.dithering&&o.enable(14),T.transmission&&o.enable(15),T.sheen&&o.enable(16),T.opaque&&o.enable(17),T.pointsUvs&&o.enable(18),T.decodeVideoTexture&&o.enable(19),T.decodeVideoTextureEmissive&&o.enable(20),T.alphaToCoverage&&o.enable(21),T.numLightProbeGrids>0&&o.enable(22),v.push(o.mask)}function S(v){let T=h[v.type],O;if(T){let D=Hi[T];O=vE.clone(D.uniforms)}else O=v.uniforms;return O}function b(v,T){let O=u.get(T);return O!==void 0?++O.usedTimes:(O=new UP(n,T,v,r),l.push(O),u.set(T,O)),O}function I(v){if(--v.usedTimes===0){let T=l.indexOf(v);l[T]=l[l.length-1],l.pop(),u.delete(v.cacheKey),v.destroy()}}function w(v){a.remove(v)}function C(){a.dispose()}return{getParameters:x,getProgramCacheKey:m,getUniforms:S,acquireProgram:b,releaseProgram:I,releaseShaderCache:w,programs:l,dispose:C}}function zP(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function GP(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function kE(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function UE(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d){let h=0;return d.isInstancedMesh&&(h+=2),d.isSkinnedMesh&&(h+=1),h}function a(d,h,g,x,m,p){let E=n[e];return E===void 0?(E={id:d.id,object:d,geometry:h,material:g,materialVariant:o(d),groupOrder:x,renderOrder:d.renderOrder,z:m,group:p},n[e]=E):(E.id=d.id,E.object=d,E.geometry=h,E.material=g,E.materialVariant=o(d),E.groupOrder=x,E.renderOrder=d.renderOrder,E.z=m,E.group=p),e++,E}function c(d,h,g,x,m,p){let E=a(d,h,g,x,m,p);g.transmission>0?i.push(E):g.transparent===!0?r.push(E):t.push(E)}function l(d,h,g,x,m,p){let E=a(d,h,g,x,m,p);g.transmission>0?i.unshift(E):g.transparent===!0?r.unshift(E):t.unshift(E)}function u(d,h){t.length>1&&t.sort(d||GP),i.length>1&&i.sort(h||kE),r.length>1&&r.sort(h||kE)}function f(){for(let d=e,h=n.length;d<h;d++){let g=n[d];if(g.id===null)break;g.id=null,g.object=null,g.geometry=null,g.material=null,g.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:f,sort:u}}function jP(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new UE,n.set(i,[o])):r>=s.length?(o=new UE,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function WP(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new F,color:new Ge};break;case"SpotLight":t={position:new F,direction:new F,color:new Ge,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new F,color:new Ge,distance:0,decay:0};break;case"HemisphereLight":t={direction:new F,skyColor:new Ge,groundColor:new Ge};break;case"RectAreaLight":t={color:new Ge,position:new F,halfWidth:new F,halfHeight:new F};break}return n[e.id]=t,t}}}function $P(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ke};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ke};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ke,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var qP=0;function XP(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function YP(n){let e=new WP,t=$P(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new F);let r=new F,s=new Ot,o=new Ot;function a(l){let u=0,f=0,d=0;for(let T=0;T<9;T++)i.probe[T].set(0,0,0);let h=0,g=0,x=0,m=0,p=0,E=0,S=0,b=0,I=0,w=0,C=0;l.sort(XP);for(let T=0,O=l.length;T<O;T++){let D=l[T],U=D.color,j=D.intensity,X=D.distance,P=null;if(D.shadow&&D.shadow.map&&(D.shadow.map.texture.format===Jr?P=D.shadow.map.texture:P=D.shadow.map.depthTexture||D.shadow.map.texture),D.isAmbientLight)u+=U.r*j,f+=U.g*j,d+=U.b*j;else if(D.isLightProbe){for(let z=0;z<9;z++)i.probe[z].addScaledVector(D.sh.coefficients[z],j);C++}else if(D.isDirectionalLight){let z=e.get(D);if(z.color.copy(D.color).multiplyScalar(D.intensity),D.castShadow){let k=D.shadow,K=t.get(D);K.shadowIntensity=k.intensity,K.shadowBias=k.bias,K.shadowNormalBias=k.normalBias,K.shadowRadius=k.radius,K.shadowMapSize=k.mapSize,i.directionalShadow[h]=K,i.directionalShadowMap[h]=P,i.directionalShadowMatrix[h]=D.shadow.matrix,E++}i.directional[h]=z,h++}else if(D.isSpotLight){let z=e.get(D);z.position.setFromMatrixPosition(D.matrixWorld),z.color.copy(U).multiplyScalar(j),z.distance=X,z.coneCos=Math.cos(D.angle),z.penumbraCos=Math.cos(D.angle*(1-D.penumbra)),z.decay=D.decay,i.spot[x]=z;let k=D.shadow;if(D.map&&(i.spotLightMap[I]=D.map,I++,k.updateMatrices(D),D.castShadow&&w++),i.spotLightMatrix[x]=k.matrix,D.castShadow){let K=t.get(D);K.shadowIntensity=k.intensity,K.shadowBias=k.bias,K.shadowNormalBias=k.normalBias,K.shadowRadius=k.radius,K.shadowMapSize=k.mapSize,i.spotShadow[x]=K,i.spotShadowMap[x]=P,b++}x++}else if(D.isRectAreaLight){let z=e.get(D);z.color.copy(U).multiplyScalar(j),z.halfWidth.set(D.width*.5,0,0),z.halfHeight.set(0,D.height*.5,0),i.rectArea[m]=z,m++}else if(D.isPointLight){let z=e.get(D);if(z.color.copy(D.color).multiplyScalar(D.intensity),z.distance=D.distance,z.decay=D.decay,D.castShadow){let k=D.shadow,K=t.get(D);K.shadowIntensity=k.intensity,K.shadowBias=k.bias,K.shadowNormalBias=k.normalBias,K.shadowRadius=k.radius,K.shadowMapSize=k.mapSize,K.shadowCameraNear=k.camera.near,K.shadowCameraFar=k.camera.far,i.pointShadow[g]=K,i.pointShadowMap[g]=P,i.pointShadowMatrix[g]=D.shadow.matrix,S++}i.point[g]=z,g++}else if(D.isHemisphereLight){let z=e.get(D);z.skyColor.copy(D.color).multiplyScalar(j),z.groundColor.copy(D.groundColor).multiplyScalar(j),i.hemi[p]=z,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=ce.LTC_FLOAT_1,i.rectAreaLTC2=ce.LTC_FLOAT_2):(i.rectAreaLTC1=ce.LTC_HALF_1,i.rectAreaLTC2=ce.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=f,i.ambient[2]=d;let v=i.hash;(v.directionalLength!==h||v.pointLength!==g||v.spotLength!==x||v.rectAreaLength!==m||v.hemiLength!==p||v.numDirectionalShadows!==E||v.numPointShadows!==S||v.numSpotShadows!==b||v.numSpotMaps!==I||v.numLightProbes!==C)&&(i.directional.length=h,i.spot.length=x,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=E,i.directionalShadowMap.length=E,i.pointShadow.length=S,i.pointShadowMap.length=S,i.spotShadow.length=b,i.spotShadowMap.length=b,i.directionalShadowMatrix.length=E,i.pointShadowMatrix.length=S,i.spotLightMatrix.length=b+I-w,i.spotLightMap.length=I,i.numSpotLightShadowsWithMaps=w,i.numLightProbes=C,v.directionalLength=h,v.pointLength=g,v.spotLength=x,v.rectAreaLength=m,v.hemiLength=p,v.numDirectionalShadows=E,v.numPointShadows=S,v.numSpotShadows=b,v.numSpotMaps=I,v.numLightProbes=C,i.version=qP++)}function c(l,u){let f=0,d=0,h=0,g=0,x=0,m=u.matrixWorldInverse;for(let p=0,E=l.length;p<E;p++){let S=l[p];if(S.isDirectionalLight){let b=i.directional[f];b.direction.setFromMatrixPosition(S.matrixWorld),r.setFromMatrixPosition(S.target.matrixWorld),b.direction.sub(r),b.direction.transformDirection(m),f++}else if(S.isSpotLight){let b=i.spot[h];b.position.setFromMatrixPosition(S.matrixWorld),b.position.applyMatrix4(m),b.direction.setFromMatrixPosition(S.matrixWorld),r.setFromMatrixPosition(S.target.matrixWorld),b.direction.sub(r),b.direction.transformDirection(m),h++}else if(S.isRectAreaLight){let b=i.rectArea[g];b.position.setFromMatrixPosition(S.matrixWorld),b.position.applyMatrix4(m),o.identity(),s.copy(S.matrixWorld),s.premultiply(m),o.extractRotation(s),b.halfWidth.set(S.width*.5,0,0),b.halfHeight.set(0,S.height*.5,0),b.halfWidth.applyMatrix4(o),b.halfHeight.applyMatrix4(o),g++}else if(S.isPointLight){let b=i.point[d];b.position.setFromMatrixPosition(S.matrixWorld),b.position.applyMatrix4(m),d++}else if(S.isHemisphereLight){let b=i.hemi[x];b.direction.setFromMatrixPosition(S.matrixWorld),b.direction.transformDirection(m),x++}}}return{setup:a,setupView:c,state:i}}function BE(n){let e=new YP(n),t=[],i=[],r=[];function s(d){f.camera=d,t.length=0,i.length=0,r.length=0}function o(d){t.push(d)}function a(d){i.push(d)}function c(d){r.push(d)}function l(){e.setup(t)}function u(d){e.setupView(t,d)}let f={lightsArray:t,shadowsArray:i,lightProbeGridArray:r,camera:null,lights:e,transmissionRenderTarget:{},textureUnits:0};return{init:s,state:f,setupLights:l,setupLightsView:u,pushLight:o,pushShadow:a,pushLightProbeGrid:c}}function ZP(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new BE(n),e.set(r,[a])):s>=o.length?(a=new BE(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var JP=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,KP=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,QP=[new F(1,0,0),new F(-1,0,0),new F(0,1,0),new F(0,-1,0),new F(0,0,1),new F(0,0,-1)],eO=[new F(0,-1,0),new F(0,-1,0),new F(0,0,1),new F(0,0,-1),new F(0,-1,0),new F(0,-1,0)],VE=new Ot,Ac=new F,my=new F;function tO(n,e,t){let i=new uc,r=new ke,s=new ke,o=new Lt,a=new Nd,c=new Pd,l={},u=t.maxTextureSize,f={[hr]:Sn,[Sn]:hr,[ki]:ki},d=new mn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new ke},radius:{value:4}},vertexShader:JP,fragmentShader:KP}),h=d.clone();h.defines.HORIZONTAL_PASS=1;let g=new Cn;g.setAttribute("position",new Xt(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let x=new Qn(g,d),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=_c;let p=this.type;this.render=function(w,C,v){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||w.length===0)return;this.type===kM&&(Te("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=_c);let T=n.getRenderTarget(),O=n.getActiveCubeFace(),D=n.getActiveMipmapLevel(),U=n.state;U.setBlending(Ui),U.buffers.depth.getReversed()===!0?U.buffers.color.setClear(0,0,0,0):U.buffers.color.setClear(1,1,1,1),U.buffers.depth.setTest(!0),U.setScissorTest(!1);let j=p!==this.type;j&&C.traverse(function(X){X.material&&(Array.isArray(X.material)?X.material.forEach(P=>P.needsUpdate=!0):X.material.needsUpdate=!0)});for(let X=0,P=w.length;X<P;X++){let z=w[X],k=z.shadow;if(k===void 0){Te("WebGLShadowMap:",z,"has no shadow.");continue}if(k.autoUpdate===!1&&k.needsUpdate===!1)continue;r.copy(k.mapSize);let K=k.getFrameExtents();r.multiply(K),s.copy(k.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/K.x),r.x=s.x*K.x,k.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/K.y),r.y=s.y*K.y,k.mapSize.y=s.y));let Q=n.state.buffers.depth.getReversed();if(k.camera._reversedDepth=Q,k.map===null||j===!0){if(k.map!==null&&(k.map.depthTexture!==null&&(k.map.depthTexture.dispose(),k.map.depthTexture=null),k.map.dispose()),this.type===Wo){if(z.isPointLight){Te("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}k.map=new Bn(r.x,r.y,{format:Jr,type:Bi,minFilter:en,magFilter:en,generateMipmaps:!1}),k.map.texture.name=z.name+".shadowMap",k.map.depthTexture=new pr(r.x,r.y,yi),k.map.depthTexture.name=z.name+".shadowMapDepth",k.map.depthTexture.format=Fi,k.map.depthTexture.compareFunction=null,k.map.depthTexture.minFilter=Qt,k.map.depthTexture.magFilter=Qt}else z.isPointLight?(k.map=new Lf(r.x),k.map.depthTexture=new Id(r.x,gi)):(k.map=new Bn(r.x,r.y),k.map.depthTexture=new pr(r.x,r.y,gi)),k.map.depthTexture.name=z.name+".shadowMap",k.map.depthTexture.format=Fi,this.type===_c?(k.map.depthTexture.compareFunction=Q?Rf:If,k.map.depthTexture.minFilter=en,k.map.depthTexture.magFilter=en):(k.map.depthTexture.compareFunction=null,k.map.depthTexture.minFilter=Qt,k.map.depthTexture.magFilter=Qt);k.camera.updateProjectionMatrix()}let le=k.map.isWebGLCubeRenderTarget?6:1;for(let ve=0;ve<le;ve++){if(k.map.isWebGLCubeRenderTarget)n.setRenderTarget(k.map,ve),n.clear();else{ve===0&&(n.setRenderTarget(k.map),n.clear());let be=k.getViewport(ve);o.set(s.x*be.x,s.y*be.y,s.x*be.z,s.y*be.w),U.viewport(o)}if(z.isPointLight){let be=k.camera,tt=k.matrix,at=z.distance||be.far;at!==be.far&&(be.far=at,be.updateProjectionMatrix()),Ac.setFromMatrixPosition(z.matrixWorld),be.position.copy(Ac),my.copy(be.position),my.add(QP[ve]),be.up.copy(eO[ve]),be.lookAt(my),be.updateMatrixWorld(),tt.makeTranslation(-Ac.x,-Ac.y,-Ac.z),VE.multiplyMatrices(be.projectionMatrix,be.matrixWorldInverse),k._frustum.setFromProjectionMatrix(VE,be.coordinateSystem,be.reversedDepth)}else k.updateMatrices(z);i=k.getFrustum(),b(C,v,k.camera,z,this.type)}k.isPointLightShadow!==!0&&this.type===Wo&&E(k,v),k.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(T,O,D)};function E(w,C){let v=e.update(x);d.defines.VSM_SAMPLES!==w.blurSamples&&(d.defines.VSM_SAMPLES=w.blurSamples,h.defines.VSM_SAMPLES=w.blurSamples,d.needsUpdate=!0,h.needsUpdate=!0),w.mapPass===null&&(w.mapPass=new Bn(r.x,r.y,{format:Jr,type:Bi})),d.uniforms.shadow_pass.value=w.map.depthTexture,d.uniforms.resolution.value=w.mapSize,d.uniforms.radius.value=w.radius,n.setRenderTarget(w.mapPass),n.clear(),n.renderBufferDirect(C,null,v,d,x,null),h.uniforms.shadow_pass.value=w.mapPass.texture,h.uniforms.resolution.value=w.mapSize,h.uniforms.radius.value=w.radius,n.setRenderTarget(w.map),n.clear(),n.renderBufferDirect(C,null,v,h,x,null)}function S(w,C,v,T){let O=null,D=v.isPointLight===!0?w.customDistanceMaterial:w.customDepthMaterial;if(D!==void 0)O=D;else if(O=v.isPointLight===!0?c:a,n.localClippingEnabled&&C.clipShadows===!0&&Array.isArray(C.clippingPlanes)&&C.clippingPlanes.length!==0||C.displacementMap&&C.displacementScale!==0||C.alphaMap&&C.alphaTest>0||C.map&&C.alphaTest>0||C.alphaToCoverage===!0){let U=O.uuid,j=C.uuid,X=l[U];X===void 0&&(X={},l[U]=X);let P=X[j];P===void 0&&(P=O.clone(),X[j]=P,C.addEventListener("dispose",I)),O=P}if(O.visible=C.visible,O.wireframe=C.wireframe,T===Wo?O.side=C.shadowSide!==null?C.shadowSide:C.side:O.side=C.shadowSide!==null?C.shadowSide:f[C.side],O.alphaMap=C.alphaMap,O.alphaTest=C.alphaToCoverage===!0?.5:C.alphaTest,O.map=C.map,O.clipShadows=C.clipShadows,O.clippingPlanes=C.clippingPlanes,O.clipIntersection=C.clipIntersection,O.displacementMap=C.displacementMap,O.displacementScale=C.displacementScale,O.displacementBias=C.displacementBias,O.wireframeLinewidth=C.wireframeLinewidth,O.linewidth=C.linewidth,v.isPointLight===!0&&O.isMeshDistanceMaterial===!0){let U=n.properties.get(O);U.light=v}return O}function b(w,C,v,T,O){if(w.visible===!1)return;if(w.layers.test(C.layers)&&(w.isMesh||w.isLine||w.isPoints)&&(w.castShadow||w.receiveShadow&&O===Wo)&&(!w.frustumCulled||i.intersectsObject(w))){w.modelViewMatrix.multiplyMatrices(v.matrixWorldInverse,w.matrixWorld);let j=e.update(w),X=w.material;if(Array.isArray(X)){let P=j.groups;for(let z=0,k=P.length;z<k;z++){let K=P[z],Q=X[K.materialIndex];if(Q&&Q.visible){let le=S(w,Q,T,O);w.onBeforeShadow(n,w,C,v,j,le,K),n.renderBufferDirect(v,null,j,le,w,K),w.onAfterShadow(n,w,C,v,j,le,K)}}}else if(X.visible){let P=S(w,X,T,O);w.onBeforeShadow(n,w,C,v,j,P,null),n.renderBufferDirect(v,null,j,P,w,null),w.onAfterShadow(n,w,C,v,j,P,null)}}let U=w.children;for(let j=0,X=U.length;j<X;j++)b(U[j],C,v,T,O)}function I(w){w.target.removeEventListener("dispose",I);for(let v in l){let T=l[v],O=w.target.uuid;O in T&&(T[O].dispose(),delete T[O])}}}function nO(n,e){function t(){let A=!1,ne=new Lt,W=null,pe=new Lt(0,0,0,0);return{setMask:function(se){W!==se&&!A&&(n.colorMask(se,se,se,se),W=se)},setLocked:function(se){A=se},setClear:function(se,Z,xe,Ue,Ft){Ft===!0&&(se*=Ue,Z*=Ue,xe*=Ue),ne.set(se,Z,xe,Ue),pe.equals(ne)===!1&&(n.clearColor(se,Z,xe,Ue),pe.copy(ne))},reset:function(){A=!1,W=null,pe.set(-1,0,0,0)}}}function i(){let A=!1,ne=!1,W=null,pe=null,se=null;return{setReversed:function(Z){if(ne!==Z){let xe=e.get("EXT_clip_control");Z?xe.clipControlEXT(xe.LOWER_LEFT_EXT,xe.ZERO_TO_ONE_EXT):xe.clipControlEXT(xe.LOWER_LEFT_EXT,xe.NEGATIVE_ONE_TO_ONE_EXT),ne=Z;let Ue=se;se=null,this.setClear(Ue)}},getReversed:function(){return ne},setTest:function(Z){Z?ie(n.DEPTH_TEST):Ae(n.DEPTH_TEST)},setMask:function(Z){W!==Z&&!A&&(n.depthMask(Z),W=Z)},setFunc:function(Z){if(ne&&(Z=gE[Z]),pe!==Z){switch(Z){case pd:n.depthFunc(n.NEVER);break;case md:n.depthFunc(n.ALWAYS);break;case gd:n.depthFunc(n.LESS);break;case Fs:n.depthFunc(n.LEQUAL);break;case yd:n.depthFunc(n.EQUAL);break;case vd:n.depthFunc(n.GEQUAL);break;case _d:n.depthFunc(n.GREATER);break;case xd:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}pe=Z}},setLocked:function(Z){A=Z},setClear:function(Z){se!==Z&&(se=Z,ne&&(Z=1-Z),n.clearDepth(Z))},reset:function(){A=!1,W=null,pe=null,se=null,ne=!1}}}function r(){let A=!1,ne=null,W=null,pe=null,se=null,Z=null,xe=null,Ue=null,Ft=null;return{setTest:function(lt){A||(lt?ie(n.STENCIL_TEST):Ae(n.STENCIL_TEST))},setMask:function(lt){ne!==lt&&!A&&(n.stencilMask(lt),ne=lt)},setFunc:function(lt,zi,vi){(W!==lt||pe!==zi||se!==vi)&&(n.stencilFunc(lt,zi,vi),W=lt,pe=zi,se=vi)},setOp:function(lt,zi,vi){(Z!==lt||xe!==zi||Ue!==vi)&&(n.stencilOp(lt,zi,vi),Z=lt,xe=zi,Ue=vi)},setLocked:function(lt){A=lt},setClear:function(lt){Ft!==lt&&(n.clearStencil(lt),Ft=lt)},reset:function(){A=!1,ne=null,W=null,pe=null,se=null,Z=null,xe=null,Ue=null,Ft=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},f={},d={},h=new WeakMap,g=[],x=null,m=!1,p=null,E=null,S=null,b=null,I=null,w=null,C=null,v=new Ge(0,0,0),T=0,O=!1,D=null,U=null,j=null,X=null,P=null,z=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),k=!1,K=0,Q=n.getParameter(n.VERSION);Q.indexOf("WebGL")!==-1?(K=parseFloat(/^WebGL (\d)/.exec(Q)[1]),k=K>=1):Q.indexOf("OpenGL ES")!==-1&&(K=parseFloat(/^OpenGL ES (\d)/.exec(Q)[1]),k=K>=2);let le=null,ve={},be=n.getParameter(n.SCISSOR_BOX),tt=n.getParameter(n.VIEWPORT),at=new Lt().fromArray(be),Ve=new Lt().fromArray(tt);function Y(A,ne,W,pe){let se=new Uint8Array(4),Z=n.createTexture();n.bindTexture(A,Z),n.texParameteri(A,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(A,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let xe=0;xe<W;xe++)A===n.TEXTURE_3D||A===n.TEXTURE_2D_ARRAY?n.texImage3D(ne,0,n.RGBA,1,1,pe,0,n.RGBA,n.UNSIGNED_BYTE,se):n.texImage2D(ne+xe,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,se);return Z}let fe={};fe[n.TEXTURE_2D]=Y(n.TEXTURE_2D,n.TEXTURE_2D,1),fe[n.TEXTURE_CUBE_MAP]=Y(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),fe[n.TEXTURE_2D_ARRAY]=Y(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),fe[n.TEXTURE_3D]=Y(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ie(n.DEPTH_TEST),o.setFunc(Fs),Gt(!1),Tt(Fg),ie(n.CULL_FACE),Et(Ui);function ie(A){u[A]!==!0&&(n.enable(A),u[A]=!0)}function Ae(A){u[A]!==!1&&(n.disable(A),u[A]=!1)}function Fe(A,ne){return d[A]!==ne?(n.bindFramebuffer(A,ne),d[A]=ne,A===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=ne),A===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=ne),!0):!1}function Re(A,ne){let W=g,pe=!1;if(A){W=h.get(ne),W===void 0&&(W=[],h.set(ne,W));let se=A.textures;if(W.length!==se.length||W[0]!==n.COLOR_ATTACHMENT0){for(let Z=0,xe=se.length;Z<xe;Z++)W[Z]=n.COLOR_ATTACHMENT0+Z;W.length=se.length,pe=!0}}else W[0]!==n.BACK&&(W[0]=n.BACK,pe=!0);pe&&n.drawBuffers(W)}function wt(A){return x!==A?(n.useProgram(A),x=A,!0):!1}let Ze={[Br]:n.FUNC_ADD,[BM]:n.FUNC_SUBTRACT,[VM]:n.FUNC_REVERSE_SUBTRACT};Ze[HM]=n.MIN,Ze[zM]=n.MAX;let ct={[GM]:n.ZERO,[jM]:n.ONE,[WM]:n.SRC_COLOR,[fd]:n.SRC_ALPHA,[JM]:n.SRC_ALPHA_SATURATE,[YM]:n.DST_COLOR,[qM]:n.DST_ALPHA,[$M]:n.ONE_MINUS_SRC_COLOR,[hd]:n.ONE_MINUS_SRC_ALPHA,[ZM]:n.ONE_MINUS_DST_COLOR,[XM]:n.ONE_MINUS_DST_ALPHA,[KM]:n.CONSTANT_COLOR,[QM]:n.ONE_MINUS_CONSTANT_COLOR,[eE]:n.CONSTANT_ALPHA,[tE]:n.ONE_MINUS_CONSTANT_ALPHA};function Et(A,ne,W,pe,se,Z,xe,Ue,Ft,lt){if(A===Ui){m===!0&&(Ae(n.BLEND),m=!1);return}if(m===!1&&(ie(n.BLEND),m=!0),A!==UM){if(A!==p||lt!==O){if((E!==Br||I!==Br)&&(n.blendEquation(n.FUNC_ADD),E=Br,I=Br),lt)switch(A){case Ls:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case kg:n.blendFunc(n.ONE,n.ONE);break;case Ug:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Bg:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Ce("WebGLState: Invalid blending: ",A);break}else switch(A){case Ls:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case kg:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case Ug:Ce("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Bg:Ce("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Ce("WebGLState: Invalid blending: ",A);break}S=null,b=null,w=null,C=null,v.set(0,0,0),T=0,p=A,O=lt}return}se=se||ne,Z=Z||W,xe=xe||pe,(ne!==E||se!==I)&&(n.blendEquationSeparate(Ze[ne],Ze[se]),E=ne,I=se),(W!==S||pe!==b||Z!==w||xe!==C)&&(n.blendFuncSeparate(ct[W],ct[pe],ct[Z],ct[xe]),S=W,b=pe,w=Z,C=xe),(Ue.equals(v)===!1||Ft!==T)&&(n.blendColor(Ue.r,Ue.g,Ue.b,Ft),v.copy(Ue),T=Ft),p=A,O=!1}function Xe(A,ne){A.side===ki?Ae(n.CULL_FACE):ie(n.CULL_FACE);let W=A.side===Sn;ne&&(W=!W),Gt(W),A.blending===Ls&&A.transparent===!1?Et(Ui):Et(A.blending,A.blendEquation,A.blendSrc,A.blendDst,A.blendEquationAlpha,A.blendSrcAlpha,A.blendDstAlpha,A.blendColor,A.blendAlpha,A.premultipliedAlpha),o.setFunc(A.depthFunc),o.setTest(A.depthTest),o.setMask(A.depthWrite),s.setMask(A.colorWrite);let pe=A.stencilWrite;a.setTest(pe),pe&&(a.setMask(A.stencilWriteMask),a.setFunc(A.stencilFunc,A.stencilRef,A.stencilFuncMask),a.setOp(A.stencilFail,A.stencilZFail,A.stencilZPass)),R(A.polygonOffset,A.polygonOffsetFactor,A.polygonOffsetUnits),A.alphaToCoverage===!0?ie(n.SAMPLE_ALPHA_TO_COVERAGE):Ae(n.SAMPLE_ALPHA_TO_COVERAGE)}function Gt(A){D!==A&&(A?n.frontFace(n.CW):n.frontFace(n.CCW),D=A)}function Tt(A){A!==LM?(ie(n.CULL_FACE),A!==U&&(A===Fg?n.cullFace(n.BACK):A===FM?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Ae(n.CULL_FACE),U=A}function In(A){A!==j&&(k&&n.lineWidth(A),j=A)}function R(A,ne,W){A?(ie(n.POLYGON_OFFSET_FILL),(X!==ne||P!==W)&&(X=ne,P=W,o.getReversed()&&(ne=-ne),n.polygonOffset(ne,W))):Ae(n.POLYGON_OFFSET_FILL)}function jt(A){A?ie(n.SCISSOR_TEST):Ae(n.SCISSOR_TEST)}function Je(A){A===void 0&&(A=n.TEXTURE0+z-1),le!==A&&(n.activeTexture(A),le=A)}function vt(A,ne,W){W===void 0&&(le===null?W=n.TEXTURE0+z-1:W=le);let pe=ve[W];pe===void 0&&(pe={type:void 0,texture:void 0},ve[W]=pe),(pe.type!==A||pe.texture!==ne)&&(le!==W&&(n.activeTexture(W),le=W),n.bindTexture(A,ne||fe[A]),pe.type=A,pe.texture=ne)}function ae(){let A=ve[le];A!==void 0&&A.type!==void 0&&(n.bindTexture(A.type,null),A.type=void 0,A.texture=void 0)}function Dt(){try{n.compressedTexImage2D(...arguments)}catch(A){Ce("WebGLState:",A)}}function M(){try{n.compressedTexImage3D(...arguments)}catch(A){Ce("WebGLState:",A)}}function y(){try{n.texSubImage2D(...arguments)}catch(A){Ce("WebGLState:",A)}}function L(){try{n.texSubImage3D(...arguments)}catch(A){Ce("WebGLState:",A)}}function $(){try{n.compressedTexSubImage2D(...arguments)}catch(A){Ce("WebGLState:",A)}}function J(){try{n.compressedTexSubImage3D(...arguments)}catch(A){Ce("WebGLState:",A)}}function ee(){try{n.texStorage2D(...arguments)}catch(A){Ce("WebGLState:",A)}}function oe(){try{n.texStorage3D(...arguments)}catch(A){Ce("WebGLState:",A)}}function G(){try{n.texImage2D(...arguments)}catch(A){Ce("WebGLState:",A)}}function q(){try{n.texImage3D(...arguments)}catch(A){Ce("WebGLState:",A)}}function he(A){return f[A]!==void 0?f[A]:n.getParameter(A)}function ge(A,ne){f[A]!==ne&&(n.pixelStorei(A,ne),f[A]=ne)}function re(A){at.equals(A)===!1&&(n.scissor(A.x,A.y,A.z,A.w),at.copy(A))}function te(A){Ve.equals(A)===!1&&(n.viewport(A.x,A.y,A.z,A.w),Ve.copy(A))}function Ne(A,ne){let W=l.get(ne);W===void 0&&(W=new WeakMap,l.set(ne,W));let pe=W.get(A);pe===void 0&&(pe=n.getUniformBlockIndex(ne,A.name),W.set(A,pe))}function He(A,ne){let pe=l.get(ne).get(A);c.get(ne)!==pe&&(n.uniformBlockBinding(ne,pe,A.__bindingPointIndex),c.set(ne,pe))}function it(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),n.pixelStorei(n.PACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,!1),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,!1),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,n.BROWSER_DEFAULT_WEBGL),n.pixelStorei(n.PACK_ROW_LENGTH,0),n.pixelStorei(n.PACK_SKIP_PIXELS,0),n.pixelStorei(n.PACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_ROW_LENGTH,0),n.pixelStorei(n.UNPACK_IMAGE_HEIGHT,0),n.pixelStorei(n.UNPACK_SKIP_PIXELS,0),n.pixelStorei(n.UNPACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_SKIP_IMAGES,0),u={},f={},le=null,ve={},d={},h=new WeakMap,g=[],x=null,m=!1,p=null,E=null,S=null,b=null,I=null,w=null,C=null,v=new Ge(0,0,0),T=0,O=!1,D=null,U=null,j=null,X=null,P=null,at.set(0,0,n.canvas.width,n.canvas.height),Ve.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ie,disable:Ae,bindFramebuffer:Fe,drawBuffers:Re,useProgram:wt,setBlending:Et,setMaterial:Xe,setFlipSided:Gt,setCullFace:Tt,setLineWidth:In,setPolygonOffset:R,setScissorTest:jt,activeTexture:Je,bindTexture:vt,unbindTexture:ae,compressedTexImage2D:Dt,compressedTexImage3D:M,texImage2D:G,texImage3D:q,pixelStorei:ge,getParameter:he,updateUBOMapping:Ne,uniformBlockBinding:He,texStorage2D:ee,texStorage3D:oe,texSubImage2D:y,texSubImage3D:L,compressedTexSubImage2D:$,compressedTexSubImage3D:J,scissor:re,viewport:te,reset:it}}function iO(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new ke,u=new WeakMap,f=new Set,d,h=new WeakMap,g=!1;try{g=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function x(M,y){return g?new OffscreenCanvas(M,y):nc("canvas")}function m(M,y,L){let $=1,J=Dt(M);if((J.width>L||J.height>L)&&($=L/Math.max(J.width,J.height)),$<1)if(typeof HTMLImageElement<"u"&&M instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&M instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&M instanceof ImageBitmap||typeof VideoFrame<"u"&&M instanceof VideoFrame){let ee=Math.floor($*J.width),oe=Math.floor($*J.height);d===void 0&&(d=x(ee,oe));let G=y?x(ee,oe):d;return G.width=ee,G.height=oe,G.getContext("2d").drawImage(M,0,0,ee,oe),Te("WebGLRenderer: Texture has been resized from ("+J.width+"x"+J.height+") to ("+ee+"x"+oe+")."),G}else return"data"in M&&Te("WebGLRenderer: Image in DataTexture is too big ("+J.width+"x"+J.height+")."),M;return M}function p(M){return M.generateMipmaps}function E(M){n.generateMipmap(M)}function S(M){return M.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:M.isWebGL3DRenderTarget?n.TEXTURE_3D:M.isWebGLArrayRenderTarget||M.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function b(M,y,L,$,J,ee=!1){if(M!==null){if(n[M]!==void 0)return n[M];Te("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+M+"'")}let oe;$&&(oe=e.get("EXT_texture_norm16"),oe||Te("WebGLRenderer: Unable to use normalized textures without EXT_texture_norm16 extension"));let G=y;if(y===n.RED&&(L===n.FLOAT&&(G=n.R32F),L===n.HALF_FLOAT&&(G=n.R16F),L===n.UNSIGNED_BYTE&&(G=n.R8),L===n.UNSIGNED_SHORT&&oe&&(G=oe.R16_EXT),L===n.SHORT&&oe&&(G=oe.R16_SNORM_EXT)),y===n.RED_INTEGER&&(L===n.UNSIGNED_BYTE&&(G=n.R8UI),L===n.UNSIGNED_SHORT&&(G=n.R16UI),L===n.UNSIGNED_INT&&(G=n.R32UI),L===n.BYTE&&(G=n.R8I),L===n.SHORT&&(G=n.R16I),L===n.INT&&(G=n.R32I)),y===n.RG&&(L===n.FLOAT&&(G=n.RG32F),L===n.HALF_FLOAT&&(G=n.RG16F),L===n.UNSIGNED_BYTE&&(G=n.RG8),L===n.UNSIGNED_SHORT&&oe&&(G=oe.RG16_EXT),L===n.SHORT&&oe&&(G=oe.RG16_SNORM_EXT)),y===n.RG_INTEGER&&(L===n.UNSIGNED_BYTE&&(G=n.RG8UI),L===n.UNSIGNED_SHORT&&(G=n.RG16UI),L===n.UNSIGNED_INT&&(G=n.RG32UI),L===n.BYTE&&(G=n.RG8I),L===n.SHORT&&(G=n.RG16I),L===n.INT&&(G=n.RG32I)),y===n.RGB_INTEGER&&(L===n.UNSIGNED_BYTE&&(G=n.RGB8UI),L===n.UNSIGNED_SHORT&&(G=n.RGB16UI),L===n.UNSIGNED_INT&&(G=n.RGB32UI),L===n.BYTE&&(G=n.RGB8I),L===n.SHORT&&(G=n.RGB16I),L===n.INT&&(G=n.RGB32I)),y===n.RGBA_INTEGER&&(L===n.UNSIGNED_BYTE&&(G=n.RGBA8UI),L===n.UNSIGNED_SHORT&&(G=n.RGBA16UI),L===n.UNSIGNED_INT&&(G=n.RGBA32UI),L===n.BYTE&&(G=n.RGBA8I),L===n.SHORT&&(G=n.RGBA16I),L===n.INT&&(G=n.RGBA32I)),y===n.RGB&&(L===n.UNSIGNED_SHORT&&oe&&(G=oe.RGB16_EXT),L===n.SHORT&&oe&&(G=oe.RGB16_SNORM_EXT),L===n.UNSIGNED_INT_5_9_9_9_REV&&(G=n.RGB9_E5),L===n.UNSIGNED_INT_10F_11F_11F_REV&&(G=n.R11F_G11F_B10F)),y===n.RGBA){let q=ee?ec:Ke.getTransfer(J);L===n.FLOAT&&(G=n.RGBA32F),L===n.HALF_FLOAT&&(G=n.RGBA16F),L===n.UNSIGNED_BYTE&&(G=q===st?n.SRGB8_ALPHA8:n.RGBA8),L===n.UNSIGNED_SHORT&&oe&&(G=oe.RGBA16_EXT),L===n.SHORT&&oe&&(G=oe.RGBA16_SNORM_EXT),L===n.UNSIGNED_SHORT_4_4_4_4&&(G=n.RGBA4),L===n.UNSIGNED_SHORT_5_5_5_1&&(G=n.RGB5_A1)}return(G===n.R16F||G===n.R32F||G===n.RG16F||G===n.RG32F||G===n.RGBA16F||G===n.RGBA32F)&&e.get("EXT_color_buffer_float"),G}function I(M,y){let L;return M?y===null||y===gi||y===qo?L=n.DEPTH24_STENCIL8:y===yi?L=n.DEPTH32F_STENCIL8:y===$o&&(L=n.DEPTH24_STENCIL8,Te("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):y===null||y===gi||y===qo?L=n.DEPTH_COMPONENT24:y===yi?L=n.DEPTH_COMPONENT32F:y===$o&&(L=n.DEPTH_COMPONENT16),L}function w(M,y){return p(M)===!0||M.isFramebufferTexture&&M.minFilter!==Qt&&M.minFilter!==en?Math.log2(Math.max(y.width,y.height))+1:M.mipmaps!==void 0&&M.mipmaps.length>0?M.mipmaps.length:M.isCompressedTexture&&Array.isArray(M.image)?y.mipmaps.length:1}function C(M){let y=M.target;y.removeEventListener("dispose",C),T(y),y.isVideoTexture&&u.delete(y),y.isHTMLTexture&&f.delete(y)}function v(M){let y=M.target;y.removeEventListener("dispose",v),D(y)}function T(M){let y=i.get(M);if(y.__webglInit===void 0)return;let L=M.source,$=h.get(L);if($){let J=$[y.__cacheKey];J.usedTimes--,J.usedTimes===0&&O(M),Object.keys($).length===0&&h.delete(L)}i.remove(M)}function O(M){let y=i.get(M);n.deleteTexture(y.__webglTexture);let L=M.source,$=h.get(L);delete $[y.__cacheKey],o.memory.textures--}function D(M){let y=i.get(M);if(M.depthTexture&&(M.depthTexture.dispose(),i.remove(M.depthTexture)),M.isWebGLCubeRenderTarget)for(let $=0;$<6;$++){if(Array.isArray(y.__webglFramebuffer[$]))for(let J=0;J<y.__webglFramebuffer[$].length;J++)n.deleteFramebuffer(y.__webglFramebuffer[$][J]);else n.deleteFramebuffer(y.__webglFramebuffer[$]);y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer[$])}else{if(Array.isArray(y.__webglFramebuffer))for(let $=0;$<y.__webglFramebuffer.length;$++)n.deleteFramebuffer(y.__webglFramebuffer[$]);else n.deleteFramebuffer(y.__webglFramebuffer);if(y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer),y.__webglMultisampledFramebuffer&&n.deleteFramebuffer(y.__webglMultisampledFramebuffer),y.__webglColorRenderbuffer)for(let $=0;$<y.__webglColorRenderbuffer.length;$++)y.__webglColorRenderbuffer[$]&&n.deleteRenderbuffer(y.__webglColorRenderbuffer[$]);y.__webglDepthRenderbuffer&&n.deleteRenderbuffer(y.__webglDepthRenderbuffer)}let L=M.textures;for(let $=0,J=L.length;$<J;$++){let ee=i.get(L[$]);ee.__webglTexture&&(n.deleteTexture(ee.__webglTexture),o.memory.textures--),i.remove(L[$])}i.remove(M)}let U=0;function j(){U=0}function X(){return U}function P(M){U=M}function z(){let M=U;return M>=r.maxTextures&&Te("WebGLTextures: Trying to use "+M+" texture units while this GPU supports only "+r.maxTextures),U+=1,M}function k(M){let y=[];return y.push(M.wrapS),y.push(M.wrapT),y.push(M.wrapR||0),y.push(M.magFilter),y.push(M.minFilter),y.push(M.anisotropy),y.push(M.internalFormat),y.push(M.format),y.push(M.type),y.push(M.generateMipmaps),y.push(M.premultiplyAlpha),y.push(M.flipY),y.push(M.unpackAlignment),y.push(M.colorSpace),y.join()}function K(M,y){let L=i.get(M);if(M.isVideoTexture&&vt(M),M.isRenderTargetTexture===!1&&M.isExternalTexture!==!0&&M.version>0&&L.__version!==M.version){let $=M.image;if($===null)Te("WebGLRenderer: Texture marked for update but no image data found.");else if($.complete===!1)Te("WebGLRenderer: Texture marked for update but image is incomplete");else{Ae(L,M,y);return}}else M.isExternalTexture&&(L.__webglTexture=M.sourceTexture?M.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,L.__webglTexture,n.TEXTURE0+y)}function Q(M,y){let L=i.get(M);if(M.isRenderTargetTexture===!1&&M.version>0&&L.__version!==M.version){Ae(L,M,y);return}else M.isExternalTexture&&(L.__webglTexture=M.sourceTexture?M.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,L.__webglTexture,n.TEXTURE0+y)}function le(M,y){let L=i.get(M);if(M.isRenderTargetTexture===!1&&M.version>0&&L.__version!==M.version){Ae(L,M,y);return}t.bindTexture(n.TEXTURE_3D,L.__webglTexture,n.TEXTURE0+y)}function ve(M,y){let L=i.get(M);if(M.isCubeDepthTexture!==!0&&M.version>0&&L.__version!==M.version){Fe(L,M,y);return}t.bindTexture(n.TEXTURE_CUBE_MAP,L.__webglTexture,n.TEXTURE0+y)}let be={[Md]:n.REPEAT,[Li]:n.CLAMP_TO_EDGE,[Ed]:n.MIRRORED_REPEAT},tt={[Qt]:n.NEAREST,[rE]:n.NEAREST_MIPMAP_NEAREST,[Mc]:n.NEAREST_MIPMAP_LINEAR,[en]:n.LINEAR,[$d]:n.LINEAR_MIPMAP_NEAREST,[Yr]:n.LINEAR_MIPMAP_LINEAR},at={[aE]:n.NEVER,[fE]:n.ALWAYS,[cE]:n.LESS,[If]:n.LEQUAL,[lE]:n.EQUAL,[Rf]:n.GEQUAL,[uE]:n.GREATER,[dE]:n.NOTEQUAL};function Ve(M,y){if(y.type===yi&&e.has("OES_texture_float_linear")===!1&&(y.magFilter===en||y.magFilter===$d||y.magFilter===Mc||y.magFilter===Yr||y.minFilter===en||y.minFilter===$d||y.minFilter===Mc||y.minFilter===Yr)&&Te("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(M,n.TEXTURE_WRAP_S,be[y.wrapS]),n.texParameteri(M,n.TEXTURE_WRAP_T,be[y.wrapT]),(M===n.TEXTURE_3D||M===n.TEXTURE_2D_ARRAY)&&n.texParameteri(M,n.TEXTURE_WRAP_R,be[y.wrapR]),n.texParameteri(M,n.TEXTURE_MAG_FILTER,tt[y.magFilter]),n.texParameteri(M,n.TEXTURE_MIN_FILTER,tt[y.minFilter]),y.compareFunction&&(n.texParameteri(M,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(M,n.TEXTURE_COMPARE_FUNC,at[y.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(y.magFilter===Qt||y.minFilter!==Mc&&y.minFilter!==Yr||y.type===yi&&e.has("OES_texture_float_linear")===!1)return;if(y.anisotropy>1||i.get(y).__currentAnisotropy){let L=e.get("EXT_texture_filter_anisotropic");n.texParameterf(M,L.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(y.anisotropy,r.getMaxAnisotropy())),i.get(y).__currentAnisotropy=y.anisotropy}}}function Y(M,y){let L=!1;M.__webglInit===void 0&&(M.__webglInit=!0,y.addEventListener("dispose",C));let $=y.source,J=h.get($);J===void 0&&(J={},h.set($,J));let ee=k(y);if(ee!==M.__cacheKey){J[ee]===void 0&&(J[ee]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,L=!0),J[ee].usedTimes++;let oe=J[M.__cacheKey];oe!==void 0&&(J[M.__cacheKey].usedTimes--,oe.usedTimes===0&&O(y)),M.__cacheKey=ee,M.__webglTexture=J[ee].texture}return L}function fe(M,y,L){return Math.floor(Math.floor(M/L)/y)}function ie(M,y,L,$){let ee=M.updateRanges;if(ee.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,y.width,y.height,L,$,y.data);else{ee.sort((ge,re)=>ge.start-re.start);let oe=0;for(let ge=1;ge<ee.length;ge++){let re=ee[oe],te=ee[ge],Ne=re.start+re.count,He=fe(te.start,y.width,4),it=fe(re.start,y.width,4);te.start<=Ne+1&&He===it&&fe(te.start+te.count-1,y.width,4)===He?re.count=Math.max(re.count,te.start+te.count-re.start):(++oe,ee[oe]=te)}ee.length=oe+1;let G=t.getParameter(n.UNPACK_ROW_LENGTH),q=t.getParameter(n.UNPACK_SKIP_PIXELS),he=t.getParameter(n.UNPACK_SKIP_ROWS);t.pixelStorei(n.UNPACK_ROW_LENGTH,y.width);for(let ge=0,re=ee.length;ge<re;ge++){let te=ee[ge],Ne=Math.floor(te.start/4),He=Math.ceil(te.count/4),it=Ne%y.width,A=Math.floor(Ne/y.width),ne=He,W=1;t.pixelStorei(n.UNPACK_SKIP_PIXELS,it),t.pixelStorei(n.UNPACK_SKIP_ROWS,A),t.texSubImage2D(n.TEXTURE_2D,0,it,A,ne,W,L,$,y.data)}M.clearUpdateRanges(),t.pixelStorei(n.UNPACK_ROW_LENGTH,G),t.pixelStorei(n.UNPACK_SKIP_PIXELS,q),t.pixelStorei(n.UNPACK_SKIP_ROWS,he)}}function Ae(M,y,L){let $=n.TEXTURE_2D;(y.isDataArrayTexture||y.isCompressedArrayTexture)&&($=n.TEXTURE_2D_ARRAY),y.isData3DTexture&&($=n.TEXTURE_3D);let J=Y(M,y),ee=y.source;t.bindTexture($,M.__webglTexture,n.TEXTURE0+L);let oe=i.get(ee);if(ee.version!==oe.__version||J===!0){if(t.activeTexture(n.TEXTURE0+L),(typeof ImageBitmap<"u"&&y.image instanceof ImageBitmap)===!1){let W=Ke.getPrimaries(Ke.workingColorSpace),pe=y.colorSpace===mr?null:Ke.getPrimaries(y.colorSpace),se=y.colorSpace===mr||W===pe?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,se)}t.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment);let q=m(y.image,!1,r.maxTextureSize);q=ae(y,q);let he=s.convert(y.format,y.colorSpace),ge=s.convert(y.type),re=b(y.internalFormat,he,ge,y.normalized,y.colorSpace,y.isVideoTexture);Ve($,y);let te,Ne=y.mipmaps,He=y.isVideoTexture!==!0,it=oe.__version===void 0||J===!0,A=ee.dataReady,ne=w(y,q);if(y.isDepthTexture)re=I(y.format===Zr,y.type),it&&(He?t.texStorage2D(n.TEXTURE_2D,1,re,q.width,q.height):t.texImage2D(n.TEXTURE_2D,0,re,q.width,q.height,0,he,ge,null));else if(y.isDataTexture)if(Ne.length>0){He&&it&&t.texStorage2D(n.TEXTURE_2D,ne,re,Ne[0].width,Ne[0].height);for(let W=0,pe=Ne.length;W<pe;W++)te=Ne[W],He?A&&t.texSubImage2D(n.TEXTURE_2D,W,0,0,te.width,te.height,he,ge,te.data):t.texImage2D(n.TEXTURE_2D,W,re,te.width,te.height,0,he,ge,te.data);y.generateMipmaps=!1}else He?(it&&t.texStorage2D(n.TEXTURE_2D,ne,re,q.width,q.height),A&&ie(y,q,he,ge)):t.texImage2D(n.TEXTURE_2D,0,re,q.width,q.height,0,he,ge,q.data);else if(y.isCompressedTexture)if(y.isCompressedArrayTexture){He&&it&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ne,re,Ne[0].width,Ne[0].height,q.depth);for(let W=0,pe=Ne.length;W<pe;W++)if(te=Ne[W],y.format!==ei)if(he!==null)if(He){if(A)if(y.layerUpdates.size>0){let se=ly(te.width,te.height,y.format,y.type);for(let Z of y.layerUpdates){let xe=te.data.subarray(Z*se/te.data.BYTES_PER_ELEMENT,(Z+1)*se/te.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,W,0,0,Z,te.width,te.height,1,he,xe)}y.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,W,0,0,0,te.width,te.height,q.depth,he,te.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,W,re,te.width,te.height,q.depth,0,te.data,0,0);else Te("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else He?A&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,W,0,0,0,te.width,te.height,q.depth,he,ge,te.data):t.texImage3D(n.TEXTURE_2D_ARRAY,W,re,te.width,te.height,q.depth,0,he,ge,te.data)}else{He&&it&&t.texStorage2D(n.TEXTURE_2D,ne,re,Ne[0].width,Ne[0].height);for(let W=0,pe=Ne.length;W<pe;W++)te=Ne[W],y.format!==ei?he!==null?He?A&&t.compressedTexSubImage2D(n.TEXTURE_2D,W,0,0,te.width,te.height,he,te.data):t.compressedTexImage2D(n.TEXTURE_2D,W,re,te.width,te.height,0,te.data):Te("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):He?A&&t.texSubImage2D(n.TEXTURE_2D,W,0,0,te.width,te.height,he,ge,te.data):t.texImage2D(n.TEXTURE_2D,W,re,te.width,te.height,0,he,ge,te.data)}else if(y.isDataArrayTexture)if(He){if(it&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ne,re,q.width,q.height,q.depth),A)if(y.layerUpdates.size>0){let W=ly(q.width,q.height,y.format,y.type);for(let pe of y.layerUpdates){let se=q.data.subarray(pe*W/q.data.BYTES_PER_ELEMENT,(pe+1)*W/q.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,pe,q.width,q.height,1,he,ge,se)}y.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,q.width,q.height,q.depth,he,ge,q.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,re,q.width,q.height,q.depth,0,he,ge,q.data);else if(y.isData3DTexture)He?(it&&t.texStorage3D(n.TEXTURE_3D,ne,re,q.width,q.height,q.depth),A&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,q.width,q.height,q.depth,he,ge,q.data)):t.texImage3D(n.TEXTURE_3D,0,re,q.width,q.height,q.depth,0,he,ge,q.data);else if(y.isFramebufferTexture){if(it)if(He)t.texStorage2D(n.TEXTURE_2D,ne,re,q.width,q.height);else{let W=q.width,pe=q.height;for(let se=0;se<ne;se++)t.texImage2D(n.TEXTURE_2D,se,re,W,pe,0,he,ge,null),W>>=1,pe>>=1}}else if(y.isHTMLTexture){if("texElementImage2D"in n){let W=n.canvas;if(W.hasAttribute("layoutsubtree")||W.setAttribute("layoutsubtree","true"),q.parentNode!==W){W.appendChild(q),f.add(y),W.onpaint=Ue=>{let Ft=Ue.changedElements;for(let lt of f)Ft.includes(lt.image)&&(lt.needsUpdate=!0)},W.requestPaint();return}let pe=0,se=n.RGBA,Z=n.RGBA,xe=n.UNSIGNED_BYTE;n.texElementImage2D(n.TEXTURE_2D,pe,se,Z,xe,q),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_MIN_FILTER,n.LINEAR),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_S,n.CLAMP_TO_EDGE),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_T,n.CLAMP_TO_EDGE)}}else if(Ne.length>0){if(He&&it){let W=Dt(Ne[0]);t.texStorage2D(n.TEXTURE_2D,ne,re,W.width,W.height)}for(let W=0,pe=Ne.length;W<pe;W++)te=Ne[W],He?A&&t.texSubImage2D(n.TEXTURE_2D,W,0,0,he,ge,te):t.texImage2D(n.TEXTURE_2D,W,re,he,ge,te);y.generateMipmaps=!1}else if(He){if(it){let W=Dt(q);t.texStorage2D(n.TEXTURE_2D,ne,re,W.width,W.height)}A&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,he,ge,q)}else t.texImage2D(n.TEXTURE_2D,0,re,he,ge,q);p(y)&&E($),oe.__version=ee.version,y.onUpdate&&y.onUpdate(y)}M.__version=y.version}function Fe(M,y,L){if(y.image.length!==6)return;let $=Y(M,y),J=y.source;t.bindTexture(n.TEXTURE_CUBE_MAP,M.__webglTexture,n.TEXTURE0+L);let ee=i.get(J);if(J.version!==ee.__version||$===!0){t.activeTexture(n.TEXTURE0+L);let oe=Ke.getPrimaries(Ke.workingColorSpace),G=y.colorSpace===mr?null:Ke.getPrimaries(y.colorSpace),q=y.colorSpace===mr||oe===G?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),t.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,q);let he=y.isCompressedTexture||y.image[0].isCompressedTexture,ge=y.image[0]&&y.image[0].isDataTexture,re=[];for(let Z=0;Z<6;Z++)!he&&!ge?re[Z]=m(y.image[Z],!0,r.maxCubemapSize):re[Z]=ge?y.image[Z].image:y.image[Z],re[Z]=ae(y,re[Z]);let te=re[0],Ne=s.convert(y.format,y.colorSpace),He=s.convert(y.type),it=b(y.internalFormat,Ne,He,y.normalized,y.colorSpace),A=y.isVideoTexture!==!0,ne=ee.__version===void 0||$===!0,W=J.dataReady,pe=w(y,te);Ve(n.TEXTURE_CUBE_MAP,y);let se;if(he){A&&ne&&t.texStorage2D(n.TEXTURE_CUBE_MAP,pe,it,te.width,te.height);for(let Z=0;Z<6;Z++){se=re[Z].mipmaps;for(let xe=0;xe<se.length;xe++){let Ue=se[xe];y.format!==ei?Ne!==null?A?W&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe,0,0,Ue.width,Ue.height,Ne,Ue.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe,it,Ue.width,Ue.height,0,Ue.data):Te("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe,0,0,Ue.width,Ue.height,Ne,He,Ue.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe,it,Ue.width,Ue.height,0,Ne,He,Ue.data)}}}else{if(se=y.mipmaps,A&&ne){se.length>0&&pe++;let Z=Dt(re[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,pe,it,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(ge){A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,re[Z].width,re[Z].height,Ne,He,re[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,it,re[Z].width,re[Z].height,0,Ne,He,re[Z].data);for(let xe=0;xe<se.length;xe++){let Ft=se[xe].image[Z].image;A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe+1,0,0,Ft.width,Ft.height,Ne,He,Ft.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe+1,it,Ft.width,Ft.height,0,Ne,He,Ft.data)}}else{A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,Ne,He,re[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,it,Ne,He,re[Z]);for(let xe=0;xe<se.length;xe++){let Ue=se[xe];A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe+1,0,0,Ne,He,Ue.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe+1,it,Ne,He,Ue.image[Z])}}}p(y)&&E(n.TEXTURE_CUBE_MAP),ee.__version=J.version,y.onUpdate&&y.onUpdate(y)}M.__version=y.version}function Re(M,y,L,$,J,ee){let oe=s.convert(L.format,L.colorSpace),G=s.convert(L.type),q=b(L.internalFormat,oe,G,L.normalized,L.colorSpace),he=i.get(y),ge=i.get(L);if(ge.__renderTarget=y,!he.__hasExternalTextures){let re=Math.max(1,y.width>>ee),te=Math.max(1,y.height>>ee);J===n.TEXTURE_3D||J===n.TEXTURE_2D_ARRAY?t.texImage3D(J,ee,q,re,te,y.depth,0,oe,G,null):t.texImage2D(J,ee,q,re,te,0,oe,G,null)}t.bindFramebuffer(n.FRAMEBUFFER,M),Je(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,$,J,ge.__webglTexture,0,jt(y)):(J===n.TEXTURE_2D||J>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&J<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,$,J,ge.__webglTexture,ee),t.bindFramebuffer(n.FRAMEBUFFER,null)}function wt(M,y,L){if(n.bindRenderbuffer(n.RENDERBUFFER,M),y.depthBuffer){let $=y.depthTexture,J=$&&$.isDepthTexture?$.type:null,ee=I(y.stencilBuffer,J),oe=y.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;Je(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,jt(y),ee,y.width,y.height):L?n.renderbufferStorageMultisample(n.RENDERBUFFER,jt(y),ee,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,ee,y.width,y.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,oe,n.RENDERBUFFER,M)}else{let $=y.textures;for(let J=0;J<$.length;J++){let ee=$[J],oe=s.convert(ee.format,ee.colorSpace),G=s.convert(ee.type),q=b(ee.internalFormat,oe,G,ee.normalized,ee.colorSpace);Je(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,jt(y),q,y.width,y.height):L?n.renderbufferStorageMultisample(n.RENDERBUFFER,jt(y),q,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,q,y.width,y.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Ze(M,y,L){let $=y.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,M),!(y.depthTexture&&y.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let J=i.get(y.depthTexture);if(J.__renderTarget=y,(!J.__webglTexture||y.depthTexture.image.width!==y.width||y.depthTexture.image.height!==y.height)&&(y.depthTexture.image.width=y.width,y.depthTexture.image.height=y.height,y.depthTexture.needsUpdate=!0),$){if(J.__webglInit===void 0&&(J.__webglInit=!0,y.depthTexture.addEventListener("dispose",C)),J.__webglTexture===void 0){J.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,J.__webglTexture),Ve(n.TEXTURE_CUBE_MAP,y.depthTexture);let he=s.convert(y.depthTexture.format),ge=s.convert(y.depthTexture.type),re;y.depthTexture.format===Fi?re=n.DEPTH_COMPONENT24:y.depthTexture.format===Zr&&(re=n.DEPTH24_STENCIL8);for(let te=0;te<6;te++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,0,re,y.width,y.height,0,he,ge,null)}}else K(y.depthTexture,0);let ee=J.__webglTexture,oe=jt(y),G=$?n.TEXTURE_CUBE_MAP_POSITIVE_X+L:n.TEXTURE_2D,q=y.depthTexture.format===Zr?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(y.depthTexture.format===Fi)Je(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,q,G,ee,0,oe):n.framebufferTexture2D(n.FRAMEBUFFER,q,G,ee,0);else if(y.depthTexture.format===Zr)Je(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,q,G,ee,0,oe):n.framebufferTexture2D(n.FRAMEBUFFER,q,G,ee,0);else throw new Error("Unknown depthTexture format")}function ct(M){let y=i.get(M),L=M.isWebGLCubeRenderTarget===!0;if(y.__boundDepthTexture!==M.depthTexture){let $=M.depthTexture;if(y.__depthDisposeCallback&&y.__depthDisposeCallback(),$){let J=()=>{delete y.__boundDepthTexture,delete y.__depthDisposeCallback,$.removeEventListener("dispose",J)};$.addEventListener("dispose",J),y.__depthDisposeCallback=J}y.__boundDepthTexture=$}if(M.depthTexture&&!y.__autoAllocateDepthBuffer)if(L)for(let $=0;$<6;$++)Ze(y.__webglFramebuffer[$],M,$);else{let $=M.texture.mipmaps;$&&$.length>0?Ze(y.__webglFramebuffer[0],M,0):Ze(y.__webglFramebuffer,M,0)}else if(L){y.__webglDepthbuffer=[];for(let $=0;$<6;$++)if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer[$]),y.__webglDepthbuffer[$]===void 0)y.__webglDepthbuffer[$]=n.createRenderbuffer(),wt(y.__webglDepthbuffer[$],M,!1);else{let J=M.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ee=y.__webglDepthbuffer[$];n.bindRenderbuffer(n.RENDERBUFFER,ee),n.framebufferRenderbuffer(n.FRAMEBUFFER,J,n.RENDERBUFFER,ee)}}else{let $=M.texture.mipmaps;if($&&$.length>0?t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer),y.__webglDepthbuffer===void 0)y.__webglDepthbuffer=n.createRenderbuffer(),wt(y.__webglDepthbuffer,M,!1);else{let J=M.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ee=y.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,ee),n.framebufferRenderbuffer(n.FRAMEBUFFER,J,n.RENDERBUFFER,ee)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Et(M,y,L){let $=i.get(M);y!==void 0&&Re($.__webglFramebuffer,M,M.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),L!==void 0&&ct(M)}function Xe(M){let y=M.texture,L=i.get(M),$=i.get(y);M.addEventListener("dispose",v);let J=M.textures,ee=M.isWebGLCubeRenderTarget===!0,oe=J.length>1;if(oe||($.__webglTexture===void 0&&($.__webglTexture=n.createTexture()),$.__version=y.version,o.memory.textures++),ee){L.__webglFramebuffer=[];for(let G=0;G<6;G++)if(y.mipmaps&&y.mipmaps.length>0){L.__webglFramebuffer[G]=[];for(let q=0;q<y.mipmaps.length;q++)L.__webglFramebuffer[G][q]=n.createFramebuffer()}else L.__webglFramebuffer[G]=n.createFramebuffer()}else{if(y.mipmaps&&y.mipmaps.length>0){L.__webglFramebuffer=[];for(let G=0;G<y.mipmaps.length;G++)L.__webglFramebuffer[G]=n.createFramebuffer()}else L.__webglFramebuffer=n.createFramebuffer();if(oe)for(let G=0,q=J.length;G<q;G++){let he=i.get(J[G]);he.__webglTexture===void 0&&(he.__webglTexture=n.createTexture(),o.memory.textures++)}if(M.samples>0&&Je(M)===!1){L.__webglMultisampledFramebuffer=n.createFramebuffer(),L.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,L.__webglMultisampledFramebuffer);for(let G=0;G<J.length;G++){let q=J[G];L.__webglColorRenderbuffer[G]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,L.__webglColorRenderbuffer[G]);let he=s.convert(q.format,q.colorSpace),ge=s.convert(q.type),re=b(q.internalFormat,he,ge,q.normalized,q.colorSpace,M.isXRRenderTarget===!0),te=jt(M);n.renderbufferStorageMultisample(n.RENDERBUFFER,te,re,M.width,M.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+G,n.RENDERBUFFER,L.__webglColorRenderbuffer[G])}n.bindRenderbuffer(n.RENDERBUFFER,null),M.depthBuffer&&(L.__webglDepthRenderbuffer=n.createRenderbuffer(),wt(L.__webglDepthRenderbuffer,M,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(ee){t.bindTexture(n.TEXTURE_CUBE_MAP,$.__webglTexture),Ve(n.TEXTURE_CUBE_MAP,y);for(let G=0;G<6;G++)if(y.mipmaps&&y.mipmaps.length>0)for(let q=0;q<y.mipmaps.length;q++)Re(L.__webglFramebuffer[G][q],M,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+G,q);else Re(L.__webglFramebuffer[G],M,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0);p(y)&&E(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(oe){for(let G=0,q=J.length;G<q;G++){let he=J[G],ge=i.get(he),re=n.TEXTURE_2D;(M.isWebGL3DRenderTarget||M.isWebGLArrayRenderTarget)&&(re=M.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(re,ge.__webglTexture),Ve(re,he),Re(L.__webglFramebuffer,M,he,n.COLOR_ATTACHMENT0+G,re,0),p(he)&&E(re)}t.unbindTexture()}else{let G=n.TEXTURE_2D;if((M.isWebGL3DRenderTarget||M.isWebGLArrayRenderTarget)&&(G=M.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(G,$.__webglTexture),Ve(G,y),y.mipmaps&&y.mipmaps.length>0)for(let q=0;q<y.mipmaps.length;q++)Re(L.__webglFramebuffer[q],M,y,n.COLOR_ATTACHMENT0,G,q);else Re(L.__webglFramebuffer,M,y,n.COLOR_ATTACHMENT0,G,0);p(y)&&E(G),t.unbindTexture()}M.depthBuffer&&ct(M)}function Gt(M){let y=M.textures;for(let L=0,$=y.length;L<$;L++){let J=y[L];if(p(J)){let ee=S(M),oe=i.get(J).__webglTexture;t.bindTexture(ee,oe),E(ee),t.unbindTexture()}}}let Tt=[],In=[];function R(M){if(M.samples>0){if(Je(M)===!1){let y=M.textures,L=M.width,$=M.height,J=n.COLOR_BUFFER_BIT,ee=M.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,oe=i.get(M),G=y.length>1;if(G)for(let he=0;he<y.length;he++)t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,oe.__webglMultisampledFramebuffer);let q=M.texture.mipmaps;q&&q.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,oe.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,oe.__webglFramebuffer);for(let he=0;he<y.length;he++){if(M.resolveDepthBuffer&&(M.depthBuffer&&(J|=n.DEPTH_BUFFER_BIT),M.stencilBuffer&&M.resolveStencilBuffer&&(J|=n.STENCIL_BUFFER_BIT)),G){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,oe.__webglColorRenderbuffer[he]);let ge=i.get(y[he]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,ge,0)}n.blitFramebuffer(0,0,L,$,0,0,L,$,J,n.NEAREST),c===!0&&(Tt.length=0,In.length=0,Tt.push(n.COLOR_ATTACHMENT0+he),M.depthBuffer&&M.resolveDepthBuffer===!1&&(Tt.push(ee),In.push(ee),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,In)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Tt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),G)for(let he=0;he<y.length;he++){t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,oe.__webglColorRenderbuffer[he]);let ge=i.get(y[he]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,ge,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,oe.__webglMultisampledFramebuffer)}else if(M.depthBuffer&&M.resolveDepthBuffer===!1&&c){let y=M.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[y])}}}function jt(M){return Math.min(r.maxSamples,M.samples)}function Je(M){let y=i.get(M);return M.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&y.__useRenderToTexture!==!1}function vt(M){let y=o.render.frame;u.get(M)!==y&&(u.set(M,y),M.update())}function ae(M,y){let L=M.colorSpace,$=M.format,J=M.type;return M.isCompressedTexture===!0||M.isVideoTexture===!0||L!==Qa&&L!==mr&&(Ke.getTransfer(L)===st?($!==ei||J!==Hn)&&Te("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Ce("WebGLTextures: Unsupported texture color space:",L)),y}function Dt(M){return typeof HTMLImageElement<"u"&&M instanceof HTMLImageElement?(l.width=M.naturalWidth||M.width,l.height=M.naturalHeight||M.height):typeof VideoFrame<"u"&&M instanceof VideoFrame?(l.width=M.displayWidth,l.height=M.displayHeight):(l.width=M.width,l.height=M.height),l}this.allocateTextureUnit=z,this.resetTextureUnits=j,this.getTextureUnits=X,this.setTextureUnits=P,this.setTexture2D=K,this.setTexture2DArray=Q,this.setTexture3D=le,this.setTextureCube=ve,this.rebindTextures=Et,this.setupRenderTarget=Xe,this.updateRenderTargetMipmap=Gt,this.updateMultisampleRenderTarget=R,this.setupDepthRenderbuffer=ct,this.setupFrameBufferTexture=Re,this.useMultisampledRTT=Je,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function rO(n,e){function t(i,r=mr){let s,o=Ke.getTransfer(r);if(i===Hn)return n.UNSIGNED_BYTE;if(i===Xd)return n.UNSIGNED_SHORT_4_4_4_4;if(i===Yd)return n.UNSIGNED_SHORT_5_5_5_1;if(i===Zg)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===Jg)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===Xg)return n.BYTE;if(i===Yg)return n.SHORT;if(i===$o)return n.UNSIGNED_SHORT;if(i===qd)return n.INT;if(i===gi)return n.UNSIGNED_INT;if(i===yi)return n.FLOAT;if(i===Bi)return n.HALF_FLOAT;if(i===Kg)return n.ALPHA;if(i===Qg)return n.RGB;if(i===ei)return n.RGBA;if(i===Fi)return n.DEPTH_COMPONENT;if(i===Zr)return n.DEPTH_STENCIL;if(i===ey)return n.RED;if(i===Zd)return n.RED_INTEGER;if(i===Jr)return n.RG;if(i===Jd)return n.RG_INTEGER;if(i===Kd)return n.RGBA_INTEGER;if(i===Ec||i===bc||i===Sc||i===wc)if(o===st)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Ec)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===bc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===Sc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===wc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Ec)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===bc)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===Sc)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===wc)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===Qd||i===ef||i===tf||i===nf)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===Qd)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===ef)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===tf)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===nf)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===rf||i===sf||i===of||i===af||i===cf||i===Tc||i===lf)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===rf||i===sf)return o===st?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===of)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===af)return s.COMPRESSED_R11_EAC;if(i===cf)return s.COMPRESSED_SIGNED_R11_EAC;if(i===Tc)return s.COMPRESSED_RG11_EAC;if(i===lf)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===uf||i===df||i===ff||i===hf||i===pf||i===mf||i===gf||i===yf||i===vf||i===_f||i===xf||i===Mf||i===Ef||i===bf)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===uf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===df)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===ff)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===hf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===pf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===mf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===gf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===yf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===vf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===_f)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===xf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Mf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===Ef)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===bf)return o===st?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Sf||i===wf||i===Tf)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Sf)return o===st?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===wf)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Tf)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Df||i===Cf||i===Dc||i===Af)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Df)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Cf)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===Dc)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===Af)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===qo?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var sO=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,oO=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,by=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new fc(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new mn({vertexShader:sO,fragmentShader:oO,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Qn(new hc(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},Sy=class extends pi{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,f=null,d=null,h=null,g=null,x=typeof XRWebGLBinding<"u",m=new by,p={},E=t.getContextAttributes(),S=null,b=null,I=[],w=[],C=new ke,v=null,T=new pn;T.viewport=new Lt;let O=new pn;O.viewport=new Lt;let D=[T,O],U=new Gd,j=null,X=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(Y){let fe=I[Y];return fe===void 0&&(fe=new Ho,I[Y]=fe),fe.getTargetRaySpace()},this.getControllerGrip=function(Y){let fe=I[Y];return fe===void 0&&(fe=new Ho,I[Y]=fe),fe.getGripSpace()},this.getHand=function(Y){let fe=I[Y];return fe===void 0&&(fe=new Ho,I[Y]=fe),fe.getHandSpace()};function P(Y){let fe=w.indexOf(Y.inputSource);if(fe===-1)return;let ie=I[fe];ie!==void 0&&(ie.update(Y.inputSource,Y.frame,l||o),ie.dispatchEvent({type:Y.type,data:Y.inputSource}))}function z(){r.removeEventListener("select",P),r.removeEventListener("selectstart",P),r.removeEventListener("selectend",P),r.removeEventListener("squeeze",P),r.removeEventListener("squeezestart",P),r.removeEventListener("squeezeend",P),r.removeEventListener("end",z),r.removeEventListener("inputsourceschange",k);for(let Y=0;Y<I.length;Y++){let fe=w[Y];fe!==null&&(w[Y]=null,I[Y].disconnect(fe))}j=null,X=null,m.reset();for(let Y in p)delete p[Y];e.setRenderTarget(S),h=null,d=null,f=null,r=null,b=null,Ve.stop(),i.isPresenting=!1,e.setPixelRatio(v),e.setSize(C.width,C.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(Y){s=Y,i.isPresenting===!0&&Te("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(Y){a=Y,i.isPresenting===!0&&Te("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(Y){l=Y},this.getBaseLayer=function(){return d!==null?d:h},this.getBinding=function(){return f===null&&x&&(f=new XRWebGLBinding(r,t)),f},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=async function(Y){if(r=Y,r!==null){if(S=e.getRenderTarget(),r.addEventListener("select",P),r.addEventListener("selectstart",P),r.addEventListener("selectend",P),r.addEventListener("squeeze",P),r.addEventListener("squeezestart",P),r.addEventListener("squeezeend",P),r.addEventListener("end",z),r.addEventListener("inputsourceschange",k),E.xrCompatible!==!0&&await t.makeXRCompatible(),v=e.getPixelRatio(),e.getSize(C),x&&"createProjectionLayer"in XRWebGLBinding.prototype){let ie=null,Ae=null,Fe=null;E.depth&&(Fe=E.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,ie=E.stencil?Zr:Fi,Ae=E.stencil?qo:gi);let Re={colorFormat:t.RGBA8,depthFormat:Fe,scaleFactor:s};f=this.getBinding(),d=f.createProjectionLayer(Re),r.updateRenderState({layers:[d]}),e.setPixelRatio(1),e.setSize(d.textureWidth,d.textureHeight,!1),b=new Bn(d.textureWidth,d.textureHeight,{format:ei,type:Hn,depthTexture:new pr(d.textureWidth,d.textureHeight,Ae,void 0,void 0,void 0,void 0,void 0,void 0,ie),stencilBuffer:E.stencil,colorSpace:e.outputColorSpace,samples:E.antialias?4:0,resolveDepthBuffer:d.ignoreDepthValues===!1,resolveStencilBuffer:d.ignoreDepthValues===!1})}else{let ie={antialias:E.antialias,alpha:!0,depth:E.depth,stencil:E.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,ie),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),b=new Bn(h.framebufferWidth,h.framebufferHeight,{format:ei,type:Hn,colorSpace:e.outputColorSpace,stencilBuffer:E.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}b.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=await r.requestReferenceSpace(a),Ve.setContext(r),Ve.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return m.getDepthTexture()};function k(Y){for(let fe=0;fe<Y.removed.length;fe++){let ie=Y.removed[fe],Ae=w.indexOf(ie);Ae>=0&&(w[Ae]=null,I[Ae].disconnect(ie))}for(let fe=0;fe<Y.added.length;fe++){let ie=Y.added[fe],Ae=w.indexOf(ie);if(Ae===-1){for(let Re=0;Re<I.length;Re++)if(Re>=w.length){w.push(ie),Ae=Re;break}else if(w[Re]===null){w[Re]=ie,Ae=Re;break}if(Ae===-1)break}let Fe=I[Ae];Fe&&Fe.connect(ie)}}let K=new F,Q=new F;function le(Y,fe,ie){K.setFromMatrixPosition(fe.matrixWorld),Q.setFromMatrixPosition(ie.matrixWorld);let Ae=K.distanceTo(Q),Fe=fe.projectionMatrix.elements,Re=ie.projectionMatrix.elements,wt=Fe[14]/(Fe[10]-1),Ze=Fe[14]/(Fe[10]+1),ct=(Fe[9]+1)/Fe[5],Et=(Fe[9]-1)/Fe[5],Xe=(Fe[8]-1)/Fe[0],Gt=(Re[8]+1)/Re[0],Tt=wt*Xe,In=wt*Gt,R=Ae/(-Xe+Gt),jt=R*-Xe;if(fe.matrixWorld.decompose(Y.position,Y.quaternion,Y.scale),Y.translateX(jt),Y.translateZ(R),Y.matrixWorld.compose(Y.position,Y.quaternion,Y.scale),Y.matrixWorldInverse.copy(Y.matrixWorld).invert(),Fe[10]===-1)Y.projectionMatrix.copy(fe.projectionMatrix),Y.projectionMatrixInverse.copy(fe.projectionMatrixInverse);else{let Je=wt+R,vt=Ze+R,ae=Tt-jt,Dt=In+(Ae-jt),M=ct*Ze/vt*Je,y=Et*Ze/vt*Je;Y.projectionMatrix.makePerspective(ae,Dt,M,y,Je,vt),Y.projectionMatrixInverse.copy(Y.projectionMatrix).invert()}}function ve(Y,fe){fe===null?Y.matrixWorld.copy(Y.matrix):Y.matrixWorld.multiplyMatrices(fe.matrixWorld,Y.matrix),Y.matrixWorldInverse.copy(Y.matrixWorld).invert()}this.updateCamera=function(Y){if(r===null)return;let fe=Y.near,ie=Y.far;m.texture!==null&&(m.depthNear>0&&(fe=m.depthNear),m.depthFar>0&&(ie=m.depthFar)),U.near=O.near=T.near=fe,U.far=O.far=T.far=ie,(j!==U.near||X!==U.far)&&(r.updateRenderState({depthNear:U.near,depthFar:U.far}),j=U.near,X=U.far),U.layers.mask=Y.layers.mask|6,T.layers.mask=U.layers.mask&-5,O.layers.mask=U.layers.mask&-3;let Ae=Y.parent,Fe=U.cameras;ve(U,Ae);for(let Re=0;Re<Fe.length;Re++)ve(Fe[Re],Ae);Fe.length===2?le(U,T,O):U.projectionMatrix.copy(T.projectionMatrix),be(Y,U,Ae)};function be(Y,fe,ie){ie===null?Y.matrix.copy(fe.matrixWorld):(Y.matrix.copy(ie.matrixWorld),Y.matrix.invert(),Y.matrix.multiply(fe.matrixWorld)),Y.matrix.decompose(Y.position,Y.quaternion,Y.scale),Y.updateMatrixWorld(!0),Y.projectionMatrix.copy(fe.projectionMatrix),Y.projectionMatrixInverse.copy(fe.projectionMatrixInverse),Y.isPerspectiveCamera&&(Y.fov=Uo*2*Math.atan(1/Y.projectionMatrix.elements[5]),Y.zoom=1)}this.getCamera=function(){return U},this.getFoveation=function(){if(!(d===null&&h===null))return c},this.setFoveation=function(Y){c=Y,d!==null&&(d.fixedFoveation=Y),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=Y)},this.hasDepthSensing=function(){return m.texture!==null},this.getDepthSensingMesh=function(){return m.getMesh(U)},this.getCameraTexture=function(Y){return p[Y]};let tt=null;function at(Y,fe){if(u=fe.getViewerPose(l||o),g=fe,u!==null){let ie=u.views;h!==null&&(e.setRenderTargetFramebuffer(b,h.framebuffer),e.setRenderTarget(b));let Ae=!1;ie.length!==U.cameras.length&&(U.cameras.length=0,Ae=!0);for(let Ze=0;Ze<ie.length;Ze++){let ct=ie[Ze],Et=null;if(h!==null)Et=h.getViewport(ct);else{let Gt=f.getViewSubImage(d,ct);Et=Gt.viewport,Ze===0&&(e.setRenderTargetTextures(b,Gt.colorTexture,Gt.depthStencilTexture),e.setRenderTarget(b))}let Xe=D[Ze];Xe===void 0&&(Xe=new pn,Xe.layers.enable(Ze),Xe.viewport=new Lt,D[Ze]=Xe),Xe.matrix.fromArray(ct.transform.matrix),Xe.matrix.decompose(Xe.position,Xe.quaternion,Xe.scale),Xe.projectionMatrix.fromArray(ct.projectionMatrix),Xe.projectionMatrixInverse.copy(Xe.projectionMatrix).invert(),Xe.viewport.set(Et.x,Et.y,Et.width,Et.height),Ze===0&&(U.matrix.copy(Xe.matrix),U.matrix.decompose(U.position,U.quaternion,U.scale)),Ae===!0&&U.cameras.push(Xe)}let Fe=r.enabledFeatures;if(Fe&&Fe.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&x){f=i.getBinding();let Ze=f.getDepthInformation(ie[0]);Ze&&Ze.isValid&&Ze.texture&&m.init(Ze,r.renderState)}if(Fe&&Fe.includes("camera-access")&&x){e.state.unbindTexture(),f=i.getBinding();for(let Ze=0;Ze<ie.length;Ze++){let ct=ie[Ze].camera;if(ct){let Et=p[ct];Et||(Et=new fc,p[ct]=Et);let Xe=f.getCameraImage(ct);Et.sourceTexture=Xe}}}}for(let ie=0;ie<I.length;ie++){let Ae=w[ie],Fe=I[ie];Ae!==null&&Fe!==void 0&&Fe.update(Ae,fe,l||o)}tt&&tt(Y,fe),fe.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:fe}),g=null}let Ve=new HE;Ve.setAnimationLoop(at),this.setAnimationLoop=function(Y){tt=Y},this.dispose=function(){}}},aO=new Ot,qE=new Le;qE.set(-1,0,0,0,1,0,0,0,1);function cO(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,oy(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,E,S,b){p.isNodeMaterial?p.uniformsNeedUpdate=!1:p.isMeshBasicMaterial?s(m,p):p.isMeshLambertMaterial?(s(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(m,p),f(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(m,p),d(m,p),p.isMeshPhysicalMaterial&&h(m,p,b)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),x(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,E,S):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===Sn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===Sn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let E=e.get(p),S=E.envMap,b=E.envMapRotation;S&&(m.envMap.value=S,m.envMapRotation.value.setFromMatrix4(aO.makeRotationFromEuler(b)).transpose(),S.isCubeTexture&&S.isRenderTargetTexture===!1&&m.envMapRotation.value.premultiply(qE),m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,E,S){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*E,m.scale.value=S*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function f(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function d(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,E){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===Sn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=E.texture,m.transmissionSamplerSize.value.set(E.width,E.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function x(m,p){let E=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(E.matrixWorld),m.nearDistance.value=E.shadow.camera.near,m.farDistance.value=E.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function lO(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(E,S){let b=S.program;i.uniformBlockBinding(E,b)}function l(E,S){let b=r[E.id];b===void 0&&(g(E),b=u(E),r[E.id]=b,E.addEventListener("dispose",m));let I=S.program;i.updateUBOMapping(E,I);let w=e.render.frame;s[E.id]!==w&&(d(E),s[E.id]=w)}function u(E){let S=f();E.__bindingPointIndex=S;let b=n.createBuffer(),I=E.__size,w=E.usage;return n.bindBuffer(n.UNIFORM_BUFFER,b),n.bufferData(n.UNIFORM_BUFFER,I,w),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,S,b),b}function f(){for(let E=0;E<a;E++)if(o.indexOf(E)===-1)return o.push(E),E;return Ce("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function d(E){let S=r[E.id],b=E.uniforms,I=E.__cache;n.bindBuffer(n.UNIFORM_BUFFER,S);for(let w=0,C=b.length;w<C;w++){let v=Array.isArray(b[w])?b[w]:[b[w]];for(let T=0,O=v.length;T<O;T++){let D=v[T];if(h(D,w,T,I)===!0){let U=D.__offset,j=Array.isArray(D.value)?D.value:[D.value],X=0;for(let P=0;P<j.length;P++){let z=j[P],k=x(z);typeof z=="number"||typeof z=="boolean"?(D.__data[0]=z,n.bufferSubData(n.UNIFORM_BUFFER,U+X,D.__data)):z.isMatrix3?(D.__data[0]=z.elements[0],D.__data[1]=z.elements[1],D.__data[2]=z.elements[2],D.__data[3]=0,D.__data[4]=z.elements[3],D.__data[5]=z.elements[4],D.__data[6]=z.elements[5],D.__data[7]=0,D.__data[8]=z.elements[6],D.__data[9]=z.elements[7],D.__data[10]=z.elements[8],D.__data[11]=0):ArrayBuffer.isView(z)?D.__data.set(new z.constructor(z.buffer,z.byteOffset,D.__data.length)):(z.toArray(D.__data,X),X+=k.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,U,D.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(E,S,b,I){let w=E.value,C=S+"_"+b;if(I[C]===void 0)return typeof w=="number"||typeof w=="boolean"?I[C]=w:ArrayBuffer.isView(w)?I[C]=w.slice():I[C]=w.clone(),!0;{let v=I[C];if(typeof w=="number"||typeof w=="boolean"){if(v!==w)return I[C]=w,!0}else{if(ArrayBuffer.isView(w))return!0;if(v.equals(w)===!1)return v.copy(w),!0}}return!1}function g(E){let S=E.uniforms,b=0,I=16;for(let C=0,v=S.length;C<v;C++){let T=Array.isArray(S[C])?S[C]:[S[C]];for(let O=0,D=T.length;O<D;O++){let U=T[O],j=Array.isArray(U.value)?U.value:[U.value];for(let X=0,P=j.length;X<P;X++){let z=j[X],k=x(z),K=b%I,Q=K%k.boundary,le=K+Q;b+=Q,le!==0&&I-le<k.storage&&(b+=I-le),U.__data=new Float32Array(k.storage/Float32Array.BYTES_PER_ELEMENT),U.__offset=b,b+=k.storage}}}let w=b%I;return w>0&&(b+=I-w),E.__size=b,E.__cache={},this}function x(E){let S={boundary:0,storage:0};return typeof E=="number"||typeof E=="boolean"?(S.boundary=4,S.storage=4):E.isVector2?(S.boundary=8,S.storage=8):E.isVector3||E.isColor?(S.boundary=16,S.storage=12):E.isVector4?(S.boundary=16,S.storage=16):E.isMatrix3?(S.boundary=48,S.storage=48):E.isMatrix4?(S.boundary=64,S.storage=64):E.isTexture?Te("WebGLRenderer: Texture samplers can not be part of an uniforms group."):ArrayBuffer.isView(E)?(S.boundary=16,S.storage=E.byteLength):Te("WebGLRenderer: Unsupported uniform value type.",E),S}function m(E){let S=E.target;S.removeEventListener("dispose",m);let b=o.indexOf(S.__bindingPointIndex);o.splice(b,1),n.deleteBuffer(r[S.id]),delete r[S.id],delete s[S.id]}function p(){for(let E in r)n.deleteBuffer(r[E]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var uO=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),Vi=null;function dO(){return Vi===null&&(Vi=new Cd(uO,16,16,Jr,Bi),Vi.name="DFG_LUT",Vi.minFilter=en,Vi.magFilter=en,Vi.wrapS=Li,Vi.wrapT=Li,Vi.generateMipmaps=!1,Vi.needsUpdate=!0),Vi}var Ff=class{constructor(e={}){let{canvas:t=hE(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:f=!1,reversedDepthBuffer:d=!1,outputBufferType:h=Hn}=e;this.isWebGLRenderer=!0;let g;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");g=i.getContextAttributes().alpha}else g=o;let x=h,m=new Set([Kd,Jd,Zd]),p=new Set([Hn,gi,$o,qo,Xd,Yd]),E=new Uint32Array(4),S=new Int32Array(4),b=new F,I=null,w=null,C=[],v=[],T=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=mi,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let O=this,D=!1,U=null;this._outputColorSpace=kn;let j=0,X=0,P=null,z=-1,k=null,K=new Lt,Q=new Lt,le=null,ve=new Ge(0),be=0,tt=t.width,at=t.height,Ve=1,Y=null,fe=null,ie=new Lt(0,0,tt,at),Ae=new Lt(0,0,tt,at),Fe=!1,Re=new uc,wt=!1,Ze=!1,ct=new Ot,Et=new F,Xe=new Lt,Gt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Tt=!1;function In(){return P===null?Ve:1}let R=i;function jt(_,N){return t.getContext(_,N)}try{let _={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:f};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"184"}`),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",xe,!1),t.addEventListener("webglcontextcreationerror",Ue,!1),R===null){let N="webgl2";if(R=jt(N,_),R===null)throw jt(N)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(_){throw Ce("WebGLRenderer: "+_.message),_}let Je,vt,ae,Dt,M,y,L,$,J,ee,oe,G,q,he,ge,re,te,Ne,He,it,A,ne,W;function pe(){Je=new vN(R),Je.init(),A=new rO(R,Je),vt=new uN(R,Je,e,A),ae=new nO(R,Je),vt.reversedDepthBuffer&&d&&ae.buffers.depth.setReversed(!0),Dt=new MN(R),M=new zP,y=new iO(R,Je,ae,M,vt,A,Dt),L=new yN(O),$=new wI(R),ne=new cN(R,$),J=new _N(R,$,Dt,ne),ee=new bN(R,J,$,ne,Dt),Ne=new EN(R,vt,y),ge=new dN(M),oe=new HP(O,L,Je,vt,ne,ge),G=new cO(O,M),q=new jP,he=new ZP(Je),te=new aN(O,L,ae,ee,g,c),re=new tO(O,ee,vt),W=new lO(R,Dt,vt,ae),He=new lN(R,Je,Dt),it=new xN(R,Je,Dt),Dt.programs=oe.programs,O.capabilities=vt,O.extensions=Je,O.properties=M,O.renderLists=q,O.shadowMap=re,O.state=ae,O.info=Dt}pe(),x!==Hn&&(T=new wN(x,t.width,t.height,r,s));let se=new Sy(O,R);this.xr=se,this.getContext=function(){return R},this.getContextAttributes=function(){return R.getContextAttributes()},this.forceContextLoss=function(){let _=Je.get("WEBGL_lose_context");_&&_.loseContext()},this.forceContextRestore=function(){let _=Je.get("WEBGL_lose_context");_&&_.restoreContext()},this.getPixelRatio=function(){return Ve},this.setPixelRatio=function(_){_!==void 0&&(Ve=_,this.setSize(tt,at,!1))},this.getSize=function(_){return _.set(tt,at)},this.setSize=function(_,N,H=!0){if(se.isPresenting){Te("WebGLRenderer: Can't change size while VR device is presenting.");return}tt=_,at=N,t.width=Math.floor(_*Ve),t.height=Math.floor(N*Ve),H===!0&&(t.style.width=_+"px",t.style.height=N+"px"),T!==null&&T.setSize(t.width,t.height),this.setViewport(0,0,_,N)},this.getDrawingBufferSize=function(_){return _.set(tt*Ve,at*Ve).floor()},this.setDrawingBufferSize=function(_,N,H){tt=_,at=N,Ve=H,t.width=Math.floor(_*H),t.height=Math.floor(N*H),this.setViewport(0,0,_,N)},this.setEffects=function(_){if(x===Hn){Ce("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(_){for(let N=0;N<_.length;N++)if(_[N].isOutputPass===!0){Te("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}T.setEffects(_||[])},this.getCurrentViewport=function(_){return _.copy(K)},this.getViewport=function(_){return _.copy(ie)},this.setViewport=function(_,N,H,B){_.isVector4?ie.set(_.x,_.y,_.z,_.w):ie.set(_,N,H,B),ae.viewport(K.copy(ie).multiplyScalar(Ve).round())},this.getScissor=function(_){return _.copy(Ae)},this.setScissor=function(_,N,H,B){_.isVector4?Ae.set(_.x,_.y,_.z,_.w):Ae.set(_,N,H,B),ae.scissor(Q.copy(Ae).multiplyScalar(Ve).round())},this.getScissorTest=function(){return Fe},this.setScissorTest=function(_){ae.setScissorTest(Fe=_)},this.setOpaqueSort=function(_){Y=_},this.setTransparentSort=function(_){fe=_},this.getClearColor=function(_){return _.copy(te.getClearColor())},this.setClearColor=function(){te.setClearColor(...arguments)},this.getClearAlpha=function(){return te.getClearAlpha()},this.setClearAlpha=function(){te.setClearAlpha(...arguments)},this.clear=function(_=!0,N=!0,H=!0){let B=0;if(_){let V=!1;if(P!==null){let de=P.texture.format;V=m.has(de)}if(V){let de=P.texture.type,ye=p.has(de),ue=te.getClearColor(),_e=te.getClearAlpha(),Me=ue.r,Be=ue.g,We=ue.b;ye?(E[0]=Me,E[1]=Be,E[2]=We,E[3]=_e,R.clearBufferuiv(R.COLOR,0,E)):(S[0]=Me,S[1]=Be,S[2]=We,S[3]=_e,R.clearBufferiv(R.COLOR,0,S))}else B|=R.COLOR_BUFFER_BIT}N&&(B|=R.DEPTH_BUFFER_BIT,this.state.buffers.depth.setMask(!0)),H&&(B|=R.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),B!==0&&R.clear(B)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.setNodesHandler=function(_){_.setRenderer(this),U=_},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",xe,!1),t.removeEventListener("webglcontextcreationerror",Ue,!1),te.dispose(),q.dispose(),he.dispose(),M.dispose(),L.dispose(),ee.dispose(),ne.dispose(),W.dispose(),oe.dispose(),se.dispose(),se.removeEventListener("sessionstart",Dy),se.removeEventListener("sessionend",Cy),es.stop()};function Z(_){_.preventDefault(),iy("WebGLRenderer: Context Lost."),D=!0}function xe(){iy("WebGLRenderer: Context Restored."),D=!1;let _=Dt.autoReset,N=re.enabled,H=re.autoUpdate,B=re.needsUpdate,V=re.type;pe(),Dt.autoReset=_,re.enabled=N,re.autoUpdate=H,re.needsUpdate=B,re.type=V}function Ue(_){Ce("WebGLRenderer: A WebGL context could not be created. Reason: ",_.statusMessage)}function Ft(_){let N=_.target;N.removeEventListener("dispose",Ft),lt(N)}function lt(_){zi(_),M.remove(_)}function zi(_){let N=M.get(_).programs;N!==void 0&&(N.forEach(function(H){oe.releaseProgram(H)}),_.isShaderMaterial&&oe.releaseShaderCache(_))}this.renderBufferDirect=function(_,N,H,B,V,de){N===null&&(N=Gt);let ye=V.isMesh&&V.matrixWorld.determinant()<0,ue=QE(_,N,H,B,V);ae.setMaterial(B,ye);let _e=H.index,Me=1;if(B.wireframe===!0){if(_e=J.getWireframeAttribute(H),_e===void 0)return;Me=2}let Be=H.drawRange,We=H.attributes.position,Se=Be.start*Me,ut=(Be.start+Be.count)*Me;de!==null&&(Se=Math.max(Se,de.start*Me),ut=Math.min(ut,(de.start+de.count)*Me)),_e!==null?(Se=Math.max(Se,0),ut=Math.min(ut,_e.count)):We!=null&&(Se=Math.max(Se,0),ut=Math.min(ut,We.count));let kt=ut-Se;if(kt<0||kt===1/0)return;ne.setup(V,B,ue,H,_e);let Ct,mt=He;if(_e!==null&&(Ct=$.get(_e),mt=it,mt.setIndex(Ct)),V.isMesh)B.wireframe===!0?(ae.setLineWidth(B.wireframeLinewidth*In()),mt.setMode(R.LINES)):mt.setMode(R.TRIANGLES);else if(V.isLine){let tn=B.linewidth;tn===void 0&&(tn=1),ae.setLineWidth(tn*In()),V.isLineSegments?mt.setMode(R.LINES):V.isLineLoop?mt.setMode(R.LINE_LOOP):mt.setMode(R.LINE_STRIP)}else V.isPoints?mt.setMode(R.POINTS):V.isSprite&&mt.setMode(R.TRIANGLES);if(V.isBatchedMesh)if(Je.get("WEBGL_multi_draw"))mt.renderMultiDraw(V._multiDrawStarts,V._multiDrawCounts,V._multiDrawCount);else{let tn=V._multiDrawStarts,me=V._multiDrawCounts,Rn=V._multiDrawCount,nt=_e?$.get(_e).bytesPerElement:1,zn=M.get(B).currentProgram.getUniforms();for(let _i=0;_i<Rn;_i++)zn.setValue(R,"_gl_DrawID",_i),mt.render(tn[_i]/nt,me[_i])}else if(V.isInstancedMesh)mt.renderInstances(Se,kt,V.count);else if(H.isInstancedBufferGeometry){let tn=H._maxInstanceCount!==void 0?H._maxInstanceCount:1/0,me=Math.min(H.instanceCount,tn);mt.renderInstances(Se,kt,me)}else mt.render(Se,kt)};function vi(_,N,H){_.transparent===!0&&_.side===ki&&_.forceSinglePass===!1?(_.side=Sn,_.needsUpdate=!0,Nc(_,N,H),_.side=hr,_.needsUpdate=!0,Nc(_,N,H),_.side=ki):Nc(_,N,H)}this.compile=function(_,N,H=null){H===null&&(H=_),w=he.get(H),w.init(N),v.push(w),H.traverseVisible(function(V){V.isLight&&V.layers.test(N.layers)&&(w.pushLight(V),V.castShadow&&w.pushShadow(V))}),_!==H&&_.traverseVisible(function(V){V.isLight&&V.layers.test(N.layers)&&(w.pushLight(V),V.castShadow&&w.pushShadow(V))}),w.setupLights();let B=new Set;return _.traverse(function(V){if(!(V.isMesh||V.isPoints||V.isLine||V.isSprite))return;let de=V.material;if(de)if(Array.isArray(de))for(let ye=0;ye<de.length;ye++){let ue=de[ye];vi(ue,H,V),B.add(ue)}else vi(de,H,V),B.add(de)}),w=v.pop(),B},this.compileAsync=function(_,N,H=null){let B=this.compile(_,N,H);return new Promise(V=>{function de(){if(B.forEach(function(ye){M.get(ye).currentProgram.isReady()&&B.delete(ye)}),B.size===0){V(_);return}setTimeout(de,10)}Je.get("KHR_parallel_shader_compile")!==null?de():setTimeout(de,10)})};let Gf=null;function JE(_){Gf&&Gf(_)}function Dy(){es.stop()}function Cy(){es.start()}let es=new HE;es.setAnimationLoop(JE),typeof self<"u"&&es.setContext(self),this.setAnimationLoop=function(_){Gf=_,se.setAnimationLoop(_),_===null?es.stop():es.start()},se.addEventListener("sessionstart",Dy),se.addEventListener("sessionend",Cy),this.render=function(_,N){if(N!==void 0&&N.isCamera!==!0){Ce("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(D===!0)return;U!==null&&U.renderStart(_,N);let H=se.enabled===!0&&se.isPresenting===!0,B=T!==null&&(P===null||H)&&T.begin(O,P);if(_.matrixWorldAutoUpdate===!0&&_.updateMatrixWorld(),N.parent===null&&N.matrixWorldAutoUpdate===!0&&N.updateMatrixWorld(),se.enabled===!0&&se.isPresenting===!0&&(T===null||T.isCompositing()===!1)&&(se.cameraAutoUpdate===!0&&se.updateCamera(N),N=se.getCamera()),_.isScene===!0&&_.onBeforeRender(O,_,N,P),w=he.get(_,v.length),w.init(N),w.state.textureUnits=y.getTextureUnits(),v.push(w),ct.multiplyMatrices(N.projectionMatrix,N.matrixWorldInverse),Re.setFromProjectionMatrix(ct,hi,N.reversedDepth),Ze=this.localClippingEnabled,wt=ge.init(this.clippingPlanes,Ze),I=q.get(_,C.length),I.init(),C.push(I),se.enabled===!0&&se.isPresenting===!0){let ye=O.xr.getDepthSensingMesh();ye!==null&&jf(ye,N,-1/0,O.sortObjects)}jf(_,N,0,O.sortObjects),I.finish(),O.sortObjects===!0&&I.sort(Y,fe),Tt=se.enabled===!1||se.isPresenting===!1||se.hasDepthSensing()===!1,Tt&&te.addToRenderList(I,_),this.info.render.frame++,wt===!0&&ge.beginShadows();let V=w.state.shadowsArray;if(re.render(V,_,N),wt===!0&&ge.endShadows(),this.info.autoReset===!0&&this.info.reset(),(B&&T.hasRenderPass())===!1){let ye=I.opaque,ue=I.transmissive;if(w.setupLights(),N.isArrayCamera){let _e=N.cameras;if(ue.length>0)for(let Me=0,Be=_e.length;Me<Be;Me++){let We=_e[Me];Iy(ye,ue,_,We)}Tt&&te.render(_);for(let Me=0,Be=_e.length;Me<Be;Me++){let We=_e[Me];Ay(I,_,We,We.viewport)}}else ue.length>0&&Iy(ye,ue,_,N),Tt&&te.render(_),Ay(I,_,N)}P!==null&&X===0&&(y.updateMultisampleRenderTarget(P),y.updateRenderTargetMipmap(P)),B&&T.end(O),_.isScene===!0&&_.onAfterRender(O,_,N),ne.resetDefaultState(),z=-1,k=null,v.pop(),v.length>0?(w=v[v.length-1],y.setTextureUnits(w.state.textureUnits),wt===!0&&ge.setGlobalState(O.clippingPlanes,w.state.camera)):w=null,C.pop(),C.length>0?I=C[C.length-1]:I=null,U!==null&&U.renderEnd()};function jf(_,N,H,B){if(_.visible===!1)return;if(_.layers.test(N.layers)){if(_.isGroup)H=_.renderOrder;else if(_.isLOD)_.autoUpdate===!0&&_.update(N);else if(_.isLightProbeGrid)w.pushLightProbeGrid(_);else if(_.isLight)w.pushLight(_),_.castShadow&&w.pushShadow(_);else if(_.isSprite){if(!_.frustumCulled||Re.intersectsSprite(_)){B&&Xe.setFromMatrixPosition(_.matrixWorld).applyMatrix4(ct);let ye=ee.update(_),ue=_.material;ue.visible&&I.push(_,ye,ue,H,Xe.z,null)}}else if((_.isMesh||_.isLine||_.isPoints)&&(!_.frustumCulled||Re.intersectsObject(_))){let ye=ee.update(_),ue=_.material;if(B&&(_.boundingSphere!==void 0?(_.boundingSphere===null&&_.computeBoundingSphere(),Xe.copy(_.boundingSphere.center)):(ye.boundingSphere===null&&ye.computeBoundingSphere(),Xe.copy(ye.boundingSphere.center)),Xe.applyMatrix4(_.matrixWorld).applyMatrix4(ct)),Array.isArray(ue)){let _e=ye.groups;for(let Me=0,Be=_e.length;Me<Be;Me++){let We=_e[Me],Se=ue[We.materialIndex];Se&&Se.visible&&I.push(_,ye,Se,H,Xe.z,We)}}else ue.visible&&I.push(_,ye,ue,H,Xe.z,null)}}let de=_.children;for(let ye=0,ue=de.length;ye<ue;ye++)jf(de[ye],N,H,B)}function Ay(_,N,H,B){let{opaque:V,transmissive:de,transparent:ye}=_;w.setupLightsView(H),wt===!0&&ge.setGlobalState(O.clippingPlanes,H),B&&ae.viewport(K.copy(B)),V.length>0&&Rc(V,N,H),de.length>0&&Rc(de,N,H),ye.length>0&&Rc(ye,N,H),ae.buffers.depth.setTest(!0),ae.buffers.depth.setMask(!0),ae.buffers.color.setMask(!0),ae.setPolygonOffset(!1)}function Iy(_,N,H,B){if((H.isScene===!0?H.overrideMaterial:null)!==null)return;if(w.state.transmissionRenderTarget[B.id]===void 0){let Se=Je.has("EXT_color_buffer_half_float")||Je.has("EXT_color_buffer_float");w.state.transmissionRenderTarget[B.id]=new Bn(1,1,{generateMipmaps:!0,type:Se?Bi:Hn,minFilter:Yr,samples:Math.max(4,vt.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:Ke.workingColorSpace})}let de=w.state.transmissionRenderTarget[B.id],ye=B.viewport||K;de.setSize(ye.z*O.transmissionResolutionScale,ye.w*O.transmissionResolutionScale);let ue=O.getRenderTarget(),_e=O.getActiveCubeFace(),Me=O.getActiveMipmapLevel();O.setRenderTarget(de),O.getClearColor(ve),be=O.getClearAlpha(),be<1&&O.setClearColor(16777215,.5),O.clear(),Tt&&te.render(H);let Be=O.toneMapping;O.toneMapping=mi;let We=B.viewport;if(B.viewport!==void 0&&(B.viewport=void 0),w.setupLightsView(B),wt===!0&&ge.setGlobalState(O.clippingPlanes,B),Rc(_,H,B),y.updateMultisampleRenderTarget(de),y.updateRenderTargetMipmap(de),Je.has("WEBGL_multisampled_render_to_texture")===!1){let Se=!1;for(let ut=0,kt=N.length;ut<kt;ut++){let Ct=N[ut],{object:mt,geometry:tn,material:me,group:Rn}=Ct;if(me.side===ki&&mt.layers.test(B.layers)){let nt=me.side;me.side=Sn,me.needsUpdate=!0,Ry(mt,H,B,tn,me,Rn),me.side=nt,me.needsUpdate=!0,Se=!0}}Se===!0&&(y.updateMultisampleRenderTarget(de),y.updateRenderTargetMipmap(de))}O.setRenderTarget(ue,_e,Me),O.setClearColor(ve,be),We!==void 0&&(B.viewport=We),O.toneMapping=Be}function Rc(_,N,H){let B=N.isScene===!0?N.overrideMaterial:null;for(let V=0,de=_.length;V<de;V++){let ye=_[V],{object:ue,geometry:_e,group:Me}=ye,Be=ye.material;Be.allowOverride===!0&&B!==null&&(Be=B),ue.layers.test(H.layers)&&Ry(ue,N,H,_e,Be,Me)}}function Ry(_,N,H,B,V,de){_.onBeforeRender(O,N,H,B,V,de),_.modelViewMatrix.multiplyMatrices(H.matrixWorldInverse,_.matrixWorld),_.normalMatrix.getNormalMatrix(_.modelViewMatrix),V.onBeforeRender(O,N,H,B,_,de),V.transparent===!0&&V.side===ki&&V.forceSinglePass===!1?(V.side=Sn,V.needsUpdate=!0,O.renderBufferDirect(H,N,B,V,_,de),V.side=hr,V.needsUpdate=!0,O.renderBufferDirect(H,N,B,V,_,de),V.side=ki):O.renderBufferDirect(H,N,B,V,_,de),_.onAfterRender(O,N,H,B,V,de)}function Nc(_,N,H){N.isScene!==!0&&(N=Gt);let B=M.get(_),V=w.state.lights,de=w.state.shadowsArray,ye=V.state.version,ue=oe.getParameters(_,V.state,de,N,H,w.state.lightProbeGridArray),_e=oe.getProgramCacheKey(ue),Me=B.programs;B.environment=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?N.environment:null,B.fog=N.fog;let Be=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap;B.envMap=L.get(_.envMap||B.environment,Be),B.envMapRotation=B.environment!==null&&_.envMap===null?N.environmentRotation:_.envMapRotation,Me===void 0&&(_.addEventListener("dispose",Ft),Me=new Map,B.programs=Me);let We=Me.get(_e);if(We!==void 0){if(B.currentProgram===We&&B.lightsStateVersion===ye)return Py(_,ue),We}else ue.uniforms=oe.getUniforms(_),U!==null&&_.isNodeMaterial&&U.build(_,H,ue),_.onBeforeCompile(ue,O),We=oe.acquireProgram(ue,_e),Me.set(_e,We),B.uniforms=ue.uniforms;let Se=B.uniforms;return(!_.isShaderMaterial&&!_.isRawShaderMaterial||_.clipping===!0)&&(Se.clippingPlanes=ge.uniform),Py(_,ue),B.needsLights=tb(_),B.lightsStateVersion=ye,B.needsLights&&(Se.ambientLightColor.value=V.state.ambient,Se.lightProbe.value=V.state.probe,Se.directionalLights.value=V.state.directional,Se.directionalLightShadows.value=V.state.directionalShadow,Se.spotLights.value=V.state.spot,Se.spotLightShadows.value=V.state.spotShadow,Se.rectAreaLights.value=V.state.rectArea,Se.ltc_1.value=V.state.rectAreaLTC1,Se.ltc_2.value=V.state.rectAreaLTC2,Se.pointLights.value=V.state.point,Se.pointLightShadows.value=V.state.pointShadow,Se.hemisphereLights.value=V.state.hemi,Se.directionalShadowMatrix.value=V.state.directionalShadowMatrix,Se.spotLightMatrix.value=V.state.spotLightMatrix,Se.spotLightMap.value=V.state.spotLightMap,Se.pointShadowMatrix.value=V.state.pointShadowMatrix),B.lightProbeGrid=w.state.lightProbeGridArray.length>0,B.currentProgram=We,B.uniformsList=null,We}function Ny(_){if(_.uniformsList===null){let N=_.currentProgram.getUniforms();_.uniformsList=Zo.seqWithValue(N.seq,_.uniforms)}return _.uniformsList}function Py(_,N){let H=M.get(_);H.outputColorSpace=N.outputColorSpace,H.batching=N.batching,H.batchingColor=N.batchingColor,H.instancing=N.instancing,H.instancingColor=N.instancingColor,H.instancingMorph=N.instancingMorph,H.skinning=N.skinning,H.morphTargets=N.morphTargets,H.morphNormals=N.morphNormals,H.morphColors=N.morphColors,H.morphTargetsCount=N.morphTargetsCount,H.numClippingPlanes=N.numClippingPlanes,H.numIntersection=N.numClipIntersection,H.vertexAlphas=N.vertexAlphas,H.vertexTangents=N.vertexTangents,H.toneMapping=N.toneMapping}function KE(_,N){if(_.length===0)return null;if(_.length===1)return _[0].texture!==null?_[0]:null;b.setFromMatrixPosition(N.matrixWorld);for(let H=0,B=_.length;H<B;H++){let V=_[H];if(V.texture!==null&&V.boundingBox.containsPoint(b))return V}return null}function QE(_,N,H,B,V){N.isScene!==!0&&(N=Gt),y.resetTextureUnits();let de=N.fog,ye=B.isMeshStandardMaterial||B.isMeshLambertMaterial||B.isMeshPhongMaterial?N.environment:null,ue=P===null?O.outputColorSpace:P.isXRRenderTarget===!0?P.texture.colorSpace:Ke.workingColorSpace,_e=B.isMeshStandardMaterial||B.isMeshLambertMaterial&&!B.envMap||B.isMeshPhongMaterial&&!B.envMap,Me=L.get(B.envMap||ye,_e),Be=B.vertexColors===!0&&!!H.attributes.color&&H.attributes.color.itemSize===4,We=!!H.attributes.tangent&&(!!B.normalMap||B.anisotropy>0),Se=!!H.morphAttributes.position,ut=!!H.morphAttributes.normal,kt=!!H.morphAttributes.color,Ct=mi;B.toneMapped&&(P===null||P.isXRRenderTarget===!0)&&(Ct=O.toneMapping);let mt=H.morphAttributes.position||H.morphAttributes.normal||H.morphAttributes.color,tn=mt!==void 0?mt.length:0,me=M.get(B),Rn=w.state.lights;if(wt===!0&&(Ze===!0||_!==k)){let _t=_===k&&B.id===z;ge.setState(B,_,_t)}let nt=!1;B.version===me.__version?(me.needsLights&&me.lightsStateVersion!==Rn.state.version||me.outputColorSpace!==ue||V.isBatchedMesh&&me.batching===!1||!V.isBatchedMesh&&me.batching===!0||V.isBatchedMesh&&me.batchingColor===!0&&V.colorTexture===null||V.isBatchedMesh&&me.batchingColor===!1&&V.colorTexture!==null||V.isInstancedMesh&&me.instancing===!1||!V.isInstancedMesh&&me.instancing===!0||V.isSkinnedMesh&&me.skinning===!1||!V.isSkinnedMesh&&me.skinning===!0||V.isInstancedMesh&&me.instancingColor===!0&&V.instanceColor===null||V.isInstancedMesh&&me.instancingColor===!1&&V.instanceColor!==null||V.isInstancedMesh&&me.instancingMorph===!0&&V.morphTexture===null||V.isInstancedMesh&&me.instancingMorph===!1&&V.morphTexture!==null||me.envMap!==Me||B.fog===!0&&me.fog!==de||me.numClippingPlanes!==void 0&&(me.numClippingPlanes!==ge.numPlanes||me.numIntersection!==ge.numIntersection)||me.vertexAlphas!==Be||me.vertexTangents!==We||me.morphTargets!==Se||me.morphNormals!==ut||me.morphColors!==kt||me.toneMapping!==Ct||me.morphTargetsCount!==tn||!!me.lightProbeGrid!=w.state.lightProbeGridArray.length>0)&&(nt=!0):(nt=!0,me.__version=B.version);let zn=me.currentProgram;nt===!0&&(zn=Nc(B,N,V),U&&B.isNodeMaterial&&U.onUpdateProgram(B,zn,me));let _i=!1,yr=!1,Hs=!1,gt=zn.getUniforms(),Ut=me.uniforms;if(ae.useProgram(zn.program)&&(_i=!0,yr=!0,Hs=!0),B.id!==z&&(z=B.id,yr=!0),me.needsLights){let _t=KE(w.state.lightProbeGridArray,V);me.lightProbeGrid!==_t&&(me.lightProbeGrid=_t,yr=!0)}if(_i||k!==_){ae.buffers.depth.getReversed()&&_.reversedDepth!==!0&&(_._reversedDepth=!0,_.updateProjectionMatrix()),gt.setValue(R,"projectionMatrix",_.projectionMatrix),gt.setValue(R,"viewMatrix",_.matrixWorldInverse);let _r=gt.map.cameraPosition;_r!==void 0&&_r.setValue(R,Et.setFromMatrixPosition(_.matrixWorld)),vt.logarithmicDepthBuffer&&gt.setValue(R,"logDepthBufFC",2/(Math.log(_.far+1)/Math.LN2)),(B.isMeshPhongMaterial||B.isMeshToonMaterial||B.isMeshLambertMaterial||B.isMeshBasicMaterial||B.isMeshStandardMaterial||B.isShaderMaterial)&&gt.setValue(R,"isOrthographic",_.isOrthographicCamera===!0),k!==_&&(k=_,yr=!0,Hs=!0)}if(me.needsLights&&(Rn.state.directionalShadowMap.length>0&&gt.setValue(R,"directionalShadowMap",Rn.state.directionalShadowMap,y),Rn.state.spotShadowMap.length>0&&gt.setValue(R,"spotShadowMap",Rn.state.spotShadowMap,y),Rn.state.pointShadowMap.length>0&&gt.setValue(R,"pointShadowMap",Rn.state.pointShadowMap,y)),V.isSkinnedMesh){gt.setOptional(R,V,"bindMatrix"),gt.setOptional(R,V,"bindMatrixInverse");let _t=V.skeleton;_t&&(_t.boneTexture===null&&_t.computeBoneTexture(),gt.setValue(R,"boneTexture",_t.boneTexture,y))}V.isBatchedMesh&&(gt.setOptional(R,V,"batchingTexture"),gt.setValue(R,"batchingTexture",V._matricesTexture,y),gt.setOptional(R,V,"batchingIdTexture"),gt.setValue(R,"batchingIdTexture",V._indirectTexture,y),gt.setOptional(R,V,"batchingColorTexture"),V._colorsTexture!==null&&gt.setValue(R,"batchingColorTexture",V._colorsTexture,y));let vr=H.morphAttributes;if((vr.position!==void 0||vr.normal!==void 0||vr.color!==void 0)&&Ne.update(V,H,zn),(yr||me.receiveShadow!==V.receiveShadow)&&(me.receiveShadow=V.receiveShadow,gt.setValue(R,"receiveShadow",V.receiveShadow)),(B.isMeshStandardMaterial||B.isMeshLambertMaterial||B.isMeshPhongMaterial)&&B.envMap===null&&N.environment!==null&&(Ut.envMapIntensity.value=N.environmentIntensity),Ut.dfgLUT!==void 0&&(Ut.dfgLUT.value=dO()),yr){if(gt.setValue(R,"toneMappingExposure",O.toneMappingExposure),me.needsLights&&eb(Ut,Hs),de&&B.fog===!0&&G.refreshFogUniforms(Ut,de),G.refreshMaterialUniforms(Ut,B,Ve,at,w.state.transmissionRenderTarget[_.id]),me.needsLights&&me.lightProbeGrid){let _t=me.lightProbeGrid;Ut.probesSH.value=_t.texture,Ut.probesMin.value.copy(_t.boundingBox.min),Ut.probesMax.value.copy(_t.boundingBox.max),Ut.probesResolution.value.copy(_t.resolution)}Zo.upload(R,Ny(me),Ut,y)}if(B.isShaderMaterial&&B.uniformsNeedUpdate===!0&&(Zo.upload(R,Ny(me),Ut,y),B.uniformsNeedUpdate=!1),B.isSpriteMaterial&&gt.setValue(R,"center",V.center),gt.setValue(R,"modelViewMatrix",V.modelViewMatrix),gt.setValue(R,"normalMatrix",V.normalMatrix),gt.setValue(R,"modelMatrix",V.matrixWorld),B.uniformsGroups!==void 0){let _t=B.uniformsGroups;for(let _r=0,zs=_t.length;_r<zs;_r++){let Oy=_t[_r];W.update(Oy,zn),W.bind(Oy,zn)}}return zn}function eb(_,N){_.ambientLightColor.needsUpdate=N,_.lightProbe.needsUpdate=N,_.directionalLights.needsUpdate=N,_.directionalLightShadows.needsUpdate=N,_.pointLights.needsUpdate=N,_.pointLightShadows.needsUpdate=N,_.spotLights.needsUpdate=N,_.spotLightShadows.needsUpdate=N,_.rectAreaLights.needsUpdate=N,_.hemisphereLights.needsUpdate=N}function tb(_){return _.isMeshLambertMaterial||_.isMeshToonMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isShadowMaterial||_.isShaderMaterial&&_.lights===!0}this.getActiveCubeFace=function(){return j},this.getActiveMipmapLevel=function(){return X},this.getRenderTarget=function(){return P},this.setRenderTargetTextures=function(_,N,H){let B=M.get(_);B.__autoAllocateDepthBuffer=_.resolveDepthBuffer===!1,B.__autoAllocateDepthBuffer===!1&&(B.__useRenderToTexture=!1),M.get(_.texture).__webglTexture=N,M.get(_.depthTexture).__webglTexture=B.__autoAllocateDepthBuffer?void 0:H,B.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(_,N){let H=M.get(_);H.__webglFramebuffer=N,H.__useDefaultFramebuffer=N===void 0};let nb=R.createFramebuffer();this.setRenderTarget=function(_,N=0,H=0){P=_,j=N,X=H;let B=null,V=!1,de=!1;if(_){let ue=M.get(_);if(ue.__useDefaultFramebuffer!==void 0){ae.bindFramebuffer(R.FRAMEBUFFER,ue.__webglFramebuffer),K.copy(_.viewport),Q.copy(_.scissor),le=_.scissorTest,ae.viewport(K),ae.scissor(Q),ae.setScissorTest(le),z=-1;return}else if(ue.__webglFramebuffer===void 0)y.setupRenderTarget(_);else if(ue.__hasExternalTextures)y.rebindTextures(_,M.get(_.texture).__webglTexture,M.get(_.depthTexture).__webglTexture);else if(_.depthBuffer){let Be=_.depthTexture;if(ue.__boundDepthTexture!==Be){if(Be!==null&&M.has(Be)&&(_.width!==Be.image.width||_.height!==Be.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");y.setupDepthRenderbuffer(_)}}let _e=_.texture;(_e.isData3DTexture||_e.isDataArrayTexture||_e.isCompressedArrayTexture)&&(de=!0);let Me=M.get(_).__webglFramebuffer;_.isWebGLCubeRenderTarget?(Array.isArray(Me[N])?B=Me[N][H]:B=Me[N],V=!0):_.samples>0&&y.useMultisampledRTT(_)===!1?B=M.get(_).__webglMultisampledFramebuffer:Array.isArray(Me)?B=Me[H]:B=Me,K.copy(_.viewport),Q.copy(_.scissor),le=_.scissorTest}else K.copy(ie).multiplyScalar(Ve).floor(),Q.copy(Ae).multiplyScalar(Ve).floor(),le=Fe;if(H!==0&&(B=nb),ae.bindFramebuffer(R.FRAMEBUFFER,B)&&ae.drawBuffers(_,B),ae.viewport(K),ae.scissor(Q),ae.setScissorTest(le),V){let ue=M.get(_.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_CUBE_MAP_POSITIVE_X+N,ue.__webglTexture,H)}else if(de){let ue=N;for(let _e=0;_e<_.textures.length;_e++){let Me=M.get(_.textures[_e]);R.framebufferTextureLayer(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0+_e,Me.__webglTexture,H,ue)}}else if(_!==null&&H!==0){let ue=M.get(_.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,ue.__webglTexture,H)}z=-1},this.readRenderTargetPixels=function(_,N,H,B,V,de,ye,ue=0){if(!(_&&_.isWebGLRenderTarget)){Ce("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let _e=M.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&ye!==void 0&&(_e=_e[ye]),_e){ae.bindFramebuffer(R.FRAMEBUFFER,_e);try{let Me=_.textures[ue],Be=Me.format,We=Me.type;if(_.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+ue),!vt.textureFormatReadable(Be)){Ce("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!vt.textureTypeReadable(We)){Ce("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}N>=0&&N<=_.width-B&&H>=0&&H<=_.height-V&&R.readPixels(N,H,B,V,A.convert(Be),A.convert(We),de)}finally{let Me=P!==null?M.get(P).__webglFramebuffer:null;ae.bindFramebuffer(R.FRAMEBUFFER,Me)}}},this.readRenderTargetPixelsAsync=async function(_,N,H,B,V,de,ye,ue=0){if(!(_&&_.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let _e=M.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&ye!==void 0&&(_e=_e[ye]),_e)if(N>=0&&N<=_.width-B&&H>=0&&H<=_.height-V){ae.bindFramebuffer(R.FRAMEBUFFER,_e);let Me=_.textures[ue],Be=Me.format,We=Me.type;if(_.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+ue),!vt.textureFormatReadable(Be))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!vt.textureTypeReadable(We))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let Se=R.createBuffer();R.bindBuffer(R.PIXEL_PACK_BUFFER,Se),R.bufferData(R.PIXEL_PACK_BUFFER,de.byteLength,R.STREAM_READ),R.readPixels(N,H,B,V,A.convert(Be),A.convert(We),0);let ut=P!==null?M.get(P).__webglFramebuffer:null;ae.bindFramebuffer(R.FRAMEBUFFER,ut);let kt=R.fenceSync(R.SYNC_GPU_COMMANDS_COMPLETE,0);return R.flush(),await mE(R,kt,4),R.bindBuffer(R.PIXEL_PACK_BUFFER,Se),R.getBufferSubData(R.PIXEL_PACK_BUFFER,0,de),R.deleteBuffer(Se),R.deleteSync(kt),de}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(_,N=null,H=0){let B=Math.pow(2,-H),V=Math.floor(_.image.width*B),de=Math.floor(_.image.height*B),ye=N!==null?N.x:0,ue=N!==null?N.y:0;y.setTexture2D(_,0),R.copyTexSubImage2D(R.TEXTURE_2D,H,0,0,ye,ue,V,de),ae.unbindTexture()};let ib=R.createFramebuffer(),rb=R.createFramebuffer();this.copyTextureToTexture=function(_,N,H=null,B=null,V=0,de=0){let ye,ue,_e,Me,Be,We,Se,ut,kt,Ct=_.isCompressedTexture?_.mipmaps[de]:_.image;if(H!==null)ye=H.max.x-H.min.x,ue=H.max.y-H.min.y,_e=H.isBox3?H.max.z-H.min.z:1,Me=H.min.x,Be=H.min.y,We=H.isBox3?H.min.z:0;else{let Ut=Math.pow(2,-V);ye=Math.floor(Ct.width*Ut),ue=Math.floor(Ct.height*Ut),_.isDataArrayTexture?_e=Ct.depth:_.isData3DTexture?_e=Math.floor(Ct.depth*Ut):_e=1,Me=0,Be=0,We=0}B!==null?(Se=B.x,ut=B.y,kt=B.z):(Se=0,ut=0,kt=0);let mt=A.convert(N.format),tn=A.convert(N.type),me;N.isData3DTexture?(y.setTexture3D(N,0),me=R.TEXTURE_3D):N.isDataArrayTexture||N.isCompressedArrayTexture?(y.setTexture2DArray(N,0),me=R.TEXTURE_2D_ARRAY):(y.setTexture2D(N,0),me=R.TEXTURE_2D),ae.activeTexture(R.TEXTURE0),ae.pixelStorei(R.UNPACK_FLIP_Y_WEBGL,N.flipY),ae.pixelStorei(R.UNPACK_PREMULTIPLY_ALPHA_WEBGL,N.premultiplyAlpha),ae.pixelStorei(R.UNPACK_ALIGNMENT,N.unpackAlignment);let Rn=ae.getParameter(R.UNPACK_ROW_LENGTH),nt=ae.getParameter(R.UNPACK_IMAGE_HEIGHT),zn=ae.getParameter(R.UNPACK_SKIP_PIXELS),_i=ae.getParameter(R.UNPACK_SKIP_ROWS),yr=ae.getParameter(R.UNPACK_SKIP_IMAGES);ae.pixelStorei(R.UNPACK_ROW_LENGTH,Ct.width),ae.pixelStorei(R.UNPACK_IMAGE_HEIGHT,Ct.height),ae.pixelStorei(R.UNPACK_SKIP_PIXELS,Me),ae.pixelStorei(R.UNPACK_SKIP_ROWS,Be),ae.pixelStorei(R.UNPACK_SKIP_IMAGES,We);let Hs=_.isDataArrayTexture||_.isData3DTexture,gt=N.isDataArrayTexture||N.isData3DTexture;if(_.isDepthTexture){let Ut=M.get(_),vr=M.get(N),_t=M.get(Ut.__renderTarget),_r=M.get(vr.__renderTarget);ae.bindFramebuffer(R.READ_FRAMEBUFFER,_t.__webglFramebuffer),ae.bindFramebuffer(R.DRAW_FRAMEBUFFER,_r.__webglFramebuffer);for(let zs=0;zs<_e;zs++)Hs&&(R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,M.get(_).__webglTexture,V,We+zs),R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,M.get(N).__webglTexture,de,kt+zs)),R.blitFramebuffer(Me,Be,ye,ue,Se,ut,ye,ue,R.DEPTH_BUFFER_BIT,R.NEAREST);ae.bindFramebuffer(R.READ_FRAMEBUFFER,null),ae.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else if(V!==0||_.isRenderTargetTexture||M.has(_)){let Ut=M.get(_),vr=M.get(N);ae.bindFramebuffer(R.READ_FRAMEBUFFER,ib),ae.bindFramebuffer(R.DRAW_FRAMEBUFFER,rb);for(let _t=0;_t<_e;_t++)Hs?R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,Ut.__webglTexture,V,We+_t):R.framebufferTexture2D(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,Ut.__webglTexture,V),gt?R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,vr.__webglTexture,de,kt+_t):R.framebufferTexture2D(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,vr.__webglTexture,de),V!==0?R.blitFramebuffer(Me,Be,ye,ue,Se,ut,ye,ue,R.COLOR_BUFFER_BIT,R.NEAREST):gt?R.copyTexSubImage3D(me,de,Se,ut,kt+_t,Me,Be,ye,ue):R.copyTexSubImage2D(me,de,Se,ut,Me,Be,ye,ue);ae.bindFramebuffer(R.READ_FRAMEBUFFER,null),ae.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else gt?_.isDataTexture||_.isData3DTexture?R.texSubImage3D(me,de,Se,ut,kt,ye,ue,_e,mt,tn,Ct.data):N.isCompressedArrayTexture?R.compressedTexSubImage3D(me,de,Se,ut,kt,ye,ue,_e,mt,Ct.data):R.texSubImage3D(me,de,Se,ut,kt,ye,ue,_e,mt,tn,Ct):_.isDataTexture?R.texSubImage2D(R.TEXTURE_2D,de,Se,ut,ye,ue,mt,tn,Ct.data):_.isCompressedTexture?R.compressedTexSubImage2D(R.TEXTURE_2D,de,Se,ut,Ct.width,Ct.height,mt,Ct.data):R.texSubImage2D(R.TEXTURE_2D,de,Se,ut,ye,ue,mt,tn,Ct);ae.pixelStorei(R.UNPACK_ROW_LENGTH,Rn),ae.pixelStorei(R.UNPACK_IMAGE_HEIGHT,nt),ae.pixelStorei(R.UNPACK_SKIP_PIXELS,zn),ae.pixelStorei(R.UNPACK_SKIP_ROWS,_i),ae.pixelStorei(R.UNPACK_SKIP_IMAGES,yr),de===0&&N.generateMipmaps&&R.generateMipmap(me),ae.unbindTexture()},this.initRenderTarget=function(_){M.get(_).__webglFramebuffer===void 0&&y.setupRenderTarget(_)},this.initTexture=function(_){_.isCubeTexture?y.setTextureCube(_,0):_.isData3DTexture?y.setTexture3D(_,0):_.isDataArrayTexture||_.isCompressedArrayTexture?y.setTexture2DArray(_,0):y.setTexture2D(_,0),ae.unbindTexture()},this.resetState=function(){j=0,X=0,P=null,ae.reset(),ne.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return hi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=Ke._getDrawingBufferColorSpace(e),t.unpackColorSpace=Ke._getUnpackColorSpace()}};var XE={type:"change"},Ty={type:"start"},ZE={type:"end"},Bf=new zr,YE=new Jn,hO=Math.cos(70*sy.DEG2RAD),Jt=new F,An=2*Math.PI,pt={NONE:-1,ROTATE:0,DOLLY:1,PAN:2,TOUCH_ROTATE:3,TOUCH_PAN:4,TOUCH_DOLLY_PAN:5,TOUCH_DOLLY_ROTATE:6},wy=1e-6,Vf=class extends vc{constructor(e,t=null){super(e,t),this.state=pt.NONE,this.target=new F,this.cursor=new F,this.minDistance=0,this.maxDistance=1/0,this.minZoom=0,this.maxZoom=1/0,this.minTargetRadius=0,this.maxTargetRadius=1/0,this.minPolarAngle=0,this.maxPolarAngle=Math.PI,this.minAzimuthAngle=-1/0,this.maxAzimuthAngle=1/0,this.enableDamping=!1,this.dampingFactor=.05,this.enableZoom=!0,this.zoomSpeed=1,this.enableRotate=!0,this.rotateSpeed=1,this.keyRotateSpeed=1,this.enablePan=!0,this.panSpeed=1,this.screenSpacePanning=!0,this.keyPanSpeed=7,this.zoomToCursor=!1,this.autoRotate=!1,this.autoRotateSpeed=2,this.keys={LEFT:"ArrowLeft",UP:"ArrowUp",RIGHT:"ArrowRight",BOTTOM:"ArrowDown"},this.mouseButtons={LEFT:$r.ROTATE,MIDDLE:$r.DOLLY,RIGHT:$r.PAN},this.touches={ONE:qr.ROTATE,TWO:qr.DOLLY_PAN},this.target0=this.target.clone(),this.position0=this.object.position.clone(),this.zoom0=this.object.zoom,this._cursorStyle="auto",this._domElementKeyEvents=null,this._lastPosition=new F,this._lastQuaternion=new Un,this._lastTargetPosition=new F,this._quat=new Un().setFromUnitVectors(e.up,new F(0,1,0)),this._quatInverse=this._quat.clone().invert(),this._spherical=new jo,this._sphericalDelta=new jo,this._scale=1,this._panOffset=new F,this._rotateStart=new ke,this._rotateEnd=new ke,this._rotateDelta=new ke,this._panStart=new ke,this._panEnd=new ke,this._panDelta=new ke,this._dollyStart=new ke,this._dollyEnd=new ke,this._dollyDelta=new ke,this._dollyDirection=new F,this._mouse=new ke,this._performCursorZoom=!1,this._pointers=[],this._pointerPositions={},this._controlActive=!1,this._onPointerMove=mO.bind(this),this._onPointerDown=pO.bind(this),this._onPointerUp=gO.bind(this),this._onContextMenu=bO.bind(this),this._onMouseWheel=_O.bind(this),this._onKeyDown=xO.bind(this),this._onTouchStart=MO.bind(this),this._onTouchMove=EO.bind(this),this._onMouseDown=yO.bind(this),this._onMouseMove=vO.bind(this),this._interceptControlDown=SO.bind(this),this._interceptControlUp=wO.bind(this),this.domElement!==null&&this.connect(this.domElement),this.update()}set cursorStyle(e){this._cursorStyle=e,e==="grab"?this.domElement.style.cursor="grab":this.domElement.style.cursor="auto"}get cursorStyle(){return this._cursorStyle}connect(e){super.connect(e),this.domElement.addEventListener("pointerdown",this._onPointerDown),this.domElement.addEventListener("pointercancel",this._onPointerUp),this.domElement.addEventListener("contextmenu",this._onContextMenu),this.domElement.addEventListener("wheel",this._onMouseWheel,{passive:!1}),this.domElement.getRootNode().addEventListener("keydown",this._interceptControlDown,{passive:!0,capture:!0}),this.domElement.style.touchAction="none"}disconnect(){this.domElement.removeEventListener("pointerdown",this._onPointerDown),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.domElement.removeEventListener("pointercancel",this._onPointerUp),this.domElement.removeEventListener("wheel",this._onMouseWheel),this.domElement.removeEventListener("contextmenu",this._onContextMenu),this.stopListenToKeyEvents(),this.domElement.getRootNode().removeEventListener("keydown",this._interceptControlDown,{capture:!0}),this.domElement.style.touchAction=""}dispose(){this.disconnect()}getPolarAngle(){return this._spherical.phi}getAzimuthalAngle(){return this._spherical.theta}getDistance(){return this.object.position.distanceTo(this.target)}listenToKeyEvents(e){e.addEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=e}stopListenToKeyEvents(){this._domElementKeyEvents!==null&&(this._domElementKeyEvents.removeEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=null)}saveState(){this.target0.copy(this.target),this.position0.copy(this.object.position),this.zoom0=this.object.zoom}reset(){this.target.copy(this.target0),this.object.position.copy(this.position0),this.object.zoom=this.zoom0,this.object.updateProjectionMatrix(),this.dispatchEvent(XE),this.update(),this.state=pt.NONE}pan(e,t){this._pan(e,t),this.update()}dollyIn(e){this._dollyIn(e),this.update()}dollyOut(e){this._dollyOut(e),this.update()}rotateLeft(e){this._rotateLeft(e),this.update()}rotateUp(e){this._rotateUp(e),this.update()}update(e=null){let t=this.object.position;Jt.copy(t).sub(this.target),Jt.applyQuaternion(this._quat),this._spherical.setFromVector3(Jt),this.autoRotate&&this.state===pt.NONE&&this._rotateLeft(this._getAutoRotationAngle(e)),this.enableDamping?(this._spherical.theta+=this._sphericalDelta.theta*this.dampingFactor,this._spherical.phi+=this._sphericalDelta.phi*this.dampingFactor):(this._spherical.theta+=this._sphericalDelta.theta,this._spherical.phi+=this._sphericalDelta.phi);let i=this.minAzimuthAngle,r=this.maxAzimuthAngle;isFinite(i)&&isFinite(r)&&(i<-Math.PI?i+=An:i>Math.PI&&(i-=An),r<-Math.PI?r+=An:r>Math.PI&&(r-=An),i<=r?this._spherical.theta=Math.max(i,Math.min(r,this._spherical.theta)):this._spherical.theta=this._spherical.theta>(i+r)/2?Math.max(i,this._spherical.theta):Math.min(r,this._spherical.theta)),this._spherical.phi=Math.max(this.minPolarAngle,Math.min(this.maxPolarAngle,this._spherical.phi)),this._spherical.makeSafe(),this.enableDamping===!0?this.target.addScaledVector(this._panOffset,this.dampingFactor):this.target.add(this._panOffset),this.target.sub(this.cursor),this.target.clampLength(this.minTargetRadius,this.maxTargetRadius),this.target.add(this.cursor);let s=!1;if(this.zoomToCursor&&this._performCursorZoom||this.object.isOrthographicCamera)this._spherical.radius=this._clampDistance(this._spherical.radius);else{let o=this._spherical.radius;this._spherical.radius=this._clampDistance(this._spherical.radius*this._scale),s=o!=this._spherical.radius}if(Jt.setFromSpherical(this._spherical),Jt.applyQuaternion(this._quatInverse),t.copy(this.target).add(Jt),this.object.lookAt(this.target),this.enableDamping===!0?(this._sphericalDelta.theta*=1-this.dampingFactor,this._sphericalDelta.phi*=1-this.dampingFactor,this._panOffset.multiplyScalar(1-this.dampingFactor)):(this._sphericalDelta.set(0,0,0),this._panOffset.set(0,0,0)),this.zoomToCursor&&this._performCursorZoom){let o=null;if(this.object.isPerspectiveCamera){let a=Jt.length();o=this._clampDistance(a*this._scale);let c=a-o;this.object.position.addScaledVector(this._dollyDirection,c),this.object.updateMatrixWorld(),s=!!c}else if(this.object.isOrthographicCamera){let a=new F(this._mouse.x,this._mouse.y,0);a.unproject(this.object);let c=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),this.object.updateProjectionMatrix(),s=c!==this.object.zoom;let l=new F(this._mouse.x,this._mouse.y,0);l.unproject(this.object),this.object.position.sub(l).add(a),this.object.updateMatrixWorld(),o=Jt.length()}else console.warn("WARNING: OrbitControls.js encountered an unknown camera type - zoom to cursor disabled."),this.zoomToCursor=!1;o!==null&&(this.screenSpacePanning?this.target.set(0,0,-1).transformDirection(this.object.matrix).multiplyScalar(o).add(this.object.position):(Bf.origin.copy(this.object.position),Bf.direction.set(0,0,-1).transformDirection(this.object.matrix),Math.abs(this.object.up.dot(Bf.direction))<hO?this.object.lookAt(this.target):(YE.setFromNormalAndCoplanarPoint(this.object.up,this.target),Bf.intersectPlane(YE,this.target))))}else if(this.object.isOrthographicCamera){let o=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),o!==this.object.zoom&&(this.object.updateProjectionMatrix(),s=!0)}return this._scale=1,this._performCursorZoom=!1,s||this._lastPosition.distanceToSquared(this.object.position)>wy||8*(1-this._lastQuaternion.dot(this.object.quaternion))>wy||this._lastTargetPosition.distanceToSquared(this.target)>wy?(this.dispatchEvent(XE),this._lastPosition.copy(this.object.position),this._lastQuaternion.copy(this.object.quaternion),this._lastTargetPosition.copy(this.target),!0):!1}_getAutoRotationAngle(e){return e!==null?An/60*this.autoRotateSpeed*e:An/60/60*this.autoRotateSpeed}_getZoomScale(e){let t=Math.abs(e*.01);return Math.pow(.95,this.zoomSpeed*t)}_rotateLeft(e){this._sphericalDelta.theta-=e}_rotateUp(e){this._sphericalDelta.phi-=e}_panLeft(e,t){Jt.setFromMatrixColumn(t,0),Jt.multiplyScalar(-e),this._panOffset.add(Jt)}_panUp(e,t){this.screenSpacePanning===!0?Jt.setFromMatrixColumn(t,1):(Jt.setFromMatrixColumn(t,0),Jt.crossVectors(this.object.up,Jt)),Jt.multiplyScalar(e),this._panOffset.add(Jt)}_pan(e,t){let i=this.domElement;if(this.object.isPerspectiveCamera){let r=this.object.position;Jt.copy(r).sub(this.target);let s=Jt.length();s*=Math.tan(this.object.fov/2*Math.PI/180),this._panLeft(2*e*s/i.clientHeight,this.object.matrix),this._panUp(2*t*s/i.clientHeight,this.object.matrix)}else this.object.isOrthographicCamera?(this._panLeft(e*(this.object.right-this.object.left)/this.object.zoom/i.clientWidth,this.object.matrix),this._panUp(t*(this.object.top-this.object.bottom)/this.object.zoom/i.clientHeight,this.object.matrix)):(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - pan disabled."),this.enablePan=!1)}_dollyOut(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale/=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_dollyIn(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale*=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_updateZoomParameters(e,t){if(!this.zoomToCursor)return;this._performCursorZoom=!0;let i=this.domElement.getBoundingClientRect(),r=e-i.left,s=t-i.top,o=i.width,a=i.height;this._mouse.x=r/o*2-1,this._mouse.y=-(s/a)*2+1,this._dollyDirection.set(this._mouse.x,this._mouse.y,1).unproject(this.object).sub(this.object.position).normalize()}_clampDistance(e){return Math.max(this.minDistance,Math.min(this.maxDistance,e))}_handleMouseDownRotate(e){this._rotateStart.set(e.clientX,e.clientY)}_handleMouseDownDolly(e){this._updateZoomParameters(e.clientX,e.clientX),this._dollyStart.set(e.clientX,e.clientY)}_handleMouseDownPan(e){this._panStart.set(e.clientX,e.clientY)}_handleMouseMoveRotate(e){this._rotateEnd.set(e.clientX,e.clientY),this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(An*this._rotateDelta.x/t.clientHeight),this._rotateUp(An*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd),this.update()}_handleMouseMoveDolly(e){this._dollyEnd.set(e.clientX,e.clientY),this._dollyDelta.subVectors(this._dollyEnd,this._dollyStart),this._dollyDelta.y>0?this._dollyOut(this._getZoomScale(this._dollyDelta.y)):this._dollyDelta.y<0&&this._dollyIn(this._getZoomScale(this._dollyDelta.y)),this._dollyStart.copy(this._dollyEnd),this.update()}_handleMouseMovePan(e){this._panEnd.set(e.clientX,e.clientY),this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd),this.update()}_handleMouseWheel(e){this._updateZoomParameters(e.clientX,e.clientY),e.deltaY<0?this._dollyIn(this._getZoomScale(e.deltaY)):e.deltaY>0&&this._dollyOut(this._getZoomScale(e.deltaY)),this.update()}_handleKeyDown(e){let t=!1;switch(e.code){case this.keys.UP:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(An*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,this.keyPanSpeed),t=!0;break;case this.keys.BOTTOM:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(-An*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,-this.keyPanSpeed),t=!0;break;case this.keys.LEFT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(An*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(this.keyPanSpeed,0),t=!0;break;case this.keys.RIGHT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(-An*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(-this.keyPanSpeed,0),t=!0;break}t&&(e.preventDefault(),this.update())}_handleTouchStartRotate(e){if(this._pointers.length===1)this._rotateStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._rotateStart.set(i,r)}}_handleTouchStartPan(e){if(this._pointers.length===1)this._panStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panStart.set(i,r)}}_handleTouchStartDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyStart.set(0,s)}_handleTouchStartDollyPan(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enablePan&&this._handleTouchStartPan(e)}_handleTouchStartDollyRotate(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enableRotate&&this._handleTouchStartRotate(e)}_handleTouchMoveRotate(e){if(this._pointers.length==1)this._rotateEnd.set(e.pageX,e.pageY);else{let i=this._getSecondPointerPosition(e),r=.5*(e.pageX+i.x),s=.5*(e.pageY+i.y);this._rotateEnd.set(r,s)}this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(An*this._rotateDelta.x/t.clientHeight),this._rotateUp(An*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd)}_handleTouchMovePan(e){if(this._pointers.length===1)this._panEnd.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panEnd.set(i,r)}this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd)}_handleTouchMoveDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyEnd.set(0,s),this._dollyDelta.set(0,Math.pow(this._dollyEnd.y/this._dollyStart.y,this.zoomSpeed)),this._dollyOut(this._dollyDelta.y),this._dollyStart.copy(this._dollyEnd);let o=(e.pageX+t.x)*.5,a=(e.pageY+t.y)*.5;this._updateZoomParameters(o,a)}_handleTouchMoveDollyPan(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enablePan&&this._handleTouchMovePan(e)}_handleTouchMoveDollyRotate(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enableRotate&&this._handleTouchMoveRotate(e)}_addPointer(e){this._pointers.push(e.pointerId)}_removePointer(e){delete this._pointerPositions[e.pointerId];for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId){this._pointers.splice(t,1);return}}_isTrackingPointer(e){for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId)return!0;return!1}_trackPointer(e){let t=this._pointerPositions[e.pointerId];t===void 0&&(t=new ke,this._pointerPositions[e.pointerId]=t),t.set(e.pageX,e.pageY)}_getSecondPointerPosition(e){let t=e.pointerId===this._pointers[0]?this._pointers[1]:this._pointers[0];return this._pointerPositions[t]}_customWheelEvent(e){let t=e.deltaMode,i={clientX:e.clientX,clientY:e.clientY,deltaY:e.deltaY};switch(t){case 1:i.deltaY*=16;break;case 2:i.deltaY*=100;break}return e.ctrlKey&&!this._controlActive&&(i.deltaY*=10),i}};function pO(n){this.enabled!==!1&&(this._pointers.length===0&&(this.domElement.setPointerCapture(n.pointerId),this.domElement.ownerDocument.addEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.addEventListener("pointerup",this._onPointerUp)),!this._isTrackingPointer(n)&&(this._addPointer(n),n.pointerType==="touch"?this._onTouchStart(n):this._onMouseDown(n),this._cursorStyle==="grab"&&(this.domElement.style.cursor="grabbing")))}function mO(n){this.enabled!==!1&&(n.pointerType==="touch"?this._onTouchMove(n):this._onMouseMove(n))}function gO(n){switch(this._removePointer(n),this._pointers.length){case 0:this.domElement.releasePointerCapture(n.pointerId),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.dispatchEvent(ZE),this.state=pt.NONE,this._cursorStyle==="grab"&&(this.domElement.style.cursor="grab");break;case 1:let e=this._pointers[0],t=this._pointerPositions[e];this._onTouchStart({pointerId:e,pageX:t.x,pageY:t.y});break}}function yO(n){let e;switch(n.button){case 0:e=this.mouseButtons.LEFT;break;case 1:e=this.mouseButtons.MIDDLE;break;case 2:e=this.mouseButtons.RIGHT;break;default:e=-1}switch(e){case $r.DOLLY:if(this.enableZoom===!1)return;this._handleMouseDownDolly(n),this.state=pt.DOLLY;break;case $r.ROTATE:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=pt.PAN}else{if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=pt.ROTATE}break;case $r.PAN:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=pt.ROTATE}else{if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=pt.PAN}break;default:this.state=pt.NONE}this.state!==pt.NONE&&this.dispatchEvent(Ty)}function vO(n){switch(this.state){case pt.ROTATE:if(this.enableRotate===!1)return;this._handleMouseMoveRotate(n);break;case pt.DOLLY:if(this.enableZoom===!1)return;this._handleMouseMoveDolly(n);break;case pt.PAN:if(this.enablePan===!1)return;this._handleMouseMovePan(n);break}}function _O(n){this.enabled===!1||this.enableZoom===!1||this.state!==pt.NONE||(n.preventDefault(),this.dispatchEvent(Ty),this._handleMouseWheel(this._customWheelEvent(n)),this.dispatchEvent(ZE))}function xO(n){this.enabled!==!1&&this._handleKeyDown(n)}function MO(n){switch(this._trackPointer(n),this._pointers.length){case 1:switch(this.touches.ONE){case qr.ROTATE:if(this.enableRotate===!1)return;this._handleTouchStartRotate(n),this.state=pt.TOUCH_ROTATE;break;case qr.PAN:if(this.enablePan===!1)return;this._handleTouchStartPan(n),this.state=pt.TOUCH_PAN;break;default:this.state=pt.NONE}break;case 2:switch(this.touches.TWO){case qr.DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchStartDollyPan(n),this.state=pt.TOUCH_DOLLY_PAN;break;case qr.DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchStartDollyRotate(n),this.state=pt.TOUCH_DOLLY_ROTATE;break;default:this.state=pt.NONE}break;default:this.state=pt.NONE}this.state!==pt.NONE&&this.dispatchEvent(Ty)}function EO(n){switch(this._trackPointer(n),this.state){case pt.TOUCH_ROTATE:if(this.enableRotate===!1)return;this._handleTouchMoveRotate(n),this.update();break;case pt.TOUCH_PAN:if(this.enablePan===!1)return;this._handleTouchMovePan(n),this.update();break;case pt.TOUCH_DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchMoveDollyPan(n),this.update();break;case pt.TOUCH_DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchMoveDollyRotate(n),this.update();break;default:this.state=pt.NONE}}function bO(n){this.enabled!==!1&&n.preventDefault()}function SO(n){n.key==="Control"&&(this._controlActive=!0,this.domElement.getRootNode().addEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}function wO(n){n.key==="Control"&&(this._controlActive=!1,this.domElement.getRootNode().removeEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}var TO=["canvas"];function DO(n,e){if(n&1&&(St(0,"div",4)(1,"div",5),ot(2),Nt(),St(3,"div",6),ot(4),Nt(),St(5,"div",7),ot(6),St(7,"span",8),ot(8,"\xB7"),Nt(),ot(9),vo(10,"date"),Nt()()),n&2){let t=un();yu("left",t.mousePos().x+16,"px")("top",t.mousePos().y-8,"px"),yt(2),ci(t.hovered().shortSha),yt(2),ci(t.hovered().message),yt(2),nr(" ",t.hovered().author," "),yt(3),nr(" ",_o(10,8,t.hovered().date,"d MMM y")," ")}}var CO=`
  attribute float aSize;
  attribute float aIndex;
  uniform float uHovered;
  varying vec3 vColor;
  varying float vGlow;

  void main() {
    vColor = color;
    bool isHovered = abs(aIndex - uHovered) < 0.5;
    float size = isHovered ? aSize * 3.5 : aSize;
    vGlow = isHovered ? 1.0 : 0.0;
    vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
    gl_PointSize = size * (350.0 / -mvPosition.z);
    gl_Position = projectionMatrix * mvPosition;
  }
`,AO=`
  varying vec3 vColor;
  varying float vGlow;

  void main() {
    vec2 coord = gl_PointCoord - vec2(0.5);
    float dist = length(coord);
    if (dist > 0.5) discard;
    float core = 1.0 - smoothstep(0.0, 0.18, dist);
    float halo = 1.0 - smoothstep(0.1, 0.5, dist);
    float alpha = core * 0.95 + halo * 0.45;
    vec3 col = vColor + vec3(core * 0.25);
    if (vGlow > 0.5) {
      col = mix(col, vec3(1.0), 0.6);
      alpha = min(alpha * 1.5, 1.0);
    }
    gl_FragColor = vec4(col, alpha);
  }
`,IO=`
  void main() {
    gl_PointSize = 1.0;
    gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
  }
`,RO=`
  void main() {
    float dist = length(gl_PointCoord - vec2(0.5));
    if (dist > 0.5) discard;
    gl_FragColor = vec4(1.0, 1.0, 1.0, 0.35);
  }
`,Hf=class n{constructor(e){this.zone=e}canvasRef;commitClicked=Eu();hovered=ln(null);mousePos=ln({x:0,y:0});interacted=ln(!1);_commits=[];sorted=[];set commits(e){this._commits=e,this.scene&&this.rebuildStars()}scene;camera;renderer;controls;starPoints=null;bgPoints=null;material;raycaster=new yc;mouse=new ke;animId=0;ro;ngAfterViewInit(){this.zone.runOutsideAngular(()=>{this.initScene(),this.buildBackground(),this._commits.length&&this.rebuildStars(),this.startLoop(),this.setupResize()})}ngOnDestroy(){cancelAnimationFrame(this.animId),this.ro?.disconnect(),this.renderer?.dispose(),this.material?.dispose()}initScene(){let e=this.canvasRef.nativeElement,t=e.clientWidth,i=e.clientHeight;this.scene=new oc,this.scene.background=new Ge(657930),this.scene.fog=new sc(657930,.0018),this.camera=new pn(55,t/i,.1,2e3),this.camera.position.set(0,40,130),this.renderer=new Ff({canvas:e,antialias:!0}),this.renderer.setSize(t,i,!1),this.renderer.setPixelRatio(Math.min(window.devicePixelRatio,2)),this.controls=new Vf(this.camera,this.renderer.domElement),this.controls.enableDamping=!0,this.controls.dampingFactor=.06,this.controls.minDistance=25,this.controls.maxDistance=600,this.controls.autoRotate=!0,this.controls.autoRotateSpeed=.25,e.addEventListener("mousemove",this.onMouseMove.bind(this)),e.addEventListener("click",this.onClick.bind(this)),this.controls.addEventListener("start",()=>{this.controls.autoRotate=!1,this.zone.run(()=>this.interacted.set(!0))})}buildBackground(){let t=new Float32Array(5400);for(let r=0;r<1800;r++){let s=300+Math.random()*700,o=Math.random()*Math.PI*2,a=Math.acos(2*Math.random()-1);t[r*3]=s*Math.sin(a)*Math.cos(o),t[r*3+1]=s*Math.sin(a)*Math.sin(o),t[r*3+2]=s*Math.cos(a)}let i=new Cn;i.setAttribute("position",new Xt(t,3)),this.bgPoints=new zo(i,new mn({vertexShader:IO,fragmentShader:RO,transparent:!0,depthWrite:!1})),this.scene.add(this.bgPoints)}rebuildStars(){this.starPoints&&(this.scene.remove(this.starPoints),this.starPoints.geometry.dispose(),this.material?.dispose()),this.sorted=[...this._commits].sort((a,c)=>a.date.getTime()-c.date.getTime());let e=this.sorted.length,t=new Float32Array(e*3),i=new Float32Array(e*3),r=new Float32Array(e),s=new Float32Array(e);for(let a=0;a<e;a++){let c=e>1?a/(e-1):0,[l,u,f]=this.starPos(c,this.sorted[a].sha);t[a*3]=l,t[a*3+1]=u,t[a*3+2]=f;let d=this.ageColor(c);i[a*3]=d.r,i[a*3+1]=d.g,i[a*3+2]=d.b,r[a]=1.8+c*1.2,s[a]=a}let o=new Cn;o.setAttribute("position",new Xt(t,3)),o.setAttribute("color",new Xt(i,3)),o.setAttribute("aSize",new Xt(r,1)),o.setAttribute("aIndex",new Xt(s,1)),this.material=new mn({uniforms:{uHovered:{value:-1}},vertexShader:CO,fragmentShader:AO,vertexColors:!0,transparent:!0,depthWrite:!1}),this.starPoints=new zo(o,this.material),this.scene.add(this.starPoints)}starPos(e,t){let i=8+e*75,s=e*Math.PI*2*5,o=10*(1-e*.6),a=(this.rng(t+"x")-.5)*o,c=(this.rng(t+"y")-.5)*o*.5,l=(this.rng(t+"z")-.5)*o;return[Math.cos(s)*i+a,c,Math.sin(s)*i+l]}ageColor(e){return e<.5?new Ge().lerpColors(new Ge(6514417),new Ge(16498468),e*2):new Ge().lerpColors(new Ge(16498468),new Ge(16777215),(e-.5)*2)}rng(e){let t=0;for(let i=0;i<e.length;i++)t=Math.imul(31,t)+e.charCodeAt(i)|0;return Math.abs(t%1e6)/1e6}onMouseMove(e){let i=this.canvasRef.nativeElement.getBoundingClientRect();this.mouse.x=(e.clientX-i.left)/i.width*2-1,this.mouse.y=-((e.clientY-i.top)/i.height)*2+1,this.zone.run(()=>this.mousePos.set({x:e.clientX,y:e.clientY})),this.checkHover()}onClick(e){let t=this.hovered();t&&(window.open(t.url,"_blank","noopener"),this.zone.run(()=>this.commitClicked.emit(t)))}checkHover(){if(!this.starPoints)return;this.raycaster.params.Points={threshold:2.5},this.raycaster.setFromCamera(this.mouse,this.camera);let e=this.raycaster.intersectObject(this.starPoints);if(e.length>0){let t=e[0].index;this.material.uniforms.uHovered.value=t,this.zone.run(()=>this.hovered.set(this.sorted[t]))}else this.material.uniforms.uHovered.value=-1,this.zone.run(()=>this.hovered.set(null))}startLoop(){let e=()=>{this.animId=requestAnimationFrame(e),this.controls.update(),this.renderer.render(this.scene,this.camera)};e()}setupResize(){this.ro=new ResizeObserver(()=>{let e=this.canvasRef.nativeElement,t=e.clientWidth,i=e.clientHeight;this.camera.aspect=t/i,this.camera.updateProjectionMatrix(),this.renderer.setSize(t,i,!1)}),this.ro.observe(this.canvasRef.nativeElement.parentElement)}static \u0275fac=function(t){return new(t||n)(Ki(Mn))};static \u0275cmp=Ss({type:n,selectors:[["app-universe"]],viewQuery:function(t,i){if(t&1&&gu(TO,5),t&2){let r;Lm(r=Fm())&&(i.canvasRef=r.first)}},inputs:{commits:"commits"},outputs:{commitClicked:"commitClicked"},decls:5,vars:3,consts:[["canvas",""],[1,"canvas"],[1,"tooltip",3,"left","top"],[1,"hint"],[1,"tooltip"],[1,"tooltip__sha"],[1,"tooltip__msg"],[1,"tooltip__meta"],[1,"dot"]],template:function(t,i){t&1&&(Ni(0,"canvas",1,0),Qi(2,DO,11,11,"div",2),St(3,"div",3),ot(4," Arrastra para orbitar \xB7 Scroll para zoom "),Nt()),t&2&&(yt(2),er(i.hovered()?2:-1),yt(),yo("hint--hidden",i.interacted()))},dependencies:[Ba],styles:["[_nghost-%COMP%]{display:block;position:relative;width:100%;height:100%}.canvas[_ngcontent-%COMP%]{display:block;width:100%;height:100%;cursor:grab}.canvas[_ngcontent-%COMP%]:active{cursor:grabbing}.tooltip[_ngcontent-%COMP%]{position:fixed;z-index:10;pointer-events:none;background:#141414eb;border:1px solid #2a2a2a;border-radius:8px;padding:10px 14px;max-width:240px;-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px);animation:_ngcontent-%COMP%_tooltip-in .12s ease}@keyframes _ngcontent-%COMP%_tooltip-in{0%{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}.tooltip__sha[_ngcontent-%COMP%]{font-size:11px;font-family:ui-monospace,monospace;color:var(--accent);margin-bottom:4px}.tooltip__msg[_ngcontent-%COMP%]{font-size:12px;color:var(--white);line-height:1.4;margin-bottom:6px;word-break:break-word}.tooltip__meta[_ngcontent-%COMP%]{font-size:11px;color:var(--muted);display:flex;align-items:center;gap:4px}.tooltip__meta[_ngcontent-%COMP%]   .dot[_ngcontent-%COMP%]{opacity:.4}.hint[_ngcontent-%COMP%]{position:absolute;bottom:24px;left:50%;transform:translate(-50%);font-size:11px;color:var(--muted);opacity:.5;pointer-events:none;transition:opacity .6s ease;white-space:nowrap}.hint--hidden[_ngcontent-%COMP%]{opacity:0}"]})};function NO(n,e){if(n&1){let t=Ir();xt(0,"app-search",4),ws("search",function(r){Di(t);let s=un();return Ci(s.onSearch(r))}),Rt()}}function PO(n,e){if(n&1&&(xt(0,"div",1)(1,"div",5),tr(2,"div",6)(3,"div",7),Rt(),xt(4,"p",8),ot(5,"Cartografiando "),xt(6,"strong"),ot(7),Rt(),ot(8,"\u2026"),Rt()()),n&2){let t=un();yt(7),ci(t.repoLabel())}}function OO(n,e){if(n&1&&(xt(0,"span",20),ot(1),vo(2,"date"),vo(3,"date"),Rt()),n&2){let t=un(2);yt(),vu(" ",_o(2,2,t.oldestDate(),"MMM y")," \u2014 ",_o(3,5,t.newestDate(),"MMM y")," ")}}function LO(n,e){if(n&1){let t=Ir();xt(0,"div",2)(1,"app-universe",9),ws("commitClicked",function(r){Di(t);let s=un();return Ci(s.selectedCommit.set(r))}),Rt(),xt(2,"header",10)(3,"button",11),ws("click",function(){Di(t);let r=un();return Ci(r.resetToSearch())}),Yi(),xt(4,"svg",12),tr(5,"path",13),Rt()(),Dr(),xt(6,"div",14),Yi(),xt(7,"svg",15),tr(8,"rect",16)(9,"path",17),Rt(),ot(10),Rt(),Dr(),xt(11,"div",18)(12,"span",19),ot(13),Rt(),Qi(14,OO,4,8,"span",20),Rt()(),xt(15,"div",21)(16,"div",22),tr(17,"span",23),xt(18,"span"),ot(19,"Primer commit"),Rt()(),xt(20,"div",22),tr(21,"span",24),xt(22,"span"),ot(23,"Mitad"),Rt()(),xt(24,"div",22),tr(25,"span",25),xt(26,"span"),ot(27,"M\xE1s reciente"),Rt()()()()}if(n&2){let t=un();yt(),mu("commits",t.commits()),yt(9),nr(" ",t.repoLabel()," "),yt(3),nr("",t.commits().length," commits"),yt(),er(t.oldestDate()&&t.newestDate()?14:-1)}}function FO(n,e){if(n&1){let t=Ir();xt(0,"div",3)(1,"p",26),ot(2,"\u2726"),Rt(),xt(3,"p",27),ot(4),Rt(),xt(5,"button",28),ws("click",function(){Di(t);let r=un();return Ci(r.resetToSearch())}),ot(6,"Volver"),Rt()()}if(n&2){let t=un();yt(4),ci(t.errorMsg())}}var zf=class n{github=Ee(Hu);state=ln("search");commits=ln([]);repoLabel=ln("");errorMsg=ln("");selectedCommit=ln(null);oldestDate=xu(()=>{let e=this.commits();return e.length?e.reduce((t,i)=>t.date<i.date?t:i).date:null});newestDate=xu(()=>{let e=this.commits();return e.length?e.reduce((t,i)=>t.date>i.date?t:i).date:null});onSearch(e){let t=this.github.parseRepo(e);if(!t){this.state.set("error"),this.errorMsg.set("No puedo parsear ese repo.");return}this.state.set("loading"),this.repoLabel.set(`${t.owner}/${t.name}`),this.commits.set([]),this.github.getCommits(t.owner,t.name).subscribe({next:i=>{this.commits.set(i),this.state.set("universe")},error:i=>{let r=i.status===404?"Repositorio no encontrado.":i.status===403?"L\xEDmite de API de GitHub alcanzado. Espera un momento.":"Error al cargar commits.";this.errorMsg.set(r),this.state.set("error")}})}resetToSearch(){this.state.set("search"),this.commits.set([]),this.repoLabel.set(""),this.selectedCommit.set(null)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Ss({type:n,selectors:[["app-root"]],decls:5,vars:1,consts:[[1,"app"],[1,"loading"],[1,"universe-shell"],[1,"error-screen"],[3,"search"],[1,"loading__orbit"],[1,"loading__planet"],[1,"loading__ring"],[1,"loading__label"],[1,"universe-canvas",3,"commitClicked","commits"],[1,"hud-top"],["aria-label","Volver",1,"hud-back",3,"click"],["width","14","height","14","viewBox","0 0 14 14","fill","none"],["d","M11 7H3M6 3L2 7l4 4","stroke","currentColor","stroke-width","1.5","stroke-linecap","round","stroke-linejoin","round"],[1,"hud-repo"],["width","14","height","14","viewBox","0 0 14 14","fill","none","aria-hidden","true"],["x","2","y","2","width","10","height","10","rx","2","stroke","currentColor","stroke-width","1.2"],["d","M5 5h4M5 7h4M5 9h2","stroke","currentColor","stroke-width","1.2","stroke-linecap","round"],[1,"hud-stats"],[1,"hud-badge"],[1,"hud-badge","hud-badge--muted"],[1,"legend"],[1,"legend__item"],[1,"legend__dot",2,"background","#6366f1"],[1,"legend__dot",2,"background","#fbbf24"],[1,"legend__dot",2,"background","#ffffff"],[1,"error-screen__icon"],[1,"error-screen__msg"],[1,"error-screen__btn",3,"click"]],template:function(t,i){if(t&1&&(xt(0,"div",0),Qi(1,NO,1,0,"app-search")(2,PO,9,1,"div",1)(3,LO,28,4,"div",2)(4,FO,7,1,"div",3),Rt()),t&2){let r;yt(),er((r=i.state())==="search"?1:r==="loading"?2:r==="universe"?3:r==="error"?4:-1)}},dependencies:[zu,Hf,Ba],styles:[".app[_ngcontent-%COMP%]{width:100%;height:100dvh;overflow:hidden;position:relative}.loading[_ngcontent-%COMP%]{position:fixed;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:32px}.loading__orbit[_ngcontent-%COMP%]{position:relative;width:64px;height:64px;display:flex;align-items:center;justify-content:center}.loading__planet[_ngcontent-%COMP%]{width:12px;height:12px;border-radius:50%;background:var(--white);box-shadow:0 0 12px #ffffff80}.loading__ring[_ngcontent-%COMP%]{position:absolute;inset:0;border-radius:50%;border:1px solid rgba(255,255,255,.15);border-top-color:var(--accent);animation:_ngcontent-%COMP%_spin 1.2s linear infinite}@keyframes _ngcontent-%COMP%_spin{to{transform:rotate(360deg)}}.loading__label[_ngcontent-%COMP%]{font-size:13px;color:var(--muted)}.loading__label[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{color:var(--white);font-weight:500}.universe-shell[_ngcontent-%COMP%]{position:relative;width:100%;height:100dvh}.universe-canvas[_ngcontent-%COMP%]{position:absolute;inset:0}.hud-top[_ngcontent-%COMP%]{position:absolute;top:0;left:0;right:0;padding:16px 20px;display:flex;align-items:center;gap:12px;background:linear-gradient(to bottom,rgba(10,10,10,.8) 0%,transparent 100%);pointer-events:none}.hud-top[_ngcontent-%COMP%] > *[_ngcontent-%COMP%]{pointer-events:auto}.hud-back[_ngcontent-%COMP%]{width:32px;height:32px;border-radius:8px;background:#ffffff0f;border:1px solid var(--stroke);display:flex;align-items:center;justify-content:center;color:var(--muted);transition:color .15s,background .15s;flex-shrink:0}.hud-back[_ngcontent-%COMP%]:hover{color:var(--white);background:#ffffff1a}.hud-repo[_ngcontent-%COMP%]{display:flex;align-items:center;gap:6px;font-size:13px;font-weight:500;color:var(--white)}.hud-stats[_ngcontent-%COMP%]{margin-left:auto;display:flex;align-items:center;gap:8px}.hud-badge[_ngcontent-%COMP%]{font-size:11px;font-weight:500;padding:4px 10px;border-radius:20px;background:#ffffff0f;border:1px solid var(--stroke);color:var(--white);letter-spacing:.02em}.hud-badge--muted[_ngcontent-%COMP%]{color:var(--muted)}.legend[_ngcontent-%COMP%]{position:absolute;bottom:24px;right:20px;display:flex;flex-direction:column;gap:8px;background:#0a0a0ab3;border:1px solid var(--stroke);border-radius:10px;padding:12px 14px;-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px)}.legend__item[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;font-size:11px;color:var(--muted)}.legend__dot[_ngcontent-%COMP%]{width:8px;height:8px;border-radius:50%;flex-shrink:0}.error-screen[_ngcontent-%COMP%]{position:fixed;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px}.error-screen__icon[_ngcontent-%COMP%]{font-size:24px;color:var(--muted)}.error-screen__msg[_ngcontent-%COMP%]{font-size:14px;color:var(--muted);max-width:300px;text-align:center;line-height:1.5}.error-screen__btn[_ngcontent-%COMP%]{margin-top:8px;padding:10px 24px;border-radius:8px;border:1px solid var(--stroke);font-size:13px;color:var(--white);background:var(--surface);transition:background .15s}.error-screen__btn[_ngcontent-%COMP%]:hover{background:#ffffff14}"]})};Km(zf,dM).catch(n=>console.error(n));
