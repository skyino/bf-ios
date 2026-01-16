import{_ as x}from"./main-BpxG5uGX.js";import"./DarkTheme-DghcCykv.js";class w{constructor(e){this.msp=e,this.isMonitoring=!1,this.metrics={totalRequests:0,completedRequests:0,failedRequests:0,timeouts:0,duplicates:0,avgResponseTime:0,maxResponseTime:0,queuePeakSize:0,requestsByCode:new Map,responseTimes:[],errorsByType:new Map},this.alerts={queueFull:!1,highTimeout:!1,slowResponses:!1,memoryLeak:!1},this.thresholds={maxQueueSize:Math.floor((this.msp.MAX_QUEUE_SIZE||100)*.8),maxAvgResponseTime:2e3,maxTimeoutRate:.1,memoryLeakThreshold:100},this.monitoringInterval=null,this.listeners=[],this._hookMSPMethods()}_hookMSPMethods(){if(this.msp._mspQueueMonitorInstrumented){console.warn("MSP instance is already instrumented by MSPQueueMonitor");return}this.originalSendMessage=this.msp.send_message.bind(this.msp),this.originalDispatchMessage=this.msp._dispatch_message.bind(this.msp),this.msp.send_message=(...e)=>(this._trackRequestStart(e[0],e[1]),this.originalSendMessage(...e)),this.msp._dispatch_message=(...e)=>{let t=null;if(Array.isArray(this.msp.callbacks)){const s=this.msp.code,i=this.msp.message_buffer;if(s!==void 0&&i)if(typeof this.msp._getRequestKey=="function"){const o=this.msp._getRequestKey(s,i);t=this.msp.callbacks.find(n=>n.requestKey===o)}else t=this.msp.callbacks.find(o=>o.code===s)}return t&&this._trackRequestCompletion(t),this._trackResponse(),this.originalDispatchMessage(...e)},this.msp._mspQueueMonitorInstrumented=!0}_trackRequestStart(e,t){this.metrics.totalRequests++;const s=this.metrics.requestsByCode.get(e)||0;this.metrics.requestsByCode.set(e,s+1);try{(this.msp.callbacks||[]).some(a=>{if(a.code!==e)return!1;const l=t,d=a.data;if(!l&&!d)return!0;if(!l||!d)return!1;if(l===d)return!0;if(l.length!==d.length)return!1;for(let h=0;h<l.length;h++)if(l[h]!==d[h])return!1;return!0})&&this.metrics.duplicates++}catch{}const i=this.msp.callbacks?.length??0;i>this.metrics.queuePeakSize&&(this.metrics.queuePeakSize=i),this._checkAlerts()}_trackResponse(){}_trackRequestCompletion(e){if(!e)return;const t=performance.now()-e.start;if(this.metrics.responseTimes.push(t),this.metrics.responseTimes.length>100&&this.metrics.responseTimes.shift(),t>this.metrics.maxResponseTime&&(this.metrics.maxResponseTime=t),this.metrics.avgResponseTime=this.metrics.responseTimes.reduce((s,i)=>s+i,0)/this.metrics.responseTimes.length,e.attempts>1&&(this.metrics.timeouts+=e.attempts-1),e.success===!1){this.metrics.failedRequests++;const s=e.errorType||"unknown",i=this.metrics.errorsByType.get(s)||0;this.metrics.errorsByType.set(s,i+1)}else this.metrics.completedRequests++;this._checkAlerts()}_checkAlerts(){const e=this.msp.callbacks?.length??0,t=this.alerts.queueFull;this.alerts.queueFull=e>this.thresholds.maxQueueSize;const s=this.metrics.totalRequests>0?this.metrics.timeouts/this.metrics.totalRequests:0,i=this.alerts.highTimeout;this.alerts.highTimeout=s>this.thresholds.maxTimeoutRate;const o=this.alerts.slowResponses;this.alerts.slowResponses=this.metrics.avgResponseTime>this.thresholds.maxAvgResponseTime;const n=this.alerts.memoryLeak;this.alerts.memoryLeak=e>this.thresholds.memoryLeakThreshold,this.alerts.queueFull!==t&&this.alerts.queueFull&&console.warn(`🚨 Queue Full Alert: size ${e}/${this.thresholds.maxQueueSize}`),this.alerts.highTimeout!==i&&this.alerts.highTimeout&&console.warn(`⏱️ High Timeout Alert: rate ${(s*100).toFixed(1)}%`),this.alerts.slowResponses!==o&&this.alerts.slowResponses&&console.warn(`🐌 Slow Response Alert: avg ${this.metrics.avgResponseTime}ms`),this.alerts.memoryLeak!==n&&this.alerts.memoryLeak&&console.warn(`💾 Memory Leak Alert: callbacks ${e}`),this._notifyListeners()}startMonitoring(e=1e3){this.isMonitoring||(this.isMonitoring=!0,this.monitoringInterval=setInterval(()=>{this._collectMetrics(),this._notifyListeners()},e),console.log("MSP Queue Monitor started"))}stopMonitoring(){this.isMonitoring&&(this.isMonitoring=!1,this.monitoringInterval&&(clearInterval(this.monitoringInterval),this.monitoringInterval=null),console.log("MSP Queue Monitor stopped"))}_collectMetrics(){this.currentQueueSize=this.msp.callbacks.length,this.metrics.successRate=this.metrics.totalRequests>0?this.metrics.completedRequests/this.metrics.totalRequests:0,this.metrics.timeoutRate=this.metrics.totalRequests>0?this.metrics.timeouts/this.metrics.totalRequests:0}getStatus(){return{isMonitoring:this.isMonitoring,currentQueueSize:this.msp.callbacks.length,maxQueueSize:this.msp.MAX_QUEUE_SIZE||100,metrics:{...this.metrics},alerts:{...this.alerts},queueContents:this.msp.callbacks.map(e=>({code:e.code,attempts:e.attempts||0,age:performance.now()-e.start,hasTimer:!!e.timer}))}}analyzeQueue(){const e=this.msp.callbacks,t=performance.now(),s={totalItems:e.length,byCode:{},ageDistribution:{fresh:0,recent:0,stale:0,ancient:0},retryDistribution:{firstAttempt:0,retrying:0,multipleRetries:0},potentialIssues:[]};return e.forEach(i=>{s.byCode[i.code]||(s.byCode[i.code]=0),s.byCode[i.code]++;const o=t-i.start;o<1e3?s.ageDistribution.fresh++:o<5e3?s.ageDistribution.recent++:o<1e4?s.ageDistribution.stale++:s.ageDistribution.ancient++;const n=i.attempts||0;n===0?s.retryDistribution.firstAttempt++:n===1?s.retryDistribution.retrying++:s.retryDistribution.multipleRetries++,o>1e4&&s.potentialIssues.push(`Ancient request: code ${i.code}, age ${Math.round(o/1e3)}s`),n>5&&s.potentialIssues.push(`High retry count: code ${i.code}, attempts ${n}`),i.timer||s.potentialIssues.push(`Missing timer: code ${i.code}`)}),s}addListener(e){this.listeners.push(e)}removeListener(e){const t=this.listeners.indexOf(e);t>-1&&this.listeners.splice(t,1)}_notifyListeners(){const e=this.getStatus();this.listeners.forEach(t=>{try{t(e)}catch(s){console.error("Error in MSP monitor listener:",s)}})}resetMetrics(){this.metrics={totalRequests:0,completedRequests:0,failedRequests:0,timeouts:0,duplicates:0,avgResponseTime:0,maxResponseTime:0,queuePeakSize:0,requestsByCode:new Map,responseTimes:[],errorsByType:new Map}}clearAlerts(){console.log("🔄 Clearing all alerts..."),this.alerts={queueFull:!1,highTimeout:!1,slowResponses:!1,memoryLeak:!1},this._notifyListeners()}resetAll(){this.resetMetrics(),this.clearAlerts()}generateReport(){const e=this.getStatus(),t=this.analyzeQueue();return{timestamp:new Date().toISOString(),summary:{queueHealth:this._assessQueueHealth(),performanceGrade:this._calculatePerformanceGrade(),recommendations:this._generateRecommendations()},status:e,analysis:t,rawMetrics:this.metrics}}_assessQueueHealth(){const t=Object.values(this.alerts).filter(s=>s).length;return t===0?"HEALTHY":t<=2?"WARNING":"CRITICAL"}_calculatePerformanceGrade(){let e=100;this.metrics.timeoutRate>.1?e-=30:this.metrics.timeoutRate>.05&&(e-=15),this.metrics.avgResponseTime>2e3?e-=25:this.metrics.avgResponseTime>1e3&&(e-=10);const s=(this.currentQueueSize||(this.msp.callbacks?.length??0))/(this.msp.MAX_QUEUE_SIZE||100);return s>.8?e-=20:s>.6&&(e-=10),(this.metrics.totalRequests>0?this.metrics.failedRequests/this.metrics.totalRequests:0)>.05&&(e-=15),e>=90?"A":e>=80?"B":e>=70?"C":e>=60?"D":"F"}_generateRecommendations(){const e=[];return this.alerts.queueFull&&e.push("Queue is near capacity. Consider implementing request prioritization or increasing queue size."),this.alerts.highTimeout&&e.push("High timeout rate detected. Check serial connection stability or increase timeout values."),this.alerts.slowResponses&&e.push("Slow response times detected. Investigate flight controller performance or reduce request frequency."),this.alerts.memoryLeak&&e.push("Potential memory leak detected. Check that all requests are being properly cleaned up."),this.metrics.maxResponseTime>5e3&&e.push("Some requests are taking very long to complete. Consider implementing request timeouts."),e}triggerTestAlerts(){console.log("🧪 Triggering test alerts...");const e={...this.alerts};return this.alerts.queueFull=!0,this.alerts.highTimeout=!0,this.alerts.slowResponses=!0,this.alerts.memoryLeak=!0,console.log("🚨 Test alerts triggered:",this.alerts),this._notifyListeners(),setTimeout(()=>{this.alerts=e,console.log("✅ Test alerts reset"),this._notifyListeners()},1e4),this.alerts}setTestThresholds(){console.log("🎯 Setting test thresholds for easier alert triggering..."),this.thresholds={maxQueueSize:1,maxAvgResponseTime:100,maxTimeoutRate:.01,memoryLeakThreshold:5},console.log("New thresholds:",this.thresholds)}setNormalThresholds(){console.log("🔧 Resetting to normal thresholds..."),this.thresholds={maxQueueSize:Math.floor((this.msp.MAX_QUEUE_SIZE||100)*.8),maxAvgResponseTime:2e3,maxTimeoutRate:.1,memoryLeakThreshold:100},console.log("Normal thresholds restored:",this.thresholds)}destroy(){this.stopMonitoring(),this.originalSendMessage&&(this.msp.send_message=this.originalSendMessage),this.originalDispatchMessage&&(this.msp._dispatch_message=this.originalDispatchMessage),this.originalRemoveRequest&&(this.msp._removeRequestFromCallbacks=this.originalRemoveRequest),this.msp._mspQueueMonitorInstrumented=void 0,this.listeners=[],T=null}}let T=null;const u={get instance(){if(!T){if(typeof window>"u"||!window.MSP)throw new Error("MSP Queue Monitor: window.MSP is not available. Make sure MSP is loaded before using the monitor.");T=new w(window.MSP)}return T},startMonitoring(...r){return this.instance.startMonitoring(...r)},stopMonitoring(...r){return this.instance.stopMonitoring(...r)},getStatus(...r){return this.instance.getStatus(...r)},analyzeQueue(...r){return this.instance.analyzeQueue(...r)},addListener(...r){return this.instance.addListener(...r)},removeListener(...r){return this.instance.removeListener(...r)},resetMetrics(...r){return this.instance.resetMetrics(...r)},clearAlerts(...r){return this.instance.clearAlerts(...r)},resetAll(...r){return this.instance.resetAll(...r)},generateReport(...r){return this.instance.generateReport(...r)},triggerTestAlerts(...r){return this.instance.triggerTestAlerts(...r)},setTestThresholds(...r){return this.instance.setTestThresholds(...r)},setNormalThresholds(...r){return this.instance.setNormalThresholds(...r)},destroy(...r){return this.instance.destroy(...r)},get isMonitoring(){return this.instance.isMonitoring},get metrics(){return this.instance.metrics},get alerts(){return this.instance.alerts},get thresholds(){return this.instance.thresholds}},q=Object.freeze(Object.defineProperty({__proto__:null,MSPQueueMonitor:w,mspQueueMonitor:u},Symbol.toStringTag,{value:"Module"}));class P{constructor(e){this.msp=e,this.monitor=u,this.isRunning=!1,this.testResults=[],this.currentTest=null,this.testCodes={MSP_IDENT:100,MSP_STATUS:101,MSP_RAW_IMU:102,MSP_SERVO:103,MSP_MOTOR:104,MSP_RC:105,MSP_RAW_GPS:106,MSP_COMP_GPS:107,MSP_ATTITUDE:108,MSP_ALTITUDE:109,MSP_ANALOG:110,MSP_RC_TUNING:111,MSP_PID:112,MSP_PIDNAMES:116,MSP_BOXNAMES:117,MSP_MISC:114,MSP_MOTOR_PINS:115}}getTestStatus(e){return!e||typeof e!="object"||e.error||e.memoryLeakDetected===!0||e.overflowHandled===!1||e.timeoutOccurred===!0&&e.recoveryTime>2e3||e.recoverySuccessful===!1||e.failed&&e.failed>0||e.duplicateRejections&&e.duplicateRejections>0||e.leaked&&e.leaked>0||e.failedWhileDisconnected&&e.failedWhileDisconnected>0&&e.recoverySuccessful===!1?"FAILED":"PASSED"}async runStressTestSuite(){console.log("🚀 Starting MSP Stress Test Suite"),this.isRunning=!0,this.monitor.startMonitoring(100);const e=[{name:"Queue Flooding",test:()=>this.testQueueFlooding()},{name:"Rapid Fire Requests",test:()=>this.testRapidFireRequests()},{name:"Duplicate Request Handling",test:()=>this.testDuplicateRequests()},{name:"Timeout Recovery",test:()=>this.testTimeoutRecovery()},{name:"Memory Leak Detection",test:()=>this.testMemoryLeaks()},{name:"Concurrent Mixed Requests",test:()=>this.testConcurrentMixedRequests()},{name:"Queue Overflow Handling",test:()=>this.testQueueOverflow()},{name:"Connection Disruption",test:()=>this.testConnectionDisruption()},{name:"Performance Under Load",test:()=>this.testPerformanceUnderLoad()}],t=[];try{for(const i of e)try{console.log(`
📋 Running: ${i.name}`),this.currentTest=i.name,this.monitor.resetAll();const o=performance.now(),n=await i.test(),a=performance.now()-o,l={name:i.name,status:this.getTestStatus(n),duration:a,result:n,metrics:this.monitor.getStatus(),timestamp:new Date().toISOString()};t.push(l),console.log(`✅ ${i.name} completed in ${Math.round(a)}ms`),await this.wait(1e3)}catch(o){console.error(`❌ ${i.name} failed:`,o),t.push({name:i.name,status:"FAILED",error:o.message,timestamp:new Date().toISOString()})}this.testResults=t;const s=this.generateTestReport(t);return console.log(`
📊 Stress Test Suite Complete`),console.log(s.summary),s}finally{this.monitor.stopMonitoring(),this.isRunning=!1,this.currentTest=null}}async testQueueFlooding(){const t=[];console.log("  Flooding queue with 110 requests...");for(let n=0;n<110;n++){const a=Object.values(this.testCodes)[n%Object.keys(this.testCodes).length],l=this.msp.promise(a,null).catch(d=>({error:d.message}));t.push(l)}const s=await Promise.allSettled(t),i=s.filter(n=>n.status==="fulfilled"&&!(n.value&&n.value.error)).length,o=s.length-i;return{requestsSent:110,successful:i,failed:o,successRate:i/110,peakQueueSize:(this.monitor.getStatus().metrics||{}).queuePeakSize??0}}async testRapidFireRequests(){console.log("  Sending 20 requests with 10ms intervals...");const s=[],i=performance.now();for(let c=0;c<20;c++){const g=this.testCodes.MSP_STATUS,f=performance.now(),m=this.msp.promise(g,null).then(()=>({success:!0,responseTime:performance.now()-f,index:c})).catch(p=>({success:!1,error:p.message,responseTime:performance.now()-f,index:c}));s.push(m),c<19&&await this.wait(10)}const o=await Promise.allSettled(s),n=performance.now()-i,a=o.map(c=>c.status==="fulfilled"?c.value:{success:!1,error:c.reason?.message||"Unknown error",responseTime:0,index:-1}),l=a.filter(c=>c.success).length,d=a.map(c=>c.responseTime).filter(c=>c>0),h=d.length>0?d.reduce((c,g)=>c+g,0)/d.length:0;return{requestCount:20,successful:l,failed:20-l,totalTime:n,avgResponseTime:h,throughput:20/(n/1e3),concurrentRequests:!0,maxConcurrentRequests:20}}async testDuplicateRequests(){const e=this.testCodes.MSP_IDENT,t=new Uint8Array([1,2,3]),s=5;console.log(`  Sending ${s} duplicate requests...`);const i=[];for(let l=0;l<s;l++)i.push(this.msp.promise(e,t).catch(d=>({error:d.message})));const o=await Promise.allSettled(i),n=o.filter(l=>l.status==="fulfilled"&&!(l.value&&l.value.error)).length,a=o.filter(l=>l.status==="rejected"||l.value&&l.value.error&&l.value.error.includes("duplicate")).length;return{duplicatesSent:s,successful:n,duplicateRejections:a,queueSizeAfter:this.msp.callbacks.length}}async testTimeoutRecovery(){console.log("  Testing timeout recovery...");const e=this.msp.TIMEOUT;this.msp.TIMEOUT=100;try{const t=this.testCodes.MSP_STATUS,s=performance.now();try{return await this.msp.promise(t,null),{error:"Expected timeout but request succeeded"}}catch{const i=performance.now()-s;this.msp.TIMEOUT=e,await this.wait(200);const o=performance.now();await this.msp.promise(this.testCodes.MSP_IDENT,null);const n=performance.now()-o;return{timeoutOccurred:!0,timeoutDuration:i,recoveryTime:n,queueCleanedUp:this.msp.callbacks.length===0}}}finally{this.msp.TIMEOUT=e}}async testMemoryLeaks(){console.log("  Testing for memory leaks...");const e=this.msp.callbacks.length,t=10,s=[];for(let n=0;n<t;n++)s.push(this.msp.promise(this.testCodes.MSP_STATUS,null).catch(()=>{}));await Promise.allSettled(s),await this.wait(100);const i=this.msp.callbacks.length,o=i-e;return{initialCallbacks:e,finalCallbacks:i,leaked:o,memoryLeakDetected:o>0,requestsProcessed:t}}async testConcurrentMixedRequests(){console.log("  Testing concurrent mixed requests...");const e=[],t=Object.values(this.testCodes);for(let a=0;a<15;a++){const l=t[a%t.length],d=a%3===0?new Uint8Array([a]):null;e.push(this.msp.promise(l,d).catch(h=>({error:h.message})))}const s=performance.now(),i=await Promise.allSettled(e),o=performance.now()-s,n=i.filter(a=>a.status==="fulfilled"&&!(a.value&&a.value.error)).length;return{totalRequests:e.length,successful:n,failed:e.length-n,totalTime:o,concurrentProcessing:!0}}async testQueueOverflow(){console.log("  Testing queue overflow handling...");const e=this.msp.MAX_QUEUE_SIZE||100,t=e+10,s=[];for(let n=0;n<t;n++)s.push(this.msp.promise(this.testCodes.MSP_STATUS,null).catch(a=>({error:a.message})));const o=(await Promise.allSettled(s)).filter(n=>n.status==="rejected"||n.value?.error).length;return{attemptedRequests:t,maxQueueSize:e,rejectedDueToOverflow:o,overflowHandled:o>0,finalQueueSize:this.msp.callbacks.length}}async testConnectionDisruption(){console.log("  Simulating connection disruption...");const e=this.msp.serial?.connected;try{this.msp.serial&&(this.msp.serial.connected=!1);const t=[];for(let n=0;n<5;n++)t.push(this.msp.promise(this.testCodes.MSP_STATUS,null).catch(a=>({error:a.message})));const i=(await Promise.allSettled(t)).filter(n=>n.status==="rejected"||n.value?.error).length;this.msp.serial&&(this.msp.serial.connected=e),await this.wait(100);const o=await this.msp.promise(this.testCodes.MSP_IDENT,null).catch(n=>({error:n.message}));return{failedWhileDisconnected:i,recoverySuccessful:!o.error,connectionHandled:i>0}}finally{this.msp.serial&&(this.msp.serial.connected=e)}}async testPerformanceUnderLoad(){console.log("  Testing performance under sustained load...");const e=5e3,t=20,s=10,i=200,o=performance.now(),n=[];let a=0,l=[],d=o;for(;performance.now()-o<e;){const m=performance.now();a++;const p=this.msp.promise(this.testCodes.MSP_STATUS,null).then(()=>({success:!0,responseTime:performance.now()-m})).catch(M=>({success:!1,responseTime:performance.now()-m,error:M.message}));l.push(p);const b=performance.now();(l.length>=s||b-d>=i)&&((await Promise.allSettled(l)).forEach(R=>{R.status==="fulfilled"?n.push(R.value):n.push({success:!1,responseTime:0,error:R.reason?.message||"Unknown error"})}),l=[],d=b),await this.wait(t)}l.length>0&&(await Promise.allSettled(l)).forEach(p=>{p.status==="fulfilled"?n.push(p.value):n.push({success:!1,responseTime:0,error:p.reason?.message||"Unknown error"})});const h=n.filter(m=>m.success).length,c=n.map(m=>m.responseTime).filter(m=>m>0),g=c.length>0?c.reduce((m,p)=>m+p,0)/c.length:0,f=c.length>0?Math.max(...c):0;return{duration:e,requestCount:a,successful:h,failed:a-h,successRate:h/a,avgResponseTime:g,maxResponseTime:f,throughput:a/(e/1e3),concurrentRequests:!0,batchSize:s,maxConcurrentRequests:s}}generateTestReport(e){const t=e.length,s=e.filter(a=>a.status==="PASSED").length,i=t-s,o={totalTests:t,passed:s,failed:i,successRate:s/t,overallGrade:this._calculateOverallGrade(e)},n=this._generateTestRecommendations(e);return{timestamp:new Date().toISOString(),summary:o,recommendations:n,detailedResults:e,monitorReport:this.monitor.generateReport()}}_calculateOverallGrade(e){const t=e.filter(s=>s.status==="PASSED").length/e.length;return t>=.95?"A+":t>=.9?"A":t>=.85?"B+":t>=.8?"B":t>=.75?"C+":t>=.7?"C":t>=.6?"D":"F"}_generateTestRecommendations(e){const t=[],s=e.filter(a=>a.status==="FAILED");return s.length>0&&t.push(`${s.length} tests failed. Review implementation for: ${s.map(a=>a.name).join(", ")}`),e.find(a=>a.name==="Performance Under Load")?.result?.avgResponseTime>1e3&&t.push("Average response time is high. Consider optimizing MSP request handling."),e.find(a=>a.name==="Memory Leak Detection")?.result?.memoryLeakDetected&&t.push("Memory leak detected. Ensure all callbacks are properly cleaned up."),e.find(a=>a.name==="Queue Overflow Handling")?.result?.overflowHandled||t.push("Queue overflow not properly handled. Implement proper queue management."),t}wait(e){return new Promise(t=>setTimeout(t,e))}async runSpecificTest(e){const s={"queue-flooding":()=>this.testQueueFlooding(),"rapid-fire":()=>this.testRapidFireRequests(),duplicates:()=>this.testDuplicateRequests(),"timeout-recovery":()=>this.testTimeoutRecovery(),"memory-leaks":()=>this.testMemoryLeaks(),"concurrent-mixed":()=>this.testConcurrentMixedRequests(),"queue-overflow":()=>this.testQueueOverflow(),"connection-disruption":()=>this.testConnectionDisruption(),"performance-load":()=>this.testPerformanceUnderLoad()}[e];if(!s)throw new Error(`Unknown test: ${e}`);console.log(`🧪 Running specific test: ${e}`),this.monitor.startMonitoring(100),this.monitor.resetAll();try{const i=await s();return{name:e,status:"PASSED",result:i,metrics:this.monitor.getStatus()}}catch(i){return{name:e,status:"FAILED",error:i.message}}finally{this.monitor.stopMonitoring()}}destroy(){this.monitor.isMonitoring&&this.monitor.stopMonitoring()}}let v=null;const S={get instance(){if(!v){if(typeof window>"u"||!window.MSP)throw new Error("MSP Stress Test: window.MSP is not available. Make sure MSP is loaded before using the stress test.");v=new P(window.MSP)}return v},runStressTestSuite(...r){return this.instance.runStressTestSuite(...r)},runSpecificTest(...r){return this.instance.runSpecificTest(...r)},generateTestReport(...r){return this.instance.generateTestReport(...r)},wait(...r){return this.instance.wait(...r)},destroy(...r){return this.instance.destroy(...r)},testQueueFlooding(...r){return this.instance.testQueueFlooding(...r)},testRapidFireRequests(...r){return this.instance.testRapidFireRequests(...r)},testDuplicateRequests(...r){return this.instance.testDuplicateRequests(...r)},testTimeoutRecovery(...r){return this.instance.testTimeoutRecovery(...r)},testMemoryLeaks(...r){return this.instance.testMemoryLeaks(...r)},testConcurrentMixedRequests(...r){return this.instance.testConcurrentMixedRequests(...r)},testQueueOverflow(...r){return this.instance.testQueueOverflow(...r)},testConnectionDisruption(...r){return this.instance.testConnectionDisruption(...r)},testPerformanceUnderLoad(...r){return this.instance.testPerformanceUnderLoad(...r)},get monitor(){return this.instance.monitor},get isRunning(){return this.instance.isRunning},get testResults(){return this.instance.testResults},get currentTest(){return this.instance.currentTest},get testCodes(){return this.instance.testCodes}};class k{constructor(){this.isVisible=!1,this.updateInterval=null,this.chartData={queueSize:[],responseTime:[],timestamps:[]},this.maxDataPoints=50,this.updatesPaused=!1,this.pauseTimeout=null,this.lastUpdateData={},this.lastChartUpdate=0,this.chartUpdatePending=!1,this.elementCache=new Map,this.createDashboard(),this.setupEventListeners()}escapeHtml(e){if(typeof e!="string")return e;const t=document.createElement("div");return t.textContent=e,t.innerHTML}createDashboard(){this.container=document.createElement("div"),this.container.id="msp-debug-dashboard",this.container.innerHTML=`
            <div class="msp-dashboard-header">
                <h3>🔧 MSP Debug Dashboard</h3>
                <div class="dashboard-controls">
                    <button id="msp-toggle-monitoring">Start Monitoring</button>
                    <button id="msp-run-stress-test">Run Stress Test</button>
                    <button id="msp-clear-metrics">Clear Metrics</button>
                    <button id="msp-close-dashboard">×</button>
                </div>
                <div id="updates-status" class="updates-status" style="display: none;">⏸️ Updates Paused</div>
            </div>
            
            <div class="msp-dashboard-content">
                <!-- Status Overview -->
                <div class="status-section">
                    <h4>📊 Status Overview</h4>
                    <div class="status-grid">
                        <div class="status-item">
                            <label>Queue Size:</label>
                            <span id="queue-size" class="value">0</span>
                            <span class="max-value">/ <span id="max-queue-size">50</span></span>
                        </div>
                        <div class="status-item">
                            <label>Success Rate:</label>
                            <span id="success-rate" class="value">100%</span>
                        </div>
                        <div class="status-item">
                            <label>Avg Response:</label>
                            <span id="avg-response-time" class="value">0ms</span>
                        </div>
                        <div class="status-item">
                            <label>Total Requests:</label>
                            <span id="total-requests" class="value">0</span>
                        </div>
                    </div>
                </div>
                
                <!-- Alerts Section -->
                <div class="alerts-section">
                    <h4>🚨 Alerts</h4>
                    <div class="alerts-header">
                        <button id="clear-alerts" style="float: right; padding: 2px 6px; font-size: 10px; background: #666; color: white; border: none; border-radius: 2px; cursor: pointer;">Clear Alerts</button>
                    </div>
                    <div id="alerts-container" class="alerts-container">
                        <div class="no-alerts">No active alerts</div>
                    </div>
                </div>
                
                <!-- Queue Analysis -->
                <div class="queue-section">
                    <h4>📋 Queue Analysis</h4>
                    <div class="queue-controls">
                        <button id="analyze-queue">Analyze Current Queue</button>
                        <button id="export-report">Export Report</button>
                    </div>
                    <div id="queue-analysis" class="queue-analysis"></div>
                </div>
                
                <!-- Live Chart -->
                <div class="chart-section">
                    <h4>📈 Live Metrics</h4>
                    <canvas id="msp-metrics-chart"></canvas>
                </div>
                
                <!-- Request Details -->
                <div class="details-section">
                    <h4>🔍 Current Queue Contents</h4>
                    <div id="queue-contents" class="queue-contents"></div>
                </div>
                
                <!-- Test Results -->
                <div class="test-section">
                    <h4>🧪 Test Results</h4>
                    <div id="test-results" class="test-results"></div>
                </div>
            </div>
        `,this.addStyles(),document.body.appendChild(this.container),this.container.style.display="none"}addStyles(){const e=document.createElement("style");e.textContent=`
            #msp-debug-dashboard {
                position: fixed;
                top: 20px;
                right: 20px;
                width: 500px;
                max-height: 80vh;
                background: #1e1e1e;
                color: #ffffff;
                border: 1px solid #444;
                border-radius: 8px;
                font-family: 'Courier New', monospace;
                font-size: 12px;
                z-index: 10000;
                overflow-y: auto;
                box-shadow: 0 4px 20px rgba(0,0,0,0.5);
            }
            
            .msp-dashboard-header {
                background: #2d2d2d;
                padding: 10px 15px;
                border-bottom: 1px solid #444;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            
            .msp-dashboard-header h3 {
                margin: 0;
                font-size: 14px;
            }
            
            .dashboard-controls {
                display: flex;
                gap: 5px;
            }
            
            .dashboard-controls button {
                padding: 4px 8px;
                font-size: 11px;
                background: #444;
                color: white;
                border: 1px solid #666;
                border-radius: 3px;
                cursor: pointer;
            }
            
            .dashboard-controls button:hover {
                background: #555;
            }
            
            .updates-status {
                position: absolute;
                top: 50%;
                right: 160px;
                transform: translateY(-50%);
                font-size: 11px;
                color: #ffaa00;
                background: rgba(255, 170, 0, 0.15);
                padding: 3px 8px;
                border-radius: 3px;
                border: 1px solid #ffaa00;
                font-weight: bold;
                z-index: 10001;
            }
            
            .msp-dashboard-content {
                padding: 15px;
            }
            
            .status-section, .alerts-section, .queue-section, .chart-section, .details-section, .test-section {
                margin-bottom: 20px;
                border: 1px solid #333;
                border-radius: 4px;
                padding: 10px;
            }
            
            .status-section h4, .alerts-section h4, .queue-section h4, .chart-section h4, .details-section h4, .test-section h4 {
                margin: 0 0 10px 0;
                font-size: 13px;
                color: #ffd700;
            }
            
            .status-grid {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 10px;
            }
            
            .status-item {
                background: #2a2a2a;
                padding: 8px;
                border-radius: 3px;
            }
            
            .status-item label {
                display: block;
                font-size: 11px;
                color: #ccc;
                margin-bottom: 3px;
            }
            
            .status-item .value {
                font-weight: bold;
                color: #00ff00;
            }
            
            .max-value {
                color: #888;
                font-size: 10px;
            }
            
            .alerts-container {
                min-height: 30px;
            }
            
            .alert-item {
                background: #ff4444;
                color: white;
                padding: 5px 10px;
                border-radius: 3px;
                margin-bottom: 5px;
                font-size: 11px;
            }
            
            .alert-item.warning {
                background: #ffaa00;
            }
            
            .no-alerts {
                color: #888;
                font-style: italic;
                text-align: center;
                padding: 10px;
            }
            
            .queue-controls {
                margin-bottom: 10px;
            }
            
            .queue-controls button {
                padding: 5px 10px;
                margin-right: 5px;
                background: #0066cc;
                color: white;
                border: none;
                border-radius: 3px;
                cursor: pointer;
                font-size: 11px;
            }
            
            .queue-controls button:hover {
                background: #0088ff;
            }
            
            .queue-analysis {
                background: #2a2a2a;
                padding: 10px;
                border-radius: 3px;
                font-size: 11px;
                max-height: 200px;
                overflow-y: auto;
                pointer-events: auto; /* Ensure clickability */
            }
            
            .queue-contents {
                background: #2a2a2a;
                padding: 10px;
                border-radius: 3px;
                max-height: 150px;
                overflow-y: auto;
                pointer-events: auto; /* Ensure clickability */
            }
            
            .queue-item {
                display: flex;
                justify-content: space-between;
                padding: 3px 0;
                border-bottom: 1px solid #333;
                font-size: 11px;
            }
            
            .queue-item:last-child {
                border-bottom: none;
            }
            
            .queue-item-empty {
                opacity: 0.3;
                font-style: italic;
            }
            
            .test-results {
                background: #2a2a2a;
                padding: 10px;
                border-radius: 3px;
                max-height: 200px;
                overflow-y: auto;
                font-size: 11px;
                pointer-events: auto; /* Ensure clickability */
            }
            
            .test-result-item {
                display: flex;
                justify-content: space-between;
                padding: 5px 0;
                border-bottom: 1px solid #333;
                cursor: pointer;
                min-height: 20px; /* Prevent height changes during updates */
            }
            
            .test-result-item:hover {
                background: rgba(255, 255, 255, 0.1);
            }
            
            .test-result-item:last-child {
                border-bottom: none;
            }
            
            .test-passed {
                color: #00ff00;
            }
            
            .test-failed {
                color: #ff4444;
            }
            
            #msp-metrics-chart {
                width: 100%;
                height: 150px;
                background: #2a2a2a;
                border-radius: 3px;
                display: block;
            }
            
            .test-result-item {
                padding: 5px 10px;
                margin: 2px 0;
                border-radius: 3px;
                cursor: pointer;
                display: flex;
                justify-content: space-between;
                align-items: center;
                background: #1a1a1a;
                border: 1px solid #444;
                transition: all 0.2s ease;
                user-select: none;
            }
            
            .test-result-item:hover {
                background: #333 !important;
                border-color: #666;
                transform: translateX(2px);
            }
            
            .test-result-item:active {
                background: #444 !important;
                transform: translateX(0px);
            }
            
            .queue-item {
                padding: 5px;
                margin: 2px 0;
                background: #1a1a1a;
                border-radius: 3px;
                display: flex;
                justify-content: space-between;
                font-size: 11px;
                border: 1px solid #333;
            }
            
            .alert-item {
                padding: 5px 10px;
                margin: 2px 0;
                background: #4a2a2a;
                border-radius: 3px;
                border-left: 3px solid #ff4444;
                color: #ffcccc;
            }
            
            #updates-status {
                position: absolute;
                top: 5px;
                right: 50px;
                background: rgba(255, 165, 0, 0.9);
                color: #000;
                padding: 2px 8px;
                border-radius: 3px;
                font-size: 10px;
                font-weight: bold;
                display: none;
                z-index: 1001;
            }
        `,document.head.appendChild(e)}setupEventListeners(){this.container.addEventListener("click",e=>{if(e.target.id==="msp-toggle-monitoring")this.toggleMonitoring();else if(e.target.id==="msp-run-stress-test")this.runStressTest();else if(e.target.id==="msp-clear-metrics")this.clearMetrics();else if(e.target.id==="clear-alerts")this.clearAlerts();else if(e.target.id==="msp-close-dashboard")this.hide();else if(e.target.id==="analyze-queue")this.analyzeQueue();else if(e.target.id==="export-report")this.exportReport();else{const t=e.target.closest(".test-result-item");if(t){const s=parseInt(t.getAttribute("data-test-index"),10);isNaN(s)||this.showTestDetails(s)}if(e.target.classList.contains("close-details-btn")){const s=e.target.closest(".test-details");s&&(s.remove(),this.pauseUpdates(1e3))}}}),this.setupInteractionHandlers(),u.addListener(e=>{this.updateDisplay(e)}),document.addEventListener("keydown",e=>{e.ctrlKey&&e.shiftKey&&e.key==="M"&&this.toggle()}),window.addEventListener("resize",()=>{this.isVisible&&setTimeout(()=>this.drawChart(),100)})}show(){this.container.style.display="block",this.isVisible=!0,this.updateDisplay()}hide(){this.container.style.display="none",this.isVisible=!1}toggle(){this.isVisible?this.hide():this.show()}setupInteractionHandlers(){const e=[".test-results",".queue-analysis",".test-result-item","button","select","input",".queue-item",".alert-item"];e.forEach(t=>{this.container.addEventListener("mouseenter",s=>{(s.target.matches(t)||s.target.closest(t))&&this.pauseUpdates(3e3)},!0)}),this.container.addEventListener("click",t=>{e.some(i=>t.target.matches(i)||t.target.closest(i))&&this.pauseUpdates(5e3)}),this.container.addEventListener("focusin",t=>{t.target.matches("input, select, textarea, button")&&this.pauseUpdates(1e4)}),this.container.addEventListener("focusout",t=>{this.container.contains(t.relatedTarget)||this.pauseUpdates(1e3)}),this.container.addEventListener("mouseenter",t=>{const s=t.target.closest(".test-result-item");s&&(s.style.backgroundColor="#333",this.pauseUpdates(2e3))},!0),this.container.addEventListener("mouseleave",t=>{const s=t.target.closest(".test-result-item");s&&(s.style.backgroundColor="")},!0)}toggleMonitoring(){const e=document.getElementById("msp-toggle-monitoring");u.isMonitoring?(u.stopMonitoring(),e.textContent="Start Monitoring",e.style.background="#444"):(u.startMonitoring(500),e.textContent="Stop Monitoring",e.style.background="#00aa00")}async runStressTest(){const e=document.getElementById("msp-run-stress-test"),t=e.textContent;e.textContent="Running Tests...",e.disabled=!0;try{const s=await S.runStressTestSuite();this.displayTestResults(s)}catch(s){console.error("Stress test failed:",s),this.displayTestResults({summary:{failed:1,error:s.message},detailedResults:[]})}finally{e.textContent=t,e.disabled=!1}}clearMetrics(){u.resetMetrics(),this.chartData={queueSize:[],responseTime:[],timestamps:[]},this.updateDisplay()}clearAlerts(){u.clearAlerts()}updateDisplay(e=null){!this.isVisible||this.updatesPaused||(e=e||u.getStatus(),this._hasDataChanged(e)&&(this._updateStatusMetrics(e),this._updateAlertsIfChanged(e.alerts),this._updateQueueContentsIfChanged(e.queueContents),this._updateChart(e),this.lastUpdateData={currentQueueSize:e.currentQueueSize,totalRequests:e.metrics.totalRequests,successRate:e.metrics.successRate,avgResponseTime:e.metrics.avgResponseTime,alerts:JSON.stringify(e.alerts),queueContents:JSON.stringify(e.queueContents)}))}_hasDataChanged(e){const t=this.lastUpdateData;if(!t||t.currentQueueSize!==e.currentQueueSize||t.totalRequests!==e.metrics.totalRequests||t.successRate!==e.metrics.successRate||t.avgResponseTime!==e.metrics.avgResponseTime)return!0;try{const s=JSON.stringify(e.alerts),i=JSON.stringify(e.queueContents);return t.alerts!==s||t.queueContents!==i}catch(s){return console.warn("JSON stringify failed in dashboard update check:",s),!0}}_updateStatusMetrics(e){this.updateElement("queue-size",e.currentQueueSize),this.updateElement("max-queue-size",e.maxQueueSize),this.updateElement("success-rate",`${Math.round((e.metrics.successRate||0)*100)}%`),this.updateElement("avg-response-time",`${Math.round(e.metrics.avgResponseTime||0)}ms`),this.updateElement("total-requests",e.metrics.totalRequests)}_updateAlertsIfChanged(e){const t=JSON.stringify(e);this.lastUpdateData.alerts!==t&&this.updateAlerts(e)}_updateQueueContentsIfChanged(e){const t=JSON.stringify(e);this.lastUpdateData.queueContents!==t&&this.updateQueueContents(e)}pauseUpdates(e=2e3){this.updatesPaused=!0;const t=document.getElementById("updates-status");t&&(t.style.display="block"),this.pauseTimeout&&clearTimeout(this.pauseTimeout),this.pauseTimeout=setTimeout(()=>{this.updatesPaused=!1,t&&(t.style.display="none"),this.updateDisplay()},e)}updateElement(e,t){let s=this.elementCache.get(e);s||(s=document.getElementById(e),s&&this.elementCache.set(e,s)),s&&s.textContent!==t&&(s.textContent=t)}updateAlerts(e){const t=document.getElementById("alerts-container");if(!t)return;const s=Object.entries(e).filter(([o,n])=>n);if(s.length===0){t.innerHTML='<div class="no-alerts">No active alerts</div>';return}const i={queueFull:"Queue is near capacity",highTimeout:"High timeout rate detected",slowResponses:"Slow response times detected",memoryLeak:"Potential memory leak detected"};t.innerHTML=s.map(([o,n])=>`<div class="alert-item">${i[o]||this.escapeHtml(o)}</div>`).join("")}updateQueueContents(e){const t=document.getElementById("queue-contents");if(!t)return;const s=5,i=e||[],o=[];for(let n=0;n<s;n++)if(n<i.length){const a=i[n];o.push(`
                    <div class="queue-item">
                        <span>Code: ${this.escapeHtml(a.code)}</span>
                        <span>Age: ${Math.round(a.age)}ms</span>
                        <span>Attempts: ${this.escapeHtml(a.attempts)}</span>
                        <span style="color: ${a.hasTimer?"#00ff00":"#ff4444"}">${a.hasTimer?"✓":"✗"}</span>
                    </div>
                `)}else o.push(`
                    <div class="queue-item queue-item-empty">
                        <span style="color: #555;">—</span>
                        <span style="color: #555;">—</span>
                        <span style="color: #555;">—</span>
                        <span style="color: #555;">—</span>
                    </div>
                `);t.innerHTML=o.join("")}_updateChart(e){this.updateChart(e)}updateChart(e){const t=Date.now();this.lastChartUpdate&&t-this.lastChartUpdate<200||(this.chartData.timestamps.push(t),this.chartData.queueSize.push(e.currentQueueSize),this.chartData.responseTime.push(e.metrics.avgResponseTime||0),this.chartData.timestamps.length>this.maxDataPoints&&(this.chartData.timestamps.shift(),this.chartData.queueSize.shift(),this.chartData.responseTime.shift()),this.lastChartUpdate=t,this.chartUpdatePending||(this.chartUpdatePending=!0,requestAnimationFrame(()=>{this.drawChart(),this.chartUpdatePending=!1})))}drawChart(){const e=document.getElementById("msp-metrics-chart");if(!e)return;const t=e.getContext("2d"),s=e.getBoundingClientRect(),i=s.width,o=s.height,n=window.devicePixelRatio||1;e.width=i*n,e.height=o*n,e.style.width=`${i}px`,e.style.height=`${o}px`,t.scale(n,n);const a=i,l=o;if(t.fillStyle="#2a2a2a",t.fillRect(0,0,a,l),this.chartData.timestamps.length<2)return;t.strokeStyle="#00ff00",t.lineWidth=2,t.beginPath();const d=Math.max(...this.chartData.queueSize,10);this.chartData.queueSize.forEach((h,c)=>{const g=c/(this.chartData.queueSize.length-1)*a,f=l-h/d*l;c===0?t.moveTo(g,f):t.lineTo(g,f)}),t.stroke(),t.fillStyle="#ffffff",t.font="10px monospace",t.fillText("Queue Size",5,15),t.fillText(`Max: ${d}`,5,l-5)}analyzeQueue(){const e=u.analyzeQueue(),t=document.getElementById("queue-analysis");t&&(t.innerHTML=`
            <div><strong>Total Items:</strong> ${e.totalItems}</div>
            <div><strong>Age Distribution:</strong></div>
            <div style="margin-left: 10px;">
                Fresh (&lt;1s): ${e.ageDistribution.fresh}<br>
                Recent (1-5s): ${e.ageDistribution.recent}<br>
                Stale (5-10s): ${e.ageDistribution.stale}<br>
                Ancient (&gt;10s): ${e.ageDistribution.ancient}
            </div>
            <div><strong>By Code:</strong></div>
            <div style="margin-left: 10px;">
                ${Object.entries(e.byCode).map(([s,i])=>`Code ${this.escapeHtml(String(s))}: ${i}`).join("<br>")}
            </div>
            ${e.potentialIssues.length>0?`
                <div><strong>Issues:</strong></div>
                <div style="margin-left: 10px; color: #ff4444;">
                    ${e.potentialIssues.map(s=>this.escapeHtml(String(s))).join("<br>")}
                </div>
            `:""}
        `)}displayTestResults(e){const t=document.getElementById("test-results");if(!t)return;const s=e.summary||{};t.innerHTML=`
            <div><strong>Test Summary:</strong></div>
            <div style="margin-left: 10px;">
                Passed: <span class="test-passed">${s.passed||0}</span><br>
                Failed: <span class="test-failed">${s.failed||0}</span><br>
                Success Rate: ${Math.round((s.successRate||0)*100)}%<br>
                Grade: ${s.overallGrade||"N/A"}
            </div>
            <div style="margin-top: 10px;"><strong>Details (click for more info):</strong></div>
            <div style="margin-left: 10px;">
                ${(e.detailedResults||[]).map((i,o)=>`
                    <div class="test-result-item" data-test-index="${o}">
                        <span class="${i.status==="PASSED"?"test-passed":"test-failed"}">
                            ${this.escapeHtml(i.name)}
                        </span>
                        <span>${this.escapeHtml(i.status)}</span>
                    </div>
                `).join("")}
            </div>
        `,this.lastTestResults=e}showTestDetails(e){if(!this?.lastTestResults?.detailedResults)return;const t=this.lastTestResults.detailedResults[e];if(!t)return;this.pauseUpdates(5e3);const s=`
            <div style="background: #1a1a1a; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <h4 style="color: #ffd700; margin: 0 0 10px 0;">📋 ${this.escapeHtml(t.name)} Details</h4>
                <div><strong>Status:</strong> <span class="${t.status==="PASSED"?"test-passed":"test-failed"}">${this.escapeHtml(t.status)}</span></div>
                ${t.duration?`<div><strong>Duration:</strong> ${Math.round(t.duration)}ms</div>`:""}
                ${t.error?`<div><strong>Error:</strong> <span style="color: #ff4444;">${this.escapeHtml(t.error)}</span></div>`:""}
                ${t.result?`
                    <div style="margin-top: 10px;"><strong>Results:</strong></div>
                    <pre style="background: #000; padding: 10px; border-radius: 3px; font-size: 10px; overflow-x: auto;">${this.escapeHtml(JSON.stringify(t.result,null,2))}</pre>
                `:""}
                ${t.metrics?`
                    <div style="margin-top: 10px;"><strong>Metrics:</strong></div>
                    <div style="margin-left: 10px;">
                        Queue Size: ${t.metrics.currentQueueSize}/${t.metrics.maxQueueSize}<br>
                        Total Requests: ${t.metrics.totalRequests}<br>
                        Success Rate: ${Math.round((t.metrics.successRate||0)*100)}%<br>
                        Avg Response: ${Math.round(t.metrics.avgResponseTime||0)}ms
                    </div>
                `:""}
                <button class="close-details-btn" 
                        style="margin-top: 10px; padding: 5px 10px; background: #666; color: white; border: none; border-radius: 3px; cursor: pointer;">
                    Close Details
                </button>
            </div>
        `,i=document.getElementById("test-results"),o=i.querySelector(".test-details");o&&o.remove();const n=document.createElement("div");n.className="test-details",n.innerHTML=s,i.appendChild(n)}exportReport(){const e=u.generateReport(),t=new Blob([JSON.stringify(e,null,2)],{type:"application/json"}),s=URL.createObjectURL(t),i=document.createElement("a");i.href=s,i.download=`msp-report-${new Date().toISOString().slice(0,19).replace(/:/g,"-")}.json`,document.body.appendChild(i),i.click(),document.body.removeChild(i),URL.revokeObjectURL(s)}}const y=new k;window.MSPDebug={dashboard:y,monitor:u,stressTest:S,show:()=>y.show(),hide:()=>y.hide(),startMonitoring:()=>u.startMonitoring(),stopMonitoring:()=>u.stopMonitoring(),runTests:()=>S.runStressTestSuite(),runFullSuite:()=>S.runStressTestSuite(),analyze:()=>u.analyzeQueue(),report:()=>u.generateReport(),showTestDetails:r=>y.showTestDetails(r),runTest:r=>S.runSpecificTest(r),quickHealthCheck:()=>window.MSPTestRunner?.quickHealthCheck?.(),stressScenario:r=>window.MSPTestRunner?.stressScenario?.(r),getStatus:()=>u.getStatus(),triggerTestAlerts:()=>u.triggerTestAlerts(),setTestThresholds:()=>u.setTestThresholds(),setNormalThresholds:()=>u.setNormalThresholds(),testAlerts:()=>(console.log("🧪 Running alert test..."),y.show(),u.startMonitoring(500),u.triggerTestAlerts())};console.log("🔧 MSP Debug Tools loaded! Use Ctrl+Shift+M to toggle dashboard or MSPDebug.show()");const C={_quickMonitorListener:null,startQuickMonitor(){return console.log("🚀 Starting MSP Quick Monitor..."),this._quickMonitorListener&&u.removeListener(this._quickMonitorListener),this._quickMonitorListener=r=>{r.alerts&&Object.values(r.alerts).some(e=>e)&&console.warn("🚨 MSP Alert:",r.alerts),Date.now()%1e4<500&&console.log(`📊 MSP Status: Queue=${r.currentQueueSize}/${r.maxQueueSize}, Requests=${r.metrics.totalRequests}, AvgTime=${Math.round(r.metrics.avgResponseTime)}ms`)},u.addListener(this._quickMonitorListener),u.startMonitoring(1e3),console.log("✅ Quick monitor started. Use MSPTestRunner.stopMonitor() to stop."),{stop:()=>this.stopMonitor(),status:()=>this.getStatus(),analyze:()=>this.analyzeQueue()}},stopMonitor(){u.stopMonitoring(),this._quickMonitorListener&&(u.removeListener(this._quickMonitorListener),this._quickMonitorListener=null),console.log("⏹️ MSP Monitor stopped")},async runTest(r){console.log(`🧪 Running MSP test: ${r}`);try{const e=await S.runSpecificTest(r);return e.status==="PASSED"?(console.log(`✅ Test ${r} PASSED`),console.table(e.result)):console.error(`❌ Test ${r} FAILED:`,e.error),e}catch(e){return console.error(`💥 Test ${r} crashed:`,e),{status:"ERROR",error:e.message}}},async runFullSuite(){console.log("🚀 Running FULL MSP Stress Test Suite..."),console.log("This may take several minutes and will stress the MSP system.");const r=Date.now();try{const e=await S.runStressTestSuite(),t=Date.now()-r;return console.log(`
📊 Test Suite Complete (${Math.round(t/1e3)}s)`),console.log(`✅ Passed: ${e.summary.passed}`),console.log(`❌ Failed: ${e.summary.failed}`),console.log(`📈 Success Rate: ${Math.round(e.summary.successRate*100)}%`),console.log(`🎯 Overall Grade: ${e.summary.overallGrade}`),e.recommendations&&e.recommendations.length>0&&(console.log(`
💡 Recommendations:`),e.recommendations.forEach(s=>console.log(`  • ${s}`))),console.log(`
📋 Detailed Results:`),console.table(e.detailedResults.map(s=>({Test:s.name,Status:s.status,Duration:s.duration?`${Math.round(s.duration)}ms`:"N/A"}))),e}catch(e){return console.error("💥 Test Suite Failed:",e),{error:e.message}}},getStatus(){const r=u.getStatus();return console.log("📊 Current MSP Status:"),console.log(`   Queue: ${r.currentQueueSize}/${r.maxQueueSize}`),console.log(`   Total Requests: ${r.metrics.totalRequests}`),console.log(`   Success Rate: ${Math.round((r.metrics.successRate||0)*100)}%`),console.log(`   Avg Response Time: ${Math.round(r.metrics.avgResponseTime||0)}ms`),console.log(`   Active Alerts: ${Object.values(r.alerts).filter(e=>e).length}`),r.queueContents.length>0&&(console.log(`
📋 Queue Contents:`),console.table(r.queueContents)),r},analyzeQueue(){const r=u.analyzeQueue();return console.log("🔍 Queue Analysis:"),console.log(`   Total Items: ${r.totalItems}`),console.log("   Age Distribution:",r.ageDistribution),console.log("   By Code:",r.byCode),r.potentialIssues.length>0&&(console.log("⚠️ Potential Issues:"),r.potentialIssues.forEach(e=>console.log(`   • ${e}`))),r},generateReport(){const r=u.generateReport();console.log("📄 Generating MSP Report..."),console.log("   Queue Health:",r.summary.queueHealth),console.log("   Performance Grade:",r.summary.performanceGrade);const e=new Blob([JSON.stringify(r,null,2)],{type:"application/json"}),t=URL.createObjectURL(e),s=document.createElement("a");return s.href=t,s.download=`msp-report-${new Date().toISOString().slice(0,19).replace(/:/g,"-")}.json`,document.body.appendChild(s),s.click(),document.body.removeChild(s),URL.revokeObjectURL(t),console.log("✅ Report downloaded"),r},showDashboard(){y.show(),console.log("🖥️ Debug dashboard opened. Press Ctrl+Shift+M to toggle.")},async quickHealthCheck(){if(console.log("🏥 Running Quick MSP Health Check..."),!window.MSP)return console.error("MSP not available"),{status:"ERROR",error:"MSP not initialized"};u.startMonitoring(100);const r=[window.MSP.promise(100,null),window.MSP.promise(101,null),window.MSP.promise(108,null)];try{const e=Date.now();await Promise.all(r);const t=Date.now()-e;await new Promise(o=>setTimeout(o,200));const s=u.getStatus();u.stopMonitoring();const i={status:"HEALTHY",responseTime:t,queueClearedAfterTest:s.currentQueueSize===0,successRate:s.metrics.successRate||0};return t>2e3&&(i.status="SLOW",i.warning="Response times are slow"),i.queueClearedAfterTest||(i.status="WARNING",i.warning="Queue not properly cleared after requests"),i.successRate<1&&(i.status="FAILING",i.warning="Some requests are failing"),console.log(`🏥 Health Check Result: ${i.status}`),console.log(`   Response Time: ${t}ms`),console.log(`   Queue Cleared: ${i.queueClearedAfterTest?"✅":"❌"}`),console.log(`   Success Rate: ${Math.round(i.successRate*100)}%`),i.warning&&console.warn(`⚠️ ${i.warning}`),i}catch(e){return u.stopMonitoring(),console.error("💥 Health check failed:",e),{status:"ERROR",error:e.message}}},async stressScenario(r){const e={"high-frequency":async()=>{console.log("🔥 High Frequency Scenario: Sending requests every 10ms for 5 seconds");const s=[],i=Date.now();for(;Date.now()-i<5e3;)s.push(window.MSP.promise(101,null).catch(n=>(console.error("MSP request failed in sustained-load scenario:",n),{error:n.message||"Unknown error"}))),await new Promise(n=>setTimeout(n,10));const o=await Promise.allSettled(s);return{totalRequests:s.length,successful:o.filter(n=>n.status==="fulfilled").length,duration:Date.now()-i}},"queue-overflow":async()=>{console.log("💥 Queue Overflow Scenario: Flooding queue beyond capacity");const s=[];for(let n=0;n<100;n++)s.push(window.MSP.promise(101,null).catch(a=>({error:a.message})));const o=(await Promise.allSettled(s)).filter(n=>n.status==="fulfilled"&&!n.value.error).length;return{requestsSent:100,successful:o,rejected:100-o}},"mixed-load":async()=>{console.log("🎭 Mixed Load Scenario: Various request types and sizes");const s=[100,101,102,104,108,110,111,112],i=[];for(let l=0;l<30;l++){const d=s[l%s.length],h=l%4===0?new Uint8Array([l,l+1,l+2]):null;i.push(window.MSP.promise(d,h).catch(c=>(console.error(`MSP request failed in mixed-load scenario (code: ${d}):`,c),{error:c.message||"Unknown error"})))}const o=Date.now(),n=await Promise.allSettled(i),a=Date.now()-o;return{totalRequests:30,successful:n.filter(l=>l.status==="fulfilled").length,duration:a,avgResponseTime:a/30}}},t=e[r];if(!t){console.error(`❌ Unknown scenario: ${r}`),console.log("Available scenarios:",Object.keys(e));return}u.startMonitoring(100);try{const s=await t(),i=u.getStatus();return console.log("📊 Scenario Results:"),console.table(s),console.log("📈 Final MSP Status:"),console.table({"Queue Size":i.currentQueueSize,"Total Requests":i.metrics.totalRequests,"Success Rate":`${Math.round((i.metrics.successRate||0)*100)}%`,"Avg Response":`${Math.round(i.metrics.avgResponseTime||0)}ms`}),{scenario:s,mspStatus:i}}catch(s){return console.error("💥 Scenario failed:",s),{error:s.message}}finally{u.stopMonitoring()}},help(){console.log(`
🔧 MSP Test Runner Commands:

Basic Monitoring:
  MSPTestRunner.startQuickMonitor()     - Start monitoring with console output
  MSPTestRunner.stopMonitor()           - Stop monitoring
  MSPTestRunner.getStatus()             - Get current status
  MSPTestRunner.analyzeQueue()          - Analyze current queue

Testing:
  MSPTestRunner.runTest('test-name')    - Run specific test
  MSPTestRunner.runFullSuite()          - Run full stress test suite
  MSPTestRunner.quickHealthCheck()      - Quick health check

Stress Scenarios:
  MSPTestRunner.stressScenario('high-frequency')  - High frequency requests
  MSPTestRunner.stressScenario('queue-overflow')  - Queue overflow test
  MSPTestRunner.stressScenario('mixed-load')      - Mixed request types

Visual Tools:
  MSPTestRunner.showDashboard()         - Show visual dashboard
  MSPTestRunner.generateReport()        - Generate and download report

Available Test Names:
  'queue-flooding', 'rapid-fire', 'duplicates', 'timeout-recovery',
  'memory-leaks', 'concurrent-mixed', 'queue-overflow', 
  'connection-disruption', 'performance-load'

Keyboard Shortcuts:
  Ctrl+Shift+M - Toggle debug dashboard
        `)}};window.MSPTestRunner=C;console.log("🔧 MSP Test Runner loaded! Type MSPTestRunner.help() for commands.");console.log(`
🔧 MSP Debug Tools Loaded Successfully!

Quick Start:
  • Press Ctrl+Shift+M to toggle the visual dashboard
  • Use MSPTestRunner.help() to see all available commands
  • Use MSPTestRunner.quickHealthCheck() for a quick test

Example Usage:
  MSPTestRunner.startQuickMonitor();     // Start monitoring
  MSPTestRunner.runTest('queue-flooding'); // Run specific test  
  MSPTestRunner.showDashboard();         // Show visual dashboard
  MSPTestRunner.runFullSuite();          // Run all stress tests

The tools will help you:
  ✓ Monitor MSP queue health in real-time
  ✓ Detect memory leaks and performance issues
  ✓ Stress test the MSP implementation
  ✓ Analyze queue contents and response times
  ✓ Export detailed diagnostic reports

Happy debugging! 🚀
`);(window.location.hostname==="localhost"||window.location.hostname==="127.0.0.1")&&(console.log("🔄 Development environment detected - auto-starting basic monitoring"),x(async()=>{const{mspQueueMonitor:r}=await Promise.resolve().then(()=>q);return{mspQueueMonitor:r}},void 0,import.meta.url).then(({mspQueueMonitor:r})=>{r.addListener(e=>{Object.values(e.alerts).filter(s=>s).length>0&&console.warn("🚨 MSP Alert detected - check dashboard for details")}),r.startMonitoring(2e3)}));
