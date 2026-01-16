class t{writeText(a,e,r){navigator.clipboard.writeText(a).then(()=>e?.(a),r)}readText(a,e){navigator.clipboard.readText().then(r=>a?.(r),e)}}const o=new t;export{o as B};
