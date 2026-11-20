export async function request(path,body,method='POST'){
 const token=JSON.parse(localStorage.getItem('survey-session')||'null')?.token;
 const response=await fetch('/api'+path,{method:body===undefined?'GET':method,headers:{'Content-Type':'application/json',...(token?{Authorization:'Bearer '+token}:{})},...(body===undefined?{}:{body:JSON.stringify(body)})});
 const data=await response.json();if(!response.ok)throw new Error(typeof data.detail==='string'?data.detail:JSON.stringify(data.detail));return data;
}
// 收件上传走multipart，原始JSON文件以字节送达，后端对canonical_json原文重算SHA256。
export async function upload(path,file){
 const token=JSON.parse(localStorage.getItem('survey-session')||'null')?.token;
 const response=await fetch('/api'+path,{method:'POST',headers:token?{Authorization:'Bearer '+token}:{},body:(()=>{const fd=new FormData();fd.append('file',file);return fd;})()});
 const data=await response.json();if(!response.ok)throw new Error(typeof data.detail==='string'?data.detail:JSON.stringify(data.detail));return data;
}
