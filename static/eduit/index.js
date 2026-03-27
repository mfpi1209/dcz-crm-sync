(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const t of document.querySelectorAll('link[rel="modulepreload"]'))i(t);new MutationObserver(t=>{for(const a of t)if(a.type==="childList")for(const n of a.addedNodes)n.tagName==="LINK"&&n.rel==="modulepreload"&&i(n)}).observe(document,{childList:!0,subtree:!0});function s(t){const a={};return t.integrity&&(a.integrity=t.integrity),t.referrerPolicy&&(a.referrerPolicy=t.referrerPolicy),t.crossOrigin==="use-credentials"?a.credentials="include":t.crossOrigin==="anonymous"?a.credentials="omit":a.credentials="same-origin",a}function i(t){if(t.ep)return;t.ep=!0;const a=s(t);fetch(t.href,a)}})();const E="https://n8n-new-n8n.ca31ey.easypanel.host/webhook/aiduit";let z="",b="",H="",O="",T=0;const P={inicio:'<svg fill="none" viewBox="0 0 24 24"><path d="M4 10.5L12 4l8 6.5V20a1 1 0 0 1-1 1h-5v-6H10v6H5a1 1 0 0 1-1-1v-9.5Z" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>',comparar:'<svg fill="none" viewBox="0 0 24 24"><path d="M7 4v11M7 4l-3 3M7 4l3 3M17 20V9M17 20l-3-3M17 20l3-3" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>',recomendacao:'<svg fill="none" viewBox="0 0 24 24"><path d="M12 3.5 9.6 9.2l-6.1.4 4.7 3.7-1.5 5.9L12 16l5.3 3.2-1.5-5.9 4.7-3.7-6.1-.4L12 3.5Z" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>',localizacao:'<svg fill="none" viewBox="0 0 24 24"><path d="M12 21s6-5.1 6-10a6 6 0 1 0-12 0c0 4.9 6 10 6 10Z" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/><path d="M12 12.5a2.5 2.5 0 1 0 0-5 2.5 2.5 0 0 0 0 5Z" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>',"teste-vocacional":'<svg fill="none" viewBox="0 0 24 24"><path d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>',"informacoes-cursos":'<svg fill="none" viewBox="0 0 24 24"><path d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>'},q={inicio:"Início",comparar:"Comparar Cursos",recomendacao:"Recomendações",localizacao:"Localizações","teste-vocacional":"Teste Vocacional","informacoes-cursos":"Informações de Cursos"},N={inicio:"Bem-vindo(a) de volta! Selecione uma opção para começar.",comparar:"Compare dois cursos lado a lado para tomar a melhor decisão.",recomendacao:"Receba sugestões de cursos similares baseadas em suas preferências.",localizacao:"Encontre os polos de atendimento mais próximos de você.","teste-vocacional":"Descubra qual curso combina mais com o seu perfil profissional.","informacoes-cursos":"Obtenha informações detalhadas sobre cursos disponíveis."},$=document.getElementById("hamburger-btn"),_=document.getElementById("sidebar"),j=document.getElementById("sidebar-overlay"),D=document.getElementById("sidebar-close");function ae(){_.classList.add("active"),j.classList.add("active"),$.classList.add("active"),document.body.style.overflow="hidden"}function S(){_.classList.remove("active"),j.classList.remove("active"),$.classList.remove("active"),document.body.style.overflow=""}$&&$.addEventListener("click",()=>{_.classList.contains("active")?S():ae()});D&&D.addEventListener("click",S);j&&j.addEventListener("click",S);function V(o){S(),document.querySelectorAll(".content-section").forEach(n=>{n.classList.remove("active")}),document.querySelectorAll(".nav-item").forEach(n=>{n.classList.remove("active")});const e=document.getElementById(`section-${o}`);e&&e.classList.add("active");const s=document.getElementById(`nav-${o}`);s&&s.classList.add("active");const i=document.getElementById("page-icon"),t=document.getElementById("page-title"),a=document.getElementById("page-subtitle");i&&P[o]&&(i.innerHTML=P[o]),t&&q[o]&&(t.textContent=q[o]),a&&N[o]&&(a.textContent=N[o])}document.querySelectorAll(".nav-item").forEach(o=>{o.addEventListener("click",()=>{const e=o.getAttribute("data-section");e&&V(e)})});document.querySelectorAll(".card").forEach(o=>{o.addEventListener("click",()=>{const e=o.getAttribute("data-action");e&&V(e)})});var F;(F=document.getElementById("clear-comparar"))==null||F.addEventListener("click",()=>{document.getElementById("form-comparar").reset(),B("comparar"),H="",O="";const o=document.getElementById("feedback-comparar");if(o){o.classList.remove("active");const e=document.getElementById("form-feedback-comparar");e&&e.reset()}});var W;(W=document.getElementById("clear-recomendacao"))==null||W.addEventListener("click",()=>{document.getElementById("form-recomendacao").reset(),B("recomendacao"),z="",b=""});var U;(U=document.getElementById("clear-localizacao"))==null||U.addEventListener("click",()=>{document.getElementById("form-localizacao").reset(),B("localizacao")});var G;(G=document.getElementById("clear-graduacao"))==null||G.addEventListener("click",()=>{document.getElementById("form-graduacao").reset(),B("graduacao")});var Z;(Z=document.getElementById("clear-pos-graduacao"))==null||Z.addEventListener("click",()=>{document.getElementById("form-pos-graduacao").reset(),B("pos-graduacao")});function ne(){h=[];const o=document.getElementById("card-inicio-teste"),e=document.getElementById("card-selecao-palavras"),s=document.getElementById("response-teste-vocacional"),i=document.getElementById("btn-iniciar-teste");o&&(o.style.display="block"),e&&(e.style.display="none"),s&&(s.style.display="none"),i&&(i.disabled=!1);const t=document.getElementById("palavras-grid");t&&(t.innerHTML="");const a=document.getElementById("contador-selecionadas");a&&(a.textContent="0")}function re(){h=[];const o=document.getElementById("card-inicio-teste"),e=document.getElementById("card-selecao-palavras"),s=document.getElementById("response-teste-vocacional"),i=document.getElementById("btn-iniciar-teste"),t=document.getElementById("loading-teste-vocacional");o&&(o.style.display="block"),e&&(e.style.display="none"),s&&(s.style.display="none"),t&&t.classList.remove("active"),i&&(i.disabled=!1);const a=document.getElementById("palavras-grid");a&&(a.innerHTML="");const n=document.getElementById("contador-selecionadas");n&&(n.textContent="0");const r=s==null?void 0:s.querySelector(".response-content");r&&(r.classList.add("empty"),r.textContent="Aguardando resultado...");const l=document.getElementById("section-teste-vocacional");l&&l.scrollIntoView({behavior:"smooth",block:"start"})}document.querySelectorAll(".info-tab").forEach(o=>{o.addEventListener("click",()=>{var s;const e=o.getAttribute("data-tab");document.querySelectorAll(".info-tab").forEach(i=>i.classList.remove("active")),document.querySelectorAll(".info-content").forEach(i=>i.classList.remove("active")),o.classList.add("active"),(s=document.getElementById(`content-${e}`))==null||s.classList.add("active")})});async function se(o,e="graduacao"){var r,l;V("informacoes-cursos"),await new Promise(c=>setTimeout(c,100));const s=e==="pos-graduacao"?"pos-graduacao":"graduacao";document.querySelectorAll(".info-tab").forEach(c=>c.classList.remove("active")),document.querySelectorAll(".info-content").forEach(c=>c.classList.remove("active")),(r=document.querySelector(`[data-tab="${s}"]`))==null||r.classList.add("active"),(l=document.getElementById(`content-${s}`))==null||l.classList.add("active");const i=e==="pos-graduacao"?"curso-pos-graduacao":"curso-graduacao",t=document.getElementById(i);t&&(t.value=o);const a=e==="pos-graduacao"?"form-pos-graduacao":"form-graduacao",n=document.getElementById(a);n&&n.dispatchEvent(new Event("submit")),window.scrollTo({top:0,behavior:"smooth"})}document.querySelectorAll(".nav-item").forEach(o=>{o.addEventListener("click",()=>{const e=o.getAttribute("data-section");e&&e!=="teste-vocacional"&&ne()})});document.querySelectorAll(".card").forEach(o=>{o.addEventListener("click",()=>{const e=o.getAttribute("data-action");e&&e!=="teste-vocacional"&&ne()})});function B(o){const e=document.querySelector(`#response-${o} .response-content`);if(e&&(e.textContent="Aguardando sua solicitação...",e.classList.add("empty")),T=0,o==="comparar"){const s=document.getElementById("feedback-comparar");s&&s.classList.remove("active")}}var K;(K=document.getElementById("form-comparar"))==null||K.addEventListener("submit",async o=>{o.preventDefault();const e=document.getElementById("curso1").value,s=document.getElementById("curso2").value;H=e,O=s,await M("comparar",{opcao:"comparar",curso1:e,curso2:s})});var X;(X=document.getElementById("form-recomendacao"))==null||X.addEventListener("submit",async o=>{o.preventDefault();const e=document.getElementById("curso-recomendacao").value,s=document.querySelector('input[name="tipo-curso"]:checked').value;z=e,b=s,console.log("Tipo de curso selecionado:",s),await M("recomendacao",{opcao:"recomendacao",curso:e,tipo:s})});var Q;(Q=document.getElementById("form-localizacao"))==null||Q.addEventListener("submit",async o=>{o.preventDefault();const e=document.getElementById("rua").value.trim(),s=document.getElementById("numero").value.trim(),i=document.getElementById("cidade").value.trim(),t=document.getElementById("cep").value.trim(),a=e&&s&&i,n=t;if(!a&&!n){alert("Por favor, preencha os campos Rua, Número e Cidade OU preencha o CEP.");return}if(a&&n){alert("Por favor, preencha apenas os campos de endereço OU o CEP, não ambos.");return}let r={};n?r={opcao:"localizacao_cep",cep:t}:r={opcao:"localizacao",rua:e,numero:s,cidade:i},await M("localizacao",r)});var Y;(Y=document.getElementById("form-graduacao"))==null||Y.addEventListener("submit",async o=>{o.preventDefault();const e=document.getElementById("curso-graduacao").value;await M("graduacao",{opcao:"informacoes_cursos",curso:e,tipo:"graduacao"})});var ee;(ee=document.getElementById("form-pos-graduacao"))==null||ee.addEventListener("submit",async o=>{o.preventDefault();const e=document.getElementById("curso-pos-graduacao").value;await M("pos-graduacao",{opcao:"informacoes_cursos",curso:e,tipo:"pos-graduacao"})});async function M(o,e){const s=document.getElementById(`loading-${o}`),i=document.querySelector(`#response-${o} .response-content`),t=document.querySelector(`#form-${o} button[type="submit"]`);t&&(t.disabled=!0),s&&s.classList.add("active"),i&&(i.textContent="Aguardando resposta...",i.classList.add("empty")),T=0;try{const a=await fetch(E,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(e)});if(!a.ok)throw new Error(`Erro na requisição: ${a.status}`);let n=await a.json();if(console.log(`=== RESPOSTA DO WEBHOOK (${o}) ===`),console.log("Resposta original:",n),Array.isArray(n)&&n.length>0&&(n=n[0],console.log("Resultado extraído do array:",n)),n.body&&typeof n.body=="object"&&(n=n.body,console.log("Resultado extraído de body:",n)),console.log("Resultado final a processar:",n),i)if(i.classList.remove("empty"),o==="comparar"&&(n.analises_md||n.conclusao_md||n.tabela_md)){i.innerHTML=ce(n);const l=document.getElementById("feedback-comparar");l&&l.classList.add("active")}else if(o==="graduacao"||o==="pos-graduacao"){console.log("Processando informações de cursos. Resultado original:",n);let l=n;n.body&&typeof n.body=="object"&&(l=n.body,console.log("Extraído de body:",l));const c=u=>{try{let m=u.trim();return m.toLowerCase().startsWith("json")&&!m.startsWith("```")&&(m=m.substring(4).trim()),m=m.replace(/```json\s*/g,"").replace(/```\s*$/g,"").replace(/^```\s*/g,""),m=m.trim(),console.log("String limpa para parse:",m.substring(0,100)+"..."),JSON.parse(m)}catch(m){return console.log("Erro ao parsear:",m),null}};if(l.mensagem&&typeof l.mensagem=="string"){console.log("Detectado campo mensagem com string, tentando extrair JSON...");const u=c(l.mensagem);u&&(l=u,console.log("JSON extraído de mensagem com sucesso:",l))}const d=["output","response","message","result","data"];for(const u of d)if(l[u]&&typeof l[u]=="string"){const m=c(l[u]);if(m){l=m,console.log(`Parseado com sucesso de ${u}:`,l);break}}const p=me(l);console.log("HTML formatado:",p?"Gerado com sucesso":"Vazio"),i.innerHTML=p}else o==="localizacao"&&(n["polo mais rápido"]||n["polo mais próximo"])?i.innerHTML=pe(n):o==="recomendacao"?n.recomendacoes?i.innerHTML=I(n):n.output?i.innerHTML=I({recomendacoes:n.output}):n.response?i.innerHTML=I({recomendacoes:n.response}):n.message?i.innerHTML=I({recomendacoes:n.message}):(console.warn("Formato de recomendação desconhecido:",n),i.textContent=JSON.stringify(n,null,2)):n.output?i.innerHTML=k(n.output):n.response?i.innerHTML=k(n.response):n.message?i.innerHTML=k(n.message):i.textContent=JSON.stringify(n,null,2);const r=document.getElementById(`form-${o}`);r&&r.reset()}catch(a){i&&(i.classList.remove("empty"),i.textContent=`Erro ao processar solicitação: ${a.message}`,i.style.color="#dc3545")}finally{s&&s.classList.remove("active"),t&&(t.disabled=!1)}}function k(o){if(!o)return"";let e=o;e=e.replace(/^### (.+)$/gm,"<h3>$1</h3>"),e=e.replace(/^## (.+)$/gm,"<h2>$1</h2>"),e=e.replace(/^# (.+)$/gm,"<h1>$1</h1>"),e=le(e),e=e.replace(/\*\*([^*\n]+?):\*\*/g,`
<h4 style="color: #4c34d1; font-size: 1.15em; margin: 18px 0 8px 0; font-weight: 600; display: block;">$1:</h4>
`),e=e.replace(/\*\*(.+?)\*\*/g,"<strong>$1</strong>"),e=e.replace(/\*(.+?)\*/g,"<em>$1</em>"),e=e.replace(/^\s*[-*]\s+(.+)$/gm,"<li>$1</li>"),e=e.replace(/(<li>.*<\/li>)/s,function(n){return"<ul>"+n+"</ul>"});const s=e.split(`
`);let i=[],t=!1,a=[];for(let n=0;n<s.length;n++){const r=s[n].trim();r.match(/^<(h[1-4]|table|div|ul|ol)/)?(t&&a.length>0&&(i.push("<p>"+a.join(" ")+"</p>"),a=[]),i.push(r),t=!1):r.match(/^<\/(table|div|ul|ol)/)?(i.push(r),t=!1):r===""?t&&a.length>0&&(i.push("<p>"+a.join(" ")+"</p>"),a=[],t=!1):r&&(r.match(/^<h4/)?(t&&a.length>0&&(i.push("<p>"+a.join(" ")+"</p>"),a=[]),i.push(r),t=!1):(a.push(r),t=!0))}return t&&a.length>0&&i.push("<p>"+a.join(" ")+"</p>"),e=i.join(`
`),e=e.replace(/<p>\s*<\/p>/g,""),e=e.replace(/\n{3,}/g,`

`),e}function le(o){let e=[],s=[],i=[],t=0;return o.split(`
`).forEach((a,n)=>{const r=a.trim();if(r.match(/^\|/)){i.push(r);let l=r;l.endsWith("|")||(l+="|"),s.push(l)}else s.length>0&&(e.push({lines:[...s],originalLines:[...i],startIndex:t,endIndex:n}),s=[],i=[]),t=n+1}),s.length>0&&e.push({lines:[...s],originalLines:[...i],startIndex:t,endIndex:o.split(`
`).length}),e.reverse().forEach(a=>{const n=a.lines;if(n.length<3)return;let r=-1;for(let g=0;g<n.length;g++)if(/^\|[\s:|-]+\|$/.test(n[g].trim())){r=g;break}if(r===-1||r===0)return;let c=n[0].trim();c.startsWith("|")&&(c=c.substring(1)),c.endsWith("|")&&(c=c.substring(0,c.length-1));const d=c.split("|").map(g=>g.trim()),p=n.slice(r+1);if(p.length===0)return;T++;const u=`table-${T}`;let m='<div class="table-wrapper">';m+=`<button class="download-table-btn" onclick="downloadTableAsPNG('${u}')"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Baixar Imagem</button>`,m+=`<table id="${u}"><thead><tr>`,d.forEach(g=>{m+=`<th>${g||"&nbsp;"}</th>`}),m+="</tr></thead><tbody>",p.forEach(g=>{const w=g.trim();if(!w||!w.includes("|"))return;let v=w;v.startsWith("|")&&(v=v.substring(1)),v.endsWith("|")&&(v=v.substring(0,v.length-1));const C=v.split("|").map(x=>x.trim());for(;C.length<d.length;)C.push("");m+="<tr>",C.slice(0,d.length).forEach(x=>{let y=x||"&nbsp;";y!=="&nbsp;"&&(y=y.replace(/\*\*(.+?)\*\*/g,"<strong>$1</strong>"),y=y.replace(/\*(.+?)\*/g,"<em>$1</em>")),m+=`<td>${y}</td>`}),m+="</tr>"}),m+="</tbody></table></div>";const f=a.originalLines.join(`
`);o=o.replace(f,m)}),o}function ce(o){let e="";return o.analises_md&&(e+=`
      <div class="comparison-section">
        <div class="section-header"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Análises Individuais dos Cursos</div>
        <div class="section-content">
          ${k(o.analises_md)}
        </div>
      </div>
    `),o.tabela_md&&(e+=`
      <div class="comparison-section">
        <div class="section-header"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Tabela Comparativa</div>
        <div class="section-content">
          ${k(o.tabela_md)}
        </div>
      </div>
    `),o.conclusao_md&&(e+=`
      <div class="comparison-section">
        <div class="section-header"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Conclusão e Recomendações</div>
        <div class="section-content">
          ${k(o.conclusao_md)}
        </div>
      </div>
    `),e}function de(o){let e='<h2 style="color: #169DBB; margin-bottom: 24px; font-size: 1.5em;">🎯 Seus Cursos Recomendados</h2>';return e+='<p style="color: #6b7280; margin-bottom: 24px; line-height: 1.6;">Com base nas suas preferências, identificamos os cursos que mais combinam com você:</p>',o.sort((i,t)=>i.rank-t.rank).forEach((i,t)=>{const n=(i.curso||"").split(" - ")[0].trim();i.area,i.grau;const r=i.rank||t+1;let l="#169DBB";r===1?l="#10b981":r===2?l="#3b82f6":r===3&&(l="#8b5cf6");const c=`vocacional-${Date.now()}-${t}`;e+=`
      <div class="curso-recomendado-item" style="background: #ffffff; border: 1px solid #e5e7eb; border-left: 4px solid ${l}; border-radius: 12px; padding: 20px; margin-bottom: 16px; transition: all 0.25s ease; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
        <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 12px;">
          <div style="flex-shrink: 0; width: 48px; height: 48px; background: ${l}; color: white; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-size: 24px; font-weight: 700; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
            ${r}
          </div>
          <div style="flex: 1;">
            <h3 style="color: #1f2937; font-size: 1.2em; font-weight: 600; margin: 0; line-height: 1.3;">
              ${n}
            </h3>
          </div>
        </div>
        <div class="vocacional-buttons-grid" style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
          <button class="recommendation-button" id="btn-just-${c}" type="button">
            <svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><circle cx="11" cy="11" r="8" stroke="currentColor" stroke-width="2"/><path d="M21 21l-4.35-4.35" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>Ver Justificativa
          </button>
          <button class="recommendation-button btn-info-curso" id="btn-info-${c}" type="button">
            <svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Mais Informações
          </button>
        </div>
        <div class="recommendation-justification" id="just-content-${c}"></div>
      </div>
    `,setTimeout(()=>{const d=document.getElementById(`btn-just-${c}`),p=document.getElementById(`just-content-${c}`),u=document.getElementById(`btn-info-${c}`);d&&p&&(d.onclick=async function(){await ge(n,d,p)}),u&&(u.onclick=function(){se(n)})},50)}),e+=`
    <div style="background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%); border: 1px solid #bae6fd; border-radius: 12px; padding: 20px; margin-top: 24px; margin-bottom: 20px;">
      <p style="color: #0369a1; font-size: 0.95em; line-height: 1.6; margin: 0;">
        <strong>💡 Dica:</strong> Explore cada um desses cursos para descobrir qual se encaixa melhor nos seus objetivos profissionais e acadêmicos!
      </p>
    </div>
    <button type="button" class="btn-submit" id="btn-refazer-teste" style="background: #6b7280; margin-top: 0;">
      <svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;">
        <path d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
      Refazer Teste
    </button>
  `,e}function me(o){console.log("=== FORMATANDO INFORMAÇÕES DE CURSOS ==="),console.log("Dados recebidos:",o);const e=t=>{if(!t)return"";const a=document.createElement("div");return a.textContent=t,a.innerHTML};let s="";o.resposta_para_usuario&&(s+=`
      <div style="background: #f0f9ff; border-left: 4px solid #169DBB; padding: 16px 20px; border-radius: 8px; margin-bottom: 24px;">
        <p style="color: #0369a1; margin: 0; line-height: 1.6;">${e(o.resposta_para_usuario)}</p>
      </div>
    `),o.mensagem&&(s+=`
      <div style="background: #f0f9ff; border-left: 4px solid #169DBB; padding: 16px 20px; border-radius: 8px; margin-bottom: 24px;">
        <p style="color: #0369a1; margin: 0; line-height: 1.6;">${e(o.mensagem)}</p>
      </div>
    `);let i=[];return o.cursos&&Array.isArray(o.cursos)?i=o.cursos:(o.nome||o.curso_nome)&&(i=[o]),i.length>0&&(console.log("Processando cursos:",i),i.forEach((t,a)=>{console.log(`Processando curso ${a+1}:`,t);const n=t.nome||t.curso_nome||t.name||"Curso não especificado",r=t.tipo||t.type||"";let l=t.modalidade||t.modality||"";(o.tipo_curso_principal==="pos-graduacao"||r.toLowerCase().includes("pós")||r.toLowerCase().includes("pos"))&&(l==="nao_informado"||l===""||!l)&&(console.log(`Pós-graduação detectado! Alterando modalidade de "${l}" para "EAD"`),l="EAD");const d=t.duracao||t.duration||"Não informado",p=t.preco_referencia||t.preco||t.price||"Não informado",u=t.resumo||t.descricao||t.description||"",m=t.areas_atuacao||t.areas||[];s+=`
        <div style="background: #ffffff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 24px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.04);">
          <!-- Cabeçalho do Curso -->
          <div style="border-bottom: 2px solid #e5e7eb; padding-bottom: 16px; margin-bottom: 20px;">
            <h2 style="color: #169DBB; font-size: 1.5em; margin: 0 0 12px 0; font-weight: 700;">
              ${e(n)}
            </h2>
            <div style="display: flex; gap: 8px; flex-wrap: wrap;">
              ${l?`
                <span style="display: inline-flex; align-items: center; background: #e0f2fe; color: #0369a1; padding: 6px 14px; border-radius: 8px; font-size: 0.85em; font-weight: 600;">
                  📚 ${e(l)}
                </span>
              `:""}
              ${r?`
                <span style="display: inline-flex; align-items: center; background: #f3e8ff; color: #7c3aed; padding: 6px 14px; border-radius: 8px; font-size: 0.85em; font-weight: 600;">
                  🎓 ${e(r==="graduacao"?"Graduação":r)}
                </span>
              `:""}
            </div>
          </div>

          <!-- Informações Principais -->
          <div style="margin-bottom: 20px;">
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 20px;">
              <div style="background: #f9fafb; padding: 16px; border-radius: 10px; border: 1px solid #e5e7eb;">
                <div style="color: #6b7280; font-size: 0.85em; font-weight: 600; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px;">
                  ⏱️ Duração
                </div>
                <div style="color: #1f2937; font-size: 1em; font-weight: 600;">
                  ${e(d)}
                </div>
              </div>
              <div style="background: #f9fafb; padding: 16px; border-radius: 10px; border: 1px solid #e5e7eb;">
                <div style="color: #6b7280; font-size: 0.85em; font-weight: 600; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px;">
                  💰 Preço
                </div>
                <div style="color: #1f2937; font-size: 1em; font-weight: 600;">
                  ${e(p==="nao_informado"||p==="Não informado"?"Consultar":p)}
                </div>
              </div>
            </div>
          </div>

          ${u?`
            <!-- Resumo -->
            <div style="margin-bottom: 20px;">
              <h3 style="color: #374151; font-size: 1.1em; font-weight: 600; margin: 0 0 12px 0; display: flex; align-items: center; gap: 8px;">
                <svg width="18" height="18" fill="none" viewBox="0 0 24 24" style="color: #169DBB;">
                  <path d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                </svg>
                Sobre o Curso
              </h3>
              <p style="color: #4b5563; line-height: 1.7; margin: 0;">
                ${e(u)}
              </p>
            </div>
          `:""}

          ${Array.isArray(m)&&m.length>0?`
            <!-- Áreas de Atuação -->
            <div>
              <h3 style="color: #374151; font-size: 1.1em; font-weight: 600; margin: 0 0 12px 0; display: flex; align-items: center; gap: 8px;">
                <svg width="18" height="18" fill="none" viewBox="0 0 24 24" style="color: #169DBB;">
                  <path d="M21 13.255A23.931 23.931 0 0112 15c-3.183 0-6.22-.62-9-1.745M16 6V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v2m4 6h.01M5 20h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                </svg>
                Áreas de Atuação
              </h3>
              <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px;">
                ${m.map(f=>`
                  <div style="background: linear-gradient(135deg, #ffffff 0%, #f9fafb 100%); border: 1px solid #e5e7eb; border-left: 3px solid #10b981; padding: 12px 16px; border-radius: 8px;">
                    <span style="color: #374151; font-size: 0.9em; font-weight: 500;">✓ ${e(f)}</span>
                  </div>
                `).join("")}
              </div>
            </div>
          `:""}
        </div>
      `})),(!s||s.trim()==="")&&(s=`
      <div style="background: #fff3cd; border-left: 4px solid #ffc107; padding: 16px 20px; border-radius: 8px;">
        <p style="color: #856404; margin: 0; line-height: 1.6;">
          <strong>Atenção:</strong> Não foi possível processar as informações do curso. 
          Tente novamente ou entre em contato conosco.
        </p>
      </div>
    `,console.warn("Dados recebidos não puderam ser formatados:",o)),s}function pe(o){let e="";if(o["polo mais rápido"]&&(e+=`
      <div class="comparison-section">
        <div class="section-header"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M13 10V3L4 14h7v7l9-11h-7z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Polo Mais Rápido</div>
        <div class="section-content">
          <h3 style="color: #5b3ff6; font-size: 1.4em; margin-bottom: 15px;">
            <svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M12 21s6-5.1 6-10a6 6 0 10-12 0c0 4.9 6 10 6 10z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><path d="M12 12.5a2.5 2.5 0 100-5 2.5 2.5 0 000 5z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>${o["polo mais rápido"]}
          </h3>
          <div style="background: white; padding: 15px; border-radius: 8px; margin-bottom: 15px;">
            <p style="margin: 8px 0;">
              <strong style="color: #764ba2;"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 4px;"><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/><path d="M12 6v6l4 2" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>Tempo estimado:</strong>
              <span style="color: #43e97b; font-size: 1.2em; font-weight: bold;">${o.tempo||"Não disponível"}</span>
            </p>
          </div>
          ${o["link da rota"]?`
            <a href="${o["link da rota"]}" target="_blank" class="location-link">
              <svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Ver Rota no Google Maps
            </a>
          `:""}
        </div>
      </div>
    `),o["polo mais próximo"]||o["polo mais próximo 2"]){if(e+=`
      <div class="comparison-section">
        <div class="section-header"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Outros Polos Próximos</div>
        <div class="section-content">
    `,o["polo mais próximo"]){const s=o["distancia mais proxima "]||o["distancia mais proxima"]||"Não disponível";e+=`
        <div style="background: white; padding: 20px; border-radius: 8px; margin-bottom: 15px;
                    border-left: 4px solid #5b3ff6;">
          <h4 style="color: #5b3ff6; font-size: 1.2em; margin-bottom: 10px;">
            <svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M12 21s6-5.1 6-10a6 6 0 10-12 0c0 4.9 6 10 6 10z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><path d="M12 12.5a2.5 2.5 0 100-5 2.5 2.5 0 000 5z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>${o["polo mais próximo"]}
          </h4>
          <p style="margin: 5px 0; color: #666;">
            <strong><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 4px;"><path d="M7 20l4-16m2 16l4-16M6 9h14M4 15h14" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Distância:</strong>
            <span style="color: #f472b6; font-weight: bold;">${s}</span>
          </p>
        </div>
      `}if(o["polo mais próximo 2"]){const s=o["distancia mais proxima2 "]||o["distancia mais proxima2"]||"Não disponível";e+=`
        <div style="background: white; padding: 20px; border-radius: 8px; margin-bottom: 15px;
                    border-left: 4px solid #764ba2;">
          <h4 style="color: #764ba2; font-size: 1.2em; margin-bottom: 10px;">
            <svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M12 21s6-5.1 6-10a6 6 0 10-12 0c0 4.9 6 10 6 10z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><path d="M12 12.5a2.5 2.5 0 100-5 2.5 2.5 0 000 5z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>${o["polo mais próximo 2"]}
          </h4>
          <p style="margin: 5px 0; color: #666;">
            <strong><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 4px;"><path d="M7 20l4-16m2 16l4-16M6 9h14M4 15h14" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Distância:</strong>
            <span style="color: #f472b6; font-weight: bold;">${s}</span>
          </p>
        </div>
      `}e+=`
        </div>
      </div>
    `}return e}function I(o){console.log("=== FORMATO DE RECOMENDAÇÕES ==="),console.log("Resultado completo:",o),console.log("result.recomendacoes:",o.recomendacoes),console.log("Tipo de curso da recomendação (global):",b);let e='<h2 style="color: #764ba2; margin-bottom: 20px;">📚 Cursos Recomendados</h2>',s=[],i="";if(Array.isArray(o.recomendacoes)?i=o.recomendacoes.join(" "):typeof o.recomendacoes=="string"&&(i=o.recomendacoes),console.log("Texto completo extraído:",i),i=i.replace(/```[a-z]*\n?/gi,"").trim(),console.log("Texto após remover markdown:",i),i.split(`
`).forEach(n=>{n=n.trim();const r=n.match(/^\d+\.\s*(.+?)$/);if(r){let l=r[1].trim(),c=l,d="",p="";console.log("Linha capturada completa:",l);const u=l.match(/\(([^)]+)\)/);u&&(d=u[1].trim(),console.log("Tipo de pós encontrado:",d));const m=l.match(/^(.+?)\s+-\s+(.+?)$/);m&&(c=m[1].trim(),p=m[2].trim(),console.log("Nome do curso:",c),console.log("Descrição extraída:",p)),c=c.replace(/\s*\([^)]*\)\s*$/g,"").trim(),c=c.replace(/[\[\]]/g,"").trim(),c=c.replace(/\s+/g," ").trim(),console.log("Nome do curso limpo:",c),c&&c.length>2&&(s.push({nome:c,tipo:d||(b==="pos-graduacao"?"Pós-graduação":""),descricao:p,textoCompleto:l}),console.log("Curso adicionado com tipo:",d||(b==="pos-graduacao"?"Pós-graduação":"sem tipo")))}}),console.log("Cursos separados (método linhas):",s),s.length===0){console.log("Tentando fallback com regex no texto completo...");const n=/(\d+)\.\s*([^0-9]+?)(?=\s*\d+\.|$)/g;let r;for(;(r=n.exec(i))!==null;){let l=r[2].trim(),c=l,d="",p="";const u=l.match(/\(([^)]+)\)/);u&&(d=u[1].trim());const m=l.match(/^(.+?)\s+-\s+(.+?)$/);m&&(c=m[1].trim(),p=m[2].trim()),c=c.replace(/\s*\([^)]*\)\s*$/g,"").trim(),c=c.replace(/[\[\]]/g,"").trim(),c=c.replace(/\s+/g," ").trim(),c&&c.length>2&&(s.push({nome:c,tipo:d||(b==="pos-graduacao"?"Pós-graduação":""),descricao:p,textoCompleto:l}),console.log("Curso (fallback) adicionado com tipo:",d||(b==="pos-graduacao"?"Pós-graduação":"sem tipo")))}}console.log("Total de cursos encontrados:",s.length),console.log("Lista final de cursos:",s);const a=document.createElement("div");return s.forEach((n,r)=>{const l=typeof n=="string"?{nome:n,tipo:"",descricao:""}:n;if(console.log(`=== PROCESSANDO CURSO ${r+1} ===`),console.log("Objeto do curso:",l),console.log("Nome:",l.nome),console.log("Tipo:",l.tipo),console.log("Descrição:",l.descricao),!l.nome||l.nome.length===0)return;const d=`rec-${Date.now()}-${r}`,p=document.createElement("div");p.className="recommendation-item",p.style.marginBottom="15px";const u=l.tipo?`<span style="display: inline-block; background: linear-gradient(135deg, #169DBB 0%, #1b7a8f 100%); color: white; padding: 4px 12px; border-radius: 12px; font-size: 0.75em; font-weight: 600; margin-left: 10px;">${l.tipo}</span>`:"";console.log("Badge de tipo gerado:",u?"SIM":"NÃO");const m=l.descricao?`<div style="color: #666; font-size: 0.9em; line-height: 1.5; margin-top: 8px;">${l.descricao}</div>`:"";p.innerHTML=`
      <div>
        <div style="display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 10px;">
          <div style="flex: 1;">
            <div>
              <span class="recommendation-number">${r+1}.</span>
              <span class="recommendation-course">${l.nome}</span>
              ${u}
            </div>
            ${m}
          </div>
          <div class="feedback-buttons">
            <button class="feedback-btn positive" id="btn-pos-${d}" type="button" title="Gostei desta recomendação">
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M7 22V11M2 13V20C2 21.1046 2.89543 22 4 22H7M7 22H17.2091C18.1129 22 18.9245 21.3992 19.1982 20.5348L21.9727 11.5348C22.3424 10.3597 21.4715 9.17071 20.2501 9.17071H14.4286C13.6395 9.17071 13 8.53121 13 7.74214V4.00004C13 2.89547 12.1046 2.00004 11 2.00004C10.4477 2.00004 10 2.44776 10 3.00004V3.17074C10 3.84286 9.77143 4.49419 9.35429 5.01562L7.30278 7.62152C7.10557 7.86286 7 8.16368 7 8.47361V11" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
              </svg>
            </button>
            <button class="feedback-btn negative" id="btn-neg-${d}" type="button" title="Não gostei desta recomendação">
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M17 2V13M22 11V4C22 2.89543 21.1046 2 20 2H17M17 2H6.79086C5.88705 2 5.07549 2.60078 4.80183 3.46518L2.02735 12.4652C1.65759 13.6403 2.5285 14.8293 3.74987 14.8293H9.57143C10.3605 14.8293 11 15.4688 11 16.2579V20C11 21.1046 11.8954 22 13 22C13.5523 22 14 21.5523 14 21V20.8293C14 20.1571 14.2286 19.5058 14.6457 18.9844L16.6972 16.3785C16.8944 16.1371 17 15.8363 17 15.5264V13" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
              </svg>
            </button>
          </div>
        </div>
      </div>
      <div class="vocacional-buttons-grid" style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 12px;">
        <button class="recommendation-button" id="btn-${d}" type="button">
          <svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><circle cx="11" cy="11" r="8" stroke="currentColor" stroke-width="2"/><path d="M21 21l-4.35-4.35" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>Ver Justificativa
        </button>
        <button class="recommendation-button btn-info-curso" id="btn-info-${d}" type="button">
          <svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Mais Informações
        </button>
      </div>
      <div class="recommendation-justification" id="just-${d}"></div>
    `,a.appendChild(p);const f=l.nome,g=l.tipo;console.log(`=== CONFIGURANDO EVENTOS PARA: ${f} ===`),console.log("Tipo capturado para eventos:",g),console.log("É pós-graduação?",g&&g.trim()!==""),setTimeout(()=>{const w=document.getElementById(`btn-${d}`),v=document.getElementById(`just-${d}`),C=document.getElementById(`btn-info-${d}`),x=document.getElementById(`btn-pos-${d}`),y=document.getElementById(`btn-neg-${d}`);w&&v&&(w.onclick=function(){ue(f,w,v)}),C&&(C.onclick=function(){console.log("========================================"),console.log("CLICOU EM MAIS INFORMAÇÕES"),console.log("Curso:",f),console.log("Tipo capturado:",g),console.log("Tipo é válido?",g&&g.trim()!=="");const R=g&&g.trim()!==""?"pos-graduacao":"graduacao";console.log("Decisão final - navegando para:",R),console.log("========================================"),se(f,R)}),x&&y&&(x.onclick=function(){J(f,"positiva",x,y)},y.onclick=function(){J(f,"negativa",x,y)})},50)}),s.length===0&&(a.innerHTML+='<p style="color: #9ca3af; font-style: italic;">Nenhum curso foi encontrado para exibir.</p>'),e+a.innerHTML}async function ue(o,e,s){if(s.classList.contains("active")){s.classList.remove("active"),e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><circle cx="11" cy="11" r="8" stroke="currentColor" stroke-width="2"/><path d="M21 21l-4.35-4.35" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>Ver Justificativa';return}if(s.innerHTML.trim()!==""){s.classList.add("active"),e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" stroke="currentColor" stroke-width="2"/><path d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Ver Menos';return}e.disabled=!0,e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px; animation: spin 1s linear infinite;"><style>@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); }}</style><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2" stroke-dasharray="31.4 31.4" stroke-linecap="round"/></svg>Carregando...';try{const i=await fetch(E,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({opcao:"justificativa",curso:o,curso_original:z,tipo:b})});if(!i.ok)throw new Error(`Erro na requisição: ${i.status}`);let t=await i.json();Array.isArray(t)&&t.length>0&&(t=t[0]),t.body&&typeof t.body=="object"&&(t=t.body);let a="";t.justificativa?a=t.justificativa:t.output?a=t.output:t.response?a=t.response:t.message?a=t.message:a="Justificativa não disponível.",s.innerHTML=`
      <div class="recommendation-justification-title"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Justificativa Detalhada:</div>
      <div class="recommendation-justification-content">${a}</div>
    `,s.classList.add("active"),e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" stroke="currentColor" stroke-width="2"/><path d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Ver Menos',e.disabled=!1}catch(i){s.innerHTML=`
      <div class="recommendation-justification-title"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/><path d="M15 9l-6 6m0-6l6 6" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>Erro:</div>
      <div class="recommendation-justification-content">Não foi possível carregar a justificativa: ${i.message}</div>
    `,s.classList.add("active"),e.disabled=!1,e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><circle cx="11" cy="11" r="8" stroke="currentColor" stroke-width="2"/><path d="M21 21l-4.35-4.35" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>Ver Justificativa'}}async function ge(o,e,s){if(s.classList.contains("active")){s.classList.remove("active"),e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><circle cx="11" cy="11" r="8" stroke="currentColor" stroke-width="2"/><path d="M21 21l-4.35-4.35" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>Ver Justificativa';return}if(s.innerHTML.trim()!==""){s.classList.add("active"),e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" stroke="currentColor" stroke-width="2"/><path d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Ver Menos';return}e.disabled=!0,e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px; animation: spin 1s linear infinite;"><style>@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); }}</style><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2" stroke-dasharray="31.4 31.4" stroke-linecap="round"/></svg>Carregando...';try{const i=await fetch(E,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({opcao:"justificativa_vocacional",curso:o,palavras_selecionadas:h})});if(!i.ok)throw new Error(`Erro na requisição: ${i.status}`);let t=await i.json();Array.isArray(t)&&t.length>0&&(t=t[0]),t.body&&typeof t.body=="object"&&(t=t.body);let a="";t.justificativa?a=t.justificativa:t.output?a=t.output:t.response?a=t.response:t.message?a=t.message:a="Justificativa não disponível.",s.innerHTML=`
      <div class="recommendation-justification-title"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Justificativa Detalhada:</div>
      <div class="recommendation-justification-content">${a}</div>
    `,s.classList.add("active"),e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><path d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" stroke="currentColor" stroke-width="2"/><path d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Ver Menos',e.disabled=!1}catch(i){s.innerHTML=`
      <div class="recommendation-justification-title"><svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/><path d="M15 9l-6 6m0-6l6 6" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>Erro:</div>
      <div class="recommendation-justification-content">Não foi possível carregar a justificativa: ${i.message}</div>
    `,s.classList.add("active"),e.disabled=!1,e.innerHTML='<svg width="16" height="16" fill="none" viewBox="0 0 24 24" style="display: inline-block; vertical-align: middle; margin-right: 6px;"><circle cx="11" cy="11" r="8" stroke="currentColor" stroke-width="2"/><path d="M21 21l-4.35-4.35" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>Ver Justificativa'}}window.downloadTableAsPNG=async function(o){const e=document.getElementById(o);if(!e)return;const s=1200,i=document.createElement("div");i.style.cssText=`
    position: fixed;
    left: -9999px;
    top: 0;
    background: white;
    padding: 30px 40px;
    width: ${Math.min(e.offsetWidth+80,s)}px;
    box-sizing: border-box;
  `;const t=document.createElement("div");t.style.cssText=`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 25px;
    padding: 0 10px;
  `;const a=document.createElement("img");a.src="https://static.wixstatic.com/media/708137_08bbc5f3b9c64e95af7aa97d89919be0~mv2.jpg/v1/crop/x_13,y_0,w_348,h_118/fill/w_348,h_118,al_c,q_80,enc_avif,quality_auto/310915463_422382353384996_43181404693693499_n_edited.jpg",a.alt="Eduit",a.style.cssText="height: 45px; width: auto; display: inline-block;",a.crossOrigin="anonymous";const n=document.createElement("img");n.src="https://static.wixstatic.com/media/708137_594a834fa9b54e7faa1c1e769c05e066~mv2.png/v1/fill/w_192,h_54,al_c,q_85,usm_0.66_1.00_0.01,enc_avif,quality_auto/logotipo_Cruzeiro%20do%20Sul%20Virtual_CruzeiroEAD.png",n.alt="Cruzeiro EAD",n.style.cssText="height: 45px; width: auto; display: inline-block;",n.crossOrigin="anonymous",t.appendChild(a),t.appendChild(n);const r=e.cloneNode(!0);r.removeAttribute("id"),r.style.cssText=`
    width: 100%;
    border-collapse: collapse;
    margin: 0;
    font-family: Arial, sans-serif;
  `,r.querySelectorAll("th").forEach(d=>{d.style.cssText=`
      background: #5b3ff6;
      color: white;
      padding: 14px 10px;
      text-align: left;
      font-weight: bold;
      font-size: 15px;
      border: none;
    `}),r.querySelectorAll("td").forEach(d=>{const p=d.parentElement,u=Array.from(p.parentElement.children).indexOf(p),m=Array.from(p.children).indexOf(d)===0;let f;m?f=u%2===0?"#f0f0f5":"#e8e8f0":f=u%2===0?"#ffffff":"#f8f9fa",d.style.cssText=`
      padding: 11px 12px;
      border: 1px solid #e0e0e0;
      background: ${f};
      font-size: 13px;
      line-height: 1.5;
      vertical-align: top;
      ${m?"font-weight: 600; color: #333;":""}
    `}),i.appendChild(t),i.appendChild(r),document.body.appendChild(i),await new Promise(d=>setTimeout(d,800));try{const d=await html2canvas(i,{backgroundColor:"#ffffff",scale:1.5,logging:!1,useCORS:!0,allowTaint:!1,windowWidth:i.scrollWidth,windowHeight:i.scrollHeight,imageTimeout:0,removeContainer:!1});document.body.removeChild(i);const p=2048;let u=d;if(d.width>p||d.height>p){const m=Math.min(p/d.width,p/d.height),f=document.createElement("canvas");f.width=d.width*m,f.height=d.height*m;const g=f.getContext("2d");g.fillStyle="#ffffff",g.fillRect(0,0,f.width,f.height),g.drawImage(d,0,0,f.width,f.height),u=f}u.toBlob(function(m){const f=URL.createObjectURL(m),g=document.createElement("a");g.download="comparacao_cursos.jpg",g.href=f,g.style.display="none",document.body.appendChild(g),g.click(),setTimeout(()=>{document.body.removeChild(g),URL.revokeObjectURL(f)},100)},"image/jpeg",.85)}catch(d){console.error("Erro ao gerar imagem:",d),document.body.contains(i)&&document.body.removeChild(i),alert("Erro ao gerar a imagem. Tente novamente.")}};async function J(o,e,s,i){if(e==="positiva"&&s.classList.contains("selected")||e==="negativa"&&i.classList.contains("selected"))return;s.disabled=!0,i.disabled=!0;const t=b==="pos-graduacao"?"avaliacao_pos":"avaliacao";console.log("Enviando avaliação:",{opcao:t,tipo_curso:b,avaliacao:e});try{const a=await fetch(E,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({opcao:t,curso:o,curso_original:z,avaliacao:e,tipo:b})});if(!a.ok)throw new Error(`Erro na requisição: ${a.status}`);e==="positiva"?(s.classList.add("selected"),i.classList.remove("selected")):(i.classList.add("selected"),s.classList.remove("selected"))}catch(a){console.error("Erro ao enviar avaliação:",a),alert("Erro ao enviar avaliação. Tente novamente.")}finally{s.disabled=!1,i.disabled=!1}}var oe;(oe=document.getElementById("form-feedback-comparar"))==null||oe.addEventListener("submit",async o=>{o.preventDefault();const e=document.getElementById("feedback-comparar-text").value,s=o.target.querySelector('button[type="submit"]');s&&(s.disabled=!0);try{const i=await fetch(E,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({opcao:"feedback_comparacao",feedback:e,curso1:H,curso2:O})});if(!i.ok)throw new Error(`Erro na requisição: ${i.status}`);alert("Feedback enviado com sucesso! Obrigado pela sua avaliação."),document.getElementById("form-feedback-comparar").reset()}catch(i){console.error("Erro ao enviar feedback:",i),alert("Erro ao enviar feedback. Tente novamente.")}finally{s&&(s.disabled=!1)}});let h=[],A=6,L=10;var te;(te=document.getElementById("btn-iniciar-teste"))==null||te.addEventListener("click",async()=>{const o=document.getElementById("btn-iniciar-teste"),e=document.getElementById("card-inicio-teste"),s=document.getElementById("loading-teste-vocacional"),i=document.getElementById("card-selecao-palavras");o&&(o.disabled=!0),e&&(e.style.display="none"),s&&s.classList.add("active");try{const t=await fetch(E,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({opcao:"teste_vocacional",acao:"iniciar"})});if(!t.ok)throw new Error(`Erro na requisição: ${t.status}`);let a=await t.json();console.log("Resposta do teste vocacional:",a),Array.isArray(a)&&a.length>0&&(a=a[0]),a.body&&typeof a.body=="object"&&(a=a.body),a.json&&(a=a.json);const n=a.words||[];A=a.min_select||6,L=a.max_select||10,console.log("Palavras recebidas:",n),console.log("Min:",A,"Max:",L),fe(n),s&&s.classList.remove("active"),i&&(i.style.display="block")}catch(t){console.error("Erro ao iniciar teste vocacional:",t),alert("Erro ao iniciar o teste. Tente novamente."),s&&s.classList.remove("active"),e&&(e.style.display="block"),o&&(o.disabled=!1)}});function fe(o){const e=document.getElementById("palavras-grid"),s=document.getElementById("contador-selecionadas");e&&(h=[],e.innerHTML="",o.forEach((i,t)=>{const a=document.createElement("div");a.className="palavra-item",a.textContent=i,a.dataset.palavra=i,a.dataset.index=t,a.addEventListener("click",()=>{if(a.classList.contains("selected"))a.classList.remove("selected"),h=h.filter(r=>r!==i);else{if(h.length>=L)return;a.classList.add("selected"),h.push(i)}s&&(s.textContent=h.length),he(),ve()}),e.appendChild(a)}))}function he(){const o=document.getElementById("btn-finalizar-teste");if(!o)return;const e=h.length;o.disabled=e<A||e>L}function ve(){document.querySelectorAll(".palavra-item").forEach(e=>{e.classList.contains("selected")||(h.length>=L?e.classList.add("disabled"):e.classList.remove("disabled"))})}var ie;(ie=document.getElementById("btn-finalizar-teste"))==null||ie.addEventListener("click",async()=>{const o=document.getElementById("btn-finalizar-teste"),e=document.getElementById("card-selecao-palavras"),s=document.getElementById("loading-teste-vocacional"),i=document.getElementById("response-teste-vocacional"),t=i==null?void 0:i.querySelector(".response-content");o&&(o.disabled=!0),e&&(e.style.display="none"),s&&s.classList.add("active");try{const a=await fetch(E,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({opcao:"teste_vocacional",acao:"finalizar",palavras_selecionadas:h})});if(!a.ok)throw new Error(`Erro na requisição: ${a.status}`);let n=await a.json();if(console.log("Resultado do teste:",n),Array.isArray(n)&&n.length>0&&(n=n[0]),n.body&&typeof n.body=="object"&&(n=n.body),n.output&&typeof n.output=="string")try{const r=JSON.parse(n.output);r.cursos_recomendados&&(n=r)}catch{console.log("Output não é JSON válido, mantendo como está")}s&&s.classList.remove("active"),i&&(i.style.display="block"),t&&(t.classList.remove("empty"),n.cursos_recomendados&&Array.isArray(n.cursos_recomendados)?(t.innerHTML=de(n.cursos_recomendados),setTimeout(()=>{const r=document.getElementById("btn-refazer-teste");r&&(r.onclick=function(){re()})},50)):n.output?t.innerHTML=k(n.output):n.response?t.innerHTML=k(n.response):n.message?t.innerHTML=k(n.message):t.innerHTML="<p>Teste finalizado com sucesso!</p><p>Palavras selecionadas: "+h.join(", ")+"</p>")}catch(a){console.error("Erro ao finalizar teste:",a),alert("Erro ao finalizar o teste. Tente novamente."),s&&s.classList.remove("active"),e&&(e.style.display="block"),o&&(o.disabled=!1)}});
