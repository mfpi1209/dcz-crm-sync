import { Fragment, type ReactNode } from "react";

/**
 * A mensagem do saldo vem do servidor com um pouco de HTML (<br>, <strong>, <span>).
 * A antiga usava innerHTML; aqui só viram elementos o <br> e o negrito, e o resto
 * fica como texto (nenhuma tag nem atributo do servidor chega ao DOM).
 */
function converter(no: Node, chave: string): ReactNode {
  if (no.nodeType === Node.TEXT_NODE) return no.textContent;
  if (no.nodeType !== Node.ELEMENT_NODE) return null;
  const el = no as Element;
  const tag = el.tagName.toLowerCase();
  if (tag === "br") return <br key={chave} />;
  const filhos = [...el.childNodes].map((f, i) => converter(f, `${chave}.${i}`));
  if (tag === "strong" || tag === "b") return <strong key={chave}>{filhos}</strong>;
  return <Fragment key={chave}>{filhos}</Fragment>;
}

export function MensagemServidor({ html }: { html?: string }) {
  if (!html) return null;
  const doc = new DOMParser().parseFromString(`<div>${html}</div>`, "text/html");
  const raiz = doc.body.firstElementChild;
  if (!raiz) return null;
  return <>{[...raiz.childNodes].map((f, i) => converter(f, String(i)))}</>;
}
