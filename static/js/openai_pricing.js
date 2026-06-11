// ---------------------------------------------------------------------------
// OpenAI pricing helper (USD/1M tokens) e conversão BRL.
// Atualizar PRICES quando OpenAI mudar tabela. USD_TO_BRL é fixo aqui pra
// simplicidade; troque pra fetch dinâmico se necessário.
// ---------------------------------------------------------------------------
const OAI_USD_TO_BRL = 5.5;

const OAI_PRICES = {
    'gpt-4.1-mini':            { input: 0.40, output: 1.60 },
    'gpt-4.1-nano':            { input: 0.10, output: 0.40 },
    'gpt-4o-mini':             { input: 0.15, output: 0.60 },
    'gpt-4o':                  { input: 2.50, output: 10.00 },
    'o3-mini':                 { input: 1.10, output: 4.40 },
    'text-embedding-3-small':  { input: 0.02, output: 0 },
    'text-embedding-3-large':  { input: 0.13, output: 0 },
};

function calcCostBRL(usage, model) {
    const p = OAI_PRICES[model] || { input: 0, output: 0 };
    const promptTokens = Number(usage && usage.prompt_tokens) || 0;
    const completionTokens = Number(usage && usage.completion_tokens) || 0;
    const usd = (promptTokens * p.input + completionTokens * p.output) / 1_000_000;
    return usd * OAI_USD_TO_BRL;
}
