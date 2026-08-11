-- =====================================================================
-- Interação de leads (Ranking de Agentes Comerciais)
-- Rodar no projeto Supabase que hospeda comercial_feedback
-- (vtlbndvcgajcoajhcnnx).
--
-- A view expande conversa_completa->messages e devolve, por lead Kommo,
-- os DIAS (fuso America/Sao_Paulo) em que o CLIENTE enviou >=1 mensagem
-- (sender_type = 'contact'). O backend cruza com os leads criados no
-- período para contar "quantos interagiram dentro do período".
-- =====================================================================

-- Acelera o filtro por lead_id na expansão da view
CREATE INDEX IF NOT EXISTS idx_comercial_feedback_lead_id
    ON comercial_feedback (lead_id);

CREATE OR REPLACE VIEW vw_lead_interacao AS
SELECT DISTINCT
    cf.lead_id::bigint AS lead_id,
    ((m.value->>'sent_at')::timestamptz AT TIME ZONE 'America/Sao_Paulo')::date AS dia
FROM comercial_feedback cf
CROSS JOIN LATERAL jsonb_array_elements(cf.conversa_completa->'messages') m
WHERE m.value->>'sender_type' = 'contact'
  AND m.value->>'sent_at' ~ '^\d{4}-\d{2}-\d{2}T'
  AND cf.lead_id IS NOT NULL;
