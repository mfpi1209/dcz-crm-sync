-- =============================================================================
-- Indices para acelerar o Dashboard Comercial (/api/comercial-rgm/data)
-- =============================================================================
-- Seguro: todos `IF NOT EXISTS`. Pode rodar a qualquer momento.
-- Em tabelas muito grandes (kommo `leads`), prefira rodar 1 por vez via SQL
-- Editor do Supabase, fora de transacao (cada CREATE em block separado).
-- Para evitar lock prolongado, troque por `CREATE INDEX CONCURRENTLY` MANUAL.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Banco principal (db_agente_comercial_csv)
-- -----------------------------------------------------------------------------

-- comercial_rgm: data/polo/nivel ja existem (idx_crgm_data/polo/nivel).
-- Falta apenas rgm para joins/lookup direto.
CREATE INDEX IF NOT EXISTS idx_crgm_rgm
  ON comercial_rgm (rgm);

-- xl_rows: lookup por snapshot e por RGM dentro do JSONB.
-- O endpoint usa "WHERE s.id = (SELECT id FROM xl_snapshots ... ORDER BY id DESC LIMIT 1)"
-- e regex no campo data->>'rgm'.
CREATE INDEX IF NOT EXISTS idx_xl_rows_snapshot_id
  ON xl_rows (snapshot_id);

-- Indice funcional para o RGM normalizado (usado em DISTINCT ON e WHERE).
-- Como o codigo aplica regexp_replace(...,'[^0-9]','','g'), o indice direto em
-- data->>'rgm' nao "casa" perfeitamente, mas ajuda no DISTINCT ON.
CREATE INDEX IF NOT EXISTS idx_xl_rows_data_rgm
  ON xl_rows ((data->>'rgm'));

-- xl_snapshots: usado em subqueries para pegar o snapshot mais recente.
CREATE INDEX IF NOT EXISTS idx_xl_snapshots_tipo_id
  ON xl_snapshots (tipo, id DESC);

-- NOTA: comercial_rgm_completa eh uma VIEW, nao pode ser indexada diretamente.
-- Para acelerar daily_history/_count_hist, os indices ja criados na tabela base
-- (comercial_rgm: data_matricula/polo/nivel/rgm) sao suficientes.

-- -----------------------------------------------------------------------------
-- Banco Kommo (leads, lead_custom_field_values)
-- -----------------------------------------------------------------------------

-- Ranking de agentes: SUM(...) FILTER por responsible_user_id, status_id, created_at, closed_at.
CREATE INDEX IF NOT EXISTS idx_leads_resp_created
  ON leads (responsible_user_id, created_at)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS idx_leads_resp_status_closed
  ON leads (responsible_user_id, status_id, closed_at)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS idx_leads_status_deleted_id
  ON leads (status_id, is_deleted, id);

-- Ticket medio: lead_custom_field_values filtra por field_name='rgm'.
CREATE INDEX IF NOT EXISTS idx_lcfv_field_lead
  ON lead_custom_field_values (lead_id)
  WHERE lower(field_name) = 'rgm';

-- =============================================================================
-- Apos rodar, valide com:
--   EXPLAIN ANALYZE SELECT ... (queries do dashboard)
-- =============================================================================
