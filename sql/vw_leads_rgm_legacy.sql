-- Versão anterior da vw_leads_rgm: RGM de custom_fields_json (prioridade 1) OU
-- lead_custom_field_values (prioridade 2). Use se precisar reverter:
--   psql ... -f sql/vw_leads_rgm_legacy.sql

CREATE OR REPLACE VIEW vw_leads_rgm AS
 WITH rgm_from_json AS (
         SELECT l.id AS lead_id,
            regexp_replace(((cf_elem.value -> 'values'::text) -> 0) ->> 'value'::text, '[^0-9]'::text, ''::text, 'g'::text) AS rgm,
            COALESCE(u.name, 'N/A'::text) AS consultora,
            l.price AS preco,
            p.name AS pipeline,
            1 AS priority
           FROM leads l
             CROSS JOIN LATERAL jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) cf_elem(value)
             JOIN pipelines p ON p.id = l.pipeline_id AND (p.name = ANY (ARRAY['Funil de vendas'::text, 'Licenciado'::text]))
             LEFT JOIN users u ON u.id = l.responsible_user_id
          WHERE l.is_deleted = false AND lower(cf_elem.value ->> 'field_name'::text) = 'rgm'::text AND (((cf_elem.value -> 'values'::text) -> 0) ->> 'value'::text) IS NOT NULL AND (((cf_elem.value -> 'values'::text) -> 0) ->> 'value'::text) <> ''::text AND length(regexp_replace(((cf_elem.value -> 'values'::text) -> 0) ->> 'value'::text, '[^0-9]'::text, ''::text, 'g'::text)) = 8
        ), rgm_from_cf_values AS (
         SELECT l.id AS lead_id,
            regexp_replace((lcf.values_json -> 0) ->> 'value'::text, '[^0-9]'::text, ''::text, 'g'::text) AS rgm,
            COALESCE(u.name, 'N/A'::text) AS consultora,
            l.price AS preco,
            p.name AS pipeline,
            2 AS priority
           FROM leads l
             JOIN lead_custom_field_values lcf ON lcf.lead_id = l.id AND lower(lcf.field_name) = 'rgm'::text AND ((lcf.values_json -> 0) ->> 'value'::text) IS NOT NULL AND ((lcf.values_json -> 0) ->> 'value'::text) <> ''::text
             JOIN pipelines p ON p.id = l.pipeline_id AND (p.name = ANY (ARRAY['Funil de vendas'::text, 'Licenciado'::text]))
             LEFT JOIN users u ON u.id = l.responsible_user_id
          WHERE l.is_deleted = false AND length(regexp_replace((lcf.values_json -> 0) ->> 'value'::text, '[^0-9]'::text, ''::text, 'g'::text)) = 8
        ), all_leads AS (
         SELECT rgm_from_json.lead_id,
            rgm_from_json.rgm,
            rgm_from_json.consultora,
            rgm_from_json.preco,
            rgm_from_json.pipeline,
            rgm_from_json.priority
           FROM rgm_from_json
        UNION ALL
         SELECT rgm_from_cf_values.lead_id,
            rgm_from_cf_values.rgm,
            rgm_from_cf_values.consultora,
            rgm_from_cf_values.preco,
            rgm_from_cf_values.pipeline,
            rgm_from_cf_values.priority
           FROM rgm_from_cf_values
        )
 SELECT DISTINCT ON (lead_id) lead_id,
    rgm,
    consultora,
    preco,
    pipeline
   FROM all_leads
  ORDER BY lead_id, priority;
