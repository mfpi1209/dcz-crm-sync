-- View usada pelo dashboard (comercial_rgm, repasse, minha_performance).
-- RGM vem de custom_fields_json (prioridade 1) OU lead_custom_field_values (prioridade 2).
-- Motivo: sync LIGHT / espelho PG pode manter cf_values atualizado antes do JSON; só JSON
-- fazia sumir leads da view e inflar transferencia/regresso.
-- Pipelines: "Funil de vendas" e "Licenciado"; RGM = 8 dígitos após normalizar.
-- Aplicar no banco Kommo (PG): psql ... -f sql/vw_leads_rgm.sql

CREATE OR REPLACE VIEW vw_leads_rgm AS
WITH rgm_from_json AS (
    SELECT
        l.id AS lead_id,
        regexp_replace(
            ((cf_elem.value -> 'values') -> 0) ->> 'value',
            '[^0-9]',
            '',
            'g'
        ) AS rgm,
        COALESCE(u.name, 'N/A'::text) AS consultora,
        l.price AS preco,
        p.name AS pipeline,
        1 AS priority
    FROM leads l
    CROSS JOIN LATERAL jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) AS cf_elem (value)
    JOIN pipelines p ON p.id = l.pipeline_id
        AND (p.name = ANY (ARRAY['Funil de vendas'::text, 'Licenciado'::text]))
    LEFT JOIN users u ON u.id = l.responsible_user_id
    WHERE l.is_deleted = false
      AND lower(cf_elem.value ->> 'field_name') = 'rgm'
      AND (((cf_elem.value -> 'values') -> 0) ->> 'value') IS NOT NULL
      AND (((cf_elem.value -> 'values') -> 0) ->> 'value') <> ''
      AND length(
          regexp_replace(
              ((cf_elem.value -> 'values') -> 0) ->> 'value',
              '[^0-9]',
              '',
              'g'
          )
      ) = 8
),
rgm_from_cf_values AS (
    SELECT
        l.id AS lead_id,
        regexp_replace(
            (lcf.values_json -> 0) ->> 'value',
            '[^0-9]',
            '',
            'g'
        ) AS rgm,
        COALESCE(u.name, 'N/A'::text) AS consultora,
        l.price AS preco,
        p.name AS pipeline,
        2 AS priority
    FROM leads l
    JOIN lead_custom_field_values lcf
        ON lcf.lead_id = l.id
        AND lower(lcf.field_name) = 'rgm'
        AND ((lcf.values_json -> 0) ->> 'value') IS NOT NULL
        AND ((lcf.values_json -> 0) ->> 'value') <> ''
    JOIN pipelines p ON p.id = l.pipeline_id
        AND (p.name = ANY (ARRAY['Funil de vendas'::text, 'Licenciado'::text]))
    LEFT JOIN users u ON u.id = l.responsible_user_id
    WHERE l.is_deleted = false
      AND length(
          regexp_replace(
              (lcf.values_json -> 0) ->> 'value',
              '[^0-9]',
              '',
              'g'
          )
      ) = 8
),
all_leads AS (
    SELECT lead_id, rgm, consultora, preco, pipeline, priority FROM rgm_from_json
    UNION ALL
    SELECT lead_id, rgm, consultora, preco, pipeline, priority FROM rgm_from_cf_values
)
SELECT DISTINCT ON (lead_id)
    lead_id,
    rgm,
    consultora,
    preco,
    pipeline
FROM all_leads
ORDER BY lead_id, priority;
