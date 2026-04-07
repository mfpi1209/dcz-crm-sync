-- View usada pelo dashboard (comercial_rgm, repasse, minha_performance).
-- Fonte única de RGM: custom_fields_json em leads (alinhado ao registro do lead no Kommo).
-- Aplica-se aos pipelines "Funil de vendas" e "Licenciado"; RGM = 8 dígitos após normalizar.
-- Reverter para JSON + lead_custom_field_values: sql/vw_leads_rgm_legacy.sql
-- Aplicar no banco kommo_sync: psql -f sql/vw_leads_rgm.sql

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
        p.name AS pipeline
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
)
SELECT DISTINCT ON (lead_id)
    lead_id,
    rgm,
    consultora,
    preco,
    pipeline
FROM rgm_from_json
ORDER BY lead_id, rgm;
