SELECT
  c.num_cnpj
FROM admcadapi.cad_sefaz_pj c
JOIN admb_cads.estabelecimento e
  ON e.num_cnpj = c.num_cnpj
WHERE
  e.data_carga < '{{CUTOFF_DATE}}'::date
ORDER BY
  c.num_cnpj ASC
FOR UPDATE OF e SKIP LOCKED
LIMIT {{LIMIT}};
