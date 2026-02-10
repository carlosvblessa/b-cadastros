select
        cad.num_cnpj
from
	admods001.ods_cadastro obc
inner join admcadapi.cadastro cad on
	obc.num_cnpj = cad.num_cnpj
inner join admcadapi.cad_sefaz_pj sfz on
	cad.num_cnpj = sfz.num_cnpj
where 
        cad.dt_ultima_atualizacao <= CURRENT_DATE - interval '4 days'
order by
        cad.num_cnpj asc

