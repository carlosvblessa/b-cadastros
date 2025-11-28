select
	num_cnpj
from
	admcadapi.lista2_cnpjs
where
	num_cnpj not in
  		(
	select
		 	num_cnpj
	from
			admcadapi.cadastro)
order by
	num_cnpj asc;
