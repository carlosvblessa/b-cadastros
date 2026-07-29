from bcadastros_etl.input_reader import read_cnpjs


def test_read_cnpjs_normalizes_deduplicates_and_keeps_order() -> None:
    valid, errors = read_cnpjs(
        [
            "\n",
            "00.123.456/0001-99\n",
            "98765432000100\n",
            "00123456000199\n",
            "aa.345.678/0003-29\n",
            "AA345678000329\n",
            "123\n",
            "123\n",
            "12.345.678/000@-08\n",
        ]
    )
    assert valid == ["00123456000199", "98765432000100", "AA345678000329"]
    assert len(errors) == 2
    assert errors[0].cnpj == "123"
    assert errors[0].etapa == "validacao_entrada"
    assert errors[1].cnpj == "12.345.678/000@-08"
