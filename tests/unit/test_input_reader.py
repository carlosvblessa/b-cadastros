from bcadastros_etl.input_reader import read_cnpjs


def test_read_cnpjs_normalizes_deduplicates_and_keeps_order() -> None:
    valid, errors = read_cnpjs(
        [
            "\n",
            "00.123.456/0001-99\n",
            "98765432000100\n",
            "00123456000199\n",
            "123\n",
            "123\n",
        ]
    )
    assert valid == ["00123456000199", "98765432000100"]
    assert len(errors) == 1
    assert errors[0].cnpj == "123"
    assert errors[0].etapa == "validacao_entrada"
