from banco360.qualidade.regras import _regra


def test_regras_detectam_problemas(cfg, spark):
    df = spark.createDataFrame([(1, "a", 5), (1, None, -1), (2, "b", 3)], "id int, nome string, valor int")
    r = lambda spec: _regra(spark, cfg, "silver", "x", df, spec, 3)  # noqa: E731
    assert r({"tipo": "unico", "colunas": ["id"]})[0].status == "falha"
    assert r({"tipo": "nao_nulo", "colunas": ["nome"]})[0].observado == 1
    assert r({"tipo": "intervalo", "coluna": "valor", "min": 0})[0].observado == 1
    assert r({"tipo": "colunas_proibidas", "colunas": ["nome", "cpf"]})[0].status == "falha"
    assert r({"tipo": "valores_aceitos", "coluna": "id", "valores": [1, 2]})[0].status == "ok"
    assert r({"tipo": "volume_minimo", "min": 10, "severidade": "alerta"})[0].status == "falha"
