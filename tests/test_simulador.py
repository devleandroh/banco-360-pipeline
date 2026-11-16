from datetime import date

from banco360.simulador import universo as u


def _cpf_valido(cpf: str) -> bool:
    nums = [int(c) for c in cpf]
    for n in (9, 10):
        s = sum(v * (n + 1 - i) for i, v in enumerate(nums[:n]))
        if (s * 10 % 11) % 10 != nums[n]:
            return False
    return True


def test_universo_e_deterministico():
    a = u.transacoes_do_dia(date(2026, 5, 4))
    b = u.transacoes_do_dia(date(2026, 5, 4))
    assert a == b and len(a) > 200


def test_cpfs_ficticios_tem_formato_valido():
    assert all(_cpf_valido(c.cpf) for c in u.clientes()[:300])


def test_posicao_contrato_regras():
    d = date(2026, 11, 15)
    for p in u.contratos_em(d):
        assert p["saldo_devedor"] >= 0 and p["dias_atraso"] >= 0
        if p["status"] == "Liquidado":
            assert p["saldo_devedor"] == 0
        if p["status"] == "Inadimplente":
            assert 90 < p["dias_atraso"] <= 360
        if p["status"] == "Em dia":
            assert p["dias_atraso"] == 0


def test_origem_injeta_problemas_reais():
    eventos = u.transacoes_do_dia(date(2026, 3, 10))
    ids = [(e["tipo"], e["seq"]) for e in eventos]
    assert len(ids) > len(set(ids)), "deve haver duplicidades para o pipeline tratar"
    assert any(e["valor"] < 0 or e["numero_conta"] == "999999-9" for e in eventos), "deve haver registros inválidos"
