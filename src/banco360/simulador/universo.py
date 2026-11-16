"""
Universo determinístico do "core bancário" fictício.

Este módulo NÃO faz parte do pipeline: ele simula os sistemas de origem de um
banco (cadastro, contas, contratos de crédito e transações) para que o pipeline
tenha de onde extrair dados. Tudo é gerado a partir de uma semente fixa, então a
mesma data de referência sempre produz exatamente o mesmo estado.

Nenhum dado é real: nomes, CPFs e e-mails são combinações aleatórias.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache

import numpy as np

SEED = 360
EPOCH = date(2023, 1, 1)
HORIZON_DAYS = 4 * 365 + 1  # o universo cobre ~4 anos a partir de EPOCH
N_CLIENTES = 4000
N_CONTRATOS = 3600

PRIMEIROS_NOMES = [
    "Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isabela", "João",
    "Karina", "Lucas", "Mariana", "Nicolas", "Olívia", "Pedro", "Rafaela", "Samuel", "Tatiane", "Vinícius",
    "Beatriz", "Caio", "Débora", "Enzo", "Fernanda", "Gustavo", "Helena", "Igor", "Júlia", "Leonardo",
    "Larissa", "Matheus", "Natália", "Otávio", "Paula", "Renato", "Sofia", "Thiago", "Valentina", "Yuri",
]
SOBRENOMES = [
    "Almeida", "Barbosa", "Cardoso", "Dias", "Esteves", "Ferraz", "Gomes", "Holanda", "Igarashi", "Jardim",
    "Lacerda", "Monteiro", "Nogueira", "Oliveira", "Prado", "Queiroz", "Rezende", "Siqueira", "Tavares", "Vasconcelos",
    "Antunes", "Bittencourt", "Campos", "Duarte", "Fontes", "Guimarães", "Leal", "Machado", "Pacheco", "Rocha",
]
UFS = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "GO", "DF", "ES", "PA", "AM", "MT"]
UF_PESOS = np.array([30, 11, 11, 7, 7, 5, 6, 4, 4, 3.5, 3, 2.5, 2.5, 1.5, 3], dtype=float)
UF_PESOS /= UF_PESOS.sum()
AGENCIAS = ["0001", "0012", "0027", "0035", "0041", "0058", "0063", "0076", "0089", "0094", "0102", "0117"]

# produto: (taxa base a.m., prazos possíveis, valor mín, valor máx, PD base)
PRODUTOS: dict[str, tuple[float, tuple[int, ...], float, float, float]] = {
    "Crédito pessoal": (0.045, (12, 18, 24, 36), 3_000, 40_000, 0.30),
    "Consignado": (0.018, (24, 36, 48, 60, 72, 84), 8_000, 90_000, 0.07),
    "Financiamento de veículos": (0.016, (24, 36, 48, 60), 25_000, 140_000, 0.13),
    "Cartão parcelado": (0.075, (3, 6, 10, 12), 800, 10_000, 0.32),
    "Crédito imobiliário": (0.009, (120, 180, 240, 360), 120_000, 420_000, 0.08),
}
PRODUTO_PROB = {  # por segmento
    "Essencial": [0.42, 0.27, 0.12, 0.185, 0.005],
    "Prime": [0.30, 0.20, 0.30, 0.14, 0.06],
    "Private": [0.15, 0.08, 0.40, 0.12, 0.25],
}


def add_months(d: date, months: int) -> date:
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    last = [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
    return date(y, m, min(d.day, last))


def _rng(*key: int) -> np.random.Generator:
    return np.random.default_rng([SEED, *key])


def _cpf(rng: np.random.Generator) -> str:
    """CPF fictício com dígitos verificadores válidos (formato realista para testar o mascaramento)."""
    base = [int(x) for x in rng.integers(0, 10, 9)]
    for n in (9, 10):
        s = sum(v * (n + 1 - i) for i, v in enumerate(base))
        dv = (s * 10) % 11
        base.append(0 if dv == 10 else dv)
    return "".join(map(str, base))


def _ascii(s: str) -> str:
    tabela = str.maketrans("áàâãéêíóôõúçÁÉÍÓÚÇ", "aaaaeeiooouc" "AEIOUC")
    return s.translate(tabela)


@dataclass(frozen=True)
class Cliente:
    cliente_id: int
    nome: str
    cpf: str
    data_nascimento: date
    email: str
    uf: str
    segmento: str
    renda_mensal: float
    score_interno: int
    data_cadastro: date


@dataclass(frozen=True)
class Conta:
    conta_id: int
    cliente_id: int
    agencia: str
    numero: str
    tipo: str
    data_abertura: date
    data_encerramento: date | None
    saldo_base: float
    fase: int


@dataclass(frozen=True)
class Contrato:
    contrato_id: int
    cliente_id: int
    produto: str
    data_contratacao: date
    valor_contratado: float
    taxa_mensal: float
    prazo_meses: int
    valor_parcela: float
    parcela_default: int | None  # 1ª parcela não paga (None = bom pagador)


@lru_cache(maxsize=1)
def clientes() -> tuple[Cliente, ...]:
    out = []
    datas = sorted(int(x) for x in _rng(1).integers(0, HORIZON_DAYS, N_CLIENTES))
    for i in range(N_CLIENTES):
        r = _rng(2, i)
        nome = f"{PRIMEIROS_NOMES[r.integers(len(PRIMEIROS_NOMES))]} {SOBRENOMES[r.integers(len(SOBRENOMES))]} {SOBRENOMES[r.integers(len(SOBRENOMES))]}"
        renda = float(round(np.exp(r.normal(math.log(4_800), 0.85)), 2))
        renda = max(1_412.0, min(renda, 180_000.0))
        segmento = "Essencial" if renda < 9_000 else "Prime" if renda < 28_000 else "Private"
        score = int(np.clip(r.normal(560 + 55 * math.log(renda / 4_800 + 1), 110), 300, 950))
        idade_dias = int(r.integers(18 * 365, 78 * 365))
        cadastro = EPOCH + timedelta(days=datas[i])
        partes = _ascii(nome).lower().split()
        out.append(
            Cliente(
                cliente_id=i + 1,
                nome=nome,
                cpf=_cpf(r),
                data_nascimento=cadastro - timedelta(days=idade_dias),
                email=f"{partes[0]}.{partes[-1]}{i + 1}@exemplo.com.br",
                uf=str(r.choice(UFS, p=UF_PESOS)),
                segmento=segmento,
                renda_mensal=renda,
                score_interno=score,
                data_cadastro=cadastro,
            )
        )
    return tuple(out)


@lru_cache(maxsize=1)
def contas() -> tuple[Conta, ...]:
    out: list[Conta] = []
    for c in clientes():
        r = _rng(3, c.cliente_id)
        tipos = ["Corrente"] + (["Poupança"] if r.random() < 0.32 else [])
        for tipo in tipos:
            abertura = c.data_cadastro + timedelta(days=0 if tipo == "Corrente" else int(r.integers(0, 60)))
            encerramento = None
            if r.random() < 0.05:
                encerramento = abertura + timedelta(days=int(r.integers(90, 900)))
            mult = r.uniform(0.15, 2.5) if tipo == "Corrente" else r.uniform(0.5, 9.0)
            conta_id = len(out) + 1
            numero = f"{100000 + conta_id * 7:06d}-{conta_id % 10}"
            out.append(
                Conta(
                    conta_id=conta_id,
                    cliente_id=c.cliente_id,
                    agencia=AGENCIAS[c.cliente_id % len(AGENCIAS)],
                    numero=numero,
                    tipo=tipo,
                    data_abertura=abertura,
                    data_encerramento=encerramento,
                    saldo_base=round(c.renda_mensal * mult, 2),
                    fase=int(r.integers(0, 30)),
                )
            )
    return tuple(out)


@lru_cache(maxsize=1)
def contratos() -> tuple[Contrato, ...]:
    cls = clientes()
    datas_cad = np.array([(c.data_cadastro - EPOCH).days for c in cls])
    datas = sorted(int(x) for x in _rng(4).integers(30, HORIZON_DAYS, N_CONTRATOS))
    out = []
    for j, dia in enumerate(datas):
        r = _rng(5, j)
        elegiveis = int(np.searchsorted(datas_cad, dia - 30, side="right"))
        if elegiveis == 0:
            continue
        cli = cls[int(r.integers(0, elegiveis))]
        produto = str(r.choice(list(PRODUTOS), p=PRODUTO_PROB[cli.segmento]))
        taxa_base, prazos, vmin, vmax, pd_base = PRODUTOS[produto]
        valor = float(np.clip(r.uniform(vmin, vmax) * (0.5 + cli.renda_mensal / 12_000), vmin, vmax * 1.5))
        valor = round(valor / 100) * 100
        # taxa sobe para score baixo e acompanha um ciclo de juros ao longo do tempo
        ciclo = 1 + 0.12 * math.sin(2 * math.pi * dia / 1100)
        taxa = round(taxa_base * ciclo * (1 + (650 - cli.score_interno) / 1500), 5)
        prazo = int(r.choice(prazos))
        parcela = round(valor * taxa / (1 - (1 + taxa) ** -prazo), 2)
        pd = min(0.75, pd_base * math.exp((600 - cli.score_interno) / 110))
        default = int(r.integers(1, prazo + 1)) if r.random() < pd else None
        out.append(
            Contrato(
                contrato_id=j + 1,
                cliente_id=cli.cliente_id,
                produto=produto,
                data_contratacao=EPOCH + timedelta(days=dia),
                valor_contratado=valor,
                taxa_mensal=taxa,
                prazo_meses=prazo,
                valor_parcela=parcela,
                parcela_default=default,
            )
        )
    return tuple(out)


# ---------------------------------------------------------------------------
# Estado "na data de referência"
# ---------------------------------------------------------------------------
def clientes_em(d: date) -> list[Cliente]:
    return [c for c in clientes() if c.data_cadastro <= d]


def saldo_conta(conta: Conta, d: date) -> float:
    """Saldo com ciclo mensal (entrada de salário) e leve tendência — determinístico."""
    ciclo = 1 + 0.25 * math.cos(2 * math.pi * ((d.toordinal() + conta.fase) % 30) / 30)
    tendencia = 1 + 0.0002 * (d - conta.data_abertura).days
    return round(conta.saldo_base * ciclo * tendencia, 2)


def contas_em(d: date) -> list[dict]:
    out = []
    for c in contas():
        if c.data_abertura > d:
            continue
        encerrada = c.data_encerramento is not None and c.data_encerramento <= d
        out.append(
            {
                "conta_id": c.conta_id,
                "cliente_id": c.cliente_id,
                "agencia": c.agencia,
                "numero": c.numero,
                "tipo": c.tipo,
                "data_abertura": c.data_abertura,
                "data_encerramento": c.data_encerramento if encerrada else None,
                "status": "Encerrada" if encerrada else "Ativa",
                "saldo": 0.0 if encerrada else saldo_conta(c, d),
            }
        )
    return out


def _atraso_temporario(contrato_id: int, d: date) -> int:
    """Atrasos curtos que se regularizam (ex.: cliente pagou alguns dias depois)."""
    h = (contrato_id * 2654435761 + d.year * 12 + d.month) % 1000
    if h < 110:  # ~11% dos contratos em dia ficam alguns dias atrasados em cada mês
        return 1 + (h * 37 + d.day) % 80
    return 0


def posicao_contrato(k: Contrato, d: date) -> dict | None:
    if k.data_contratacao > d:
        return None
    vencidas = 0
    while vencidas < k.prazo_meses and add_months(k.data_contratacao, vencidas + 1) <= d:
        vencidas += 1
    dias_atraso = 0
    if k.parcela_default is not None and vencidas >= k.parcela_default:
        pagas = k.parcela_default - 1
        dias_atraso = (d - add_months(k.data_contratacao, k.parcela_default)).days
        status = "Baixado para prejuízo" if dias_atraso > 360 else "Inadimplente" if dias_atraso > 90 else "Em atraso"
    elif vencidas == k.prazo_meses:
        pagas, status = k.prazo_meses, "Liquidado"
    else:
        temp = _atraso_temporario(k.contrato_id, d) if vencidas > 0 else 0
        if temp > 0:
            # atraso de N dias deixa 1 + N // 31 parcelas em aberto
            pagas, dias_atraso, status = max(0, vencidas - 1 - temp // 31), temp, "Em atraso"
        else:
            pagas, status = vencidas, "Em dia"
    restantes = k.prazo_meses - pagas
    i = k.taxa_mensal
    saldo = 0.0 if status == "Liquidado" else round(k.valor_parcela * (1 - (1 + i) ** -restantes) / i, 2)
    return {
        "contrato_id": k.contrato_id,
        "cliente_id": k.cliente_id,
        "produto": k.produto,
        "data_contratacao": k.data_contratacao,
        "valor_contratado": k.valor_contratado,
        "taxa_mensal": k.taxa_mensal,
        "prazo_meses": k.prazo_meses,
        "valor_parcela": k.valor_parcela,
        "parcelas_pagas": pagas,
        "saldo_devedor": saldo,
        "valor_em_atraso": round(k.valor_parcela * (vencidas - pagas), 2),
        "dias_atraso": dias_atraso,
        "status": status,
    }


def contratos_em(d: date) -> list[dict]:
    return [p for k in contratos() if (p := posicao_contrato(k, d)) is not None]


# ---------------------------------------------------------------------------
# Transações do dia (cartões em CSV; Pix/TED/boletos em JSON)
# ---------------------------------------------------------------------------
TIPOS = ["PIX", "CARTAO_DEBITO", "CARTAO_CREDITO", "TED", "BOLETO"]
MCC = ["Supermercado", "Restaurantes", "Combustível", "Farmácia", "E-commerce", "Transporte", "Lazer", "Vestuário"]
BANCOS = [("00000000", "Banco do Brasil"), ("00360305", "Caixa"), ("60746948", "Bradesco"), ("60701190", "Itaú"),
          ("90400888", "Santander"), ("18236120", "Nubank"), ("00416968", "Inter"), ("31872495", "C6 Bank")]
LAMBDA_SEG = {"Essencial": 0.20, "Prime": 0.32, "Private": 0.42}


def transacoes_do_dia(d: date) -> list[dict]:
    """Lista de eventos brutos do dia, já no formato de cada sistema de origem."""
    cls = {c.cliente_id: c for c in clientes()}
    ativas = [c for c in contas() if c.tipo == "Corrente" and c.data_abertura <= d
              and (c.data_encerramento is None or c.data_encerramento > d)]
    r = _rng(9, d.toordinal())
    fator_dia = (0.6 if d.weekday() >= 5 else 1.0) * (1.25 if d.day <= 10 else 1.0)
    # participação do Pix cresce ao longo do tempo
    t = (d - EPOCH).days / HORIZON_DAYS
    pesos = np.array([0.46 + 0.18 * t, 0.17 - 0.05 * t, 0.20, 0.07 - 0.04 * t, 0.10 - 0.04 * t])
    pesos /= pesos.sum()
    eventos: list[dict] = []
    seq = 0
    for conta in ativas:
        cli = cls[conta.cliente_id]
        n = r.poisson(LAMBDA_SEG[cli.segmento] * fator_dia)
        for _ in range(n):
            seq += 1
            tipo = TIPOS[int(r.choice(5, p=pesos))]
            escala = math.sqrt(cli.renda_mensal / 4_800)
            media = {"PIX": 140, "CARTAO_DEBITO": 75, "CARTAO_CREDITO": 160, "TED": 2_500, "BOLETO": 420}[tipo]
            valor = round(float(np.exp(r.normal(math.log(media * escala), 0.9))), 2)
            hora = datetime(d.year, d.month, d.day) + timedelta(seconds=int(r.integers(6 * 3600, 23 * 3600)))
            base = {"seq": seq, "tipo": tipo, "data_hora": hora, "agencia": conta.agencia, "numero_conta": conta.numero, "valor": valor}
            if tipo.startswith("CARTAO"):
                base.update(
                    mcc=MCC[int(r.integers(len(MCC)))],
                    parcelas=int(r.choice([1, 1, 1, 2, 3, 6, 10])) if tipo == "CARTAO_CREDITO" else 1,
                    status="NEGADA" if r.random() < 0.03 else "APROVADA",
                )
            else:
                banco = BANCOS[int(r.integers(len(BANCOS)))]
                base.update(
                    direcao=("RECEBIDO" if r.random() < (0.42 if tipo == "PIX" else 0.3) else "ENVIADO") if tipo != "BOLETO" else "ENVIADO",
                    ispb=banco[0],
                    banco=banco[1],
                    chave=str(r.choice(["CPF", "EMAIL", "TELEFONE", "ALEATORIA"])) if tipo == "PIX" else None,
                )
            eventos.append(base)
    # Problemas reais de origem, para o pipeline tratar: duplicidades e registros inválidos.
    n_dup = max(1, int(len(eventos) * 0.004))
    for idx in r.choice(len(eventos), n_dup, replace=False):
        eventos.append(dict(eventos[int(idx)]))
    n_inv = max(1, int(len(eventos) * 0.001))
    for idx in r.choice(len(eventos), n_inv, replace=False):
        ruim = dict(eventos[int(idx)])
        seq += 1
        ruim["seq"] = seq
        if r.random() < 0.5:
            ruim["valor"] = -abs(ruim["valor"])
        else:
            ruim["numero_conta"] = "999999-9"
        eventos.append(ruim)
    return eventos
