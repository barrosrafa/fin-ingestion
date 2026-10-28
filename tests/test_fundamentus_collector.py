"""Testes unitários exaustivos para o FundamentusCollector no fin-ingestion."""

import pytest

from fin_ingestion.collectors.sites.fundamentus import FundamentusCollector


@pytest.fixture
def collector():
    return FundamentusCollector()


def test_parse_br_number(collector):
    assert collector.parse_br_number("41,19") == 41.19
    assert collector.parse_br_number("1.586.020.000") == 1586020000.0
    assert collector.parse_br_number("22,4%") == 0.224
    assert collector.parse_br_number("-1,22%") == -0.0122
    assert collector.parse_br_number("-") is None
    assert collector.parse_br_number("--") is None
    assert collector.parse_br_number(None) is None


def test_parse_br_int(collector):
    assert collector.parse_br_int("11.026.900.000") == 11026900000
    assert collector.parse_br_int("37") == 37
    assert collector.parse_br_int("-") is None


def test_parse_screener_acoes(collector):
    sample_html = """
    <table>
      <tr><th>Papel</th><th>Cotacao</th><th>P/L</th><th>P/VP</th><th>PSR</th><th>Div.Yield</th><th>P/Ativo</th><th>P/Cap.Giro</th><th>P/EBIT</th><th>P/Ativ Circ.Liq</th><th>EV/EBIT</th><th>EV/EBITDA</th><th>Mrg Bruta</th><th>Mrg Ebit</th><th>Mrg. Liq</th><th>Liq. Corr.</th><th>ROIC</th><th>ROE</th><th>Liq.2meses</th><th>Patrim. Liq</th><th>Div.Liq/ Patrim.</th><th>Cresc. Rec.5a</th></tr>
      <tr><td>ITUB4</td><td>41,19</td><td>9,74</td><td>2,19</td><td>-</td><td>7,6%</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>0,0%</td><td>-</td><td>-</td><td>22,4%</td><td>1.586.020.000</td><td>207.648.000.000</td><td>-</td><td>30,3%</td></tr>
    </table>
    """
    records = collector.parse({"route": "screener_acoes", "is_json": False, "html": sample_html})
    assert len(records) == 1
    r = records[0]
    assert r["ticker"] == "ITUB4"
    assert r["cotacao"] == 41.19
    assert r["pl"] == 9.74
    assert r["pvp"] == 2.19
    assert r["roe"] == 0.224
    assert r["dividend_yield"] == 0.076
    assert r["patrimonio_liquido"] == 207648000000.0
    assert r["divida_liquida_patrimonio"] is None


def test_parse_cotacoes_historicas_json(collector):
    payload = {
        "ticker": "HGLG11",
        "route": "cotacoes_historicas",
        "is_json": True,
        "content": [
            [1299024000000, 21.6036],
            [1300665600000, 21.6036],
            [1301961600000, 21.7534],
        ],
    }
    records = collector.parse(payload)
    assert len(records) == 3
    assert records[0]["ticker"] == "HGLG11"
    assert records[0]["preco_fechamento_ajustado"] == 21.6036
    assert "2011" in records[0]["data_pregao"]


def test_parse_fii_imoveis(collector):
    sample_html = """
    <table>
      <tr><th>FII</th><th>Imóvel</th><th>Endereço</th><th>Outras características</th></tr>
      <tr><td>HGLG11</td><td>Centro Logístico Betim</td><td>Rodovia BR 381, Km 490, Betim - MG</td><td>Galpão Logístico</td></tr>
    </table>
    """
    records = collector.parse({"route": "fii_imoveis", "is_json": False, "html": sample_html})
    assert len(records) == 1
    assert records[0]["ticker"] == "HGLG11"
    assert records[0]["nome_imovel"] == "Centro Logístico Betim"
    assert "Betim - MG" in records[0]["endereco"]
    assert records[0]["caracteristicas"] == "Galpão Logístico"


def test_parse_fatos_relevantes(collector):
    sample_html = """
    <table>
      <tr><th>Data</th><th>Empresa</th><th>Tipo</th><th>Descrição</th><th>Download</th><th></th><th>Analisar</th></tr>
      <tr><td>27/10/2026 19:51</td><td>HYPERA</td><td>FR</td><td>Aprovação de Recompra</td><td><a href="https://rad.cvm.gov.br/doc.pdf">Download</a></td><td></td><td>HYPE3</td></tr>
    </table>
    """
    records = collector.parse({"route": "fatos_relevantes", "is_json": False, "html": sample_html})
    assert len(records) == 1
    assert records[0]["ticker"] == "HYPE3"
    assert records[0]["tipo_documento"] == "FR"
    assert records[0]["download_cvm_url"] == "https://rad.cvm.gov.br/doc.pdf"


def test_parse_detalhes_acao_bancaria(collector):
    sample_html = """
    <table>
      <tr><td>Papel</td><td>ITUB4</td><td>Cotacao</td><td>41,19</td></tr>
      <tr><td>Tipo</td><td>PN N1</td><td>Data ult cot</td><td>27/10/2026</td></tr>
      <tr><td>Empresa</td><td>ITAUUNIBANCO PN N1</td><td>Min 52 sem</td><td>34,89</td></tr>
      <tr><td>Setor</td><td>Intermediários Financeiros</td><td>Max 52 sem</td><td>50,47</td></tr>
      <tr><td>Subsetor</td><td>Bancos</td><td>Vol $ med (2m)</td><td>1.586.020.000</td></tr>
    </table>
    <table>
      <tr><td>Valor de mercado</td><td>454.197.000.000</td><td>Ult balanco processado</td><td>30/06/2026</td></tr>
      <tr><td>Valor da firma</td><td>-</td><td>Nro. Acoes</td><td>11.026.900.000</td></tr>
    </table>
    <table>
      <tr><td>P/L</td><td>9,74</td><td>LPA</td><td>4,23</td></tr>
      <tr><td>P/VP</td><td>2,19</td><td>VPA</td><td>18,83</td></tr>
      <tr><td>Div. Yield</td><td>7,6%</td><td>ROE</td><td>22,4%</td></tr>
    </table>
    <table>
      <tr><td>Ativo</td><td>500.196.000.000</td><td>Depositos</td><td>106.222.000.000</td></tr>
      <tr><td>Cart. de Credito</td><td>172.877.000.000</td><td>Patrim. Liq</td><td>207.648.000.000</td></tr>
    </table>
    <table>
      <tr><th>Ultimos 12 meses</th><th></th><th>Ultimos 3 meses</th><th></th></tr>
      <tr><td>Result Int Financ</td><td>8.637.000.000</td><td>Result Int Financ</td><td>-25.000.000</td></tr>
      <tr><td>Rec Servicos</td><td>13.960.000.000</td><td>Rec Servicos</td><td>3.495.000.000</td></tr>
      <tr><td>Lucro Liquido</td><td>46.613.000.000</td><td>Lucro Liquido</td><td>11.551.000.000</td></tr>
    </table>
    """
    records = collector.parse({"ticker": "ITUB4", "route": "detalhes", "is_json": False, "html": sample_html})
    assert len(records) == 1
    r = records[0]
    assert r["header"]["is_instituicao_financeira"] is True
    assert r["header"]["cotacao"] == 41.19
    assert r["balanco"]["carteira_credito"] == 172877000000.0
    assert r["balanco"]["depositos"] == 106222000000.0
    assert r["dre_12m"]["resultado_intermediacao"] == 8637000000.0
    assert r["dre_3m"]["resultado_intermediacao"] == -25000000.0
    assert r["dre_12m"]["lucro_liquido"] == 46613000000.0
