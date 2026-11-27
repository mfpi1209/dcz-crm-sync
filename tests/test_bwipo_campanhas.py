"""Origem do lead (Meta / Google / Sem Campanha) a partir do contato do Bwipo e corte de 01/10/2026."""

from datetime import date

from services.bwipo_campanhas import (
    campos_do_contato,
    nome_campanha,
    origem,
    periodo_bwipo,
    periodo_legado,
)


def _a(**kw):
    base = {"coletado": True, "erro": None}
    base.update(campos_do_contato({}))
    base.update(kw)
    return base


def test_ctwa_do_whatsapp_e_meta():
    contato = {
        "adCtwaClid": "Afje05", "adSourceType": "ad", "adUtmSource": "Meta",
        "adUtmMedium": "whatsapp_ads", "adUtmCampaign": "cs.pos.whats.bf1126.geral",
    }
    a = _a(**campos_do_contato(contato))
    assert origem(a) == "meta"
    assert nome_campanha(a) == "cs.pos.whats.bf1126.geral"


def test_anuncio_sem_ctwa_mas_com_source_type_ad_e_meta():
    assert origem(_a(ad_source_type="ad", ad_source_id="1202")) == "meta"


def test_utm_source_facebook_ou_instagram_e_meta():
    for src in ("facebook", "Instagram", "fb", "ig", "meta_ads"):
        assert origem(_a(utm_source=src)) == "meta", src


def test_form_google_nao_e_meta_nem_sem_campanha():
    assert origem(_a(utm_source="google_cruzeiroead", gclid="EAIa")) == "google"
    assert origem(_a(gclid="EAIa")) == "google"


def test_fbclid_sozinho_e_meta():
    assert origem(_a(fbclid="IwAR")) == "meta"


def test_outra_fonte_nao_entra_em_sem_campanha():
    assert origem(_a(utm_source="chatgpt.com")) == "outra"


def test_sem_nada_e_sem_campanha():
    assert origem(_a(contact_source="Comercial")) == "sem"


def test_nao_coletado_e_pendente_e_erro_sem_dados_e_erro():
    assert origem(None) == "pendente"
    assert origem({"coletado": False}) == "pendente"
    assert origem(_a(erro="404: not found")) == "erro"


def test_campanha_cai_no_nome_resolvido_e_depois_sem_utm():
    assert nome_campanha(_a(ad_campaign_name="cs.grad.whats.saude.0826")) == "cs.grad.whats.saude.0826"
    assert nome_campanha(_a(utm_campaign="x,x")) == "x"
    assert nome_campanha(_a()) == "(sem utm_campaign)"


def test_corte_em_01_10():
    assert periodo_legado(date(2026, 9, 20), date(2026, 10, 5)) == (date(2026, 9, 20), date(2026, 9, 30))
    assert periodo_bwipo(date(2026, 9, 20), date(2026, 10, 5)) == (date(2026, 10, 1), date(2026, 10, 5))
    assert periodo_legado(date(2026, 10, 1), date(2026, 10, 5)) is None
    assert periodo_bwipo(date(2026, 9, 1), date(2026, 9, 30)) is None
