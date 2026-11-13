"""Tipos do motor. Só identificadores do CRM, datas e status: nome, telefone e CPF ficam no CRM."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Optional


class Estado(str, Enum):
    SEM_HISTORICO = "sem_historico"
    EM_DIA = "em_dia"
    SILENCIADO = "silenciado"
    TARDIO = "tardio"
    PRIMEIRO_ATRASO = "primeiro_atraso"
    NAO_REAGE = "nao_reage"


ESTADO_ROTULO = {
    Estado.SEM_HISTORICO: "Sem histórico",
    Estado.EM_DIA: "Em dia",
    Estado.SILENCIADO: "Silenciado",
    Estado.TARDIO: "Tardio",
    Estado.PRIMEIRO_ATRASO: "Primeiro atraso",
    Estado.NAO_REAGE: "Não reage",
}


class Classe(str, Enum):
    """Como o aluno fechou um ciclo, pelo campo Dia 10."""
    EM_DIA = "em_dia"        # baixa até o corte do dia 10
    TARDIO = "tardio"        # baixa depois do corte, dentro do ciclo
    NAO_PAGOU = "nao_pagou"  # sem baixa no ciclo
    ABERTO = "aberto"        # ciclo corrente, ainda sem baixa


class Frente(str, Enum):
    DIA10 = "dia10"
    DIA25 = "dia25"
    RECUPERACAO = "recuperacao"


class Mensagem(str, Enum):
    LEMBRETE_DIA10 = "lembrete_dia10"
    AVISO_CALOURO = "aviso_calouro"
    RECONHECIMENTO = "reconhecimento"
    LEMBRETE_DIA25 = "lembrete_dia25"
    RECUPERACAO = "recuperacao"
    FIRME = "firme"


class Decisao(str, Enum):
    LIBERAR = "liberar"
    SEGURAR = "segurar"
    PAUSAR = "pausar"


@dataclass(frozen=True)
class Aluno:
    """Retrato de um aluno (contato do CRM) no ciclo corrente."""
    contact_id: str
    deal_id: str = ""
    situacao: str = "Em Curso"
    calouro: bool = False
    nivel: str = ""
    dia10_aberto: bool = False        # campo "Dia 10" = Sim (dado D-1)
    financeira_aberta: bool = False   # campo "Financeira" = Sim (dado D-1)
    tem_telefone: bool = True
    nivel_misto: bool = False         # contato com negócio em mais de um nível (fica fora)
    suspenso: bool = False            # reclamou neste ciclo: régua suspensa até o próximo
    etiquetas: frozenset = frozenset()  # etiquetas do contato no CRM (nomes sem acento, minúsculas)
    controle_etiquetado: bool = False   # controle do mês já etiquetado no CRM
    bloqueio: Optional[str] = None    # "erro_131026" | "recusou_mensagens" | "numero_errado"
    historico: tuple[Classe, ...] = ()  # ciclos fechados do semestre, do mais antigo ao mais recente
    estado: Estado = Estado.SEM_HISTORICO
    controle: bool = False
    pediu_silencio: bool = False
    declarou_pagamento_em: Optional[date] = None
    lembretes_sem_reacao: int = 0
    msgs_ciclo: int = 0
    frentes_ciclo: frozenset[Frente] = frozenset()
    mensagens_ciclo: frozenset[Mensagem] = frozenset()
    dias_com_mensagem: frozenset[date] = frozenset()

    @property
    def primeira_mensalidade(self) -> bool:
        """Calouro sem nenhum ciclo fechado: desconto até o dia 25, nunca o texto do dia 10."""
        return self.calouro and not self.historico


@dataclass(frozen=True)
class Fila:
    """Leitura de GET /api/painel/service?section=agora."""
    aguardando: int
    maior_espera_s: int
    atendentes_online: int
    lida_em: datetime
    sla_estourado: int = 0


@dataclass(frozen=True)
class LoteAtivo:
    """Último lote liberado, para a regra de vida mínima e de falha de envio."""
    lote_id: int
    liberado_em: datetime
    enviados: int = 0   # destinatários já processados (enviados, entregues, lidos ou com falha)
    falhas: int = 0
    fila_base: Optional[int] = None  # conversas aguardando quando o lote saiu


@dataclass(frozen=True)
class Experiencia:
    """Pedidos para parar e reclamações do dia, contra o que foi entregue."""
    casos: int = 0       # alunos distintos que pediram para parar ou reclamaram
    entregues: int = 0   # mensagens entregues no dia (ou enviadas, enquanto a entrega não é conhecida)

    @property
    def por_mil(self) -> float:
        return self.casos * 1000.0 / self.entregues if self.entregues > 0 else 0.0


@dataclass(frozen=True)
class Acao:
    """Uma linha do calendário do ciclo: qual mensagem sai hoje e para quais estados."""
    frente: Frente
    mensagem: Mensagem
    estados: frozenset[Estado]
    prioridade: int  # menor sai primeiro quando as frentes disputam a mesma fila


@dataclass
class Selecao:
    acao: Acao
    elegiveis: list[Aluno] = field(default_factory=list)
    exclusoes: dict[str, int] = field(default_factory=dict)
    fora_do_alvo: int = 0


@dataclass(frozen=True)
class Lote:
    ordem: int
    frente: Frente
    mensagem: Mensagem
    contact_ids: tuple[str, ...]
    variante: str = "A"

    @property
    def tamanho(self) -> int:
        return len(self.contact_ids)


@dataclass(frozen=True)
class Controle:
    """Chaves que a página do painel grava (tabela motor_controle)."""
    ligado: bool = False
    parada_geral: bool = False
    parada_motivo: str = ""
    atualizado_por: str = ""
