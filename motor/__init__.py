"""Motor de Ativações — avisos de pagamento comandados de fora do CRM.

O motor decide quem recebe, quando e em que ritmo, e executa pela API pública
do CRM acadêmico. Sobe desligado (`MOTOR_ENABLED=0`) e em simulação
(`MOTOR_DRY_RUN=1`): em simulação monta os lotes, registra o que faria e não
chama nenhum endpoint que escreve no CRM.

Camadas:
  trilhos     limites fixos em código (janela, tetos, exclusões, controle, parada)
  governador  fórmula de liberar / segurar / pausar pela fila de atendimento
  piloto      escolhe frente e tamanho dentro dos trilhos (fórmula ou agente)
  razao       livro-razão por aluno e ciclo (tabelas motor_ no dcz_sync)
  respostas   webhook de respostas do CRM, classificadas pelo Jev com texto mascarado
  envio       envio real: uma campanha por dia sobre a etiqueta do dia, dosada por
              pausar e retomar (travado por quatro chaves)
  worker      laço em processo próprio (`python -m motor.worker`), com trava no banco

Decisões e operação: ver AGENTS.md, entradas "Motor de Ativações".
"""

VERSAO = "0.2.0"
