# Correção de confiabilidade — 31/10/2026

Escopo implementado: retomada de conexão Neon, fila de concorrência Actions,
remoção de dependência da CI no caminho operacional, centralização das agendas,
ocorrências duráveis, catch-up, reservas atômicas, renovação de lease, retry
limitado, rejeição de sucesso incompleto e confirmação após testes Gold.

`runtime_cursor`, `runtime_job` e `runtime_heartbeat` são estruturas novas,
aditivas e separadas da fila legada. Isso permite transferir a agenda por perfil
sem fazer workers legados consumirem contratos incompatíveis. Cada job congela
o universo/versão de contrato; mudança desse universo exige revisão explícita.
As referências de batch usam a ocorrência original, mesmo após meia-noite.

O cron da manutenção foi removido; a engine passa a acordar a cada 15 minutos e
reconstruir todos os horários faltantes desde o cursor. A primeira inicialização
começa à meia-noite BRT do dia do deploy. O cron mantém a limitação de atraso do
GitHub: a correção garante recuperação durável, não horário exato do runner.

O disparador Cloudflare gratuito foi preparado em fin-infra e testado com
simulações de entrega perdida e dispatch duplicado. Sua configuração no painel
depende de autenticação humana quando houver CAPTCHA; código preparado não é
evidência de Worker implantado.

A skill `skills/fin-ingestion-ops` reúne o procedimento de configuração,
publicação, promoção da release e comprovação da recuperação, sem credenciais.

O plano abrangente anterior continua como roadmap para expansão multiativo,
cadastro canônico, manifests globais e fontes que ainda requerem contratos/chaves.
As declarações de “somente local” naquele documento descrevem a primeira etapa;
o estado final de publicação deve ser conferido nas evidências desta release.

Validação local: 188 testes passaram sem ignorados na suíte completa com
PostgreSQL 16.2 e Redis isolados. Após acrescentar cobertura para Retry-After e
esgotamento de tentativas após crash, os nove testes do runtime passaram novamente.
Os quatro testes do disparador Cloudflare também passaram; Ruff e sintaxe
YAML/shell aprovados. A migration SQL passou no validador schema-only de fin-db.
