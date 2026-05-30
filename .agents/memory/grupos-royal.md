---
name: Grupos do Royal (chat IDs e papéis)
description: Os três grupos Telegram do bot e o papel de cada um — qual pontua, qual é teste, e o grupo-ponte da IA @Mira.
---

# Grupos do Royal

Três grupos Telegram com papéis distintos (informados pelo dono):

- **Grupo REAL / produção do jogo:** `-1002556760909` — ÚNICO grupo onde o jogo roda e
  PONTUA. Cuidado redobrado com estado/migração pra não perder progresso.
- **Grupo de TESTE:** `-1004225775299` — poucas pessoas testam só pra caçar erro; NADA
  pontua aqui (deve ficar fora do scoring, tipo `TEST_CHAT_IDS`).
- **Grupo PONTE IA (@Mira):** `-5204321141` — só o dono, o bot do jogo e a interface de
  IA "@Mira". Base do módulo novo **"inteligência royal"**.

**@Mira / inteligência royal (ainda em definição com o dono):** mensagens com prefixo
`@mira` solicitam a IA; resposta vem como reply à msg original.

⚠️ **Limitação do Telegram:** um bot NÃO recebe mensagens de OUTRO bot (mesmo sendo admin
com privacy off). Então se a @Mira for um bot, o jogo NÃO consegue ler as mensagens dela
via Telegram — só as do dono (humano). Isso contradiz a suposição "tudo ele pode ler mesmo
que seja msg de bot". Confirmar arquitetura antes de implementar.

⚠️ **Regra do dono (custo zero):** IA não pode usar serviço pago. Se o jogo for quem chama
a IA pra responder como Mira, precisa de um backend de IA gratuito (ou a @Mira é um
bot/serviço externo separado que o dono já mantém).

⚠️ `-5204321141` não tem o prefixo `-100` dos supergrupos — provável grupo básico (legado).
Se virar supergrupo, o chat_id muda (migração já tratada por `on_chat_migration`).
