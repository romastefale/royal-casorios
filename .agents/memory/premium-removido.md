---
name: Premium removido — o bot nunca cobra nada
description: Decisão do dono de remover todo o Premium/cobrança; o que ficou inerte no DB e o que não reintroduzir.
---

# Premium removido por completo

O dono decidiu (escolha explícita) **remover todo o sistema Premium** do bot: loja Premium,
pagamento via **Telegram Stars (XTR)**, assinatura **Royal Plus** e todos os perks pagos.

**Regra durável:** o bot **NUNCA cobra nada do usuário** — nem mesmo Telegram Stars (que antes era
a única exceção da regra de custo-zero). A única economia é interna: **florins 🪙** (ganhos no jogo,
gastos na loja in-game Poção/Anel e em `/royalpresentear`).

**Why:** o dono não quer monetização de nenhum tipo no bot. Reintroduzir cobrança (Stars, gateway,
assinatura) contraria uma decisão de produto explícita.

**How to apply:** ao mexer em loja, XP, cards ou comandos, não reintroduzir nenhum fluxo de
pagamento/invoice/perk pago. Manter florins como única moeda.

## DB inerte — NÃO dropar (sem perda de dados)
As migrations **v10/v11** foram mantidas de propósito; as colunas/tabela existem mas **nenhum código
lê/escreve nelas** (marcadas LEGADO/INERTE nos docstrings): `xp_boost_until`, `prm_hints`,
`prm_ressurrects`, `prm_skin_gold`, `royal_plus_until`, `royal_plus_charge_id`, tabela
`stars_purchases`. Não criar migration de DROP — preserva histórico e evita risco.
