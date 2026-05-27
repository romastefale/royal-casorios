"""Banco de palavras PT-BR para a Palavra da Hora.

Critérios:
- Apenas substantivos, verbos e adjetivos comuns
- Entre 4 e 8 letras
- Sem temas pesados ou ofensivos
- Sem hifens, sem espacos, sem digitos

A normalizacao (lowercase, sem acento) e feita em runtime.
"""

PALAVRAS: list[str] = [
    # Animais
    "gato", "pato", "urso", "sapo", "lobo", "rato", "foca", "leao",
    "peixe", "tigre", "pomba", "zebra", "panda", "coelho", "cavalo",
    "raposa", "abelha", "coruja", "formiga", "borboleta", "macaco",
    "lagarto", "cobra", "tartaruga", "girafa", "elefante", "dragao",
    "pinguim", "papagaio", "tucano", "polvo", "lagosta",

    # Natureza
    "arvore", "flor", "folha", "raiz", "campo", "monte", "serra",
    "vale", "praia", "deserto", "floresta", "nuvem", "chuva", "neve",
    "vento", "terra", "fogo", "agua", "rocha", "pedra", "areia",
    "gelo", "estrela", "planta", "jardim", "ilha", "brisa",

    # Comida e bebida
    "arroz", "feijao", "leite", "queijo", "carne", "frango", "batata",
    "cebola", "alho", "pimenta", "acucar", "suco", "vinho", "cafe",
    "bolo", "doce", "fruta", "banana", "laranja", "manga", "melao",
    "uva", "morango", "abacaxi", "limao", "tomate", "azeite", "milho",

    # Casa
    "mesa", "cadeira", "cama", "porta", "janela", "parede", "telhado",
    "casa", "sala", "cozinha", "quarto", "banho", "lampada", "escada",
    "sofa", "tapete", "espelho", "vaso", "livro", "prato", "copo",
    "faca", "colher", "panela", "garfo",

    # Corpo
    "cabeca", "olho", "nariz", "boca", "orelha", "perna", "dedo",
    "dente", "lingua", "cabelo", "pele", "peito", "costas", "joelho",
    "ombro", "punho", "calcanhar",

    # Tempo / lugares
    "noite", "semana", "manha", "tarde", "hora", "ontem", "hoje",
    "amanha", "cidade", "vila", "escola", "hospital", "igreja",
    "mercado", "loja", "parque", "praca", "ponte", "estrada", "caminho",

    # Verbos
    "amar", "falar", "ouvir", "comer", "beber", "andar", "pular",
    "dancar", "cantar", "chorar", "pensar", "sonhar", "viver",
    "correr", "dormir", "ler", "escrever",

    # Adjetivos
    "bonito", "grande", "alto", "baixo", "quente", "frio", "novo",
    "velho", "forte", "fraco", "rapido", "lento", "rico", "calmo",
    "claro", "doce", "amargo", "sabio", "livre", "macio",

    # Cores
    "verde", "azul", "preto", "branco", "cinza", "roxo", "rosa",
    "marrom", "vermelho", "amarelo",

    # Tema reino (toque temático sutil)
    "coroa", "trono", "rei", "rainha", "principe", "espada", "escudo",
    "castelo", "torre", "reino", "nobre", "magico", "cavaleiro",
    "guerreiro", "festa", "ouro", "prata", "joia",
]


CHARADAS: list[tuple[str, str]] = [
    ("Aquece o dia e ilumina o céu", "sol"),
    ("Brilha à noite no firmamento", "lua"),
    ("Cai das nuvens em tempestades", "chuva"),
    ("Sopra forte e move as folhas", "vento"),
    ("Mata a sede de qualquer um", "agua"),
    ("Queima, ilumina e esquenta", "fogo"),
    ("Cresce alto e oferece sombra", "arvore"),
    ("Tem pétalas e perfume", "flor"),
    ("Voa de flor em flor e faz mel", "abelha"),
    ("Late, abana o rabo e ronca", "cachorro"),
    ("Mia e tem sete vidas", "gato"),
    ("Rei da selva com juba dourada", "leao"),
    ("Listras pretas em pelagem branca", "zebra"),
    ("Carrega cavaleiros no lombo", "cavalo"),
    ("Móvel onde se dorme à noite", "cama"),
    ("Onde se senta para comer à mesa", "cadeira"),
    ("Cômodo onde se prepara a comida", "cozinha"),
    ("Veste a cabeça de um rei", "coroa"),
    ("Assento real do monarca", "trono"),
    ("Cidade pequena do interior", "vila"),
    ("Construída para atravessar rios", "ponte"),
    ("Local onde se vai às compras", "loja"),
    ("Cai do céu no inverno, branca e fria", "neve"),
    ("Estação do ano mais quente", "verao"),
    ("Período entre o pôr do sol e madrugada", "noite"),
    ("Refeição da parte da manhã", "manha"),
    ("Arma branca usada por cavaleiros", "espada"),
    ("Proteção que se carrega no braço", "escudo"),
    ("Edificação fortificada com torres", "castelo"),
    ("Coberta com folhas, faz sombra", "arvore"),
    ("Ato de soltar a voz para cantar", "cantar"),
    ("Move as pernas com pressa", "correr"),
    ("Movimento dos pés ao ritmo da música", "dancar"),
    ("Pequeno animal que tece teia", "aranha"),
    ("Metal precioso, brilha amarelo", "ouro"),
]
