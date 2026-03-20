"""
profanity_wordlist.py — Multilingual profanity, slur, scam, and discrimination word list.

Organised by market/language. Used by the quality scorer to flag inappropriate job listings.

All non-Latin-script languages are represented via romanisation/transliteration so that
simple string matching works without Unicode-aware tokenisation. Where a term appears in
multiple markets it is kept in the most specific key rather than duplicated into "all".

Categories covered per market:
  - General profanity / obscenities
  - Genitalia references used as insults
  - Racial / ethnic slurs common in that region
  - Sexist / body-shaming terms
  - Scam / MLM red-flag phrases

Usage:
    from app.utils.profanity_wordlist import PROFANITY_WORDLIST
    terms = PROFANITY_WORDLIST["all"] + PROFANITY_WORDLIST.get("AU", [])
"""

PROFANITY_WORDLIST: dict[str, list[str]] = {

    # ──────────────────────────────────────────────────────────────────────────
    # ALL — applies to every market (English-language universals + scam/MLM terms)
    # ──────────────────────────────────────────────────────────────────────────
    "all": [
        # ── Core English profanity ────────────────────────────────────────────
        "fuck",
        "fucking",
        "fucked",
        "fucker",
        "fucks",
        "f*ck",
        "f**k",
        "fuk",
        "fuq",
        "fck",
        "shit",
        "shitting",
        "shitty",
        "shite",
        "sheit",
        "sh1t",
        "sh!t",
        "cunt",
        "c*nt",
        "c**t",
        "kunt",
        "asshole",
        "arsehole",
        "arse",
        "ass",
        "asses",
        "bastard",
        "bitch",
        "bitches",
        "bitchy",
        "b*tch",
        "dick",
        "dickhead",
        "cock",
        "cocksucker",
        "cock sucker",
        "motherfucker",
        "motherfucking",
        "mf",
        "pussy",
        "pussies",
        "whore",
        "whores",
        "slut",
        "sluts",
        "slutty",
        "prick",
        "wanker",
        "wank",
        "twat",
        "tit",
        "tits",
        "boobs",
        "bollocks",
        "bullshit",
        "horseshit",
        "crap",
        "damn",
        "damned",
        "hell",
        "idiot",
        "moron",
        "retard",
        "retarded",
        "spastic",
        "imbecile",
        "dumbass",
        "dumb ass",
        "jackass",
        "jerk",
        "creep",
        "pervert",
        "perv",
        "pedophile",
        "paedophile",
        "nazi",
        "faggot",
        "fag",
        "dyke",
        "homo",
        "tranny",
        "shemale",
        "nigger",
        "nigga",
        "n-word",
        "chink",
        "gook",
        "spook",
        "kike",
        "spic",
        "wetback",
        "cracker",
        "honky",
        "whitey",
        "towelhead",
        "raghead",
        "sandnigger",
        "sand nigger",
        "terrorist",
        "jihadi",

        # ── Scam / MLM red-flag phrases ───────────────────────────────────────
        "get rich quick",
        "get rich fast",
        "make money fast",
        "make money online",
        "earn from home",
        "work from home earn",
        "unlimited income",
        "unlimited earning potential",
        "unlimited earnings",
        "passive income opportunity",
        "be your own boss",
        "boss babe",
        "girl boss",
        "financial freedom",
        "financial independence opportunity",
        "six figure income",
        "six-figure income",
        "seven figure income",
        "residual income",
        "mlm",
        "multi level marketing",
        "multi-level marketing",
        "multilevel marketing",
        "pyramid scheme",
        "pyramid selling",
        "network marketing",
        "direct sales opportunity",
        "direct selling",
        "independent distributor",
        "brand ambassador opportunity",
        "recruitment fee",
        "processing fee",
        "training fee",
        "starter kit fee",
        "starter kit purchase",
        "buy your own stock",
        "purchase inventory",
        "no experience required earn",
        "no experience needed earn",
        "earn while you sleep",
        "earn while sleeping",
        "easy money",
        "quick money",
        "guaranteed income",
        "guaranteed salary",
        "guaranteed earnings",
        "guaranteed profit",
        "wire transfer required",
        "send money first",
        "advance fee",
        "investment required",
        "upfront payment",
        "upfront investment",
        "limited spots available",
        "limited opportunity",
        "once in a lifetime opportunity",
        "exclusive opportunity",
        "ground floor opportunity",
        "join our team earn",
        "build your own team",
        "recruit your friends",
        "recruit family members",
        "downline",
        "upline",
        "commission only",
        "100% commission",
        "performance only pay",
        "no base salary",
        "send cv to gmail",
        "send cv to yahoo",
        "send cv to hotmail",
        "whatsapp to apply",
        "apply via whatsapp",
        "no interview required",
        "job guaranteed",
        "immediate start guaranteed",
        "work your own hours earn",
        "set your own hours",
        "be your own manager",

        # ── Discriminatory job ad language (English) ──────────────────────────
        "females only",
        "males only",
        "female only",
        "male only",
        "men only",
        "women only",
        "ladies only",
        "gentlemen only",
        "no women",
        "no men",
        "family man preferred",
        "single applicants only",
        "must be single",
        "age limit",
        "age requirement",
        "must be under 30",
        "must be under 25",
        "must be under 35",
        "chinese preferred",
        "malay preferred",
        "indian preferred",
        "caucasian preferred",
        "asian preferred",
        "local only",
        "locals only",
        "no foreigners",
        "citizens only",
        "nationals only",
        "christian only",
        "muslim only",
        "must be christian",
        "must be muslim",
        "must be religious",
        "good looking",
        "good-looking",
        "presentable appearance",
        "attractive appearance",
        "must be slim",
        "weight requirement",
        "height requirement",
        "minimum height",
        "fair skin",
        "fair complexion required",
    ],

    # ──────────────────────────────────────────────────────────────────────────
    # AU — Australia (English; AU-specific slang + racial slurs used in AU)
    # ──────────────────────────────────────────────────────────────────────────
    "AU": [
        # AU slang profanity
        "ya cunt",
        "ya fuckin",
        "deadshit",
        "dead shit",
        "shitkicker",
        "shit kicker",
        "shitcan",
        "shit-can",
        "bugger off",
        "piss off",
        "get rooted",
        "get stuffed",
        "rack off",
        "drongo",
        "muppet",
        "dropkick",
        "drop kick",
        "gronk",
        "dog act",
        "mongrel",
        "sook",
        "weak as piss",

        # Racial slurs used in AU context
        "abo",          # highly offensive slur for Aboriginal Australians
        "boong",        # highly offensive slur for Aboriginal Australians
        "coon",         # deeply offensive racial slur
        "gin",          # offensive term for Aboriginal woman
        "lubra",        # offensive term for Aboriginal woman
        "myall",        # offensive term for Aboriginal person
        "jacky jacky",  # derogatory term for Aboriginal person
        "slope",        # anti-Asian slur
        "slant eye",
        "slant-eye",
        "wog",          # anti-Southern European/Middle Eastern slur in AU
        "curry muncher",
        "curry-muncher",
        "fob",          # offensive for Pacific Islander migrants ("fresh off the boat")
        "skip",         # sometimes used pejoratively for Anglo-Australians
        "lebbo",        # derogatory for Lebanese Australians
        "wuss",
        "poofter",      # derogatory for gay men (AU specific usage)
        "poofta",
    ],

    # ──────────────────────────────────────────────────────────────────────────
    # NZ — New Zealand (English + Māori context; NZ-specific slurs)
    # ──────────────────────────────────────────────────────────────────────────
    "NZ": [
        # NZ slang profanity
        "bugger",
        "hori",         # offensive slur for Māori people
        "boonga",       # offensive slur for Māori/Pacific Islanders in NZ
        "fob",          # derogatory for Pacific Island migrants
        "coconut",      # derogatory for Pacific Islanders
        "pakeha",       # not always offensive but can be used derogatorily
        "bro culture",
        "porangi",      # "crazy" in Māori, used as insult
        "pokokohua",    # Māori profanity — "boil your head"

        # Racial slurs circulating in NZ
        "slope",
        "slant",
        "chink",
        "curry",
        "desi",         # not always offensive but used derogatorily
        "fijian",       # sometimes used as a slur in NZ context
        "islander",     # used pejoratively
        "piss off",
        "get stuffed",
        "ya muppet",
    ],

    # ──────────────────────────────────────────────────────────────────────────
    # SG — Singapore
    # Languages: English (Singlish), Hokkien, Mandarin, Malay, Tamil
    # ──────────────────────────────────────────────────────────────────────────
    "SG": [
        # ── Singlish / English profanity ─────────────────────────────────────
        "talk cock",
        "lan jiao",     # Hokkien: penis
        "lanjiao",
        "kan ni na",    # Hokkien: fuck your mother (very offensive)
        "kanina",
        "kannina",
        "knn",          # abbreviation of above
        "knns",
        "cb",           # abbreviation of chee bye
        "chee bye",     # Hokkien: vagina (very offensive)
        "chibai",
        "chi bai",
        "chi bye",
        "knn cb",
        "puki",         # Malay: vagina (see also MY)
        "pukimak",      # Malay: your mother's vagina (very offensive)
        "bodoh",        # Malay: stupid
        "babi",         # Malay: pig (offensive in Muslim context)
        "celaka",       # Malay: damn/cursed
        "sial",         # Malay: damn/cursed

        # ── Hokkien profanity (used heavily in SG) ───────────────────────────
        "kan",          # Hokkien: fuck
        "diu",          # Cantonese/Hokkien overlap: fuck
        "pua chao chee bye",  # very offensive compound
        "gao lan",      # Hokkien: penis
        "siao",         # Hokkien: crazy/mad (mild)
        "siao eh",
        "jialat",       # Hokkien: terrible/severe (mild)
        "sibeh",        # Hokkien: very (literally "die father")
        "tiko",         # Hokkien: pervert/dirty old man
        "ti ko pek",
        "tikopek",
        "zao geng",     # Hokkien: to expose oneself deliberately
        "lanciao",

        # ── Mandarin profanity (romanised, SG usage) ─────────────────────────
        "cao ni ma",    # Mandarin: fuck your mother
        "ta ma de",     # Mandarin: his mother's (expletive)
        "sha bi",       # Mandarin: stupid cunt
        "shabi",
        "gun dan",      # Mandarin: get lost / screw off
        "gun ni de dan",
        "cao",          # Mandarin: fuck
        "wo cao",
        "sha gua",      # Mandarin: stupid melon / idiot
        "ben dan",      # Mandarin: stupid egg / idiot
        "hundan",       # Mandarin: bastard
        "hun dan",
        "wang ba dan",  # Mandarin: son of a turtle / bastard
        "wang ba",
        "zhu ba jie",   # used as insult (pig)
        "diao",         # Mandarin: penis (vulgar)
        "ji ba",        # Mandarin: penis (vulgar)
        "jiba",
        "biao zi",      # Mandarin: prostitute
        "biaozi",
        "po fu",        # Mandarin: bitch/shrew

        # ── Tamil profanity (romanised, SG/MY usage) ─────────────────────────
        "otha",         # Tamil: fuck / sexual act (very offensive)
        "otta",
        "thevdiya",     # Tamil: prostitute / bitch
        "thevudiya",
        "thayoli",      # Tamil: son of a prostitute (very offensive)
        "punda",        # Tamil: vagina / fuck (very offensive)
        "pundai",
        "sunni",        # Tamil: penis (vulgar)
        "soothu",       # Tamil: arse
        "loosu",        # Tamil: crazy/idiot
        "mayiru",       # Tamil: pubic hair (offensive)
        "koodhi",       # Tamil: vagina (offensive)
        "naaye",        # Tamil: dog (offensive)
        "naye",
        "baadu",        # Tamil: prostitute
        "gomma",        # Tamil: idiot / fool

        # ── Racial slurs used in SG ───────────────────────────────────────────
        "cina",         # derogatory for Chinese in Malay
        "ah beng",      # derogatory for uncultured Chinese male
        "ah lian",      # derogatory for uncultured Chinese female
        "ah tiong",     # derogatory for mainland Chinese
        "prc",          # used pejoratively for People's Republic of China nationals
        "ang moh",      # not always offensive but used derogatorily for Caucasians
        "mat",          # derogatory abbreviation for Malay men
        "minah",        # derogatory for Malay women
        "keling",       # highly offensive slur for Indians/Tamils in SG/MY
        "negro",
        "darkie",
    ],

    # ──────────────────────────────────────────────────────────────────────────
    # MY — Malaysia
    # Languages: Malay (Bahasa Malaysia), English, Mandarin (similar to SG)
    # ──────────────────────────────────────────────────────────────────────────
    "MY": [
        # ── Malay profanity ───────────────────────────────────────────────────
        "puki",         # vagina (highly offensive)
        "pukimak",      # your mother's vagina (very offensive)
        "pukimak kau",
        "babi",         # pig (very offensive in Muslim-majority context)
        "celaka",       # damn/cursed
        "celakak",
        "sial",         # damn/accursed
        "haramjadah",   # bastard / illegitimate
        "haram jadah",
        "anjing",       # dog (offensive)
        "bangsat",      # scoundrel / bastard (very offensive)
        "bajingan",     # scoundrel / bastard
        "bedebah",      # damn / wretch
        "kimak",        # shortened pukimak (very offensive)
        "lancau",       # penis (vulgar)
        "pantat",       # buttocks/vagina (offensive)
        "bontot",       # arse (offensive)
        "bodoh",        # stupid
        "bodoh sombong",# arrogantly stupid
        "gila",         # crazy/mad
        "tolol",        # stupid/idiot
        "bahlol",       # idiot/fool
        "dungu",        # stupid
        "kepala bapak kau",  # your father's head (offensive)
        "kepala hotak kau",  # your father's brain (offensive)
        "mampus",       # drop dead / die
        "pergi mampus",
        "syaitan",      # devil/satan
        "iblis",        # devil
        "biadab",       # rude/uncouth
        "kurang ajar",  # ill-mannered (strong insult)
        "tak malu",     # shameless

        # ── Mandarin profanity (MY Chinese community, same as SG) ────────────
        "cao ni ma",
        "sha bi",
        "shabi",
        "gun dan",
        "hun dan",
        "wang ba dan",
        "ji ba",
        "jiba",
        "biao zi",
        "sha gua",
        "ben dan",

        # ── Racial slurs used in MY ───────────────────────────────────────────
        "keling",       # highly offensive slur for Indians
        "cina babi",    # extremely offensive (Chinese pig)
        "cina",         # derogatory for Chinese in some contexts
        "ah kow",       # derogatory for Chinese
        "indon",        # derogatory for Indonesians
        "bangla",       # derogatory for Bangladeshi migrants
        "mamak",        # sometimes used derogatorily for Indian-Muslim
        "pariah",       # caste-based slur (used as general insult)
        "negro",
        "hitam",        # "black" used as racial slur
    ],

    # ──────────────────────────────────────────────────────────────────────────
    # HK — Hong Kong
    # Languages: Cantonese (primary), English
    # ──────────────────────────────────────────────────────────────────────────
    "HK": [
        # ── The "Outstanding Five" Cantonese profanities ──────────────────────
        "diu",          # 屌 — fuck (most common)
        "gau",          # 㞗 — penis
        "lan",          # 𡳞 — penis
        "tsat",         # 柒 — penis (also means idiot)
        "hai",          # 閪 — vagina / cunt

        # ── Common Cantonese compound insults ─────────────────────────────────
        "diu nei",      # fuck you
        "diu nei lo mo",# fuck your mother (very offensive)
        "diu lei",
        "diu lei lo mo",
        "puk gai",      # 仆街 — drop dead / go die on the street
        "puk kai",
        "ham ka chan",  # 冚家鏟 — damn your whole family
        "ham ka ling",  # variant
        "on9",          # internet slang: idiot (from 戇鳩)
        "on gau",       # idiot
        "lun yeung",    # penis face / idiot
        "tiu",          # variant of diu
        "sei gau",      # dead dog / asshole
        "sei lo",       # dead old person (insult)
        "gao lan",      # dog penis (offensive)
        "chau hai",     # smelly vagina
        "ham sup",      # pervert / lecherous
        "ham sup lo",   # pervert (male)
        "sei ban",      # idiot
        "bak chi",      # idiot / crazy
        "sot hai",      # crazy cunt (offensive)
        "chat",         # penis (slang)
        "baan",         # idiot / bastard
        "sek si",       # offensive compound

        # ── Racial slurs used in HK ───────────────────────────────────────────
        "gweilo",       # 鬼佬 — foreigner/Caucasian (can be offensive)
        "gwai mui",     # Caucasian woman (can be offensive)
        "cha siu",      # BBQ pork — used as slur against mainland Chinese
        "ah chan",      # derogatory for mainland Chinese
        "dai luk zai",  # mainlander boy (derogatory)
        "ah mao",       # offensive for Southeast Asians / Filipinos
        "fei chai",     # fatty (body shaming)
        "hak gwai",     # black ghost — anti-Black slur
        "hei guai",     # variant
        "naan mui",     # Indian girl (derogatory)
    ],

    # ──────────────────────────────────────────────────────────────────────────
    # PH — Philippines
    # Languages: Filipino/Tagalog (primary), English
    # ──────────────────────────────────────────────────────────────────────────
    "PH": [
        # ── Core Tagalog profanity ────────────────────────────────────────────
        "putang ina",   # your mother is a whore (very offensive)
        "putangina",
        "puta",         # whore/bitch
        "putang ina mo",# directed: your mother is a whore
        "tang ina",     # shortened version (still very offensive)
        "tangina",
        "tang ina mo",
        "gago",         # stupid/fool (most common insult)
        "gaga",         # female form
        "tanga",        # stupid/dumb
        "bobo",         # stupid/dumb (common)
        "boba",         # female form
        "ulol",         # idiot/crazy
        "buang",        # crazy (Visayan usage, common in Tagalog now)
        "tarantado",    # idiot/incompetent
        "tarantada",    # female form
        "leche",        # damn/milk (used as expletive from Spanish "leche")
        "lintik",       # lightning strike / damn
        "diyablo",      # devil
        "sira ulo",     # crazy (literally "broken head")
        "siraulo",
        "hinayupak",    # stupid/fool (from "hayop" animal)
        "hayop ka",     # you animal
        "animal ka",
        "punyeta",      # expletive / asshole (from Spanish "puñeta")
        "salot",        # pest/plague (used as insult)
        "buwisit",      # damn/nuisance
        "pakyu",        # fuck you (Filipino phonetic)
        "pak yu",
        "kantot",       # fuck (sexual act, very vulgar)
        "kantotin",
        "jakol",        # masturbation (very vulgar)
        "jakulin",
        "hindot",       # sexual act (vulgar)
        "hindutan",
        "pekpek",       # vagina (vulgar)
        "titi",         # penis (vulgar)
        "etits",        # penis (child/slang form, still vulgar in adult context)
        "bayag",        # testicles (vulgar)
        "pwet",         # buttocks/arse (mild-moderate)
        "pwetan",
        "bilat",        # vagina (Bisaya/Visayan, used in PH)
        "boto",         # penis (Visayan, used broadly)

        # ── PH scam/MLM terms ─────────────────────────────────────────────────
        "negosyo",      # "business" — often used in scam job ads
        "easy money",
        "walang puhunan",# no capital needed (scam signal)
        "kumita agad",  # earn immediately
        "online selling",

        # ── Racial/discriminatory slurs in PH ────────────────────────────────
        "nognog",       # anti-dark skin slur
        "negro",
        "ni*gger",
        "intsik",       # derogatory for Chinese Filipinos
        "bumbay",       # derogatory for Indian-Filipinos (from Mumbai)
        "hapon",        # Japanese (sometimes used derogatorily)
        "kano",         # American (sometimes derogatory)
        "tisoy",        # mixed-race (can be used derogatorily)
        "baduy",        # unfashionable/provincial (classist)
        "probinsyano",  # provincial person (used classistly)
        "jologs",       # lower-class person (classist)
        "squatter",     # derogatory for informal settlers
        "bakla",        # gay man (derogatory when used as insult)
        "tomboy",       # lesbian (derogatory when used as insult)
    ],

    # ──────────────────────────────────────────────────────────────────────────
    # ID — Indonesia
    # Language: Bahasa Indonesia (primary)
    # ──────────────────────────────────────────────────────────────────────────
    "ID": [
        # ── Core Indonesian profanity ─────────────────────────────────────────
        "bangsat",      # scoundrel / bastard (very offensive)
        "bajingan",     # scoundrel / bastard
        "anjing",       # dog (very offensive)
        "babi",         # pig (very offensive in Muslim context)
        "keparat",      # damn / scoundrel
        "bedebah",      # damn / wretch
        "sialan",       # damn / cursed
        "sial",         # damn
        "brengsek",     # jerk / asshole
        "ngentot",      # fuck (very vulgar)
        "entot",        # fuck (variant)
        "jancok",       # Javanese expletive — very offensive (also: jancuk)
        "jancuk",
        "dancok",
        "dancuk",
        "kontol",       # penis (very vulgar)
        "peler",        # penis (vulgar)
        "memek",        # vagina (very vulgar)
        "pepek",        # vagina (vulgar)
        "toket",        # breasts (vulgar)
        "pantat",       # arse / buttocks (offensive)
        "bokong",       # arse (mild-moderate)
        "goblok",       # stupid/idiot
        "bego",         # stupid/idiot (milder)
        "tolol",        # stupid/idiot
        "idiot",
        "bodoh",        # stupid
        "dungu",        # stupid/dumb
        "mampus",       # drop dead
        "tai",          # shit
        "taik",         # shit (variant)
        "brengsek",     # jerk
        "kampret",      # bat (used as insult, from Javanese)
        "kampungan",    # uncultured / village-like (derogatory)
        "kurang ajar",  # ill-mannered (strong insult)
        "kepala batu",  # stubborn (literally "stone head")
        "monyet",       # monkey (offensive)
        "ketek",        # armpit / monkey (offensive)
        "asu",          # Javanese: dog (offensive)
        "celeng",       # Javanese: wild boar (offensive)
        "jembut",       # pubic hair (vulgar)
        "perek",        # promiscuous woman / easy woman
        "jablay",       # slut / promiscuous
        "lonte",        # prostitute / slut
        "pelacur",      # prostitute
        "lacur",        # prostitute
        "bispak",       # slang: sexually available (from "bisa dipakai")
        "cibai",        # vagina (Chinese-Indonesian slang, from Hokkien)

        # ── Racial slurs in ID context ────────────────────────────────────────
        "cina",         # offensive for Chinese Indonesians
        "cino",         # variant
        "china babi",   # highly offensive compound
        "keling",       # offensive for South Asians
        "negro",
        "bule",         # foreigner/Caucasian (can be derogatory)
        "inlander",     # colonial-era slur for indigenous Indonesians
        "pribumi",      # indigenous (sometimes used as slur against non-indigenous)
        "kafir",        # infidel (used as insult outside religious context)

        # ── ID scam/MLM terms ─────────────────────────────────────────────────
        "kerja sampingan",  # side job (common in ID scam ads)
        "kerja online",     # online work (often scam signal)
        "penghasilan tambahan",  # extra income
        "bisnis online",    # online business (common MLM signal)
        "reseller",         # often MLM signal in ID context
        "dropshipper",
        "tanpa modal",      # no capital (scam signal)
        "modal kecil",      # small capital (scam signal)
        "untung besar",     # big profit (scam signal)
        "rekrut downline",  # recruit downline
    ],

    # ──────────────────────────────────────────────────────────────────────────
    # TH — Thailand
    # Language: Thai (romanised)
    # ──────────────────────────────────────────────────────────────────────────
    "TH": [
        # ── Core Thai profanity (romanised) ──────────────────────────────────
        "hia",          # เหี้ย — monitor lizard (offensive, like "bastard")
        "ai hia",       # ไอ้เหี้ย — male insult (fuck you / you bastard)
        "ee hia",       # อีเหี้ย — female equivalent
        "hia mung",
        "kwai",         # ควาย — water buffalo (you idiot/stupid)
        "ai kwai",      # ไอ้ควาย — you stupid buffalo
        "ee kwai",      # อีควาย — female form
        "mung",         # มึง — rude "you" (highly offensive to strangers)
        "goo",          # กู — rude "I/me" (highly offensive in formal context)
        "eed",          # อีด — fuck (variant)
        "yet",          # เย็ด — fuck (sexual act, very vulgar)
        "yet mae",      # เย็ดแม่ — fuck your mother
        "yet mae mung",
        "hee",          # หี — vagina (very vulgar)
        "hee mung",
        "kwak",         # ควก — penis (vulgar)
        "aa jiao",      # อาจิ๋ว / vulgar for genitalia
        "ai sat",       # ไอ้สัตว์ — you animal
        "ee sat",       # อีสัตว์ — female form
        "sat",          # สัตว์ — animal (as insult)
        "ai ngo",       # ไอ้งั่ง — stupid male
        "ee ngo",       # อีงั่ง — stupid female
        "baa",          # บ้า — crazy / mad
        "ai baa",       # ไอ้บ้า — crazy guy
        "ee baa",       # อีบ้า — crazy woman
        "chib hai",     # ฉิบหาย — damnation / fuck it
        "chip hai",
        "talok",        # ตลก — clown / ridiculous (used as insult)
        "ngo",          # โง่ / งั่ง — stupid/dumb
        "ting tong",    # ติ๊งต๊อง — crazy/dumb (mild)
        "apai",         # อภัย - not offensive alone but part of insult phrases
        "fai cha",      # ไฟฉาย — idiot in slang
        "kee nok",      # ขี้นก — bird shit (mild)
        "kee maa",      # ขี้หมา — dog shit (offensive)
        "kee niao",     # ขี้เหนียว — stingy (mild)
        "sei",          # ไส — die / dead (offensive used as curse)
        "pai nai",      # ไปไหน — "go away" (rude dismissal)
        "maa",          # หมา — dog (as insult)
        "ai maa",       # ไอ้หมา — you dog
        "ee maa",       # อีหมา — female form
        "kuan",         # กวน — annoying
        "guan jai",     # กวนใจ — annoying person
        "rok",          # โรค — disease (calling someone a disease)

        # ── Thai racial/discriminatory slurs ─────────────────────────────────
        "farang",       # ฝรั่ง — foreigner/Caucasian (can be derogatory)
        "kak",          # แขก — Indian/South Asian (derogatory)
        "ai kak",
        "jin",          # จีน — Chinese (can be derogatory)
        "ai jin",
        "khamen",       # เขมร — Cambodian (derogatory)
        "lao",          # ลาว — Laotian (can be derogatory)
        "mahn",         # แมน — used to describe effeminate man
        "kathoey",      # กะเทย — transgender/ladyboy (highly offensive as insult)
        "tut",          # ตุ๊ด — derogatory for gay men
        "adam",         # อาดัม — derogatory slang
        "negro",
        "dam",          # ดำ — black skin (used as slur)

        # ── TH scam/MLM terms ─────────────────────────────────────────────────
        "rub ngan thi ban",  # work from home (often scam signal in TH)
        "raiid phiset",      # special income
        "khommishan",        # commission-only (Thai phonetic)
        "samakr wandee",     # join today (scam signal)
        "rian ru dai ngoen", # learn and earn (scam signal)
        "tang an laew dai ngoen",  # set up and earn
    ],
}


# ──────────────────────────────────────────────────────────────────────────────
# Convenience helpers
# ──────────────────────────────────────────────────────────────────────────────

def get_wordlist_for_market(market_code: str) -> list[str]:
    """
    Return combined word list for a specific market: universal "all" terms
    plus market-specific terms.

    Args:
        market_code: ISO market code, e.g. "AU", "SG", "HK"

    Returns:
        Deduplicated list of terms (lowercase).
    """
    universal = PROFANITY_WORDLIST.get("all", [])
    market_specific = PROFANITY_WORDLIST.get(market_code.upper(), [])
    combined = universal + market_specific
    # Deduplicate while preserving order
    seen: set[str] = set()
    result: list[str] = []
    for term in combined:
        key = term.lower()
        if key not in seen:
            seen.add(key)
            result.append(term.lower())
    return result


def get_all_terms() -> list[str]:
    """Return every term across all markets, deduplicated."""
    all_terms: list[str] = []
    seen: set[str] = set()
    for terms in PROFANITY_WORDLIST.values():
        for term in terms:
            key = term.lower()
            if key not in seen:
                seen.add(key)
                all_terms.append(term.lower())
    return all_terms
