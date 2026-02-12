# -*- coding: utf-8 -*-
"""
Crawler Instagram por perfis com login automático via Playwright.

Coleta:
- Likes
- Views (quando existir, principalmente Reels e vídeos)
- Shares e Reach (somente se houver acesso ao painel de Insights)

Exporta CSV.

Observação:
Reach e Shares costumam existir apenas em Insights, que geralmente só aparece para dono do post
ou contas com permissão. Para perfis de terceiros, tende a vir vazio.
"""

import os
import time
import random
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple

import pandas as pd
from dateutil import tz
from unidecode import unidecode
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError

# ======================== CREDENCIAIS ========================
# Recomendo usar variáveis de ambiente:
# Windows (PowerShell):
#   setx IG_USER "seu_user"
#   setx IG_PASS "sua_senha"
# Depois reabra o terminal
IG_USER = "mkt_sport_squad_br"
IG_PASS = " "

# ======================== CONFIG ========================

PROFILES_RAW = """
joolapickleballbrasil
joolabrasil
joolapickleball.korea
joolaglobal
joolapickleball
joolataiwan
joolaapac.pb
joolausa
joolainindia
joolaaustralia
joolaeurope
joolapickleball.japan
joolaeurope.pb
joolapickleball.tqk.vn_pb
joolaph_pickleball
nike
adidasbrasil
asicsbrasil
butterflyttofficial
newbalance
lining.official
stigatabletennis
olympikus
pumabrasil
nikerunning
filabr
babolat
asics
mizunobr
pikklusa
paddletekpickleball
victorsport_official
yonex_badminton
wilson
wilsontennis
asicsrunning
on
skechersbrasil
babolatpickleball
reloadpickleball
mizuno_table_tennis_jp
tibharbrasil
konabrand.kona
duprpb
mlp.australia
orlandosqueeze
atlantabouncers
brooklynpickleball.team
carolina_hogs
chicagoslicemlp
columbusslidersmlp
dallasflashpb
miamipc
phxflames
rancherspickleball
utahblackdiamonds
ppatour
ppa.tour.asia
majorleaguepb
ligasupremo
pickleplayalliance
pickleball.transforma
viapickleball
cbpickleball
pcklpark
bolafuradapickleball
pickleballtvbr
joy_tenisdemesa
matchpointtabletennis
life_pong
tmsantoandre
itaimkeikojjyamada
topspinabc
tenisdemesabunkyo
ctar_tenisdemesa
atemetm
grupokenzentm
starstenisdemesa
nippon.tenisdemesa
itaqueratm
trescoroas_tm
aceas.tm
ucegtm
tmcasaverde_oficial
acrepatm
tonanteamt.mesa
ttvmakiuchi
calderanotm
motiro_tt
ligavaletm
wtt
brasileirao
nbabrasil
cbvolei
nfl
mlb
premierleague
nhl
championsleague
f1
digitaltabletennis
digitalsportt
ligapaulistadepickleball
mixeddoublesports
pickleballwsnet
ceretpickleballofc
aya.sportss
bolafuradapickleball
masasport.oficial
brazilpickleballstore
santospickleballclube
eleven11.pickleball
boabolatennis
adidaspickleball
cristiano
leomessi
virat.kohli
neymarjr
k.mbappe
davidbeckham
ronaldinho
marcelotwelve
lewishamilton
lebron
vinijr
kingjames
stephencurry30
kevindurant
serenawilliams
naomiosaka
carlitosalcarazz
djokernole
thenotoriousmma
usainbolt
israeladesanya
timo_boll_official
fan_zhendong
"""

OUT_CSV = "instagram_perfis_auto.csv"

USER_TZ = tz.gettz("America/Sao_Paulo")

# agora buscamos os posts das últimas 24 horas por perfil
WINDOW_HOURS = 480  # janela de tempo para considerar o post recente

MIN_DELAY = 0.8
MAX_DELAY = 1.6
OPEN_POST_RETRIES = 3

USERNAME_RE = re.compile(r"instagram\.com/([A-Za-z0-9._]+)/?", re.IGNORECASE)
HASHTAG_RE = re.compile(r"#\w+", re.UNICODE)

# ======================== FUNÇÕES AUXILIARES ========================

def human_delay(a=MIN_DELAY, b=MAX_DELAY):
    time.sleep(random.uniform(a, b))


def to_user_tz(dt_utc: datetime) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=tz.UTC)
    return dt_utc.astimezone(USER_TZ)


def extract_hashtags(text: str) -> str:
    if not text:
        return ""
    tags = HASHTAG_RE.findall(text)
    return ", ".join(sorted(set(t.strip() for t in tags)))


def classify_tone(caption: str) -> str:
    if not caption:
        return "Campanha"
    text = unidecode(caption.lower())
    produto_kw = [
        "compre","garanta","aproveite","desconto","oferta","cupom","frete","estoque","tamanho",
        "cores","modelo","produto","colecao","linha","edicao","preco","por apenas","link na bio",
        "especificacoes","caracteristicas","material","tecido","madeira","borracha","raquete","paddle",
        "tenis","camiseta","shorts","jaqueta","mochila","oculos","meias","lancamento de produto"
    ]
    campanha_kw = [
        "campanha","evento","torneio","inscricoes","inscricao","regulamento","programacao",
        "cronograma","datas","agenda","ao vivo","live","estreia","parceria","oficial",
        "comunicado","novidade","lancamento","edicao","serie","capitulo","episodio"
    ]
    atleta_kw = [
        "atleta","treino","treinar","partida","jogo","match","vitoria","derrota","titulo",
        "medalha","podio","campeonato","torneio","ranking","desempenho","sets","games",
        "sparring","coach","tecnico","convocacao","selecao","selecao brasileira",
        "meu jogo","minha partida","minha vitoria","minha derrota","estou treinando","eu joguei"
    ]
    if any(k in text for k in produto_kw):
        return "Produto"
    if any(k in text for k in atleta_kw):
        return "Atleta"
    if any(k in text for k in campanha_kw):
        return "Campanha"
    return "Campanha"


def extract_main_theme(caption: str) -> str:
    if not caption:
        return ""
    tags = [t.lstrip("#") for t in HASHTAG_RE.findall(caption)]
    if tags:
        uniq = []
        for t in tags:
            tclean = t.strip().lower()
            if tclean not in uniq:
                uniq.append(tclean)
        return ", ".join(uniq[:2])

    first_sentence = re.split(r"[.!?\n\r]", caption.strip())[0]
    tokens = re.findall(r"\w+", unidecode(first_sentence.lower()))
    pt_stop = set("""
a o os as de da do das dos e em para por com sem um uma umas uns no na nos nas ao à às aos que ser estar é são foi foram era eram vai vão
como mais menos muito muita muitos muitas pouco poucos poucas este esta isto esse essa isso aquele aquela aquilo sobre entre até
""".split())
    tokens = [w for w in tokens if w not in pt_stop]
    return " ".join(tokens[:8])


def parse_profiles(raw: str) -> List[str]:
    items = re.split(r"[\s,]+", raw.strip())
    usernames: Set[str] = set()
    for it in items:
        s = it.strip()
        if not s:
            continue
        if s.startswith("@"):
            usernames.add(s[1:])
            continue
        if "instagram.com" in s:
            m = USERNAME_RE.search(s)
            if m:
                usernames.add(m.group(1))
                continue
        usernames.add(s)
    return sorted(usernames)

# ======================== MÉTRICAS ========================

def _parse_count(text: str) -> Optional[int]:
    if not text:
        return None
    t = unidecode(text.lower()).strip()

    t = re.sub(r"(curtidas?|likes?|visualizacoes?|views?|reproducoes?|plays?|contas?|alcancadas?|reach|compartilhamentos?|shares?)", "", t).strip()
    t = t.replace("\u202f", " ").replace("\xa0", " ")
    t = t.replace(".", "").replace(" ", "")

    mult = 1
    if "mil" in t or t.endswith("k"):
        mult = 1_000
        t = t.replace("mil", "").replace("k", "")
    if "mi" in t or t.endswith("m"):
        mult = 1_000_000
        t = t.replace("mi", "").replace("m", "")

    t = t.replace(",", ".")
    t = re.sub(r"[^0-9.]", "", t)
    if not t:
        return None
    try:
        val = float(t)
        return int(val * mult)
    except Exception:
        return None


def extract_likes(page) -> Optional[int]:
    try:
        candidates = page.query_selector_all("main section span, main section a, main section div")
        for c in candidates[:450]:
            tx = (c.inner_text() or "").strip()
            if not tx:
                continue
            low = unidecode(tx.lower())
            if "curtida" in low or "likes" in low:
                n = _parse_count(tx)
                if n is not None:
                    return n
    except Exception:
        pass

    try:
        candidates = page.query_selector_all('[aria-label*="like" i], [aria-label*="curtida" i]')
        for c in candidates[:200]:
            tx = (c.get_attribute("aria-label") or "").strip()
            n = _parse_count(tx)
            if n is not None:
                return n
    except Exception:
        pass

    return None


def extract_views(page, media_type: str) -> Optional[int]:
    if media_type.lower() not in ("reels", "vídeo", "video"):
        return None

    try:
        candidates = page.query_selector_all("main span, main div, main a")
        for c in candidates[:600]:
            tx = (c.inner_text() or "").strip()
            if not tx:
                continue
            low = unidecode(tx.lower())
            if "visualiz" in low or "views" in low or "reproduc" in low or "plays" in low:
                n = _parse_count(tx)
                if n is not None:
                    return n
    except Exception:
        pass

    return None


def try_extract_insights(page) -> Dict[str, Optional[int]]:
    out = {"Reach": None, "Shares": None}

    btn_selectors = [
        'text="Ver insights"',
        'text="Ver insight"',
        'text="View insights"',
        'a:has-text("Ver insights")',
        'button:has-text("Ver insights")',
        'a:has-text("View insights")',
        'button:has-text("View insights")',
    ]

    insights_clicked = False
    for sel in btn_selectors:
        try:
            el = page.query_selector(sel)
            if el:
                el.click()
                page.wait_for_timeout(1500)
                insights_clicked = True
                break
        except Exception:
            continue

    if not insights_clicked:
        return out

    try:
        nodes = page.query_selector_all("div[role='dialog'] span, div[role='dialog'] div")
        texts = []
        for n in nodes[:900]:
            tx = (n.inner_text() or "").strip()
            if tx:
                texts.append(tx)

        joined = "\n".join(texts)
        j = unidecode(joined.lower())

        reach_patterns = [
            r"contas alcancadas\s*([\d\.,]+)\s*(mil|mi|m|k)?",
            r"accounts reached\s*([\d\.,]+)\s*(mil|mi|m|k)?",
            r"alcancadas\s*([\d\.,]+)\s*(mil|mi|m|k)?",
            r"reached\s*([\d\.,]+)\s*(mil|mi|m|k)?",
        ]
        for pat in reach_patterns:
            m = re.search(pat, j, flags=re.IGNORECASE)
            if m:
                raw = m.group(1)
                suf = m.group(2) or ""
                out["Reach"] = _parse_count(f"{raw} {suf}".strip())
                break

        shares_patterns = [
            r"compartilhamentos\s*([\d\.,]+)\s*(mil|mi|m|k)?",
            r"shares\s*([\d\.,]+)\s*(mil|mi|m|k)?",
            r"compartilhou\s*([\d\.,]+)\s*(mil|mi|m|k)?",
        ]
        for pat in shares_patterns:
            m = re.search(pat, j, flags=re.IGNORECASE)
            if m:
                raw = m.group(1)
                suf = m.group(2) or ""
                out["Shares"] = _parse_count(f"{raw} {suf}".strip())
                break

    except Exception:
        return out

    return out

# ======================== LOGIN AUTOMÁTICO ========================

def login_instagram(page) -> bool:
    try:
        page.goto("https://www.instagram.com/accounts/login/", timeout=90000)
        page.wait_for_timeout(4000)

        if not IG_PASS:
            print("[ERRO] IG_PASS vazio. Configure a variável de ambiente IG_PASS.")
            return False

        page.fill('input[name="username"]', IG_USER)
        page.fill('input[name="password"]', IG_PASS)
        human_delay()
        page.click('button[type="submit"]')

        page.wait_for_load_state("networkidle", timeout=90000)
        page.wait_for_timeout(5000)

        if "accounts/login" in page.url.lower():
            print("[ERRO] Parece que o login falhou. Verifique usuário e senha.")
            return False

        print("[OK] Login realizado com sucesso.")
        return True
    except Exception as e:
        print("[ERRO] Falha ao fazer login:", e)
        return False

# ======================== COLETA DOS LINKS ========================

def collect_profile_post_urls(page, username: str, max_posts: int) -> List[str]:
    urls: List[str] = []
    seen: Set[str] = set()

    profile_url = f"https://www.instagram.com/{username}/"
    print(f"  > Abrindo perfil {profile_url}")

    for attempt in range(2):
        try:
            page.goto(profile_url, timeout=90000)
            page.wait_for_timeout(4000)
            break
        except (PWTimeout, PWError) as e:
            print(f"  [WARN] Erro ao abrir perfil @{username}, tentativa {attempt+1}/2: {e}")
            if attempt == 1:
                return []
            page.wait_for_timeout(3000)

    try:
        anchors = page.query_selector_all('a[href*="/p/"], a[href*="/reel/"]')
    except PWError as e:
        print(f"[ERRO] Coleta de links em @{username}: {e}")
        return []

    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
        except Exception:
            continue
        if not href:
            continue
        if href.startswith("/"):
            href = "https://www.instagram.com" + href
        href = href.split("?")[0]
        if ("/p/" in href or "/reel/" in href) and href not in seen:
            seen.add(href)
            urls.append(href)
            if len(urls) >= max_posts:
                break

    print(f"  > Encontrados {len(urls)} posts na primeira tela de @{username}")
    return urls

# ======================== EXTRAÇÃO DO POST ========================

def extract_post(page, url: str, profile: Optional[str] = None) -> Optional[Dict[str, Any]]:
    for attempt in range(OPEN_POST_RETRIES):
        try:
            page.goto(url, timeout=90000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)

            dt_local: Optional[datetime] = None
            data_str = ""
            try:
                time_el = page.query_selector("time")
                if not time_el:
                    page.wait_for_timeout(1500)
                    time_el = page.query_selector("time")
                if time_el:
                    dtdt = time_el.get_attribute("datetime")
                    if dtdt:
                        dt_utc = datetime.fromisoformat(dtdt.replace("Z", "+00:00"))
                        dt_local = to_user_tz(dt_utc)
                        data_str = dt_local.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                dt_local = None
                data_str = ""

            caption = ""
            try:
                texts = page.query_selector_all("h1, h2")
                for t in texts:
                    txt = (t.inner_text() or "").strip()
                    if txt:
                        caption += txt + " "
            except Exception:
                pass

            if not caption:
                try:
                    spans = page.query_selector_all("main span")
                    capture = []
                    for s in spans[:250]:
                        txt = (s.inner_text() or "").strip()
                        if txt:
                            capture.append(txt)
                    if capture:
                        capture_sorted = sorted(set(capture), key=lambda x: (-len(x), x))
                        caption = capture_sorted[0]
                except Exception:
                    caption = ""

            media_type = "Foto"
            try:
                current_url = page.url
                if "/reel/" in current_url:
                    media_type = "Reels"
                else:
                    video = page.query_selector("video")
                    if video:
                        media_type = "vídeo"
                    else:
                        next_btn = page.query_selector('button[aria-label*="Next"], button[aria-label*="Próximo"]')
                        prev_btn = page.query_selector('button[aria-label*="Previous"], button[aria-label*="Anterior"]')
                        if next_btn or prev_btn:
                            media_type = "Carrossel"
            except Exception:
                pass

            perfil_final = profile or ""
            if not perfil_final:
                try:
                    header_links = page.query_selector_all('header a[href^="/"]')
                    for a in header_links[:8]:
                        href = a.get_attribute("href") or ""
                        if not href:
                            continue
                        u = href.strip("/").split("/")[0]
                        if u and u not in ("explore", "reel", "p"):
                            perfil_final = u
                            break
                except Exception:
                    pass

            likes = extract_likes(page)
            views = extract_views(page, media_type)
            insights = try_extract_insights(page)
            reach = insights.get("Reach")
            shares = insights.get("Shares")

            return {
                "PERFIL": perfil_final,
                "Data/Hora da Publicação": data_str,
                "_DT_LOCAL": dt_local,
                "Legenda/Descrição do Post": caption.strip(),
                "Tema Central (assunto principal)": extract_main_theme(caption),
                "Tipo de Mídia": media_type,
                "Tom da Comunicação": classify_tone(caption),
                "Hashtags utilizadas": extract_hashtags(caption),
                "Likes": likes,
                "Views": views,
                "Shares": shares,
                "Reach": reach,
                "URL": url
            }

        except (PWTimeout, PWError) as e:
            print(f"    [WARN] Erro ao abrir {url}, tentativa {attempt+1}/{OPEN_POST_RETRIES}: {e}")
            page.wait_for_timeout(2000)
            continue
        except Exception as e:
            print(f"    [ERRO] Falha inesperada em {url}: {e}")
            return None

    return None

# ======================== MAIN ========================

def main():
    usernames = parse_profiles(PROFILES_RAW)
    if not usernames:
        print("Nenhum perfil válido em PROFILES_RAW.")
        return

    rows: List[Dict[str, Any]] = []

    now_local = datetime.now(USER_TZ)
    cutoff = now_local - timedelta(hours=WINDOW_HOURS)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )

        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        )

        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
            Object.defineProperty(navigator, 'languages', {get: () => ['pt-BR','pt','en']});
        """)

        # Login
        login_page = context.new_page()
        if not login_instagram(login_page):
            try:
                login_page.close()
            except Exception:
                pass
            context.close()
            browser.close()
            return
        login_page.close()

        # Coleta por perfil
        for idx, u in enumerate(usernames, start=1):
            print(f"\n[Perfil {idx}/{len(usernames)}] @{u}")
            page = context.new_page()

            try:
                post_urls = collect_profile_post_urls(page, u, WINDOW_HOURS)

                old_streak = 0  # posts seguidos fora da janela

                for jdx, url in enumerate(post_urls, start=1):
                    print(f"    [{jdx}/{len(post_urls)}] {url}")

                    row = extract_post(page, url, profile=u)
                    if not row:
                        human_delay()
                        continue

                    dt_local = row.get("_DT_LOCAL")

                    # Se a data existe e está fora da janela, ignora e tenta parar cedo
                    if isinstance(dt_local, datetime) and dt_local < cutoff:
                        print("      > Fora da janela de tempo, ignorado.")
                        old_streak += 1

                        # Como a grade vem do mais recente para o mais antigo,
                        # vários seguidos fora da janela indicam que o resto também estará
                        if old_streak >= 6:
                            print("      > Sequência fora da janela, parando coleta deste perfil.")
                            break

                        human_delay()
                        continue

                    # Se chegou aqui, está dentro da janela ou não foi possível ler a data
                    old_streak = 0
                    rows.append(row)
                    human_delay()

            except Exception as e:
                print(f"[ERRO PERFIL @{u}] {e}")

            finally:
                try:
                    page.close()
                except Exception:
                    pass
                human_delay(1.0, 2.0)

        context.close()
        browser.close()

    if not rows:
        print("Nenhum post foi coletado.")
        return

    # Remove campo auxiliar de data
    for r in rows:
        if "_DT_LOCAL" in r:
            del r["_DT_LOCAL"]

    df = pd.DataFrame(rows, columns=[
        "PERFIL",
        "Data/Hora da Publicação",
        "Legenda/Descrição do Post",
        "Tema Central (assunto principal)",
        "Tipo de Mídia",
        "Tom da Comunicação",
        "Hashtags utilizadas",
        "Likes",
        "Views",
        "Shares",
        "Reach",
        "URL"
    ])

    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n[OK] Exportado {len(df)} posts para {OUT_CSV}")
if __name__ == "__main__":
    main()
