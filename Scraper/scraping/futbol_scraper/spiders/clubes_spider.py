import scrapy
import re
from ..items import ClubItem
from db.connection import get_connection

# Mapeo de slugs que estan diferentes en aprtado club y equipos
slugs_mapeo = {"jesus-y-maria-ud":"jesus-i-maria-ud",
               "remences-ae-unio": "remences-associacio-esportiva-unio",
               "costa-daurada-fc": "costa-daurada-salou-fc",
               "bescano-cd": "bescano-ce",
               "palafrugell-cf": "palafrugell-fc",
               "efb-ulldecona":"escola-futbol-base-ulldecona-assoc",
               "unificacion-cfsantaperpetua": "unificacion-cfsanta-perpetua",
               "fundacio-esport-hospitalet-at": "fundacio-esporthospitalet-at",
               "escola-f-pobla-segur-i-comarc":"escola-f-pobla-segur-i-comarca",
               "vilaseca-cf":"vila-seca-cf",
               "agramunt-escolagerard-gatell-cf":"agramunt-escola-gerard-gatell-cf",
               "montroig-at":"mont-roig-at",
               "vilanova-geltru-cf":"vilanova-i-la-geltru-cf",
               "lleida-esportiu-club":"lleida-ponent-esportiu-club",
               "sant-jaume-denveija-ue":"sant-jaume-denveja-ue",
               "vila-olimpica-club-esp":"vila-olimpica-club-esportiu",
               "alcanar-2015-escola-futbol":"escola-futbol-alcanar-2015",
               "les-corts-de-barcelona-club-esp":"les-corts-de-barcelona-club-esportiu"}

class ClubesSpider(scrapy.Spider):
    name = "clubes"

    def __init__(self, temporada_ruta="2526", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.temporada_ruta = temporada_ruta

    def start_requests(self):
        """
        Cargar todos los slugs desde la tabla Clubes en SQL y construir URLs como:
        https://www.fcf.cat/club/{temporada_ruta}/{slug}
        """

        conn = get_connection()
        cur = conn.cursor()
        cur.execute('SELECT slug FROM public."Clubes";')
        rows = cur.fetchall()
        cur.close()
        conn.close()

        print(f"[CLUBES] Voy a scrapear {len(rows)} clubes")

        for slug, in rows:
            # Aplicar el mapeo si existe
            slug_real = slugs_mapeo.get(slug, slug)

            url = f"https://www.fcf.cat/club/{self.temporada_ruta}/{slug_real}"
            yield scrapy.Request(url, callback=self.parse_club, meta={"slug_original": slug, "slug_usable": slug_real})

    def extraer_valor(self, texto):
        if not texto:
            return None
        # Quitar lo que hay antes de los dos puntos
        return re.sub(r'^[^:]+:\s*', '', texto).strip()

    def normalizar_delegacion(self, texto):
        if not texto:
            return None

        # Eliminar espacios al inicio/fin
        limpio = texto.strip()

        # Quitar palabra Delegació / Delegacio (con o sin acento, mayúsculas, etc.)
        limpio = re.sub(r"(?i)delegaci[oó]\s*[:\-]?\s*", "", limpio).strip()

        # Formato "Title Case": Baix Llobregat, Vallès Occidental, etc.
        limpio = limpio.lower().title()

        return limpio

    def parse_club(self, response):
        slug_original = response.meta["slug_original"]
        slug_usable = response.meta["slug_usable"]

        # EXTRAER DATOS SEGÚN EL HTML REAL
        raw_delegacion = response.xpath("normalize-space(//span[contains(text(),'Delegació')]/parent::td)").get()
        raw_delegacion = self.extraer_valor(raw_delegacion)

        raw_localidad = response.xpath("normalize-space(//span[contains(text(),'Localitat')]/parent::td)").get()
        localidad = self.extraer_valor(raw_localidad)

        raw_provincia = response.xpath("normalize-space(//span[contains(text(),'Provincia')]/parent::td)").get()
        provincia = self.extraer_valor(raw_provincia)

        delegacion = self.normalizar_delegacion(raw_delegacion)

        item = ClubItem()
        item["slug"] = slug_original
        item["localidad"] = localidad.strip() if localidad else None
        item["delegacion"] = delegacion.strip() if delegacion else None
        item["provincia"] = provincia.strip() if provincia else None

        yield item
