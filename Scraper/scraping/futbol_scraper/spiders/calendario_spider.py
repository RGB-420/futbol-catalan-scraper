import scrapy
import re
from urllib.parse import urljoin
from db.connection import get_connection
from ..items import CalendarioItem
from scrapy import signals

class CalendarioSpider(scrapy.Spider):
    name = "calendario"
    
    def __init__(self, temporada="2025-2026", temporada_ruta="2526", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.temporada = temporada
        self.temporada_ruta = temporada_ruta

    def start_requests(self):
        """
        Cargar todos los grupos desde SQL con su slug, nº de grupo y slug de competición.
        """
        conn = get_connection()
        cur = conn.cursor()

        sql = """
        SELECT g.id_grupo, g.numero_grupo,
               c.slug AS competicion_slug
        FROM public.grupos g
        JOIN public.competiciones c ON c.id_competicion = g.id_competicion
        """

        cur.execute(sql)
        grupos = cur.fetchall()
        cur.close()
        conn.close()

        for id_grupo, nro, comp_slug in grupos:
            url = f"https://www.fcf.cat/calendari/{self.temporada_ruta}/futbol-11/{comp_slug}/grup-{nro}"

            yield scrapy.Request(
                url,
                callback=self.parse_calendari,
                meta={"id_grupo": id_grupo,
                      "competicion_slug":comp_slug,
                      "numero_grupo":nro
                      },
            )


    def parse_calendari(self, response):
        id_grupo = response.meta["id_grupo"]
        comp_slug = response.meta["competicion_slug"]
        nro_grupo = response.meta["numero_grupo"]

        tablas = response.xpath('//table[contains(@class,"calendaritable")]')
        link_acta = False

        for tabla in tablas:
            ths = tabla.xpath('./thead/tr/th/text()').getall()
            jornada = int(ths[0].replace("Jornada", "").strip())
            print(f"[JORNADA] Jornada: {jornada}")
            rows = tabla.xpath('./tbody/tr')

            for row in rows:
                tds = row.xpath("./td")
                item = CalendarioItem()

                if not link_acta:
                    acta_url = tds[3].xpath(".//a/@href").get()
                    item["slug_competicion"] = acta_url.split("/")[6]
                    item["abreviatura_competicion"] = acta_url.split("/")[8] 
                    link_acta=True

                if len(tds) != 7:
                    print("[WARN] Fila inesperada:", row.get())

                item["id_grupo"] = id_grupo
                item["jornada"] = jornada
                item["temporada"] = self.temporada

                # LOCAL
                local_href = tds[0].xpath(".//a/@href").get()
                if not local_href:
                    print("[WARN] Sin href local:", row.get())
                    continue
                item["equipo_local_slug"] = local_href.rstrip("/").split("/")[-1]

                # VISITANTE
                visitante_href = tds[6].xpath(".//a/@href").get()
                if not visitante_href:
                    print("[WARN] Sin href visitante:", row.get())
                    continue
                item["equipo_visitante_slug"] = visitante_href.rstrip("/").split("/")[-1]

                yield item