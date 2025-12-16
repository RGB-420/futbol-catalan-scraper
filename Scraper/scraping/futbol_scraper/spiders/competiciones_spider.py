import scrapy
import re
from collections import defaultdict
from ..items import CompeticionItem


class CompeticionesSpider(scrapy.Spider):
    name = "competiciones"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.contador_juvenil = 0
        self.contadores_por_edad = defaultdict(int)

    def start_requests(self):
        url = "https://www.fcf.cat/cargar_competiciones"
        formdata = {
            "temporada": "21",
            "categoria": "19308233",
        }
        yield scrapy.FormRequest(
            url=url,
            formdata=formdata,
            callback=self.parse_competiciones,
        )

    def parse_competiciones(self, response):
        self.logger.info(f"Status respuesta competiciones: {response.status}")

        for comp in response.css("p.competicion"):
            nombre_raw = comp.css("::text").get(default="").strip()

            # --- FILTRO ---
            if not (
                re.search(r"juvenils?", nombre_raw, re.IGNORECASE)
                or re.search(r"S\d+$", nombre_raw)
            ):
                continue

            item = CompeticionItem()
            item["nombre"] = nombre_raw
            item["codigo_web"] = comp.attrib.get("title")

            # -------- ORGANIZADOR --------
            nombre_upper = nombre_raw.upper()
            if "JUVENIL" in nombre_upper and (
                "HONOR" in nombre_upper or "NACIONAL" in nombre_upper
            ):
                item["organizador"] = "RFEF"
            else:
                item["organizador"] = "FCF"

            # -------- CATEGORIA --------
            nombre_mayus = nombre_raw.upper()
            if "JUVENIL" in nombre_mayus:
                item["categoria"] = "Juvenil"
            elif "CADET" in nombre_mayus:
                item["categoria"] = "Cadete"
            elif "INFANTIL" in nombre_mayus:
                item["categoria"] = "Infantil"
            else:
                item["categoria"] = None

            # -------- EDAD MÁXIMA --------
            if "JUVENIL" in nombre_mayus:
                item["edad_maxima"] = 19
            else:
                m = re.search(r"S(\d+)$", nombre_raw)
                item["edad_maxima"] = int(m.group(1)) if m else None

            # -------- NIVEL --------
            if "JUVENIL" in nombre_mayus:
                self.contador_juvenil += 1
                item["nivel"] = self.contador_juvenil
            else:
                if item["edad_maxima"]:
                    self.contadores_por_edad[item["edad_maxima"]] += 1
                    item["nivel"] = self.contadores_por_edad[item["edad_maxima"]]
                else:
                    item["nivel"] = None

            # -------- SLUG --------
            item["slug"] = (
                nombre_raw.lower()
                .replace(" ", "-")
                .replace("ó", "o")
                .replace("ò", "o")
                .replace("ú", "u")
                .replace("ù", "u")
                .replace("í", "i")
                .replace("ì", "i")
                .replace("é", "e")
                .replace("è", "e")
                .replace("à", "a")
            )

            yield item
