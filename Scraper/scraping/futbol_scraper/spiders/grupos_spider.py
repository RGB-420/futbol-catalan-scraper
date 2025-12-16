import re
import scrapy
from ..items import GrupoItem

class GruposSpider(scrapy.Spider):
    name = "grupos"

    def __init__(self, codigo_competicion=None, temporada="21",
                 tipo="futbol-11", categoria="19308233", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.codigo_competicion = str(codigo_competicion) if codigo_competicion else None
        self.temporada = str(temporada)
        self.tipo = tipo
        self.categoria = str(categoria)

    def start_requests(self):
        if not self.codigo_competicion:
            self.logger.error("Debes pasar 'codigo_competicion' (CodigoWeb de la competencia).")
            return
        url = "https://www.fcf.cat/cargar_grupos"
        formdata = {
            "tipo": self.tipo,
            "categoria": self.categoria,
            "competicion": self.codigo_competicion,
            "temporada": self.temporada,
        }
        yield scrapy.FormRequest(url=url, formdata=formdata, callback=self.parse_grupos)

    def parse_grupos(self, response):
        for a in response.css("a.grupo"):
            texto = a.css("p::text").get(default="").strip()
            m = re.search(r"(\d+)", texto)
            numero = int(m.group(1)) if m else None
            href = a.attrib.get("href", "")
            slug = href.rstrip("/").split("/")[-1] or f"grup-{numero}"

            item = GrupoItem()
            item["codigo_competicion"] = int(self.codigo_competicion)
            item["numero_grupo"] = numero
            item["temporada"] = "2025-26"
            item["region"] = None  # si aparece en otra parte, lo añadimos luego
            item["slug"] = slug
            yield item
