import scrapy
import re
from db.connection import get_connection
from ..items import CamposItem
from scrapy import signals

class CamposSpider(scrapy.Spider):
    name = "campo"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def start_requests(self):
        """
        Cargar todos los grupos desde SQL con su slug, nº de grupo y slug de competición.
        """
        conn = get_connection()
        cur = conn.cursor()

        sql = """
        SELECT "CodigoWeb"
            FROM public."Campos" 
            WHERE "NombreCampo" IS NULL
        """

        cur.execute(sql)
        codigos = cur.fetchall()
        cur.close()
        conn.close()

        for codigo in codigos:
            url = f"https://www.fcf.cat/camp/{codigo[0]}"

            yield scrapy.Request(
                url,
                callback=self.parse_campos,
                meta={"codigo" : codigo},
            )

    def parse_campos(self, response):
        item = CamposItem()

        item["codigo"] = response.meta['codigo']

        item["nombre_campo"] = response.xpath('normalize-space(//div[contains(@class, "mt-20")]//p[contains(@class, "bigtitle")][1])').get()
        
        item["terreno"] = response.xpath('//td[span[contains(text(), "Superfície de joc")]]/following-sibling::td/text()').get()

        item["direccion"] = response.xpath('//td[span[contains(text(), "Direcció")]]/following-sibling::td/text()').get()

        item["localidad"] = response.xpath('//td[span[contains(text(), "Localitat")]]/following-sibling::td/text()').get()

        item["provincia"] = response.xpath('//td[span[contains(text(), "Província")]]/following-sibling::td/text()').get()

        yield item