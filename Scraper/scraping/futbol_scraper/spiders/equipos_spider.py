import re
import scrapy
from db.connection import get_connection
from ..items import EquipoItem


class EquiposSpider(scrapy.Spider):
    name = "equipos"

    def __init__(self, temporada="2025-26", temporada_ruta="2526",
                 tipo="futbol-11", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.temporada = temporada              # "2025-26"
        self.temporada_ruta = temporada_ruta    # "2526" para la URL
        self.tipo = tipo                        # "futbol-11"

    # Helpers para slugs
    @staticmethod
    def normalize_slug(s: str) -> str:
        s = s.lower().strip()
        s = s.replace(" ", "-")
        s = (s
             .replace("à","a").replace("á","a").replace("ä","a")
             .replace("è","e").replace("é","e").replace("ë","e")
             .replace("ì","i").replace("í","i").replace("ï","i")
             .replace("ò","o").replace("ó","o").replace("ö","o")
             .replace("ù","u").replace("ú","u").replace("ü","u"))
        s = re.sub(r"[^a-z0-9\-]", "", s)
        s = re.sub(r"-+", "-", s)
        return s

    @staticmethod
    def derive_club_slug(equipo_slug: str) -> str:
        # p.ej. damm-cf-a -> damm-cf
        return re.sub(r"-(?:[a-h]|u\d+)$", "", equipo_slug)

    def start_requests(self):
        # Leer todos los grupos + slug de competición desde la BD
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT g.id_grupo,
                   g.numero_grupo,
                   g.temporada,
                   g.slug AS slug_grupo,
                   c.slug AS slug_competicion
            FROM public.grupos g
            JOIN public.competiciones c
              ON c.id_competicion = g.id_competicion
            WHERE g.temporada = %s;
            ''',
            (self.temporada,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        self.logger.info(f"[EQUIPOS] Voy a procesar {len(rows)} grupos para temporada {self.temporada}")

        for id_grupo, num_grupo, temp, slug_grupo, slug_comp in rows:
            # Por si algún grupo no tiene slug, lo construimos:
            grupo_slug = slug_grupo or f"grup-{num_grupo}"

            url = f"https://www.fcf.cat/classificacio/{self.temporada_ruta}/{self.tipo}/{slug_comp}/{grupo_slug}"
            yield scrapy.Request(
                url,
                callback=self.parse_grupo_page,
                cb_kwargs={"id_grupo": id_grupo},
            )

    def extraer_nivel(self, nombre_equipo: str):
        """
        Extrae la letra A/B/C/D que indica el nivel del equipo.
        Soporta formatos como:
        - "C.F. DAMM A"
        - "MAS CATARRO, CF.B. A"
        - "AMPOSTA C"
        - "FUNDACIÓ U.E. CORNELLÀ  B"
        """
        nombre = nombre_equipo.strip()

        # Buscar letra A-D al final ignorando puntos/espacios
        m = re.search(r"([A-D])[\.\s]*$", nombre, flags=re.IGNORECASE)
        if not m:
            return 1  # Sin letra → se asume el A (nivel 1)

        letra = m.group(1).upper()
        return {"A": 1, "B": 2, "C": 3, "D": 4}.get(letra, 1)

    def parse_grupo_page(self, response, id_grupo):
        # Recorremos todas las filas de la tabla de clasificación
        for row in response.css("table.fcftable-e tbody tr"):
            # Celda con el nombre del equipo (vista 'resumida')
            link = row.css("td.tl.resumida a")
            nombre = link.css("::text").get()
            href_cal = link.attrib.get("href")

            if not nombre or not href_cal:
                continue  # por si hay alguna fila rara

            nombre = nombre.strip()

            # slug de equipo = último segmento de la URL
            # .../grup-1/espanyol-rcd-a  -> espanyol-rcd-a
            equipo_slug = href_cal.rstrip("/").split("/")[-1]

            # Si quieres, también puedes sacar el href de /equip/... (no obligatorio)
            href_equip = row.css("td.tc.pr-0 a::attr(href)").get()
            # ej: https://www.fcf.cat/equip/2526/hc16/espanyol-rcd-a
            codigo_equipo = None
            if href_equip:
                partes = href_equip.rstrip("/").split("/")
                # [..., 'equip', '2526', 'hc16', 'espanyol-rcd-a']
                if len(partes) >= 4:
                    codigo_equipo = partes[-2]  # hc16

            # Derivar slug de club (quitando la letra final tipo -a, -b...)
            club_slug = self.derive_club_slug(equipo_slug)

            item = EquipoItem()
            item["id_grupo"] = id_grupo
            item["temporada"] = self.temporada
            item["nombre_equipo"] = nombre
            item["equipo_slug"] = equipo_slug
            item["club_slug"] = club_slug
            item["nivel"] = self.extraer_nivel(nombre)

            # Ejemplo simple de categoría por nombre (puedes refinarlo)
            name_up = nombre.upper()
            if "JUVENIL" in name_up:
                item["categoria"] = "Juvenil"
            elif "CADET" in name_up or "CADETE" in name_up:
                item["categoria"] = "Cadete"
            elif "INFANTIL" in name_up:
                item["categoria"] = "Infantil"
            else:
                item["categoria"] = None

            item["nivel"] = None  # de momento lo dejamos vacío
            # item["codigo_equipo"] = codigo_equipo  # solo si añades este campo al Item/BD
            
            yield item