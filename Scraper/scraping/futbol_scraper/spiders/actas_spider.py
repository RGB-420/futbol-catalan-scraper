import scrapy
import re
from datetime import date
from db.connection import get_connection
from ..items import ActasItem
from scrapy import signals

class ActasSpider(scrapy.Spider):
    name = "acta"
    
    def __init__(self, temporada="2025-2026", temporada_ruta="2526", toda_temporada=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.temporada = temporada
        self.temporada_ruta = temporada_ruta
        self.toda_temporada = toda_temporada

    def start_requests(self):
        """
        Cargar todos los grupos desde SQL con su slug, nº de grupo y slug de competición.
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                SELECT 
                    co."slug" AS slug_competicion,
                    co."Abreviatura" AS abreviatura_grupo,
                    gr."slug" AS slug_grupo,
                    gr."idGrupo"
                FROM "Competiciones" co
                JOIN "Grupos" gr ON co."idCompeticion" = gr."idCompeticion";
                """
                cur.execute(sql)
                competiciones = cur.fetchall()

        for slug_competicion, abreviatura, slug_grupo, id_grupo in competiciones:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    if self.toda_temporada:
                        sql = """
                            SELECT eql."slug", eqv."slug", pa."idEquipoLocal", pa."idEquipoVisitante"
                            FROM "Partidos" pa
                            JOIN "Equipos" eql ON pa."idEquipoLocal" = eql."idEquipo"
                            JOIN "Equipos" eqv ON pa."idEquipoVisitante" = eqv."idEquipo"
                            WHERE pa."idGrupo" = %s
                        """
                        cur.execute(sql, (id_grupo,))
                        equipos = cur.fetchall()
                    else:
                        sql = """
                            SELECT eql."slug", eqv."slug", pa."idEquipoLocal", pa."idEquipoVisitante"
                            FROM "Partidos" pa
                            JOIN "Equipos" eql ON pa."idEquipoLocal" = eql."idEquipo"
                            JOIN "Equipos" eqv ON pa."idEquipoVisitante" = eqv."idEquipo"
                            WHERE pa."idGrupo" = %s 
                                and pa."EstadoPartido" != 'Acabado' 
                                and pa."FechaPartido" < %s
                        """
                        cur.execute(sql, (id_grupo, date.today()))
                        equipos = cur.fetchall()

            for eq_local, eq_visitante, id_local, id_visitante in equipos:
                url = f"https://www.fcf.cat/acta/{self.temporada_ruta}/futbol-11/{slug_competicion}/{slug_grupo}/{abreviatura}/{eq_local}/{abreviatura}/{eq_visitante}"

                yield scrapy.Request(
                    url,
                    callback=self.parse_acta,
                    meta={"id_grupo" : id_grupo,
                          "id_local" : id_local,
                          "id_visitante" : id_visitante}
                )

    def extraer_estado(self, texto):
        if not texto:
            return None

        t = texto.strip().lower()

        if t == "acta tancada":
            return "Acabado"
        if t in ("ajornat", "suspès", "suspendit"):
            return "Suspendido"
        
        return "Pendiente"
    
    def parse_jugadores(self, tabla):
        jugadores = []

        for row in tabla.xpath('.//tbody/tr'):
            texto = row.xpath('.//td[2]/a/text()').get()

            if texto:
                if "," in texto:
                    partes = [p.strip() for p in texto.split(',')]
                    apellidos = partes[0]
                    nombre = partes[1]
                else:
                    apellidos = ""
                    nombre = texto

                dorsal = row.xpath('.//td[1]/span/text()').get()

                jugadores.append({
                    "nombre": nombre,
                    "apellidos": apellidos,
                    "dorsal" : dorsal
                })

        return jugadores

    def parse_equip_tecnic(self, tabla):
        equipo_tecnico = []

        for row in tabla.xpath('.//tbody/tr'):
            texto = row.xpath('normalize-space(.//td[1])').get()

            if "," in texto:
                partes = [p.strip() for p in texto.split(',')]
                apellidos = partes[0]
                nombre = partes[1]
            else:
                apellidos = ""
                nombre = texto

            # Obtener rol según la clase del span
            rol = row.xpath('.//td[2]//span/@class').get()
            rol_formateado = rol.capitalize() if rol else ""

            equipo_tecnico.append({
                "nombre": nombre,
                "apellidos": apellidos,
                "rol": rol_formateado
            })

        return equipo_tecnico
    
    def parse_goles(self, tabla, escudo_local, escudo_visitante, id_local, id_visitante):
        goles = []

        for row in tabla.xpath('.//tbody/tr'):
            tipo_raw = row.xpath('.//div[@class="gol"]//div[contains(@class, "gol-")]/@class').get()
            tipo = "Normal"

            if tipo_raw:
                if "gol-penal" in tipo_raw:
                    tipo = "Penal"
                elif "gol-propia" in tipo_raw:
                    tipo = "Propia"
                else:
                    tipo = "Normal"

            # --- Jugador ---
            texto = row.xpath('.//td[3]/a/text()').get()
            texto = texto.strip() if texto else ""

            if "," in texto:
                partes = [p.strip() for p in texto.split(',')]
                apellidos = partes[0]
                nombre = partes[1]
            else:
                apellidos = ""
                nombre = texto

            # --- Minuto ---
            minuto = row.xpath('./td[last()]/text()').get()
            minuto = minuto.replace("'", "").strip() if minuto else ""

            escudo_gol = row.xpath(
                './/img[contains(@class, "acta-escut-gol")]/@src'
            ).get()

            escudo_gol_norm = self.normalizar_escudo(escudo_gol)

            if escudo_gol_norm == escudo_local:
                id_equipo = id_local

            elif escudo_gol_norm == escudo_visitante:
                id_equipo = id_visitante

            else:
                id_equipo = None

            goles.append({
                "tipo": tipo,
                "nombre": nombre,
                "apellidos": apellidos,
                "minuto": minuto,
                "id_equipo":id_equipo
            })

        return goles
    
    def parse_tarjetas(self, tabla):
        tarjetas = []
        for row in tabla.xpath('.//tbody/tr'):
            # --- Jugador ---
            texto = row.xpath('.//a/text()').get()
            texto = texto.strip() if texto else ""

            if "," in texto:
                partes = [p.strip() for p in texto.split(',')]
                apellidos = partes[0]
                nombre = partes[1]
            else:
                apellidos = ""
                nombre = texto

            # --- Tipo de tarjeta ---
            tarjeta_raw = row.xpath('.//div[@class="acta-stat-box"]/*[contains(@class, "groga") or contains(@class, "vermella")]/@class').get()

            tipo = "Desconocida"

            if tarjeta_raw:
                if "groga-2" in tarjeta_raw:
                    tipo = "Segona Groga"     # Segunda amarilla
                elif "groga" in tarjeta_raw:
                    tipo = "Groga"            # Amarilla
                elif "vermella" in tarjeta_raw:
                    tipo = "Vermella"         # Roja directa

            # --- Minuto ---
            minuto_raw = row.xpath('.//div[@class="acta-minut-targeta"]/text()').get()

            if minuto_raw:
                minuto = minuto_raw.replace("'", "").strip()
            else:
                minuto = None

            dorsal = row.xpath('.//td[1]/span/text()').get()

            tarjetas.append({
                "nombre": nombre,
                "apellidos": apellidos,
                "tipo": tipo,
                "minuto": minuto,
                "dorsal":dorsal
            })

        return tarjetas
    
    def normalizar_escudo(self, url):
        return url.split('/')[-1] if url else None

    def parse_acta(self, response):
        item = ActasItem()

        item["id_grupo"] = response.meta["id_grupo"]
        id_local = response.meta["id_local"]
        id_visitante = response.meta["id_visitante"]

        item["id_local"] = id_local
        item["id_visitante"] = id_visitante
        # -----------------------------
        # ESCUDOS
        # -----------------------------
        escudo_local = response.xpath(
            '//div[@class="acta-head"]/div[contains(@class, "acta-escut")][1]/a/img/@src'
        ).get()

        escudo_visitante = response.xpath(
            '//div[@class="acta-head"]/div[contains(@class, "acta-escut")][last()]/a/img/@src'
        ).get()

        escudo_local_norm = self.normalizar_escudo(escudo_local)
        escudo_visitante_norm = self.normalizar_escudo(escudo_visitante)
        # -----------------------------
        # FECHA Y HORA
        # -----------------------------
        texto_fecha_hora = response.xpath(
            '//div[contains(@class, "print-acta-data")]/text()'
        ).get()

        fecha = None
        hora = None

        if texto_fecha_hora:
            texto_fecha_hora = texto_fecha_hora.strip()
            m = re.search(r'Data:\s*(\d{2}-\d{2}-\d{4}),\s*([\d:]+)h', texto_fecha_hora)
            if m:
                fecha = m.group(1)
                hora = m.group(2)

        item["fecha"] = fecha
        item["hora"] = hora

        # -----------------------------
        # ESTADO DEL PARTIDO
        # -----------------------------
        raw_estado = response.xpath('//div[@class="acta-estat"]/span/text()').get()
        raw_estado = raw_estado.strip() if raw_estado else None
        item["estado"] = self.extraer_estado(raw_estado)

        # -----------------------------
        # RESULTADO
        # -----------------------------
        resultado = response.xpath('//div[@class="acta-marcador"]/span/text()').get()

        goles_local = None
        goles_visitante = None

        if resultado:
            resultado = resultado.strip()
            try:
                g_local, g_visitante = resultado.split("-")
                goles_local = int(g_local.strip())
                goles_visitante = int(g_visitante.strip())
            except:
                pass

        item["goles_local"] = goles_local
        item["goles_visitante"] = goles_visitante

        # -----------------------------
        # ESTADIO
        # -----------------------------
        tabla_estadi = response.xpath(
            '//table[@class="acta-table"][thead/tr/th[contains(text(), "Estadi")]]'
        )

        url_estadio = tabla_estadi.xpath('.//tbody//a[1]/@href').get()
        codigo_estadio = None

        if url_estadio:
            m = re.search(r'/camp/(\d+)', url_estadio)
            if m:
                codigo_estadio = m.group(1)

        item["codigo_estadio"] = codigo_estadio

        # -----------------------------
        # ÁRBITRO PRINCIPAL
        # -----------------------------
        tabla_arbitres = response.xpath(
            '//table[@class="acta-table"][thead/tr/th[contains(text(), "Àrbitres")]]'
        )

        primer_arbitro = tabla_arbitres.xpath('.//tbody/tr[1]')

        nombre_arbitro = None
        apellidos_arbitro = None
        delegacion = None

        # Nombre completo
        arbitro = primer_arbitro.xpath('.//td[2]/text()').get()

        if arbitro:
            arbitro = arbitro.strip()
            if "," in arbitro:
                ap, nom = [p.strip() for p in arbitro.split(",", 1)]
                apellidos_arbitro = ap
                nombre_arbitro = nom

        # Delegación
        deleg = primer_arbitro.xpath('.//td[2]/span/text()').get()
        if deleg:
            delegacion = deleg.strip("()")

        item["nombre_arbitro"] = nombre_arbitro
        item["apellidos_arbitro"] = apellidos_arbitro
        item["delegacion_arbitro"] = delegacion

        col_local = response.xpath('//div[contains(@class, "col-md-4")][1]')
        col_visitante = response.xpath('//div[contains(@class, "col-md-4")][last()]')

        # -----------------------------
        # TITULARES
        # -----------------------------
        jugadores_local = []
        jugadores_visitante = []

        tabla_jugadores_local = col_local.xpath(
            './/table[@class="acta-table"][thead/tr/th[contains(text(), "Titulars")]]'
        )

        if tabla_jugadores_local:
            jugadores_local = self.parse_jugadores(tabla_jugadores_local)

        tabla_jugadores_visitante = col_visitante.xpath(
            './/table[@class="acta-table"][thead/tr/th[contains(text(), "Titulars")]]'
        )

        if tabla_jugadores_visitante:
            jugadores_visitante = self.parse_jugadores(tabla_jugadores_visitante)

        item['jugadores_local'] = jugadores_local
        item['jugadores_visitante'] = jugadores_visitante

        # -----------------------------
        # SUPLENTES
        # -----------------------------
        suplentes_local = []
        suplentes_visitante = []

        tablas_suplentes_local = col_local.xpath(
            './/table[@class="acta-table"][thead/tr/th[contains(text(), "Suplents")]]'
        )

        if tablas_suplentes_local:
            suplentes_local = self.parse_jugadores(tablas_suplentes_local)

        tablas_suplentes_visitante = col_visitante.xpath(
            './/table[@class="acta-table"][thead/tr/th[contains(text(), "Suplents")]]'
        )

        if tablas_suplentes_visitante:
            suplentes_visitante = self.parse_jugadores(tablas_suplentes_visitante)

        item['suplentes_local'] = suplentes_local
        item['suplentes_visitante'] = suplentes_visitante

        # -----------------------------
        # CUERPO TÉCNICO
        # -----------------------------
        staff_local = []
        staff_visitante = []

        tablas_staff_local = col_local.xpath(
            './/table[@class="acta-table"][thead/tr/th[contains(text(), "Equip Tècnic")]]'
        )

        if tablas_staff_local:
            staff_local = self.parse_equip_tecnic(tablas_staff_local)

        tablas_staff_visitante = col_visitante.xpath(
            './/table[@class="acta-table"][thead/tr/th[contains(text(), "Equip Tècnic")]]'
        )

        if tablas_staff_visitante:
            staff_visitante = self.parse_equip_tecnic(tablas_staff_visitante)

        item['staff_local'] = staff_local
        item['staff_visitante'] = staff_visitante

        # -----------------------------
        # GOLES
        # -----------------------------
        tablas_goles = response.xpath(
            '//table[@class="acta-table"][thead/tr/th[contains(text(), "Gols")]]'
        )

        goles = []

        if len(tablas_goles) == 0:
            print("⚠️ No hay tablas de goles en este partido.")
        elif len(tablas_goles) == 1:
            goles = self.parse_goles(tablas_goles[0], escudo_local_norm, escudo_visitante_norm, id_local, id_visitante)

        item['goles'] = goles

        # -----------------------------
        # TARJETAS
        # -----------------------------
        tarjetas_local = []
        tarjetas_visitante = []

        tablas_tarjetas_local = col_local.xpath(
            './/table[@class="acta-table"][thead/tr/th[contains(text(), "Targetes")]]'
        )

        if tablas_tarjetas_local:
            tarjetas_local = self.parse_tarjetas(tablas_tarjetas_local)

        tablas_tarjetas_visitante = col_visitante.xpath(
            './/table[@class="acta-table"][thead/tr/th[contains(text(), "Targetes")]]'
        )

        if tablas_tarjetas_visitante:
            tarjetas_visitante = self.parse_tarjetas(tablas_tarjetas_visitante)

        item['tarjetas_local'] = tarjetas_local
        item['tarjetas_visitante'] = tarjetas_visitante

        yield item

       