import re
from db.connection import get_connection


class DebugPrintPipeline:
    def process_item(self, item, spider):
        print(f"[DEBUG PIPELINE] Spider={spider.name} Item={dict(item)}")
        return item

class CompeticionesPostgresPipeline:
    def open_spider(self, spider):
        # Solo nos interesa cuando el spider es 'competiciones'
        if spider.name != "competiciones":
            return

        self.conn = get_connection()
        self.conn.autocommit = True  # para no preocuparnos de commits aún
        self.cur = self.conn.cursor()
        print("[PIPELINE] Conectado a PostgreSQL para Competiciones")

    def close_spider(self, spider):
        if spider.name != "competiciones":
            return

        self.cur.close()
        self.conn.close()
        print("[PIPELINE] Conexión PostgreSQL cerrada")

    def process_item(self, item, spider):
        if spider.name != "competiciones":
            return item

        sql = """
        INSERT INTO public."Competiciones"
            ("NombreCompeticion", "Categoria", "EdadMaxima",
             "Organizador", slug, "CodigoWeb", "Nivel")
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        self.cur.execute(
            sql,
            (
                item.get("nombre"),
                item.get("categoria"),
                item.get("edad_maxima"),
                item.get("organizador"),
                item.get("slug"),
                int(item["codigo_web"]) if item.get("codigo_web") else None,
                item.get("nivel"),
            ),
        )

        print(f'[PIPELINE] Insertada competición {item.get("nombre")}')
        return item

class GruposPostgresPipeline:
    def open_spider(self, spider):
        if spider.name != "grupos":
            return
        self.conn = get_connection()
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        print("[PIPELINE GRUPOS] Conectado a PostgreSQL")

    def close_spider(self, spider):
        if spider.name != "grupos":
            return
        self.cur.close()
        self.conn.close()
        print("[PIPELINE GRUPOS] Conexión cerrada")

    def process_item(self, item, spider):
        if spider.name != "grupos":
            return item

        # Buscar idCompeticion a partir de CodigoWeb
        self.cur.execute(
            'SELECT "idCompeticion" FROM public."Competiciones" WHERE "CodigoWeb" = %s',
            (item["codigo_competicion"],),
        )
        row = self.cur.fetchone()
        if not row:
            print(f"[PIPELINE GRUPOS] Competición CodigoWeb={item['codigo_competicion']} NO encontrada")
            return item

        id_competicion = row[0]

        sql = """
        INSERT INTO public."Grupos"
            ("idCompeticion","NumeroGrupo","Temporada","Region",slug)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT ("idCompeticion","NumeroGrupo","Temporada")
        DO UPDATE SET
            "Region" = EXCLUDED."Region",
            slug     = EXCLUDED.slug;
        """

        self.cur.execute(
            sql,
            (
                id_competicion,
                item.get("numero_grupo"),
                item.get("temporada"),
                item.get("region"),
                item.get("slug"),
            ),
        )

        print(f"[PIPELINE GRUPOS] Upsert grupo {item.get('numero_grupo')} de competicion {id_competicion}")
        return item
    
class EquiposYClubesPostgresPipeline:
    def open_spider(self, spider):
        if spider.name != "equipos":
            return
        self.conn = get_connection()
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        print("[PIPELINE EQUIPOS] Conectado a PostgreSQL")

    def close_spider(self, spider):
        if spider.name != "equipos":
            return
        self.cur.close()
        self.conn.close()
        print("[PIPELINE EQUIPOS] Conexión cerrada")

    # ---------- helpers ----------

    def _infer_nivel(self, nombre_equipo: str) -> int | None:
        """
        A -> 1, B -> 2, C -> 3, D -> 4; si no hay letra al final, 1 por defecto.
        Ej: "DAMM, C.F. A" -> 1
        """
        m = re.search(r"\s+([A-D])$", nombre_equipo.strip(), flags=re.IGNORECASE)
        if not m:
            return 1  # si no hay letra, asumimos primer equipo
        letra = m.group(1).upper()
        mapping = {"A": 1, "B": 2, "C": 3, "D": 4}
        return mapping.get(letra, 1)

    def _get_categoria_por_grupo(self, id_grupo: int) -> str | None:
        """
        Mira en Grupos + Competiciones la categoría (Juvenil, Cadete, Infantil…)
        para este idGrupo.
        """
        self.cur.execute(
            '''
            SELECT c."Categoria"
            FROM public."Grupos" g
            JOIN public."Competiciones" c
              ON g."idCompeticion" = c."idCompeticion"
            WHERE g."idGrupo" = %s
            ''',
            (id_grupo,),
        )
        row = self.cur.fetchone()
        return row[0] if row else None

    def _get_or_create_club(self, slug, nombre_club):
        """
        Devuelve idClub. Si no existe, lo crea.
        """
        self.cur.execute(
            'SELECT "idClub" FROM public."Clubes" WHERE slug = %s',
            (slug,),
        )
        row = self.cur.fetchone()
        if row:
            id_club = row[0]
            # Actualizamos nombre por si cambia cómo lo muestra la FCF
            self.cur.execute(
                'UPDATE public."Clubes" SET "NombreClub" = %s WHERE "idClub" = %s',
                (nombre_club, id_club),
            )
            return id_club

        self.cur.execute('SELECT COALESCE(MAX("idClub"), 0) + 1 FROM public."Clubes"')
        id_club = self.cur.fetchone()[0]

        self.cur.execute(
            '''
            INSERT INTO public."Clubes" ("idClub", slug, "NombreClub")
            VALUES (%s, %s, %s)
            ''',
            (id_club, slug, nombre_club),
        )
        return id_club

    def _get_or_create_equipo(self, id_club, id_grupo, categoria, nivel, equipo_slug):
        """
        Crea o actualiza equipo. Consideramos único (idClub, idGrupo, Nivel).
        Devuelve idEquipo.
        """
        self.cur.execute(
            '''
            SELECT "idEquipo"
            FROM public."Equipos"
            WHERE "idClub" = %s
              AND "idGrupo" = %s
              AND COALESCE("Nivel", 1) = COALESCE(%s, 1)
            ''',
            (id_club, id_grupo, nivel),
        )
        row = self.cur.fetchone()
        if row:
            id_equipo = row[0]
            self.cur.execute(
                '''
                UPDATE public."Equipos"
                SET "Categoria" = COALESCE(%s, "Categoria"),
                    "Nivel"     = COALESCE(%s, "Nivel"),
                    slug        = COALESCE(%s, slug)
                WHERE "idEquipo" = %s
                ''',
                (categoria, nivel, equipo_slug, id_equipo),
            )
            return id_equipo

        # No existe todavía -> crear nuevo
        self.cur.execute('SELECT COALESCE(MAX("idEquipo"), 0) + 1 FROM public."Equipos"')
        id_equipo = self.cur.fetchone()[0]

        self.cur.execute(
            '''
            INSERT INTO public."Equipos"
                ("idEquipo", "idClub", "Categoria", "Nivel", "idGrupo", slug)
            VALUES (%s, %s, %s, %s, %s, %s)
            ''',
            (id_equipo, id_club, categoria, nivel, id_grupo, equipo_slug),
        )
        return id_equipo

    # ---------- process_item ----------

    def process_item(self, item, spider):
        if spider.name != "equipos":
            return item

        id_grupo = item["id_grupo"]

        # ---- CLUB ----
        nombre_equipo = item["nombre_equipo"]

        # Nombre de club = sin la letra A/B/C final
        nombre_club = re.sub(r"\s+[A-D]$", "", nombre_equipo, flags=re.IGNORECASE)

        id_club = self._get_or_create_club(
            slug=item["club_slug"],
            nombre_club=nombre_club,
        )

        # ---- CATEGORÍA y NIVEL ----
        categoria = item.get("categoria")
        if categoria is None:
            categoria = self._get_categoria_por_grupo(id_grupo)

        nivel = item.get("nivel")
        if nivel is None:
            nivel = self._infer_nivel(nombre_equipo)

        equipo_slug = item.get("equipo_slug")

        id_equipo = self._get_or_create_equipo(
            id_club=id_club,
            id_grupo=id_grupo,
            categoria=categoria,
            nivel=nivel,
            equipo_slug=equipo_slug,
        )

        print(
            f'[PIPELINE EQUIPOS] Club={item["club_slug"]} (idClub={id_club}) '
            f'→ Equipo id={id_equipo} slug={equipo_slug} en grupo {id_grupo}, '
            f'categoria={categoria}, nivel={nivel}'
        )

        return item

class ClubesPostgresPipeline:
    def open_spider(self, spider):
        if spider.name != "clubes":
            return
        self.conn = get_connection()
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        print("[PIPELINE CLUBES] Conectado a PostgreSQL")

    def close_spider(self, spider):
        if spider.name != "clubes":
            return
        self.cur.close()
        self.conn.close()
        print("[PIPELINE CLUBES] Conexión cerrada")

    def process_item(self, item, spider):
        if spider.name != "clubes":
            return item

        sql = """
        UPDATE public."Clubes"
        SET "Localidad" = COALESCE(%s, "Localidad"),
            "Delegacion" = COALESCE(%s, "Delegacion"),
            "Provincia" = COALESCE(%s, "Provincia")
        WHERE slug = %s;
        """

        self.cur.execute(
            sql,
            (
                item["localidad"],
                item["delegacion"],
                item["provincia"],
                item["slug"],
            ),
        )

        print(f'[PIPELINE CLUBES] Actualizado club {item["slug"]}')
        return item

class CalendariosPostgresPipeline:
    def open_spider(self, spider):
        if spider.name != "calendario":
            return

        self.conn = get_connection()
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        
        self.competiciones_actualizadas = set()

        print("[PIPELINE CALENDARIO] Conectado a PostgreSQL")

    def close_spider(self, spider):
        if spider.name != "calendario":
            return

        self.cur.close()
        self.conn.close()
        print("[PIPELINE CALENDARIO] Conexión cerrada")

    # -------------------------------
    # HELPERS
    # -------------------------------

    def _get_equipo_id(self, slug):
        self.cur.execute(
            'SELECT "idEquipo" FROM public."Equipos" WHERE slug = %s',
            (slug,)
        )
        return self.cur.fetchone()

    def _update_abreviatura_competicion(self, slug, abreviatura):
        sql = """
            UPDATE "Competiciones"
            SET "Abreviatura" = %s
            WHERE "slug" = %s;
        """
        self.cur.execute(sql, (abreviatura, slug))


    # -------------------------------
    # PROCESS ITEM
    # -------------------------------

    def process_item(self, item, spider):
        if spider.name != "calendario":
            return item

        id_grupo = item.get("id_grupo")
        jornada = int(item["jornada"])

        equipo_local = item.get("equipo_local_slug")
        equipo_visitante = item.get("equipo_visitante_slug")

        # ----------------------------------
        # COMPETICIONES 
        # ----------------------------------
        slug_competicion = item.get("slug_competicion")
        abreviatura_competicion = item.get("abreviatura_competicion")
        if slug_competicion and slug_competicion not in self.competiciones_actualizadas:

            print(f"[PIPELINE] Guardando abreviatura {abreviatura_competicion} para slug {slug_competicion}")

            self._update_abreviatura_competicion(
                slug_competicion,
                abreviatura_competicion
            )

            # evitar repetir el update
            self.competiciones_actualizadas.add(slug_competicion)
        # ----------------------------------
        # EQUIPOS
        # ----------------------------------
        local_data = self._get_equipo_id(equipo_local)
        visitante_data = self._get_equipo_id(equipo_visitante)

        if not local_data or not visitante_data:
            print(f"[CALENDARIO][ERROR] Equipo NO encontrado: "
                  f"{item['equipo_local_slug']} vs {item['equipo_visitante_slug']}")
            return item

        id_local = local_data[0]
        id_visitante = visitante_data[0]

        # ----------------------------------
        # INSERT PARTIDO
        # ----------------------------------
        sql = """
        INSERT INTO public."Partidos"
            ("idEquipoLocal", "idEquipoVisitante", "Jornada", "idGrupo")
        VALUES (%s,%s,%s,%s)
        ON CONFLICT ("idEquipoLocal","idEquipoVisitante","Jornada","idGrupo")
        DO NOTHING
        RETURNING "idPartido";
        """

        self.cur.execute(sql, (
            id_local, id_visitante,jornada, id_grupo
        ))

        row = None
        try:
            row = self.cur.fetchone()
        except Exception as e:
            print("[PIPELINE CALENDARIO] ERROR fetchone():", e)
            raise

        if row:
            id_partido = row[0]
            print(f"[CALENDARIO][INSERT][G{item['id_grupo']}][J{jornada}] id={id_partido}")
        else:
            print(f"[CALENDARIO][DUPLICADO][G{item['id_grupo']}][J{jornada}] ya existe")

        return item

class ActasPostgresPipeline:
    def open_spider(self, spider):
        if spider.name != "acta":
            return

        self.conn = get_connection()
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        
        self.competiciones_actualizadas = set()

        print("[PIPELINE ACTA] Conectado a PostgreSQL")

    def close_spider(self, spider):
        if spider.name != "acta":
            return

        self.cur.close()
        self.conn.close()
        print("[PIPELINE ACTA] Conexión cerrada")

    def _get_or_create_arbitro(self, nombre, apellidos, delegacion):
        self.cur.execute(
            'SELECT "idArbitro" FROM public."Arbitros" '
            'WHERE "NombreArbitro" = %s AND "ApellidosArbitro" = %s AND "Delegacion" = %s',
            (nombre, apellidos, delegacion)
        )
        row = self.cur.fetchone()

        if row:
            print(f"[ACTAS][ARBITRO] Ya existe → id {row[0]}")
            return row[0]

        # Crear árbitro
        self.cur.execute(
            'INSERT INTO public."Arbitros" '
            '("NombreArbitro", "ApellidosArbitro", "Delegacion") '
            'VALUES (%s, %s, %s) RETURNING "idArbitro";',
            (nombre, apellidos, delegacion)
        )
        new_id = self.cur.fetchone()[0]

        print(f"[ACTAS][ÁRBITRO NUEVO] {nombre} {apellidos} ({delegacion}) → id={new_id}")

        return new_id

    def _get_or_create_campo(self, codigo):
        self.cur.execute(
            'SELECT "idCampo" FROM public."Campos" WHERE "CodigoWeb" = %s',
            (codigo,)
        )
        row = self.cur.fetchone()

        if row:
            print(f"[ACTAS][CAMPO] Ya existe → id {row[0]}")
            return row[0]

        # Insert nuevo campo
        self.cur.execute(
            'INSERT INTO public."Campos" ("CodigoWeb") VALUES (%s) RETURNING "idCampo";',
            (codigo,)
        )
        new_id = self.cur.fetchone()[0]

        print(f"[ACTAS][CAMPO NUEVO] CódigoWeb={codigo} → idCampo={new_id}")

        return new_id
    
    def get_or_create_jugador(self, nombre, apellidos):
        # Buscar jugador existente
        self.cur.execute("""
            SELECT "idJugador" FROM "Jugadores"
            WHERE "NombreJugador" = %s AND "ApellidosJugador" = %s
        """, (nombre, apellidos))

        result = self.cur.fetchone()

        if result:
            print(f"[ACTAS][JUGADORES] Ya existe → id {result[0]}")
            return result[0]  # id_jugador ya existe

        # Crear jugador nuevo
        self.cur.execute("""
            INSERT INTO "Jugadores" ("NombreJugador", "ApellidosJugador")
            VALUES (%s, %s)
            RETURNING "idJugador"
        """, (nombre, apellidos))

        new_id = self.cur.fetchone()[0]
        self.conn.commit()

        print(f"[ACTAS][JUGADOR NUEVO] {nombre}{apellidos} → {new_id}")

        return new_id

    def ensure_jugador_equipo(self, id_jugador, id_equipo):
        self.cur.execute("""
            SELECT 1 FROM "JugadoresEquipos"
            WHERE "idJugador" = %s AND "idEquipo" = %s
        """, (id_jugador, id_equipo))

        result = self.cur.fetchone()

        if result:
            print(f"[ACTAS][JUGADORES EQUIPO] Ya existe relación")
            return  # Ya existe

        self.cur.execute("""
            INSERT INTO "JugadoresEquipos" ("idJugador", "idEquipo")
            VALUES (%s, %s)
        """, (id_jugador, id_equipo))

        self.conn.commit()
    
    def insert_alineacion(self, id_jugador, id_equipo, id_partido, titular, dorsal):
        self.cur.execute("""
            INSERT INTO "Alineaciones" ("idPartido", "idEquipo", "idJugador", "Titular", "Dorsal")
            VALUES (%s, %s, %s, %s, %s)
        """, (id_partido, id_equipo, id_jugador, titular, dorsal))

    def get_or_create_staff(self, nombre, apellidos):
        # Buscar jugador existente
        self.cur.execute("""
            SELECT "idStaff" FROM "CuerpoTecnico"
            WHERE "NombreStaff" = %s AND "ApellidoStaff" = %s
        """, (nombre, apellidos))

        result = self.cur.fetchone()

        if result:
            print(f"[ACTAS][STAFF] Ya existe → id {result[0]}")
            return result[0]  # id_jugador ya existe

        # Crear jugador nuevo
        self.cur.execute("""
            INSERT INTO "CuerpoTecnico" ("NombreStaff", "ApellidoStaff")
            VALUES (%s, %s)
            RETURNING "idStaff"
        """, (nombre, apellidos))

        new_id = self.cur.fetchone()[0]
        self.conn.commit()

        print(f"[ACTAS][STAFF NUEVO] {nombre}{apellidos} → {new_id}")

        return new_id

    def ensure_staff_equipo(self, id_staff, id_equipo):
        self.cur.execute("""
            SELECT 1 FROM "StaffEquipos"
            WHERE "idStaff" = %s AND "idEquipo" = %s
        """, (id_staff, id_equipo))

        result = self.cur.fetchone()

        if result:
            print(f"[ACTAS][STAFF EQUIPO] Ya existe relación")
            return  # Ya existe

        self.cur.execute("""
            INSERT INTO "StaffEquipos" ("idStaff", "idEquipo")
            VALUES (%s, %s)
        """, (id_staff, id_equipo))

        self.conn.commit()
    
    def insert_staff_partido(self, id_staff, id_equipo, id_partido, rol):
        self.cur.execute("""
            INSERT INTO "StaffPartidos" ("idPartido", "idEquipo", "idStaff", "Rol")
            VALUES (%s, %s, %s, %s)
        """, (id_partido, id_equipo, id_staff, rol))

    def map_tipo_tarjeta(self, tipo):
        if tipo == "Groga":
            return "TarjetaAmarilla"
        elif tipo == "Segona Groga":
            return "TarjetaSegundaAmarilla"
        elif tipo == "Vermella":
            return "TarjetaRoja"
        else:
            return None
    
    def insert_evento(self, id_partido, id_jugador, id_equipo, minuto, tipo_evento):
        self.cur.execute("""
            INSERT INTO "Eventos" ("idPartido", "idJugador", "idEquipo", "Minuto", "TipoEvento")
            VALUES (%s, %s, %s, %s, %s)
        """, (id_partido, id_jugador, id_equipo, minuto, tipo_evento))

        self.conn.commit()
        print(f"[ACTAS][EVENTO] {tipo_evento} → Jugador {id_jugador} ({id_equipo}) min {minuto}")

    def map_tipo_gol(self, tipo):
        if tipo == "Normal":
            return "Gol"
        elif tipo == "Penal":
            return "GolPenal"
        elif tipo == "Propia":
            return "GolPropia"
        else:
            return None

    def get_equipo_del_jugador(self, id_jugador):
        self.cur.execute("""
            SELECT "idEquipo" FROM "JugadoresEquipos"
            WHERE "idJugador" = %s
        """, (id_jugador,))
        
        row = self.cur.fetchone()
        return row[0] if row else None
    
    def deducir_equipo_gol(self, id_jugador, id_local, id_visitante):
        id_equipo_jugador = self.get_equipo_del_jugador(id_jugador)

        if id_equipo_jugador == id_local:
            return id_local
        elif id_equipo_jugador == id_visitante:
            return id_visitante
        else:
            print(f"[WARNING] Jugador {id_jugador} no está asociado a local ni visitante.")
            return None
    
    def process_item(self, item, spider):
        if spider.name != "acta":
            return item
        
        print("\n" + "="*60)
        print(f"[ACTAS][ITEM] Procesando acta de partido {item.get('id_local')} vs {item.get('id_visitante')}")
        print("="*60)

        id_grupo = item.get("id_grupo")
        id_local = item.get("id_local")
        id_visitante = item.get("id_visitante")

        fecha = item.get("fecha") or None
        hora = item.get("hora") or None

        estado = item.get("estado")

        goles_local = item.get("goles_local")
        goles_visitante = item.get("goles_visitante")

        # ----------------------------------
        # ÁRBITRO
        # ----------------------------------
        nombre_arbitro = item.get("nombre_arbitro")
        apellidos_arbitro = item.get("apellidos_arbitro")
        delegacion = item.get("delegacion_arbitro")

        id_arbitro = None
        if nombre_arbitro or apellidos_arbitro:
            id_arbitro = self._get_or_create_arbitro(nombre_arbitro, apellidos_arbitro, delegacion)
        else:
            print("[ACTAS][ARBITRO] No hay árbitro en el acta.")

        # ----------------------------------
        # CAMPO
        # ----------------------------------
        codigo_estadio = item.get("codigo_estadio")
        
        id_campo = None
        if codigo_estadio:
            id_campo = self._get_or_create_campo(codigo_estadio)
        else:
            print("[ACTAS][CAMPO] No hay código de estadio.")

        # ----------------------------------
        # PARTIDOS
        # ----------------------------------
        sql = """
            UPDATE public."Partidos"
            SET "EstadoPartido" = COALESCE(%s, "EstadoPartido"),
                "GolesLocal" = COALESCE(%s, "GolesLocal"),
                "GolesVisitante" = COALESCE(%s, "GolesVisitante"),
                "idCampo" = COALESCE(%s, "idCampo"),
                "idArbitro" = COALESCE(%s, "idArbitro"),
                "FechaPartido" = COALESCE(%s, "FechaPartido"),
                "HoraPartido" = COALESCE(%s, "HoraPartido")
            WHERE "idEquipoLocal" = %s and "idEquipoVisitante" = %s and "idGrupo" = %s
            RETURNING "idPartido";
        """

        self.cur.execute(
            sql,
            (estado, goles_local, goles_visitante, id_campo,
             id_arbitro, fecha, hora, id_local, id_visitante, id_grupo
            ),
        )

        result = self.cur.fetchone()
        id_partido = result[0] if result else None

        print(f"[PARTIDOS][UPDATE] id_grupo={id_grupo}, id_local={id_local}, id_visitante={id_visitante}")

        # ----------------------------------
        # TITULARES
        # ----------------------------------
        jugadores_local = item.get("jugadores_local")
        jugadores_visitante = item.get("jugadores_visitante")

        for jugador in jugadores_local:
            nombre = jugador["nombre"]
            apellidos = jugador["apellidos"]
            dorsal = jugador.get("dorsal", None)

            id_jugador = self.get_or_create_jugador(nombre, apellidos)
            self.ensure_jugador_equipo(id_jugador, id_local)

            self.insert_alineacion(
                id_jugador=id_jugador,
                id_equipo=id_local,
                id_partido=id_partido,
                titular=True,
                dorsal=dorsal
            )

        for jugador in jugadores_visitante:
            nombre = jugador["nombre"]
            apellidos = jugador["apellidos"]
            dorsal = jugador.get("dorsal", None)

            id_jugador = self.get_or_create_jugador(nombre, apellidos)
            self.ensure_jugador_equipo(id_jugador, id_visitante)

            self.insert_alineacion(
                id_jugador=id_jugador,
                id_equipo=id_visitante,
                id_partido=id_partido,
                titular=True,
                dorsal=dorsal
            )

        # ----------------------------------
        # SUPLENTES
        # ----------------------------------
        suplentes_local = item.get("suplentes_local")
        suplentes_visitante = item.get("suplentes_visitante")

        for jugador in suplentes_local:
            nombre = jugador["nombre"]
            apellidos = jugador["apellidos"]
            dorsal = jugador.get("dorsal", None)

            id_jugador = self.get_or_create_jugador(nombre, apellidos)
            self.ensure_jugador_equipo(id_jugador, id_local)

            self.insert_alineacion(
                id_jugador=id_jugador,
                id_equipo=id_local,
                id_partido=id_partido,
                titular=False,
                dorsal=dorsal
            )
        
        for jugador in suplentes_visitante:
            nombre = jugador["nombre"]
            apellidos = jugador["apellidos"]
            dorsal = jugador.get("dorsal", None)

            id_jugador = self.get_or_create_jugador(nombre, apellidos)
            self.ensure_jugador_equipo(id_jugador, id_visitante)

            self.insert_alineacion(
                id_jugador=id_jugador,
                id_equipo=id_visitante,
                id_partido=id_partido,
                titular=False,
                dorsal=dorsal
            )
        
        # ----------------------------------
        # CUERPO TÉCNICO
        # ----------------------------------
        staff_local = item.get("staff_local")
        staff_visitante = item.get("staff_visitante")

        for staff in staff_local:
            nombre = staff["nombre"]
            apellidos = staff["apellidos"]
            rol = staff.get("rol", None)

            id_staff = self.get_or_create_staff(nombre, apellidos)
            self.ensure_staff_equipo(id_staff, id_local)

            self.insert_staff_partido(
                id_staff=id_staff,
                id_equipo=id_local,
                id_partido=id_partido,
                rol=rol
            )
        
        for staff in staff_visitante:
            nombre = staff["nombre"]
            apellidos = staff["apellidos"]
            rol = staff.get("rol", None)

            id_staff = self.get_or_create_staff(nombre, apellidos)
            self.ensure_staff_equipo(id_staff, id_visitante)

            self.insert_staff_partido(
                id_staff=id_staff,
                id_equipo=id_visitante,
                id_partido=id_partido,
                rol=rol
            )
        
        # ----------------------------------
        # EVENTOS
        # ----------------------------------
        tarjetas_local = item.get("tarjetas_local", [])
        tarjetas_visitante = item.get("tarjetas_visitante", [])
        goles = item.get("goles", [])

        for tarjeta in tarjetas_local:
            nombre = tarjeta["nombre"]
            apellidos = tarjeta["apellidos"]
            tipo = self.map_tipo_tarjeta(tarjeta["tipo"])
            minuto = tarjeta["minuto"]
            minuto = int(minuto) if minuto not in (None, "") else None
            dorsal_raw = tarjeta["dorsal"]
            
            dorsal_clean = (
                dorsal_raw.replace('\xa0', '').strip()
                if dorsal_raw else None
            )

            if dorsal_clean:
                dorsal = int(dorsal_clean)
                id_jugador = self.get_or_create_jugador(nombre, apellidos)

                self.ensure_jugador_equipo(id_jugador, id_local)
                
                self.insert_evento(id_partido, id_jugador, id_local, minuto, tipo)

            else:
                pass

        for tarjeta in tarjetas_visitante:
            nombre = tarjeta["nombre"]
            apellidos = tarjeta["apellidos"]
            tipo = self.map_tipo_tarjeta(tarjeta["tipo"])
            minuto = tarjeta["minuto"]
            minuto = int(minuto) if minuto not in (None, "") else None
            dorsal_raw = tarjeta["dorsal"]

            dorsal_clean = (
                dorsal_raw.replace('\xa0', '').strip()
                if dorsal_raw else None
            )

            if dorsal_clean:
                dorsal = int(dorsal_clean)
                id_jugador = self.get_or_create_jugador(nombre, apellidos)

                self.ensure_jugador_equipo(id_jugador, id_visitante)
                
                self.insert_evento(id_partido, id_jugador, id_visitante, minuto, tipo)
            else:
                pass

        for gol in goles:
            nombre = gol["nombre"]
            apellidos = gol["apellidos"]
            tipo = self.map_tipo_gol(gol["tipo"])
            minuto = gol["minuto"]

            id_jugador = self.get_or_create_jugador(nombre, apellidos)

            id_equipo = self.deducir_equipo_gol(id_jugador, id_local, id_visitante)

            self.insert_evento(id_partido, id_jugador, id_equipo, minuto, tipo)

        return item

class CamposPostgresPipeline:
    def open_spider(self, spider):
        if spider.name != "campo":
            return

        self.conn = get_connection()
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

        print("[PIPELINE CAMPO] Conectado a PostgreSQL")

    def close_spider(self, spider):
        if spider.name != "campo":
            return

        self.cur.close()
        self.conn.close()
        print("[PIPELINE CAMPO] Conexión cerrada")

    def process_item(self, item, spider):
        if spider.name != "campo":
            return item

        codigo_web = item.get("codigo")
        nombre_campo = item.get("nombre_campo")
        terreno = item.get("terreno")
        direccion = item.get("direccion")
        localidad = item.get("localidad")
        provincia = item.get("provincia")

        sql = """
            UPDATE public."Campos"
            SET "NombreCampo" = COALESCE(%s, "NombreCampo"),
                "Terreno" = COALESCE(%s, "Terreno"),
                "Direccion" = COALESCE(%s, "Direccion"),
                "Localidad" = COALESCE(%s, "Localidad"),
                "Provincia" = COALESCE(%s, "Provincia")
            WHERE "CodigoWeb" = %s;
        """

        self.cur.execute(
            sql,
            (nombre_campo, terreno, direccion, localidad, provincia, codigo_web),
        )

        print(f"[CAMPOS][UPDATE] Nombre: {nombre_campo}, Codigo: {codigo_web}")

        return item