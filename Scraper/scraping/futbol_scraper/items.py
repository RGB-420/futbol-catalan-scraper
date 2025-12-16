import scrapy


class CompeticionItem(scrapy.Item):
    nombre = scrapy.Field()
    categoria = scrapy.Field()
    edad_maxima = scrapy.Field()
    organizador = scrapy.Field()
    nivel = scrapy.Field()
    slug = scrapy.Field()
    codigo_web = scrapy.Field()

class GrupoItem(scrapy.Item):
    codigo_competicion = scrapy.Field() 
    numero_grupo = scrapy.Field()
    temporada = scrapy.Field()
    region = scrapy.Field()
    slug = scrapy.Field()

class EquipoItem(scrapy.Item):
    id_grupo = scrapy.Field()     
    temporada = scrapy.Field()     

    nombre_equipo = scrapy.Field()
    equipo_slug = scrapy.Field()
    club_slug = scrapy.Field()
    categoria = scrapy.Field()
    nivel = scrapy.Field()    

class ClubItem(scrapy.Item):
    slug = scrapy.Field()
    localidad = scrapy.Field()
    delegacion = scrapy.Field()
    provincia = scrapy.Field()

class CalendarioItem(scrapy.Item):
    # Datos de la competición
    id_grupo = scrapy.Field()
    temporada = scrapy.Field()
    jornada = scrapy.Field()

    # Equipos
    equipo_local_slug = scrapy.Field()
    equipo_visitante_slug = scrapy.Field()

    slug_competicion = scrapy.Field()
    abreviatura_competicion = scrapy.Field()

class ActasItem(scrapy.Item):
    id_grupo = scrapy.Field()
    id_local = scrapy.Field()
    id_visitante = scrapy.Field()

    fecha = scrapy.Field()
    hora = scrapy.Field()

    estado = scrapy.Field()

    goles_local = scrapy.Field()
    goles_visitante = scrapy.Field()

    codigo_estadio = scrapy.Field()

    nombre_arbitro = scrapy.Field()
    apellidos_arbitro = scrapy.Field()
    delegacion_arbitro = scrapy.Field()

    jugadores_local = scrapy.Field()
    jugadores_visitante = scrapy.Field()

    suplentes_local = scrapy.Field()
    suplentes_visitante = scrapy.Field()

    staff_local = scrapy.Field()
    staff_visitante = scrapy.Field()

    goles = scrapy.Field()

    tarjetas_local = scrapy.Field()
    tarjetas_visitante = scrapy.Field()

class CamposItem(scrapy.Item):
    codigo = scrapy.Field()
    nombre_campo = scrapy.Field()
    terreno = scrapy.Field()
    direccion = scrapy.Field()
    localidad = scrapy.Field()
    provincia = scrapy.Field()