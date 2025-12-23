import argparse
import os
from pathlib import Path

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scraping.futbol_scraper.spiders.competiciones_spider import CompeticionesSpider
from scraping.futbol_scraper.spiders.grupos_spider import GruposSpider
from scraping.futbol_scraper.spiders.equipos_spider import EquiposSpider
from scraping.futbol_scraper.spiders.clubes_spider import ClubesSpider
from scraping.futbol_scraper.spiders.calendario_spider import CalendarioSpider
from scraping.futbol_scraper.spiders.actas_spider import ActasSpider
from scraping.futbol_scraper.spiders.campos_spider import CamposSpider

from db.connection import get_connection

BASE_DIR = Path(__file__).resolve().parent

def get_process():
    """Crea un CrawlerProcess con los settings del proyecto."""
    # 1) Decirle a Scrapy dónde está el settings.py
    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "scraping.futbol_scraper.settings")

    # 2) Asegurar que la carpeta actual es la raíz del proyecto (donde está cli.py)
    os.chdir(BASE_DIR)

    # 3) Cargar settings del proyecto
    settings = get_project_settings()

    # Debug opcional para comprobar que está leyendo bien:
    print("[SETTINGS] BOT_NAME:", settings.get("BOT_NAME"))
    print("[SETTINGS] ITEM_PIPELINES:", dict(settings.get("ITEM_PIPELINES", {})))

    # 4) Crear el proceso con esos settings
    return CrawlerProcess(settings)

def run_competiciones_spider():
    process = get_process()
    process.crawl(CompeticionesSpider)
    process.start()

def run_grupos_spider(codigo_competicion):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT codigo_web FROM public.competiciones WHERE codigo_web IS NOT NULL;')
    rows = cur.fetchall()
    cur.close()
    conn.close()

    codigos = [r[0] for r in rows]
    print(f"[CLI] Voy a generar grupos para {len(codigos)} competiciones")

    # 2) Lanzar un spider por cada competición
    process = get_process()

    for codigo in codigos:
        process.crawl(
            GruposSpider,
            codigo_competicion=str(codigo),
        )

    process.start()

def run_equipos_spider(temporada="2025-26", temporada_ruta="2526", tipo="futbol-11"):
    process = get_process()
    process.crawl(
        EquiposSpider,
        temporada=temporada,
        temporada_ruta=temporada_ruta,
        tipo=tipo,
    )
    process.start()

def run_clubes_spider(temporada_ruta="2526"):
    process = get_process()
    process.crawl(
        ClubesSpider,
        temporada_ruta=temporada_ruta,
    )
    process.start()

def run_calendario_spider(temporada_ruta="2526"):
    process = get_process()
    process.crawl(
        CalendarioSpider,
        temporada_ruta=temporada_ruta,
    )
    process.start()

def run_acta_spider(temporada_ruta="2526", toda_temporada=False):
    process = get_process()
    process.crawl(
        ActasSpider,
        temporada_ruta=temporada_ruta,
        toda_temporada=toda_temporada
    )
    process.start()

def run_campo_spider():
    process = get_process()
    process.crawl(
        CamposSpider
    )
    process.start()


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape_parser = subparsers.add_parser("scrape")
    scrape_parser.add_argument("target", choices=["competiciones", "grupos", "equipos", 
                                                  "clubes", "calendarios", "actas", "campos"])

    scrape_parser.add_argument("--temporada", default="2025-26")
    scrape_parser.add_argument("--temporada_ruta", default="2526")
    scrape_parser.add_argument("--tipo", default="futbol-11")
    scrape_parser.add_argument("--toda_temporada", action="store_true")
    args = parser.parse_args()


    if args.command == "scrape":
        if args.target == "competiciones":
            run_competiciones_spider()
        elif args.target == "grupos":
            run_grupos_spider() 
        elif args.target == "equipos":
            run_equipos_spider(
                temporada=args.temporada,
                temporada_ruta=args.temporada_ruta,
                tipo=args.tipo,
            )
        elif args.target == "clubes":
            run_clubes_spider(temporada_ruta=args.temporada_ruta) 
        elif args.target == "calendarios":
            run_calendario_spider() 
        elif args.target == "actas":
            run_acta_spider(temporada_ruta=args.temporada_ruta, toda_temporada=args.toda_temporada) 
        elif args.target == "campos":
            run_campo_spider() 


if __name__ == "__main__":
    main()
