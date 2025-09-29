import os
import logging
from datetime import datetime
from utils import (
    procesar_archivo,
    guardar_fallos,
    guardar_reporte_notificados,
    mover_a_procesados,
)

INPUT_DIR = "data/input"
OUTPUT_DIR = "data/output"
PROCESADOS_DIR = "data/procesados"

# Configuración de logs
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Nombre dinámico con fecha (dd-mm-yy)
    fecha_str = datetime.now().strftime("%d-%m-%y")
    output_fallos = os.path.join(OUTPUT_DIR, f"fallos_{fecha_str}.xlsx")
    output_notificados = os.path.join(OUTPUT_DIR, f"notificados_{fecha_str}.xlsx")

    fallos_totales: list[dict] = []
    notificados_totales: list[dict] = []

    # Iterar sobre todos los archivos en data/input
    for filename in os.listdir(INPUT_DIR):
        if not filename.lower().endswith((".xlsx", ".xls")):
            continue

        input_path = os.path.join(INPUT_DIR, filename)
        logging.info(f"Procesando archivo: {input_path}")

        fallos, notificados = procesar_archivo(input_path)
        fallos_totales.extend(fallos)
        notificados_totales.extend(notificados)

        mover_a_procesados(input_path, PROCESADOS_DIR)

    # Guardar reportes
    guardar_fallos(fallos_totales, output_fallos)
    guardar_reporte_notificados(notificados_totales, output_notificados)

    logging.info("Proceso completado")
    if fallos_totales:
        logging.warning(f"Se registraron fallos. Revisar {output_fallos}")
    if notificados_totales:
        logging.info(f"Reporte de notificados disponible en {output_notificados}")


if __name__ == "__main__":
    main()
