import os
import shutil
import pandas as pd
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, To
from dotenv import load_dotenv

# --- Logging ---
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# --- Configuración ---
load_dotenv()
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
SENDGRID_TEMPLATE_ID = os.getenv("SENDGRID_TEMPLATE_ID")
EMAIL_FROM = os.getenv("EMAIL_FROM")



def leer_excel(path_excel: str) -> list[dict]:
    df = pd.read_excel(path_excel)
    return df.to_dict(orient="records")



def decidir_link(fila: dict) -> tuple[str, str]:
    """
    Dada una fila de datos, retorna (link, sender_team).
    Lanza ValueError si el source no es válido.
    """
    source = str(fila.get("source", "")).strip().lower()

    if source == "pluxee":
        return "https://enlace-pluxee.com", "Onboarding Cobee by Pluxee"
    elif source == "cobee":
        return "https://enlace-cobee.com", "New Business Onboarding"

    raise ValueError(f"Source inválido: {fila}")


def enviar_correo(to_emails: list[str], link: str, company_name: str, sender_team: str):
    """
    Envía un correo usando SendGrid con un template dinámico.
    """
    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=[To(email) for email in to_emails],
    )
    message.template_id = SENDGRID_TEMPLATE_ID
    message.dynamic_template_data = {
        "company_name": company_name,
        "link": link,
        "sender_team": sender_team
    }

    sg = SendGridAPIClient(SENDGRID_API_KEY)
    response = sg.send(message)
    logging.info(
        f"Correo enviado a {', '.join(to_emails)}, status {response.status_code}"
    )


def limpiar_emails(raw_email: str) -> list[str]:
    """
    Convierte un string como '[a@dom.com, b@dom.com]' en ['a@dom.com', 'b@dom.com']
    """
    if not raw_email:
        return []
    cleaned = raw_email.strip("[] ")
    return [e.strip() for e in cleaned.split(",") if e.strip()]



def procesar_archivo(path_excel: str) -> tuple[list[dict], list[dict]]:
    """
    Procesa un Excel y devuelve:
      - lista de fallos (errores reales)
      - lista de notificaciones enviadas (para reporte)
    """
    filas = leer_excel(path_excel)
    fallos: list[dict] = []
    enviados: set[str] = set()
    notificados: list[dict] = []

    for fila in filas:
        corp = str(fila.get("corporation_name", "")).strip().lower()

        # Si ya enviamos a esta corporación → skip (NO es fallo)
        if corp and corp in enviados:
            logging.info(f"Saltada fila duplicada, corporation ya notificada: {corp}")
            continue

        emails = limpiar_emails(fila.get("email", ""))
        if not emails:
            logging.warning(f"Fila sin emails: {fila}")
            fallos.append({**fila, "motivo_fallo": "sin_emails"})
            continue

        try:
            link, sender_team = decidir_link(fila)
        except ValueError as e:
            fallos.append({**fila, "motivo_fallo": str(e)})
            continue

        try:
            enviar_correo(
                emails,
                link,
                fila.get("company_name", ""),
                sender_team
            )
            if corp:
                enviados.add(corp)
            notificados.append({
                "corporation_name": fila.get("corporation_name", ""),
                "emails_enviados": ", ".join(emails),
                "link": link,
                "sender_team": sender_team
            })
        except Exception as e:
            logging.error(f"Error enviando correo a {emails}: {e}")
            fallos.append({**fila, "motivo_fallo": f"error_envio: {e}"})

    return fallos, notificados



def guardar_fallos(fallos: list[dict], output_path: str):
    if fallos:
        df = pd.DataFrame(fallos)
        df.to_excel(output_path, index=False)
        logging.info(f"Archivo de fallos guardado en {output_path}")


def guardar_reporte_notificados(notificados: list[dict], output_path: str):
    if not notificados:
        return
    df = pd.DataFrame(notificados)
    df.to_excel(output_path, index=False)
    logging.info(f"Reporte de notificados guardado en {output_path}")



def mover_a_procesados(input_path: str, procesados_dir: str):
    os.makedirs(procesados_dir, exist_ok=True)
    dest_path = os.path.join(procesados_dir, os.path.basename(input_path))
    shutil.move(input_path, dest_path)
    logging.info(f"Archivo movido a {dest_path}")
