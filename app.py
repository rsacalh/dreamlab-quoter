import streamlit as st
import pandas as pd
import os
import json
import altair as alt
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
from difflib import get_close_matches

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

# Email
import smtplib
from email.message import EmailMessage

# PDF (ReportLab)
try:
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import inch
    from io import BytesIO
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False


# ==================== CONFIG INICIAL ====================
st.set_page_config(
    page_title="Dream Lab Quoter",
    page_icon="🧾",
    layout="centered"
)

# ==================== .ENV ====================
load_dotenv()

def _get_secret(name: str, default: str = "") -> str:
    try:
        if name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass
    return os.getenv(name, default)

MONGODB_URI = _get_secret("MONGODB_URI", "")
MONGO_DB_NAME = _get_secret("MONGO_DB_NAME", "dreamlab")
INFLUENCERS_ADMIN_PASSWORD = _get_secret("INFLUENCERS_ADMIN_PASSWORD", "123456789")
EMAIL_SENDER = _get_secret("EMAIL_SENDER", "")
EMAIL_RECEIVER = _get_secret("EMAIL_RECEIVER", "rsacalh@gmail.com")
EMAIL_APP_PASSWORD = _get_secret("EMAIL_APP_PASSWORD", "")
APP_USERS_JSON = _get_secret("APP_USERS_JSON", "")

DEFAULT_APP_USERS = {
    "dreamlab": {
        "password": "DreamLab2026!",
        "role": "admin",
        "display_name": "Dream Lab"
    },
    "be_group": {
        "password": "be_group_123",
        "role": "client",
        "display_name": "Be Group"
    },
    "erre_lab": {
        "password": "erre_lab_123",
        "role": "client",
        "display_name": "Erre Lab"
    },
    "universal": {
        "password": "universal_123",
        "role": "client",
        "display_name": "Universal"
    },
    "media_lab": {
        "password": "media_lab_123",
        "role": "client",
        "display_name": "Media Lab"
    },
    "lineal_agency": {
        "password": "lineal_agency_123",
        "role": "client",
        "display_name": "Lineal Agency"
    },
    "vking": {
        "password": "vking_123",
        "role": "client",
        "display_name": "Vking"
    },
}


def _normalize_username(username: str) -> str:
    return (username or "").strip().lower()


def load_app_users() -> Dict[str, Dict[str, str]]:
    raw_users = DEFAULT_APP_USERS

    if APP_USERS_JSON.strip():
        try:
            raw_users = json.loads(APP_USERS_JSON)
        except Exception as e:
            print(f"❌ Error leyendo APP_USERS_JSON: {e}. Se usarán usuarios por defecto.")

    normalized = {}
    for username, data in raw_users.items():
        uname = _normalize_username(username)
        if not uname:
            continue

        normalized[uname] = {
            "password": str(data.get("password", "")).strip(),
            "role": str(data.get("role", "client")).strip().lower(),
            "display_name": str(data.get("display_name", uname)).strip(),
        }

    return normalized


APP_USERS = load_app_users()
ADMIN_USERS = {u: d for u, d in APP_USERS.items() if d.get("role") == "admin"}
CLIENT_SEED_USERS = {u: d for u, d in APP_USERS.items() if d.get("role") == "client"}


# ==================== CONFIG ====================
LOGO_PATH = "dreamlab_logo.png"
BANNER_PATH = "dreamlab_banner.png"


DEFAULT_BRANDS = [
    "Samsung", "Starbucks", "Spotify", "Ruffles", "Apple", "Pilgrims", "Microsoft", "HBO",
    "Netflix", "Disney+", "Prime Video", "Paramount+", "Max", "YouTube", "YouTube Music",
    "TikTok", "Instagram", "Facebook", "WhatsApp", "Meta", "Google", "OpenAI", "ChatGPT",
    "Amazon", "Mercado Libre", "Liverpool", "Palacio de Hierro", "Walmart", "Costco",
    "Sam's Club", "OXXO", "7-Eleven", "Chedraui", "Soriana", "La Comer", "City Market",
    "Uber", "Uber Eats", "Didi", "Rappi", "Airbnb", "Booking.com", "Expedia",
    "Nike", "Adidas", "Puma", "Under Armour", "New Balance", "Reebok", "Lululemon",
    "Zara", "H&M", "Pull&Bear", "Bershka", "Massimo Dutti", "Mango", "Uniqlo",
    "Levi's", "Calvin Klein", "Tommy Hilfiger", "Gucci", "Prada", "Louis Vuitton",
    "Dior", "Rolex", "Cartier", "Ray-Ban", "Oakley",
    "Coca-Cola", "Pepsi", "Sprite", "Fanta", "Dr Pepper", "Gatorade", "Powerade",
    "Electrolit", "Red Bull", "Monster", "Ciel", "Bonafont", "Topo Chico",
    "Bimbo", "Sabritas", "Doritos", "Cheetos", "Takis", "Barcel", "Oreo", "Gamesa",
    "Marinela", "Ricolino", "Hershey's", "Ferrero", "Ferrero Rocher", "Nutella",
    "M&M's", "Snickers", "KitKat", "Nestlé", "Kellogg's", "Lala", "Alpura", "Danone",
    "Yakult", "Jumex", "Del Valle", "Heinz", "Hellmann's", "Knorr", "McCormick",
    "McDonald's", "Burger King", "Carl's Jr.", "KFC", "Domino's", "Pizza Hut",
    "Subway", "Little Caesars", "Chili's", "Starbucks Reserve",
    "Telcel", "AT&T", "Movistar", "Telmex", "Izzi", "Totalplay", "Megacable",
    "BBVA", "Santander", "Banorte", "HSBC", "Citibanamex", "Scotiabank", "Inbursa",
    "American Express", "Visa", "Mastercard", "PayPal", "Stripe", "Nu", "Klar",
    "Toyota", "Honda", "Nissan", "Ford", "Chevrolet", "Kia", "Hyundai", "Mazda",
    "Volkswagen", "Audi", "BMW", "Mercedes-Benz", "Tesla", "BYD", "Jeep", "Peugeot",
    "Intel", "AMD", "NVIDIA", "Dell", "HP", "Lenovo", "Acer", "Asus", "Logitech",
    "Sony", "PlayStation", "Xbox", "Nintendo", "JBL", "Bose", "Canon", "GoPro",
    "L'Oréal", "Maybelline", "Garnier", "Dove", "Nivea", "Axe", "Old Spice",
    "Gillette", "Colgate", "Oral-B", "Pantene", "Head & Shoulders", "Sedal",
    "Sephora", "MAC Cosmetics", "Clinique",
    "Corona", "Modelo", "Victoria", "Heineken", "Bud Light", "Jose Cuervo",
    "Don Julio", "Bacardí", "Smirnoff", "Absolut",
    "HBO Max", "Crunchyroll", "Twitch", "EA Sports", "Activision", "Riot Games",
    "Adobe", "Canva", "Notion", "Slack", "Zoom", "Dropbox", "Xiaomi", "Huawei",
    "Motorola", "OnePlus", "Nothing", "Aerie", "Abercrombie & Fitch", "Hollister",
    "Cinépolis", "Cinemex", "Tecate", "AstraZeneca", "Pfizer", "Bayer", "Genomma Lab"
]


# ==================== ESTILOS ====================
def inject_dreamlab_styles():
    st.markdown("""
    <style>
    .stApp {
        background: #ffffff;
        color: #202124;
    }

    [data-testid="stSidebar"] {
        background: #f7f8fb;
        border-right: 1px solid #ececec;
    }

    h1, h2, h3, h4 {
        color: #1f2430;
    }

    .dreamlab-red-text {
        color: #e53935 !important;
        font-weight: 700;
    }

    .dreamlab-subtle {
        color: #7a7a7a;
        font-size: 0.98rem;
    }

    .dreamlab-banner-wrap {
        margin-top: 0.35rem;
        margin-bottom: 1rem;
    }

    .dreamlab-section-label {
        color: #e53935;
        font-weight: 700;
        font-size: 1.02rem;
        margin-bottom: 0.2rem;
    }

    .dreamlab-big-section-label {
        color: #e53935;
        font-weight: 800;
        font-size: 1.85rem;
        margin-top: 0.2rem;
        margin-bottom: 0.8rem;
        line-height: 1.2;
    }

    .dreamlab-small-label {
        color: #e53935;
        font-weight: 600;
        font-size: 0.95rem;
    }

    div[data-baseweb="select"] > div,
    div[data-baseweb="input"] > div,
    .stTextInput > div > div > input,
    .stNumberInput input,
    textarea {
        border-radius: 14px !important;
    }

    .stButton > button,
    .stDownloadButton > button {
        border-radius: 12px !important;
        border: 1px solid #e53935 !important;
    }

    .stButton > button[kind="primary"] {
        background-color: #e53935 !important;
        color: white !important;
    }

    .stButton > button:hover,
    .stDownloadButton > button:hover {
        border-color: #c62828 !important;
        color: #c62828 !important;
    }

    .stSlider [data-baseweb="slider"] div[role="slider"] {
        background-color: #ff5252 !important;
        box-shadow: 0 0 0 4px rgba(255,82,82,0.15) !important;
    }

    .stSlider [data-testid="stTickBar"] div {
        background-color: #ffe5e5 !important;
    }

    .stCheckbox label span,
    .stRadio label span {
        color: #202124 !important;
    }

    .st-emotion-cache-16txtl3 {
        padding-top: 2rem;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    .red-divider {
        border: none;
        height: 1px;
        background: #ececec;
        margin-top: 1.1rem;
        margin-bottom: 1.1rem;
    }

    .metric-red [data-testid="stMetricLabel"] {
        color: #e53935 !important;
        font-weight: 700 !important;
    }

    .brand-add-box {
        border: 1px solid #ececec;
        border-radius: 14px;
        padding: 16px;
        margin-top: 8px;
        margin-bottom: 8px;
        background: #fafafa;
    }
    </style>
    """, unsafe_allow_html=True)


inject_dreamlab_styles()


# ==================== HELPERS ====================
def _fmt_mxn(x: float) -> str:
    return f"MXN {x:,.2f}"


def _sanitize_app_password(pwd: str) -> str:
    return (pwd or "").replace(" ", "").strip()


def _safe_float(val, default=0.0):
    try:
        if val is None:
            return default
        return float(val)
    except Exception:
        return default


def _get_logo_path() -> Optional[str]:
    p = Path(LOGO_PATH)
    return str(p) if p.exists() else None


def _get_banner_path() -> Optional[str]:
    p = Path(BANNER_PATH)
    return str(p) if p.exists() else None


def render_main_header():
    logo = _get_logo_path()

    st.markdown('<div style="margin-top:15px; margin-bottom:10px;">', unsafe_allow_html=True)
    col_logo, col_title = st.columns([0.8, 8])

    with col_logo:
        if logo:
            st.markdown('<div style="margin-top:8px;"></div>', unsafe_allow_html=True)
            st.image(logo, width=52)

    with col_title:
        st.title("Dream Lab Quoter")

    st.markdown('</div>', unsafe_allow_html=True)


def render_cotizador_banner():
    banner = _get_banner_path()
    if banner:
        st.markdown('<div class="dreamlab-banner-wrap">', unsafe_allow_html=True)
        st.image(banner, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)


def normalize_brand_name(name: str) -> str:
    return " ".join((name or "").strip().lower().split())


def clean_brand_display_name(name: str) -> str:
    return " ".join((name or "").strip().split())


def unique_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    output = []
    for item in items:
        if item not in seen:
            seen.add(item)
            output.append(item)
    return output


def set_brand_pending_updates(
    search_query: Optional[str] = None,
    selected_brand: Optional[str] = None,
    new_brand_input: Optional[str] = None,
    notice_level: Optional[str] = None,
    notice_message: Optional[str] = None,
):
    st.session_state["brand_pending_updates"] = {
        "search_query": search_query,
        "selected_brand": selected_brand,
        "new_brand_input": new_brand_input,
        "notice_level": notice_level,
        "notice_message": notice_message,
    }


def apply_brand_pending_updates():
    pending = st.session_state.pop("brand_pending_updates", None)
    if not pending:
        return

    if "selected_brand" in pending:
        st.session_state["brand_select_choice"] = pending["selected_brand"]

    if pending.get("new_brand_input") is not None:
        st.session_state["new_brand_input"] = pending["new_brand_input"]

    if pending.get("notice_level") and pending.get("notice_message"):
        st.session_state["brand_notice"] = {
            "level": pending["notice_level"],
            "message": pending["notice_message"],
        }


def show_brand_notice():
    notice = st.session_state.pop("brand_notice", None)
    if not notice:
        return

    level = notice.get("level", "info")
    message = notice.get("message", "")
    if not message:
        return

    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)


def safe_json_loads(value):
    try:
        return json.loads(value)
    except Exception:
        return {}


def clean_owner_display_name(val) -> str:
    text = str(val or "").strip()
    return text if text else "Sin cliente"


# ==================== LOGIN ====================
def logout_user():
    keys_to_remove = [
        "user_authenticated",
        "user_info",
        "admin_section_authenticated",
        "last_quote_payload",
        "last_actions_rows",
        "last_extras_rows",
        "brand_select_choice",
        "new_brand_input",
        "similar_brand_radio",
        "brand_pending_updates",
        "brand_notice",
    ]
    for key in keys_to_remove:
        st.session_state.pop(key, None)
    st.rerun()


def require_admin_role():
    user_info = st.session_state.get("user_info", {})
    if user_info.get("role") != "admin":
        st.error("No tienes acceso a esta sección.")
        st.stop()


def require_admin_section_password(section_key: str):
    require_admin_role()

    if "admin_section_authenticated" not in st.session_state:
        st.session_state.admin_section_authenticated = False

    if not st.session_state.admin_section_authenticated:
        st.info("Sección protegida (admin).")
        pwd = st.text_input("Contraseña admin:", type="password", key=f"admin_pwd_{section_key}")
        if st.button("Entrar", key=f"admin_enter_{section_key}"):
            if pwd == INFLUENCERS_ADMIN_PASSWORD:
                st.session_state.admin_section_authenticated = True
                st.rerun()
            else:
                st.error("Contraseña incorrecta")
        st.stop()


# ==================== PDF ====================
def build_quote_pdf_bytes(
    quote_payload: Dict[str, Any],
    breakdown_actions_df: pd.DataFrame,
    breakdown_extras_df: pd.DataFrame
) -> bytes:
    if not REPORTLAB_AVAILABLE:
        return b""

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    _, height = letter

    left = 0.8 * inch
    y = height - 0.9 * inch

    c.setFont("Helvetica-Bold", 16)
    c.drawString(left, y, "Dream Lab — Cotización de Campaña")
    y -= 0.35 * inch

    c.setFont("Helvetica", 10)
    c.drawString(left, y, f"Fecha: {quote_payload.get('timestamp','-')}")
    y -= 0.25 * inch

    c.setFont("Helvetica-Bold", 11)
    c.drawString(left, y, "Datos de la campaña")
    y -= 0.20 * inch

    c.setFont("Helvetica", 10)
    c.drawString(left, y, f"Marca: {quote_payload.get('marca','')}")
    y -= 0.18 * inch
    c.drawString(left, y, f"Cliente: {quote_payload.get('cliente','')}")
    y -= 0.18 * inch
    c.drawString(left, y, f"Campaña: {quote_payload.get('campania','')}")
    y -= 0.18 * inch
    c.drawString(left, y, f"Talento: {quote_payload.get('talento','')}")
    y -= 0.18 * inch
    c.drawString(left, y, f"Temporalidad: {quote_payload.get('temporalidad','')}")
    y -= 0.30 * inch

    c.setFont("Helvetica-Bold", 11)
    c.drawString(left, y, "Acciones cotizadas")
    y -= 0.22 * inch

    c.setFont("Helvetica-Bold", 9)
    c.drawString(left, y, "Acción")
    c.drawString(left + 2.4 * inch, y, "Cantidad")
    c.drawRightString(left + 4.8 * inch, y, "P. unitario")
    c.drawRightString(left + 6.0 * inch, y, "Subtotal")
    y -= 0.16 * inch
    c.setFont("Helvetica", 9)

    if breakdown_actions_df.empty:
        c.drawString(left, y, "No se cotizaron acciones.")
        y -= 0.20 * inch
    else:
        for _, r in breakdown_actions_df.iterrows():
            if y < 1.2 * inch:
                c.showPage()
                y = height - 0.9 * inch
                c.setFont("Helvetica-Bold", 11)
                c.drawString(left, y, "Acciones (continuación)")
                y -= 0.25 * inch
                c.setFont("Helvetica", 9)

            c.drawString(left, y, str(r["Acción"]))
            c.drawString(left + 2.4 * inch, y, str(int(r["Cantidad"])))
            c.drawRightString(left + 4.8 * inch, y, _fmt_mxn(float(r["Precio unitario (MXN)"])))
            c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(r["Subtotal (MXN)"])))
            y -= 0.16 * inch

    y -= 0.10 * inch

    c.setFont("Helvetica-Bold", 11)
    c.drawString(left, y, "Extras")
    y -= 0.22 * inch

    c.setFont("Helvetica-Bold", 9)
    c.drawString(left, y, "Extra")
    c.drawRightString(left + 6.0 * inch, y, "Monto")
    y -= 0.16 * inch
    c.setFont("Helvetica", 9)

    if breakdown_extras_df.empty:
        c.drawString(left, y, "No se agregaron extras.")
        y -= 0.20 * inch
    else:
        for _, r in breakdown_extras_df.iterrows():
            if y < 1.2 * inch:
                c.showPage()
                y = height - 0.9 * inch
                c.setFont("Helvetica-Bold", 11)
                c.drawString(left, y, "Extras (continuación)")
                y -= 0.25 * inch
                c.setFont("Helvetica", 9)

            c.drawString(left, y, str(r["Extra"]))
            c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(r["Monto (MXN)"])))
            y -= 0.16 * inch

    y -= 0.20 * inch
    if y < 1.5 * inch:
        c.showPage()
        y = height - 0.9 * inch

    c.setFont("Helvetica-Bold", 12)
    c.drawString(left, y, "Total base:")
    c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(quote_payload.get("total_base", 0.0))))
    y -= 0.20 * inch

    c.drawString(left, y, "Total extras:")
    c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(quote_payload.get("total_extras", 0.0))))
    y -= 0.20 * inch

    c.drawString(left, y, "Total final:")
    c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(quote_payload.get("total_final", 0.0))))
    y -= 0.35 * inch

    c.setFont("Helvetica", 8)
    c.drawString(left, y, "Nota: Cotización educativa. Vigencia y condiciones se pueden formalizar en la siguiente versión.")
    c.showPage()
    c.save()

    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def build_history_pdf_bytes(quotes_payloads: List[Dict[str, Any]]) -> bytes:
    if not REPORTLAB_AVAILABLE:
        return b""

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    _, height = letter
    left = 0.8 * inch

    c.setFont("Helvetica-Bold", 16)
    c.drawString(left, height - 0.9 * inch, "Dream Lab — Historial de Cotizaciones")
    c.setFont("Helvetica", 10)
    c.drawString(left, height - 1.2 * inch, f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    c.showPage()

    for q in quotes_payloads:
        y = height - 0.9 * inch

        c.setFont("Helvetica-Bold", 13)
        c.drawString(left, y, f"Cotización — {q.get('timestamp','-')}")
        y -= 0.25 * inch

        cliente_pdf = q.get("owner_display_name") or q.get("cliente", "")

        c.setFont("Helvetica", 10)
        c.drawString(left, y, f"Cliente: {cliente_pdf} | Marca: {q.get('marca','')} | Campaña: {q.get('campania','')}")
        y -= 0.18 * inch
        c.drawString(left, y, f"Talento: {q.get('talento','')}")
        y -= 0.18 * inch
        c.drawString(left, y, f"Temporalidad: {q.get('temporalidad','')} | Total: {_fmt_mxn(float(q.get('total_final', 0.0)))}")
        y -= 0.25 * inch

        actions = q.get("acciones", {})
        tarif = q.get("tarifario_unitario", {})

        c.setFont("Helvetica-Bold", 11)
        c.drawString(left, y, "Acciones")
        y -= 0.18 * inch
        c.setFont("Helvetica", 9)

        printed_any = False
        for accion, qty in actions.items():
            if int(qty) > 0:
                printed_any = True
                subtotal = float(tarif.get(accion, 0.0)) * int(qty)
                c.drawString(left, y, f"- {accion} x{qty}")
                c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(subtotal))
                y -= 0.14 * inch
                if y < 1.3 * inch:
                    c.showPage()
                    y = height - 0.9 * inch
                    c.setFont("Helvetica", 9)

        if not printed_any:
            c.drawString(left, y, "- (sin acciones)")
            y -= 0.14 * inch

        y -= 0.12 * inch

        extras = q.get("extras", {})
        c.setFont("Helvetica-Bold", 11)
        c.drawString(left, y, "Extras")
        y -= 0.18 * inch
        c.setFont("Helvetica", 9)

        extras_any = False
        for k, v in extras.items():
            if v.get("enabled"):
                extras_any = True
                c.drawString(left, y, f"- {k}: {v.get('detail','')}")
                c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(v.get("amount_mxn", 0.0))))
                y -= 0.14 * inch
                if y < 1.3 * inch:
                    c.showPage()
                    y = height - 0.9 * inch
                    c.setFont("Helvetica", 9)

        if not extras_any:
            c.drawString(left, y, "- (sin extras)")
            y -= 0.14 * inch

        y -= 0.12 * inch
        c.setFont("Helvetica-Bold", 10)
        c.drawString(left, y, "Total final:")
        c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(q.get("total_final", 0.0))))

        c.showPage()

    c.save()
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def build_sent_history_pdf_bytes(sent_payloads: List[Dict[str, Any]]) -> bytes:
    if not REPORTLAB_AVAILABLE:
        return b""

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    _, height = letter
    left = 0.8 * inch

    c.setFont("Helvetica-Bold", 16)
    c.drawString(left, height - 0.9 * inch, "Dream Lab — Historial de Propuestas Enviadas")
    c.setFont("Helvetica", 10)
    c.drawString(left, height - 1.2 * inch, f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    c.showPage()

    for p in sent_payloads:
        payload = p.get("payload", {}) or {}
        y = height - 0.9 * inch

        c.setFont("Helvetica-Bold", 13)
        c.drawString(left, y, f"Envío — {p.get('sent_at','-')}")
        y -= 0.25 * inch

        c.setFont("Helvetica", 10)
        c.drawString(left, y, f"Para: {p.get('to_email','-')} | Éxito: {p.get('success', False)}")
        y -= 0.18 * inch
        c.drawString(left, y, f"Asunto: {p.get('subject','-')}")
        y -= 0.18 * inch

        cliente_pdf = payload.get("owner_display_name") or payload.get("cliente", "")
        c.drawString(left, y, f"Cliente: {cliente_pdf} | Marca: {payload.get('marca','')} | Campaña: {payload.get('campania','')}")
        y -= 0.18 * inch
        c.drawString(left, y, f"Talento: {payload.get('talento','')}")
        y -= 0.18 * inch
        c.drawString(left, y, f"Temporalidad: {payload.get('temporalidad','')}")
        y -= 0.25 * inch

        c.setFont("Helvetica-Bold", 11)
        c.drawString(left, y, "Acciones")
        y -= 0.18 * inch
        c.setFont("Helvetica", 9)

        acciones = payload.get("acciones", {})
        tarifario = payload.get("tarifario_unitario", {})
        printed_any = False
        for accion, qty in acciones.items():
            qty = int(qty or 0)
            if qty > 0:
                printed_any = True
                unit = float(tarifario.get(accion, 0.0))
                subtotal = unit * qty
                c.drawString(left, y, f"- {accion} x{qty}")
                c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(subtotal))
                y -= 0.14 * inch
                if y < 1.3 * inch:
                    c.showPage()
                    y = height - 0.9 * inch
                    c.setFont("Helvetica", 9)

        if not printed_any:
            c.drawString(left, y, "- (sin acciones)")
            y -= 0.14 * inch

        y -= 0.12 * inch
        c.setFont("Helvetica-Bold", 11)
        c.drawString(left, y, "Extras")
        y -= 0.18 * inch
        c.setFont("Helvetica", 9)

        extras = payload.get("extras", {})
        extras_any = False
        for extra_name, extra_val in extras.items():
            if extra_val.get("enabled"):
                extras_any = True
                detail = extra_val.get("detail", "")
                amount = float(extra_val.get("amount_mxn", 0.0))
                c.drawString(left, y, f"- {extra_name}: {detail}")
                c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(amount))
                y -= 0.14 * inch
                if y < 1.3 * inch:
                    c.showPage()
                    y = height - 0.9 * inch
                    c.setFont("Helvetica", 9)

        if not extras_any:
            c.drawString(left, y, "- (sin extras)")
            y -= 0.14 * inch

        y -= 0.15 * inch
        c.setFont("Helvetica-Bold", 10)
        c.drawString(left, y, "Total base:")
        c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(payload.get("total_base", 0.0))))
        y -= 0.16 * inch

        c.drawString(left, y, "Total extras:")
        c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(payload.get("total_extras", 0.0))))
        y -= 0.16 * inch

        c.drawString(left, y, "Total final:")
        c.drawRightString(left + 6.0 * inch, y, _fmt_mxn(float(payload.get("total_final", 0.0))))
        y -= 0.22 * inch

        err = (p.get("error") or "").strip()
        if err:
            c.setFont("Helvetica", 9)
            c.drawString(left, y, f"Error: {err[:90]}")
            y -= 0.2 * inch

        c.showPage()

    c.save()
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


# ==================== EMAIL ====================
def send_proposal_email(
    sender_email: str,
    app_password: str,
    to_email: str,
    subject: str,
    body_text: str,
    pdf_bytes: Optional[bytes] = None,
    pdf_filename: str = "cotizacion_dreamlab.pdf",
    json_str: Optional[str] = None,
    json_filename: str = "cotizacion_dreamlab.json"
) -> Dict[str, Any]:
    try:
        sender_email = (sender_email or "").strip()
        to_email = (to_email or "").strip()
        pwd = _sanitize_app_password(app_password)

        if not sender_email or not to_email or not pwd:
            return {"ok": False, "error": "Falta EMAIL_SENDER, EMAIL_APP_PASSWORD o EMAIL_RECEIVER en tu .env."}

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = to_email
        msg.set_content(body_text)

        if pdf_bytes:
            msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=pdf_filename)
        if json_str:
            msg.add_attachment(json_str.encode("utf-8"), maintype="application", subtype="json", filename=json_filename)

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, pwd)
            server.send_message(msg)

        return {"ok": True, "error": ""}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ==================== SQLITE ====================
REQUIRED_RATE_COLUMNS = [
    "ig_reel", "ig_story", "ig_post", "ig_live",
    "tiktok_post", "tiktok_story", "tiktok_live",
    "yt_short", "yt_full", "yt_mtn",
    "podcast_mencion",
]

EXTRA_RATE_COLUMNS = [
    "img_pct_1_3",
    "img_pct_4_6",
    "img_pct_7_12",
    "pauta_pct_mensual",
    "exclusividad_pct_mensual",
]

DEFAULT_RATES = {
    "Eduardo Sacal": {
        "ig_reel": 28000, "ig_story": 8000, "ig_post": 12000, "ig_live": 30000,
        "tiktok_post": 30000, "tiktok_story": 7000, "tiktok_live": 32000,
        "yt_short": 24000, "yt_full": 45000, "yt_mtn": 20000,
        "podcast_mencion": 20000,
    },
    "Soy Tu Alex": {
        "ig_reel": 22000, "ig_story": 6000, "ig_post": 9000, "ig_live": 24000,
        "tiktok_post": 24000, "tiktok_story": 5500, "tiktok_live": 26000,
        "yt_short": 19000, "yt_full": 35000, "yt_mtn": 16000,
        "podcast_mencion": 16000,
    },
    "Par de Tres": {
        "ig_reel": 26000, "ig_story": 7000, "ig_post": 10500, "ig_live": 28000,
        "tiktok_post": 28000, "tiktok_story": 6500, "tiktok_live": 30000,
        "yt_short": 22000, "yt_full": 40000, "yt_mtn": 18000,
        "podcast_mencion": 18000,
    },
    "Alex Tacher": {
        "ig_reel": 24000, "ig_story": 6500, "ig_post": 10000, "ig_live": 26000,
        "tiktok_post": 26000, "tiktok_story": 6000, "tiktok_live": 28000,
        "yt_short": 20000, "yt_full": 38000, "yt_mtn": 17000,
        "podcast_mencion": 17000,
    },
    "Paulina Sanchez": {
        "ig_reel": 20000, "ig_story": 5500, "ig_post": 8500, "ig_live": 22000,
        "tiktok_post": 22000, "tiktok_story": 5000, "tiktok_live": 24000,
        "yt_short": 17000, "yt_full": 32000, "yt_mtn": 14000,
        "podcast_mencion": 14000,
    },
    "Marlene Marquez": {
        "ig_reel": 18000, "ig_story": 5000, "ig_post": 8000, "ig_live": 20000,
        "tiktok_post": 20000, "tiktok_story": 4500, "tiktok_live": 22000,
        "yt_short": 16000, "yt_full": 30000, "yt_mtn": 13000,
        "podcast_mencion": 13000,
    },
}

DEFAULT_EXTRA_RATES = {
    "img_pct_1_3": 0.25,
    "img_pct_4_6": 0.40,
    "img_pct_7_12": 0.60,
    "pauta_pct_mensual": 0.30,
    "exclusividad_pct_mensual": 0.20,
}


@st.cache_resource
def get_mongo_client():
    if not MONGODB_URI:
        raise RuntimeError("Falta configurar MONGODB_URI en secrets o variables de entorno.")
    return MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)


def get_db():
    return get_mongo_client()[MONGO_DB_NAME]


def get_collection(name: str):
    return get_db()[name]


def clear_app_caches() -> None:
    try:
        st.cache_data.clear()
    except Exception:
        pass


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _next_id(counter_name: str) -> int:
    doc = get_collection("counters").find_one_and_update(
        {"_id": counter_name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return int(doc["seq"])


def _doc_to_row(doc: dict) -> dict:
    row = dict(doc)
    row.pop("_id", None)
    return row


def _collection_to_df(collection_name: str, sort_field: Optional[str] = None, ascending: bool = True, limit: Optional[int] = None, projection: Optional[dict] = None) -> pd.DataFrame:
    collection = get_collection(collection_name)
    cursor = collection.find({}, projection or {"_id": 0})
    if sort_field:
        cursor = cursor.sort(sort_field, ASCENDING if ascending else DESCENDING)
    if limit:
        cursor = cursor.limit(limit)
    docs = list(cursor)
    return pd.DataFrame(docs)


@st.cache_resource
def init_db_once():
    db = get_db()

    db["influencers"].create_index([("id", ASCENDING)], unique=True)
    db["influencers"].create_index([("nombre", ASCENDING)], unique=True)
    db["brands"].create_index([("id", ASCENDING)], unique=True)
    db["brands"].create_index([("normalized_name", ASCENDING)], unique=True)
    db["clients"].create_index([("id", ASCENDING)], unique=True)
    db["clients"].create_index([("username", ASCENDING)], unique=True)
    db["quotes"].create_index([("id", ASCENDING)], unique=True)
    db["quotes"].create_index([("owner_username", ASCENDING), ("timestamp", DESCENDING)])
    db["sent_proposals"].create_index([("id", ASCENDING)], unique=True)
    db["sent_proposals"].create_index([("owner_username", ASCENDING), ("sent_at", DESCENDING)])

    now = _now_str()

    if db["brands"].estimated_document_count() == 0:
        brand_docs = []
        next_brand_id = 1
        for brand_name in DEFAULT_BRANDS:
            clean_name = clean_brand_display_name(brand_name)
            norm_name = normalize_brand_name(brand_name)
            if clean_name and norm_name:
                brand_docs.append({
                    "id": next_brand_id,
                    "display_name": clean_name,
                    "normalized_name": norm_name,
                    "created_at": now,
                    "created_by": "system",
                })
                next_brand_id += 1
        if brand_docs:
            db["brands"].insert_many(brand_docs, ordered=False)
            db["counters"].update_one({"_id": "brands"}, {"$max": {"seq": len(brand_docs)}}, upsert=True)

    if db["clients"].estimated_document_count() == 0 and CLIENT_SEED_USERS:
        client_docs = []
        next_client_id = 1
        now_clients = _now_str()
        for username, data in CLIENT_SEED_USERS.items():
            uname = _normalize_username(username)
            if not uname:
                continue
            client_docs.append({
                "id": next_client_id,
                "username": uname,
                "password": str(data.get("password", "")).strip(),
                "display_name": str(data.get("display_name", username)).strip(),
                "created_at": now_clients,
                "updated_at": now_clients,
            })
            next_client_id += 1
        if client_docs:
            db["clients"].insert_many(client_docs, ordered=False)
            db["counters"].update_one({"_id": "clients"}, {"$max": {"seq": len(client_docs)}}, upsert=True)

    if db["influencers"].estimated_document_count() == 0 and DEFAULT_RATES:
        influencer_docs = []
        next_influencer_id = 1
        for nombre, rates in DEFAULT_RATES.items():
            doc = {
                "id": next_influencer_id,
                "nombre": nombre,
                "updated_at": now,
            }
            doc.update(rates)
            doc.update(DEFAULT_EXTRA_RATES)
            influencer_docs.append(doc)
            next_influencer_id += 1
        if influencer_docs:
            db["influencers"].insert_many(influencer_docs, ordered=False)
            db["counters"].update_one({"_id": "influencers"}, {"$max": {"seq": len(influencer_docs)}}, upsert=True)

    return True


@st.cache_data(ttl=900, show_spinner=False)
def load_influencers_df() -> pd.DataFrame:
    cols = ["id", "nombre"] + REQUIRED_RATE_COLUMNS + EXTRA_RATE_COLUMNS + ["updated_at"]
    df = _collection_to_df("influencers", "nombre", True)
    if df.empty:
        return pd.DataFrame(columns=cols)
    for col in cols:
        if col not in df.columns:
            df[col] = None
    return df[cols]


@st.cache_data(ttl=900, show_spinner=False)
def load_influencer_lookup() -> Dict[str, dict]:
    df = load_influencers_df()
    if df.empty:
        return {}
    return {str(row["nombre"]): row.to_dict() for _, row in df.iterrows()}


def add_influencer(nombre: str, rates: dict, extra_rates: dict) -> None:
    try:
        get_collection("influencers").insert_one({
            "id": _next_id("influencers"),
            "nombre": nombre.strip(),
            **{col: rates[col] for col in REQUIRED_RATE_COLUMNS},
            **{col: extra_rates[col] for col in EXTRA_RATE_COLUMNS},
            "updated_at": _now_str(),
        })
        clear_app_caches()
    except DuplicateKeyError as e:
        raise ValueError("Ese influencer ya existe.") from e


def update_influencer(influencer_id: int, nombre: str, rates: dict, extra_rates: dict) -> None:
    existing = get_collection("influencers").find_one({"nombre": nombre.strip(), "id": {"$ne": influencer_id}})
    if existing:
        raise ValueError("Ese influencer ya existe en otro registro.")
    get_collection("influencers").update_one(
        {"id": influencer_id},
        {"$set": {
            "nombre": nombre.strip(),
            **{col: rates[col] for col in REQUIRED_RATE_COLUMNS},
            **{col: extra_rates[col] for col in EXTRA_RATE_COLUMNS},
            "updated_at": _now_str(),
        }}
    )
    clear_app_caches()


def delete_influencer(influencer_id: int) -> None:
    get_collection("influencers").delete_one({"id": influencer_id})
    clear_app_caches()


@st.cache_data(ttl=900, show_spinner=False)
def load_clients_df() -> pd.DataFrame:
    df = _collection_to_df("clients", "display_name", True)
    cols = ["id", "username", "password", "display_name", "created_at", "updated_at"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    for col in cols:
        if col not in df.columns:
            df[col] = None
    return df[cols].sort_values(["display_name", "username"], key=lambda s: s.astype(str).str.lower())


@st.cache_data(ttl=900, show_spinner=False)
def load_client_auth_map() -> Dict[str, Dict[str, str]]:
    rows = list(get_collection("clients").find({}, {"_id": 0, "username": 1, "password": 1, "display_name": 1}))
    result: Dict[str, Dict[str, str]] = {}
    for row in rows:
        uname = _normalize_username(row.get("username", ""))
        if not uname:
            continue
        result[uname] = {
            "username": uname,
            "password": str(row.get("password", "")),
            "role": "client",
            "display_name": str(row.get("display_name", uname)),
        }
    return result


@st.cache_data(ttl=900, show_spinner=False)
def get_client_user_by_username(username: str) -> Optional[Dict[str, str]]:
    uname = _normalize_username(username)
    if not uname:
        return None
    return load_client_auth_map().get(uname)


def add_client_account(username: str, password: str, display_name: str) -> Dict[str, Any]:
    uname = _normalize_username(username)
    pwd = str(password or "").strip()
    dname = str(display_name or "").strip()

    if not uname:
        return {"ok": False, "error": "Pon un usuario válido."}
    if not pwd:
        return {"ok": False, "error": "Pon una contraseña válida."}
    if not dname:
        dname = uname

    if get_collection("clients").find_one({"username": uname}):
        return {"ok": False, "error": "Ese usuario ya existe."}

    now = _now_str()
    get_collection("clients").insert_one({
        "id": _next_id("clients"),
        "username": uname,
        "password": pwd,
        "display_name": dname,
        "created_at": now,
        "updated_at": now,
    })
    clear_app_caches()
    return {"ok": True}


def update_client_account(client_id: int, username: str, password: str, display_name: str) -> Dict[str, Any]:
    uname = _normalize_username(username)
    pwd = str(password or "").strip()
    dname = str(display_name or "").strip()

    if not uname:
        return {"ok": False, "error": "Pon un usuario válido."}
    if not pwd:
        return {"ok": False, "error": "Pon una contraseña válida."}
    if not dname:
        dname = uname

    if get_collection("clients").find_one({"username": uname, "id": {"$ne": client_id}}):
        return {"ok": False, "error": "Ese usuario ya existe en otro cliente."}

    get_collection("clients").update_one(
        {"id": client_id},
        {"$set": {
            "username": uname,
            "password": pwd,
            "display_name": dname,
            "updated_at": _now_str(),
        }}
    )
    clear_app_caches()
    return {"ok": True}


def delete_client_account(client_id: int) -> None:
    get_collection("clients").delete_one({"id": client_id})
    clear_app_caches()


@st.cache_data(ttl=1800, show_spinner=False)
def load_brands_df() -> pd.DataFrame:
    df = _collection_to_df("brands", "display_name", True)
    cols = ["id", "display_name", "normalized_name", "created_at", "created_by"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    for col in cols:
        if col not in df.columns:
            df[col] = None
    return df[cols]


@st.cache_data(ttl=1800, show_spinner=False)
def load_brand_options() -> List[str]:
    df = load_brands_df()
    if df.empty:
        return []
    return df["display_name"].dropna().astype(str).tolist()


@st.cache_data(ttl=1800, show_spinner=False)
def find_existing_brand_name(name: str) -> Optional[str]:
    normalized = normalize_brand_name(name)
    if not normalized:
        return None
    row = get_collection("brands").find_one({"normalized_name": normalized}, {"_id": 0, "display_name": 1})
    return row["display_name"] if row else None


def add_brand(display_name: str, created_by: str) -> Dict[str, Any]:
    cleaned = clean_brand_display_name(display_name)
    normalized = normalize_brand_name(cleaned)

    if not cleaned or not normalized:
        return {"ok": False, "error": "Escribe una marca válida."}

    existing = find_existing_brand_name(cleaned)
    if existing:
        return {"ok": False, "error": f"Esa marca ya existe: {existing}", "existing": existing}

    try:
        get_collection("brands").insert_one({
            "id": _next_id("brands"),
            "display_name": cleaned,
            "normalized_name": normalized,
            "created_at": _now_str(),
            "created_by": created_by,
        })
        clear_app_caches()
        return {"ok": True, "brand": cleaned}
    except DuplicateKeyError:
        existing = find_existing_brand_name(cleaned)
        return {"ok": False, "error": f"Esa marca ya existe: {existing or cleaned}", "existing": existing or cleaned}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def suggest_similar_brands(input_brand: str, brand_options: List[str], n: int = 5) -> List[str]:
    cleaned = clean_brand_display_name(input_brand)
    if not cleaned:
        return []

    cleaned_lower = cleaned.lower()
    lower_to_original = {b.lower(): b for b in brand_options}

    close_lower = get_close_matches(cleaned_lower, list(lower_to_original.keys()), n=n, cutoff=0.55)
    suggestions = [lower_to_original[x] for x in close_lower]

    contains_matches = [
        brand for brand in brand_options
        if cleaned_lower in brand.lower() or brand.lower() in cleaned_lower
    ]

    final = []
    for brand in suggestions + contains_matches:
        if brand not in final and normalize_brand_name(brand) != normalize_brand_name(cleaned):
            final.append(brand)

    return final[:n]


def save_quote_to_db(payload: dict) -> int:
    quote_id = _next_id("quotes")
    doc = {
        "id": quote_id,
        "timestamp": payload.get("timestamp"),
        "marca": payload.get("marca"),
        "cliente": payload.get("cliente"),
        "campania": payload.get("campania"),
        "talento": payload.get("talento"),
        "temporalidad_meses": int(payload.get("temporalidad_meses", 0)),
        "total_base": float(payload.get("total_base", 0.0)),
        "total_extras": float(payload.get("total_extras", 0.0)),
        "total_final": float(payload.get("total_final", 0.0)),
        "payload_json": json.dumps(payload, ensure_ascii=False),
        "owner_username": payload.get("owner_username"),
        "owner_display_name": payload.get("owner_display_name"),
        "created_by_role": payload.get("created_by_role"),
    }
    get_collection("quotes").insert_one(doc)
    clear_app_caches()
    return quote_id


def update_quote_payload_json(quote_id: int, payload: dict) -> None:
    get_collection("quotes").update_one(
        {"id": quote_id},
        {"$set": {"payload_json": json.dumps(payload, ensure_ascii=False)}}
    )
    clear_app_caches()


@st.cache_data(ttl=300, show_spinner=False)
def load_quotes_df(limit: int = 50) -> pd.DataFrame:
    cols = ["id", "timestamp", "marca", "cliente", "campania", "talento", "temporalidad_meses", "total_base", "total_extras", "total_final", "payload_json", "owner_username", "owner_display_name", "created_by_role"]
    df = _collection_to_df("quotes", "timestamp", False, limit=limit, projection={"_id": 0})
    if df.empty:
        return pd.DataFrame(columns=cols)
    for col in cols:
        if col not in df.columns:
            df[col] = None
    return df[cols]


def save_sent_proposal(payload: dict, to_email: str, subject: str, quote_id: int, success: bool, error: str) -> None:
    get_collection("sent_proposals").insert_one({
        "id": _next_id("sent_proposals"),
        "sent_at": _now_str(),
        "to_email": to_email,
        "subject": subject,
        "quote_id": int(quote_id),
        "success": 1 if success else 0,
        "error": error or "",
        "payload_json": json.dumps(payload, ensure_ascii=False),
        "owner_username": payload.get("owner_username"),
        "owner_display_name": payload.get("owner_display_name"),
        "created_by_role": payload.get("created_by_role"),
    })
    clear_app_caches()


@st.cache_data(ttl=300, show_spinner=False)
def load_sent_df(limit: int = 50) -> pd.DataFrame:
    cols = ["id", "sent_at", "to_email", "subject", "quote_id", "success", "error", "payload_json", "owner_username", "owner_display_name", "created_by_role"]
    df = _collection_to_df("sent_proposals", "sent_at", False, limit=limit, projection={"_id": 0})
    if df.empty:
        return pd.DataFrame(columns=cols)
    for col in cols:
        if col not in df.columns:
            df[col] = None
    return df[cols]


init_db_once()


# ==================== LOGIN GATE ====================
def login_gate():
    if "user_authenticated" not in st.session_state:
        st.session_state.user_authenticated = False

    if not st.session_state.user_authenticated:
        render_main_header()
        st.subheader("Iniciar sesión")
        st.caption("Cada cliente entra con su propio usuario y contraseña.")

        with st.form("login_form"):
            username_input = st.text_input("Usuario")
            password_input = st.text_input("Contraseña", type="password")
            login_btn = st.form_submit_button("Entrar")

        if login_btn:
            normalized_username = _normalize_username(username_input)

            user_data = ADMIN_USERS.get(normalized_username)
            if not user_data:
                user_data = get_client_user_by_username(normalized_username)

            if user_data and password_input == user_data["password"]:
                st.session_state.user_authenticated = True
                st.session_state.user_info = {
                    "username": normalized_username,
                    "display_name": user_data["display_name"],
                    "role": user_data["role"],
                }
                st.session_state.admin_section_authenticated = False
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos.")

        st.stop()


login_gate()


# ==================== UI ====================
render_main_header()

current_user = st.session_state.get("user_info", {})
current_role = current_user.get("role", "client")
current_display_name = current_user.get("display_name", "")
current_username = current_user.get("username", "")

with st.sidebar:
    st.header("Menú")


    st.caption(f"Correo fijo de recepción: {EMAIL_RECEIVER}")
    st.success(f"Sesión: {current_display_name} ({current_role})")
    st.caption(f"Usuario: {current_username}")

    if st.button("Cerrar sesión"):
        logout_user()

    menu_options = ["Cotizador"] if current_role != "admin" else ["Cotizador", "Gestión de influencers", "Gestión de clientes", "Historial"]
    opcion = st.radio("Secciones", menu_options)


# ==================== COTIZADOR ====================
if opcion == "Cotizador":
    df_inf = load_influencers_df()
    influencer_lookup = load_influencer_lookup()
    render_cotizador_banner()
    st.markdown(
        '<div class="dreamlab-subtle">Cotiza acciones + extras. Guarda historial y permite enviar propuesta (correo fijo).</div>',
        unsafe_allow_html=True
    )

    if df_inf.empty:
        st.warning("No hay influencers registrados.")
        st.stop()

    influencer_nombre = st.selectbox("Talento / Influencer", df_inf["nombre"].tolist())
    st.markdown('<div class="dreamlab-section-label">Temporalidad de campaña (meses)</div>', unsafe_allow_html=True)
    meses = st.slider("", min_value=1, max_value=12, value=3, label_visibility="collapsed")
    temporalidad = f"{meses} mes" if meses == 1 else f"{meses} meses"

    row = influencer_lookup.get(influencer_nombre, {})

    img_pct_1_3 = _safe_float(row.get("img_pct_1_3"), 0.25)
    img_pct_4_6 = _safe_float(row.get("img_pct_4_6"), 0.40)
    img_pct_7_12 = _safe_float(row.get("img_pct_7_12"), 0.60)
    pauta_pct_mensual = _safe_float(row.get("pauta_pct_mensual"), 0.30)
    exclusividad_pct_mensual = _safe_float(row.get("exclusividad_pct_mensual"), 0.20)

    st.markdown('<hr class="red-divider">', unsafe_allow_html=True)
    st.markdown('<div class="dreamlab-section-label">Datos de la campaña</div>', unsafe_allow_html=True)

    cliente_actual = current_display_name
    username_actual = current_username
    role_actual = current_role

    apply_brand_pending_updates()
    show_brand_notice()

    available_brands = load_brand_options()

    if "brand_select_choice" not in st.session_state:
        st.session_state["brand_select_choice"] = None

    marca_seleccionada = st.selectbox(
        "Marca",
        options=available_brands,
        index=None,
        placeholder="Selecciona o agrega una marca",
        accept_new_options=True,
        key="brand_select_choice"
    )

    marca = ""

    if marca_seleccionada:
        marca_normalizada_existente = find_existing_brand_name(marca_seleccionada)

        if marca_normalizada_existente:
            marca = marca_normalizada_existente
        else:
            nueva_marca = clean_brand_display_name(marca_seleccionada)
            similar_brands = suggest_similar_brands(nueva_marca, available_brands)

            st.markdown('<div class="brand-add-box">', unsafe_allow_html=True)
            st.markdown('<div class="dreamlab-small-label">Registrar nueva marca</div>', unsafe_allow_html=True)
            st.write(f"Marca capturada: {nueva_marca}")

            if similar_brands:
                st.info("No te refieres a esta marca?")
                selected_suggestion = st.radio(
                    "Marcas parecidas",
                    similar_brands,
                    index=0,
                    key="similar_brand_radio"
                )

                col_s1, col_s2 = st.columns(2)
                with col_s1:
                    if st.button("Usar marca sugerida", key="use_similar_brand_btn"):
                        set_brand_pending_updates(
                            selected_brand=selected_suggestion,
                            new_brand_input="",
                            notice_level="success",
                            notice_message=f"Se seleccionó la marca sugerida: {selected_suggestion}"
                        )
                        st.rerun()

                with col_s2:
                    if st.button("Guardar como nueva marca", key="save_new_brand_btn"):
                        result = add_brand(nueva_marca, username_actual)
                        if result.get("ok"):
                            nueva_guardada = result["brand"]
                            set_brand_pending_updates(
                                selected_brand=nueva_guardada,
                                new_brand_input="",
                                notice_level="success",
                                notice_message=f"Marca agregada correctamente: {nueva_guardada}"
                            )
                            st.rerun()
                        else:
                            st.error(result.get("error", "No se pudo guardar la marca."))
            else:
                st.caption("No encontramos marcas parecidas.")
                col_n1, col_n2 = st.columns(2)

                with col_n1:
                    if st.button("Guardar esta nueva marca", key="save_new_brand_direct_btn"):
                        result = add_brand(nueva_marca, username_actual)
                        if result.get("ok"):
                            nueva_guardada = result["brand"]
                            set_brand_pending_updates(
                                selected_brand=nueva_guardada,
                                new_brand_input="",
                                notice_level="success",
                                notice_message=f"Marca agregada correctamente: {nueva_guardada}"
                            )
                            st.rerun()
                        else:
                            st.error(result.get("error", "No se pudo guardar la marca."))

                with col_n2:
                    if st.button("Cancelar", key="cancel_new_brand_btn"):
                        set_brand_pending_updates(
                            selected_brand=None,
                            new_brand_input="",
                            notice_level="info",
                            notice_message="Se canceló el registro de la nueva marca."
                        )
                        st.rerun()

            st.markdown('</div>', unsafe_allow_html=True)
            marca = ""

    st.caption(f"Cliente conectado: {cliente_actual}")
    campania = st.text_input("Campaña", key="campania_input")

    st.markdown('<hr class="red-divider">', unsafe_allow_html=True)
    st.markdown('<div class="dreamlab-section-label">Extras (opcional)</div>', unsafe_allow_html=True)

    ex1, ex2, ex3 = st.columns(3)

    with ex1:
        uso_imagen = st.checkbox("Uso de imagen", key="uso_imagen")
        meses_uso_imagen = st.number_input(
            "Meses uso imagen",
            min_value=1,
            max_value=12,
            value=meses,
            step=1,
            disabled=not uso_imagen,
            key="meses_uso_imagen"
        )

    with ex2:
        pauta_digital = st.checkbox("Pauta digital", key="pauta_digital")
        meses_pauta = st.number_input(
            "Meses pauta",
            min_value=1,
            max_value=12,
            value=meses,
            step=1,
            disabled=not pauta_digital,
            key="meses_pauta"
        )

    with ex3:
        exclusividad = st.checkbox("Exclusividad categoría", key="exclusividad")
        meses_exclusividad = st.number_input(
            "Meses exclusividad",
            min_value=1,
            max_value=12,
            value=meses,
            step=1,
            disabled=not exclusividad,
            key="meses_exclusividad"
        )

    categoria_exclusiva = st.text_input(
        "Categoría (solo si hay exclusividad)",
        disabled=not exclusividad,
        key="categoria_exclusiva"
    )

    st.markdown('<hr class="red-divider">', unsafe_allow_html=True)

    st.markdown('<div class="dreamlab-section-label">Acciones por pieza</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)

    with c1:
        qty_reel = st.number_input("Reel", min_value=0, step=1, value=0, key="qty_reel")
        qty_ig_story = st.number_input("IG Story", min_value=0, step=1, value=0, key="qty_ig_story")
        qty_ig_post = st.number_input("IG Post", min_value=0, step=1, value=0, key="qty_ig_post")

    with c2:
        qty_ig_live = st.number_input("IG Live", min_value=0, step=1, value=0, key="qty_ig_live")
        qty_tt_post = st.number_input("Tiktok Post", min_value=0, step=1, value=0, key="qty_tt_post")
        qty_tt_story = st.number_input("Tiktok Story", min_value=0, step=1, value=0, key="qty_tt_story")

    with c3:
        qty_tt_live = st.number_input("Tiktok Live", min_value=0, step=1, value=0, key="qty_tt_live")
        qty_yt_short = st.number_input("Youtube Short", min_value=0, step=1, value=0, key="qty_yt_short")
        qty_yt_full = st.number_input("Youtube Full", min_value=0, step=1, value=0, key="qty_yt_full")
        qty_yt_mtn = st.number_input("Youtube MTN", min_value=0, step=1, value=0, key="qty_yt_mtn")
        qty_podcast = st.number_input("Mención en podcast", min_value=0, step=1, value=0, key="qty_podcast")

    submitted = st.button("Calcular cotización", type="primary")

    if submitted:
        if not marca:
            st.error("Selecciona una marca o agrega una nueva antes de calcular.")
        else:
            price_map = {
                "Reel": float(row.get("ig_reel") or 0),
                "IG Story": float(row.get("ig_story") or 0),
                "IG Post": float(row.get("ig_post") or 0),
                "IG Live": float(row.get("ig_live") or 0),
                "Tiktok Post": float(row.get("tiktok_post") or 0),
                "Tiktok Story": float(row.get("tiktok_story") or 0),
                "Tiktok Live": float(row.get("tiktok_live") or 0),
                "Youtube Short": float(row.get("yt_short") or 0),
                "Youtube Full": float(row.get("yt_full") or 0),
                "Youtube MTN": float(row.get("yt_mtn") or 0),
                "Mención en podcast": float(row.get("podcast_mencion") or 0),
            }

            qty_map = {
                "Reel": int(qty_reel),
                "IG Story": int(qty_ig_story),
                "IG Post": int(qty_ig_post),
                "IG Live": int(qty_ig_live),
                "Tiktok Post": int(qty_tt_post),
                "Tiktok Story": int(qty_tt_story),
                "Tiktok Live": int(qty_tt_live),
                "Youtube Short": int(qty_yt_short),
                "Youtube Full": int(qty_yt_full),
                "Youtube MTN": int(qty_yt_mtn),
                "Mención en podcast": int(qty_podcast),
            }

            items = []
            total_base = 0.0
            for accion, precio in price_map.items():
                cantidad = qty_map[accion]
                subtotal = precio * cantidad
                total_base += subtotal
                items.append({
                    "Acción": accion,
                    "Cantidad": cantidad,
                    "Precio unitario (MXN)": precio,
                    "Subtotal (MXN)": subtotal
                })

            df_actions = pd.DataFrame(items)
            df_actions_show = df_actions[df_actions["Cantidad"] > 0].copy()

            extras_rows = []
            total_extras = 0.0

            uso_imagen_amount = 0.0
            uso_imagen_detail = ""
            uso_imagen_pct_applied = 0.0
            if uso_imagen and total_base > 0:
                m = int(meses_uso_imagen)
                if 1 <= m <= 3:
                    pct = img_pct_1_3
                elif 4 <= m <= 6:
                    pct = img_pct_4_6
                else:
                    pct = img_pct_7_12
                uso_imagen_pct_applied = pct
                uso_imagen_amount = total_base * pct
                uso_imagen_detail = f"{m} meses (licencia) | {pct:.0%} sobre base"
                extras_rows.append({"Extra": "Uso de imagen", "Monto (MXN)": uso_imagen_amount})
                total_extras += uso_imagen_amount

            pauta_amount = 0.0
            pauta_detail = ""
            if pauta_digital and total_base > 0:
                m = int(meses_pauta)
                pauta_amount = total_base * pauta_pct_mensual * m
                pauta_detail = f"{m} meses (pauta) | {pauta_pct_mensual:.0%} mensual sobre base"
                extras_rows.append({"Extra": "Pauta digital", "Monto (MXN)": pauta_amount})
                total_extras += pauta_amount

            excl_amount = 0.0
            excl_detail = ""
            if exclusividad and total_base > 0:
                m = int(meses_exclusividad)
                cat = (categoria_exclusiva or "").strip()
                excl_amount = total_base * exclusividad_pct_mensual * m
                excl_detail = f"{m} meses | {exclusividad_pct_mensual:.0%} mensual sobre base" + (f" | categoría: {cat}" if cat else "")
                extras_rows.append({"Extra": "Exclusividad de categoría", "Monto (MXN)": excl_amount})
                total_extras += excl_amount

            df_extras = pd.DataFrame(extras_rows)
            total_final = total_base + total_extras

            extras_payload = {
                "Uso de imagen": {
                    "enabled": bool(uso_imagen),
                    "detail": uso_imagen_detail,
                    "amount_mxn": uso_imagen_amount,
                    "params": {
                        "months": int(meses_uso_imagen),
                        "pct_applied": uso_imagen_pct_applied
                    }
                },
                "Pauta digital": {
                    "enabled": bool(pauta_digital),
                    "detail": pauta_detail,
                    "amount_mxn": pauta_amount,
                    "params": {
                        "months": int(meses_pauta),
                        "pct_mensual": pauta_pct_mensual
                    }
                },
                "Exclusividad de categoría": {
                    "enabled": bool(exclusividad),
                    "detail": excl_detail,
                    "amount_mxn": excl_amount,
                    "params": {
                        "months": int(meses_exclusividad),
                        "pct_mensual": exclusividad_pct_mensual,
                        "categoria": categoria_exclusiva
                    }
                },
            }

            quote_payload = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "marca": marca,
                "cliente": cliente_actual,
                "campania": campania,
                "talento": influencer_nombre,
                "temporalidad": temporalidad,
                "temporalidad_meses": meses,
                "moneda": "MXN",
                "acciones": qty_map,
                "tarifario_unitario": price_map,
                "extras": extras_payload,
                "extra_rules_snapshot": {
                    "img_pct_1_3": img_pct_1_3,
                    "img_pct_4_6": img_pct_4_6,
                    "img_pct_7_12": img_pct_7_12,
                    "pauta_pct_mensual": pauta_pct_mensual,
                    "exclusividad_pct_mensual": exclusividad_pct_mensual,
                },
                "owner_username": username_actual,
                "owner_display_name": cliente_actual,
                "created_by_role": role_actual,
                "total_base": total_base,
                "total_extras": total_extras,
                "total_final": total_final
            }

            quote_id = save_quote_to_db(quote_payload)
            quote_payload["quote_id"] = quote_id
            update_quote_payload_json(quote_id, quote_payload)

            st.session_state.last_quote_payload = quote_payload
            st.session_state.last_actions_rows = df_actions_show.to_dict(orient="records")
            st.session_state.last_extras_rows = df_extras.to_dict(orient="records")

            st.success("Cotización calculada y guardada en historial ✅")

    if "last_quote_payload" in st.session_state:
        qp = st.session_state.last_quote_payload
        actions_df = pd.DataFrame(st.session_state.get("last_actions_rows", []))
        extras_df = pd.DataFrame(st.session_state.get("last_extras_rows", []))

        st.write(f"Talento: {qp.get('talento','')} | Temporalidad: {qp.get('temporalidad','')}")

        st.markdown('<div class="dreamlab-section-label">Acciones cotizadas</div>', unsafe_allow_html=True)
        if actions_df.empty:
            st.info("Sin acciones.")
        else:
            st.dataframe(actions_df, use_container_width=True)

        st.markdown('<div class="dreamlab-section-label">Extras</div>', unsafe_allow_html=True)
        if extras_df.empty:
            st.info("Sin extras.")
        else:
            st.dataframe(extras_df, use_container_width=True)

        st.markdown('<div class="dreamlab-section-label">Totales</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown('<div class="metric-red">', unsafe_allow_html=True)
            st.metric("Total base", _fmt_mxn(float(qp.get("total_base", 0.0))))
            st.markdown('</div>', unsafe_allow_html=True)
        with c2:
            st.markdown('<div class="metric-red">', unsafe_allow_html=True)
            st.metric("Total extras", _fmt_mxn(float(qp.get("total_extras", 0.0))))
            st.markdown('</div>', unsafe_allow_html=True)
        with c3:
            st.markdown('<div class="metric-red">', unsafe_allow_html=True)
            st.metric("TOTAL FINAL", _fmt_mxn(float(qp.get("total_final", 0.0))))
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("---")

        json_data = json.dumps(qp, ensure_ascii=False, indent=2)
        pdf_bytes = build_quote_pdf_bytes(qp, actions_df, extras_df) if REPORTLAB_AVAILABLE else b""

        colA, colB, colC = st.columns(3)
        with colA:
            st.download_button("📥 Descargar JSON", data=json_data, file_name="cotizacion_dreamlab.json", mime="application/json")
        with colB:
            if not REPORTLAB_AVAILABLE:
                st.warning("Instala reportlab para PDF: pip install reportlab")
            else:
                st.download_button("📄 Descargar PDF", data=pdf_bytes, file_name="cotizacion_dreamlab.pdf", mime="application/pdf")
        with colC:
            st.caption(f"Se enviará a: {EMAIL_RECEIVER}")
            if st.button("✉️ Enviar propuesta"):
                if not EMAIL_SENDER or not EMAIL_APP_PASSWORD or not EMAIL_RECEIVER:
                    st.error("Falta configurar EMAIL_SENDER / EMAIL_APP_PASSWORD / EMAIL_RECEIVER en tu .env.")
                else:
                    subject = f"Dream Lab — Propuesta | {qp.get('marca','')} | {qp.get('campania','')}"
                    body = (
                        f"Hola,\n\n"
                        f"Adjunto encontrarás la propuesta cotizada en Dream Lab.\n\n"
                        f"Marca: {qp.get('marca','')}\n"
                        f"Cliente: {qp.get('cliente','')}\n"
                        f"Campaña: {qp.get('campania','')}\n"
                        f"Talento: {qp.get('talento','')}\n"
                        f"Temporalidad: {qp.get('temporalidad','')}\n"
                        f"Total final: {_fmt_mxn(float(qp.get('total_final',0.0)))}\n\n"
                        f"Saludos,\nDream Lab"
                    )

                    send_res = send_proposal_email(
                        sender_email=EMAIL_SENDER,
                        app_password=EMAIL_APP_PASSWORD,
                        to_email=EMAIL_RECEIVER,
                        subject=subject,
                        body_text=body,
                        pdf_bytes=(pdf_bytes if REPORTLAB_AVAILABLE else None),
                        pdf_filename=f"cotizacion_dreamlab_{qp.get('quote_id','')}.pdf",
                        json_str=json_data,
                        json_filename=f"cotizacion_dreamlab_{qp.get('quote_id','')}.json"
                    )

                    ok = bool(send_res.get("ok"))
                    err = send_res.get("error", "")

                    save_sent_proposal(
                        payload=qp,
                        to_email=EMAIL_RECEIVER,
                        subject=subject,
                        quote_id=int(qp.get("quote_id", 0) or 0),
                        success=ok,
                        error=err
                    )

                    if ok:
                        st.success("✅ Propuesta enviada por correo y registrada en historial de envíos.")
                    else:
                        st.error(f"❌ No se pudo enviar. Error: {err}")


# ==================== GESTIÓN DE INFLUENCERS ====================
elif opcion == "Gestión de influencers":
    require_admin_role()

    st.markdown('<div class="dreamlab-big-section-label">👥 Gestión de influencers (tarifario)</div>', unsafe_allow_html=True)
    st.write("Agregar/editar precios por pieza (MXN) y extras por influencer.")

    with st.expander("➕ Agregar influencer"):
        with st.form("form_add_inf"):
            nombre = st.text_input("Nombre del influencer")

            st.markdown('<div class="dreamlab-small-label">IG</div>', unsafe_allow_html=True)
            ig1, ig2 = st.columns(2)
            with ig1:
                ig_reel = st.number_input("Reel (MXN)", min_value=0.0, step=500.0, value=20000.0)
                ig_story = st.number_input("IG Story (MXN)", min_value=0.0, step=250.0, value=5000.0)
            with ig2:
                ig_post = st.number_input("IG Post (MXN)", min_value=0.0, step=250.0, value=8000.0)
                ig_live = st.number_input("IG Live (MXN)", min_value=0.0, step=500.0, value=22000.0)

            st.markdown('<div class="dreamlab-small-label">TikTok</div>', unsafe_allow_html=True)
            tt1, tt2 = st.columns(2)
            with tt1:
                tiktok_post = st.number_input("Tiktok Post (MXN)", min_value=0.0, step=500.0, value=22000.0)
                tiktok_story = st.number_input("Tiktok Story (MXN)", min_value=0.0, step=250.0, value=5000.0)
            with tt2:
                tiktok_live = st.number_input("Tiktok Live (MXN)", min_value=0.0, step=500.0, value=24000.0)

            st.markdown('<div class="dreamlab-small-label">YouTube + Podcast</div>', unsafe_allow_html=True)
            yt1, yt2 = st.columns(2)
            with yt1:
                yt_short = st.number_input("Youtube Short (MXN)", min_value=0.0, step=500.0, value=17000.0)
                yt_full = st.number_input("Youtube Full (MXN)", min_value=0.0, step=500.0, value=32000.0)
            with yt2:
                yt_mtn = st.number_input("Youtube MTN (MXN)", min_value=0.0, step=500.0, value=14000.0)
                podcast = st.number_input("Mención en podcast (MXN)", min_value=0.0, step=500.0, value=15000.0)

            st.markdown('<div class="dreamlab-small-label">Extras por influencer</div>', unsafe_allow_html=True)
            exa1, exa2, exa3 = st.columns(3)
            with exa1:
                img_pct_1_3_new = st.number_input("Uso de imagen 1-3 meses (%)", min_value=0.0, step=0.01, value=0.25, format="%.2f")
                img_pct_4_6_new = st.number_input("Uso de imagen 4-6 meses (%)", min_value=0.0, step=0.01, value=0.40, format="%.2f")
            with exa2:
                img_pct_7_12_new = st.number_input("Uso de imagen 7-12 meses (%)", min_value=0.0, step=0.01, value=0.60, format="%.2f")
                pauta_pct_mensual_new = st.number_input("Pauta digital mensual (%)", min_value=0.0, step=0.01, value=0.30, format="%.2f")
            with exa3:
                exclusividad_pct_mensual_new = st.number_input("Exclusividad mensual (%)", min_value=0.0, step=0.01, value=0.20, format="%.2f")

            submitted_add = st.form_submit_button("Guardar influencer")

        if submitted_add:
            if not nombre.strip():
                st.error("Pon un nombre válido.")
            else:
                try:
                    add_influencer(
                        nombre,
                        {
                            "ig_reel": ig_reel, "ig_story": ig_story, "ig_post": ig_post, "ig_live": ig_live,
                            "tiktok_post": tiktok_post, "tiktok_story": tiktok_story, "tiktok_live": tiktok_live,
                            "yt_short": yt_short, "yt_full": yt_full, "yt_mtn": yt_mtn,
                            "podcast_mencion": podcast,
                        },
                        {
                            "img_pct_1_3": img_pct_1_3_new,
                            "img_pct_4_6": img_pct_4_6_new,
                            "img_pct_7_12": img_pct_7_12_new,
                            "pauta_pct_mensual": pauta_pct_mensual_new,
                            "exclusividad_pct_mensual": exclusividad_pct_mensual_new,
                        }
                    )
                    st.success("Influencer guardado.")
                    st.rerun()
                except ValueError:
                    st.error("Ese influencer ya existe. Edita el existente.")
                except Exception as e:
                    st.error(f"Error: {e}")

    st.divider()
    st.markdown('<div class="dreamlab-section-label">Influencers registrados</div>', unsafe_allow_html=True)

    df_inf2 = load_influencers_df()
    for _, r in df_inf2.iterrows():
        with st.expander(f"{r['nombre']} (actualizado: {r['updated_at']})"):
            with st.form(f"form_edit_{int(r['id'])}"):
                nombre_new = st.text_input("Nombre", value=r["nombre"])

                st.markdown('<div class="dreamlab-small-label">IG</div>', unsafe_allow_html=True)
                ig1, ig2 = st.columns(2)
                with ig1:
                    ig_reel_new = st.number_input("Reel (MXN)", min_value=0.0, step=500.0, value=float(r["ig_reel"] or 0), key=f"ig_reel_{r['id']}")
                    ig_story_new = st.number_input("IG Story (MXN)", min_value=0.0, step=250.0, value=float(r["ig_story"] or 0), key=f"ig_story_{r['id']}")
                with ig2:
                    ig_post_new = st.number_input("IG Post (MXN)", min_value=0.0, step=250.0, value=float(r["ig_post"] or 0), key=f"ig_post_{r['id']}")
                    ig_live_new = st.number_input("IG Live (MXN)", min_value=0.0, step=500.0, value=float(r["ig_live"] or 0), key=f"ig_live_{r['id']}")

                st.markdown('<div class="dreamlab-small-label">TikTok</div>', unsafe_allow_html=True)
                tt1, tt2 = st.columns(2)
                with tt1:
                    tiktok_post_new = st.number_input("Tiktok Post (MXN)", min_value=0.0, step=500.0, value=float(r["tiktok_post"] or 0), key=f"tiktok_post_{r['id']}")
                    tiktok_story_new = st.number_input("Tiktok Story (MXN)", min_value=0.0, step=250.0, value=float(r["tiktok_story"] or 0), key=f"tiktok_story_{r['id']}")
                with tt2:
                    tiktok_live_new = st.number_input("Tiktok Live (MXN)", min_value=0.0, step=500.0, value=float(r["tiktok_live"] or 0), key=f"tiktok_live_{r['id']}")

                st.markdown('<div class="dreamlab-small-label">YouTube + Podcast</div>', unsafe_allow_html=True)
                yt1, yt2 = st.columns(2)
                with yt1:
                    yt_short_new = st.number_input("Youtube Short (MXN)", min_value=0.0, step=500.0, value=float(r["yt_short"] or 0), key=f"yt_short_{r['id']}")
                    yt_full_new = st.number_input("Youtube Full (MXN)", min_value=0.0, step=500.0, value=float(r["yt_full"] or 0), key=f"yt_full_{r['id']}")
                with yt2:
                    yt_mtn_new = st.number_input("Youtube MTN (MXN)", min_value=0.0, step=500.0, value=float(r["yt_mtn"] or 0), key=f"yt_mtn_{r['id']}")
                    podcast_new = st.number_input("Mención en podcast (MXN)", min_value=0.0, step=500.0, value=float(r["podcast_mencion"] or 0), key=f"podcast_{r['id']}")

                st.markdown('<div class="dreamlab-small-label">Extras por influencer</div>', unsafe_allow_html=True)
                ex1, ex2, ex3 = st.columns(3)
                with ex1:
                    img_pct_1_3_edit = st.number_input("Uso de imagen 1-3 meses (%)", min_value=0.0, step=0.01, value=float(r["img_pct_1_3"] or 0), format="%.2f", key=f"img1_{r['id']}")
                    img_pct_4_6_edit = st.number_input("Uso de imagen 4-6 meses (%)", min_value=0.0, step=0.01, value=float(r["img_pct_4_6"] or 0), format="%.2f", key=f"img2_{r['id']}")
                with ex2:
                    img_pct_7_12_edit = st.number_input("Uso de imagen 7-12 meses (%)", min_value=0.0, step=0.01, value=float(r["img_pct_7_12"] or 0), format="%.2f", key=f"img3_{r['id']}")
                    pauta_pct_mensual_edit = st.number_input("Pauta digital mensual (%)", min_value=0.0, step=0.01, value=float(r["pauta_pct_mensual"] or 0), format="%.2f", key=f"pauta_{r['id']}")
                with ex3:
                    exclusividad_pct_mensual_edit = st.number_input("Exclusividad mensual (%)", min_value=0.0, step=0.01, value=float(r["exclusividad_pct_mensual"] or 0), format="%.2f", key=f"excl_{r['id']}")

                col_save, col_del = st.columns(2)
                with col_save:
                    save_btn = st.form_submit_button("Guardar cambios")
                with col_del:
                    del_btn = st.form_submit_button("Eliminar influencer")

            if save_btn:
                update_influencer(
                    int(r["id"]),
                    nombre_new,
                    {
                        "ig_reel": ig_reel_new, "ig_story": ig_story_new, "ig_post": ig_post_new, "ig_live": ig_live_new,
                        "tiktok_post": tiktok_post_new, "tiktok_story": tiktok_story_new, "tiktok_live": tiktok_live_new,
                        "yt_short": yt_short_new, "yt_full": yt_full_new, "yt_mtn": yt_mtn_new,
                        "podcast_mencion": podcast_new,
                    },
                    {
                        "img_pct_1_3": img_pct_1_3_edit,
                        "img_pct_4_6": img_pct_4_6_edit,
                        "img_pct_7_12": img_pct_7_12_edit,
                        "pauta_pct_mensual": pauta_pct_mensual_edit,
                        "exclusividad_pct_mensual": exclusividad_pct_mensual_edit,
                    }
                )
                st.success("Cambios guardados.")
                st.rerun()

            if del_btn:
                delete_influencer(int(r["id"]))
                st.success("Influencer eliminado.")
                st.rerun()


# ==================== GESTIÓN DE CLIENTES ====================
elif opcion == "Gestión de clientes":
    require_admin_role()

    st.markdown('<div class="dreamlab-big-section-label">🏢 Gestión de clientes</div>', unsafe_allow_html=True)
    st.write("Aquí puedes ver, agregar, editar y eliminar clientes del sistema.")

    with st.expander("➕ Agregar cliente"):
        with st.form("form_add_client"):
            client_display_name = st.text_input("Nombre visible del cliente")
            client_username = st.text_input("Usuario")
            client_password = st.text_input("Contraseña")

            submitted_add_client = st.form_submit_button("Guardar cliente")

        if submitted_add_client:
            result = add_client_account(
                username=client_username,
                password=client_password,
                display_name=client_display_name
            )
            if result.get("ok"):
                st.success("Cliente guardado correctamente.")
                st.rerun()
            else:
                st.error(result.get("error", "No se pudo guardar el cliente."))

    st.divider()
    st.markdown('<div class="dreamlab-section-label">Clientes registrados</div>', unsafe_allow_html=True)

    df_clients = load_clients_df()

    if df_clients.empty:
        st.info("No hay clientes registrados.")
    else:
        for _, r in df_clients.iterrows():
            with st.expander(f"{r['display_name']} | usuario: {r['username']}"):
                with st.form(f"form_edit_client_{int(r['id'])}"):
                    display_name_new = st.text_input(
                        "Nombre visible",
                        value=str(r["display_name"] or ""),
                        key=f"display_name_client_{r['id']}"
                    )
                    username_new = st.text_input(
                        "Usuario",
                        value=str(r["username"] or ""),
                        key=f"username_client_{r['id']}"
                    )
                    password_new = st.text_input(
                        "Contraseña",
                        value=str(r["password"] or ""),
                        key=f"password_client_{r['id']}"
                    )

                    col_save, col_del = st.columns(2)
                    with col_save:
                        save_client_btn = st.form_submit_button("Guardar cambios")
                    with col_del:
                        del_client_btn = st.form_submit_button("Eliminar cliente")

                if save_client_btn:
                    result = update_client_account(
                        client_id=int(r["id"]),
                        username=username_new,
                        password=password_new,
                        display_name=display_name_new
                    )
                    if result.get("ok"):
                        st.success("Cliente actualizado correctamente.")
                        st.rerun()
                    else:
                        st.error(result.get("error", "No se pudo actualizar el cliente."))

                if del_client_btn:
                    delete_client_account(int(r["id"]))
                    st.success("Cliente eliminado correctamente.")
                    st.rerun()


# ==================== HISTORIAL ====================
elif opcion == "Historial":
    require_admin_role()

    st.markdown('<div class="dreamlab-big-section-label">📂 Historial (cotizaciones + propuestas enviadas)</div>', unsafe_allow_html=True)

    # -------- COTIZACIONES --------
    st.markdown('<div class="dreamlab-big-section-label">🧾 Cotizaciones</div>', unsafe_allow_html=True)
    df_q = load_quotes_df()

    if df_q.empty:
        st.info("Aún no hay cotizaciones guardadas.")
    else:
        if "owner_display_name" not in df_q.columns:
            df_q["owner_display_name"] = df_q["cliente"].fillna("")

        df_q["owner_display_name"] = df_q["owner_display_name"].fillna(df_q["cliente"]).fillna("").apply(clean_owner_display_name)
        df_q["total_final"] = pd.to_numeric(df_q["total_final"], errors="coerce").fillna(0.0)
        df_q["timestamp_dt"] = pd.to_datetime(df_q["timestamp"], errors="coerce")

        clientes_disponibles = sorted([c for c in df_q["owner_display_name"].dropna().unique().tolist() if str(c).strip()])
        filtro_cliente = st.selectbox("Filtrar cotizaciones por cliente", ["Todos"] + clientes_disponibles)

        if filtro_cliente != "Todos":
            df_q = df_q[df_q["owner_display_name"] == filtro_cliente]

        st.caption(f"Total de cotizaciones mostradas: {len(df_q)}")

        if df_q.empty:
            st.warning("No hay cotizaciones para ese cliente.")
        else:
            total_cotizaciones = len(df_q)
            monto_total = float(df_q["total_final"].sum())
            ticket_promedio = float(df_q["total_final"].mean()) if total_cotizaciones > 0 else 0.0

            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Cotizaciones", total_cotizaciones)
            with m2:
                st.metric("Monto total cotizado", _fmt_mxn(monto_total))
            with m3:
                st.metric("Ticket promedio", _fmt_mxn(ticket_promedio))

            quotes_payloads = []
            for _, r in df_q.iterrows():
                payload = safe_json_loads(r["payload_json"])
                if not payload:
                    payload = {"raw": r["payload_json"]}

                if not payload.get("owner_display_name"):
                    payload["owner_display_name"] = r.get("owner_display_name", "") or payload.get("cliente", "") or "Sin cliente"
                if not payload.get("owner_username"):
                    payload["owner_username"] = r.get("owner_username", "")
                if not payload.get("created_by_role"):
                    payload["created_by_role"] = r.get("created_by_role", "")

                quotes_payloads.append(payload)

            serie_q = (
                df_q.dropna(subset=["timestamp_dt"])
                .assign(fecha=df_q["timestamp_dt"].dt.date)
                .groupby("fecha", as_index=False)
                .agg(
                    cotizaciones=("id", "count"),
                    monto_total=("total_final", "sum")
                )
            )

            top_talentos = (
                df_q.groupby("talento", as_index=False)
                .agg(cotizaciones=("id", "count"))
                .sort_values("cotizaciones", ascending=False)
                .head(10)
            )

            top_marcas = (
                df_q.groupby("marca", as_index=False)
                .agg(monto_total=("total_final", "sum"))
                .sort_values("monto_total", ascending=False)
                .head(10)
            )

            c1, c2 = st.columns(2)

            with c1:
                st.markdown('<div class="dreamlab-section-label">Cotizaciones por día</div>', unsafe_allow_html=True)
                if serie_q.empty:
                    st.info("No hay suficientes datos para esta gráfica.")
                else:
                    chart_q_count = alt.Chart(serie_q).mark_line(point=True).encode(
                        x=alt.X("fecha:T", title="Fecha"),
                        y=alt.Y("cotizaciones:Q", title="Cotizaciones")
                    ).properties(height=300)
                    st.altair_chart(chart_q_count, use_container_width=True)

            with c2:
                st.markdown('<div class="dreamlab-section-label">Monto cotizado por día</div>', unsafe_allow_html=True)
                if serie_q.empty:
                    st.info("No hay suficientes datos para esta gráfica.")
                else:
                    chart_q_amount = alt.Chart(serie_q).mark_line(point=True).encode(
                        x=alt.X("fecha:T", title="Fecha"),
                        y=alt.Y("monto_total:Q", title="Monto total")
                    ).properties(height=300)
                    st.altair_chart(chart_q_amount, use_container_width=True)

            c3, c4 = st.columns(2)

            with c3:
                st.markdown('<div class="dreamlab-section-label">Cotizaciones por talento</div>', unsafe_allow_html=True)
                if top_talentos.empty:
                    st.info("No hay suficientes datos para esta gráfica.")
                else:
                    chart_talentos = alt.Chart(top_talentos).mark_bar().encode(
                        x=alt.X("cotizaciones:Q", title="Cotizaciones"),
                        y=alt.Y("talento:N", sort="-x", title="Talento")
                    ).properties(height=320)
                    st.altair_chart(chart_talentos, use_container_width=True)

            with c4:
                st.markdown('<div class="dreamlab-section-label">Monto cotizado por marca</div>', unsafe_allow_html=True)
                if top_marcas.empty:
                    st.info("No hay suficientes datos para esta gráfica.")
                else:
                    chart_marcas = alt.Chart(top_marcas).mark_bar().encode(
                        x=alt.X("monto_total:Q", title="Monto total"),
                        y=alt.Y("marca:N", sort="-x", title="Marca")
                    ).properties(height=320)
                    st.altair_chart(chart_marcas, use_container_width=True)

            json_all = json.dumps(quotes_payloads, ensure_ascii=False, indent=2)
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "📥 Descargar historial cotizaciones (JSON)",
                    json_all,
                    "historial_cotizaciones_dreamlab.json",
                    "application/json"
                )
            with col2:
                if not REPORTLAB_AVAILABLE:
                    st.warning("Instala reportlab para PDF: pip install reportlab")
                else:
                    pdf_all = build_history_pdf_bytes(quotes_payloads)
                    st.download_button(
                        "📄 Descargar historial cotizaciones (PDF)",
                        pdf_all,
                        "historial_cotizaciones_dreamlab.pdf",
                        "application/pdf"
                    )

    st.divider()

    # -------- PROPUESTAS ENVIADAS --------
    st.markdown('<div class="dreamlab-big-section-label">✉️ Propuestas enviadas</div>', unsafe_allow_html=True)
    df_s = load_sent_df()

    if df_s.empty:
        st.info("Aún no hay propuestas enviadas registradas.")
    else:
        if "owner_display_name" not in df_s.columns:
            df_s["owner_display_name"] = ""

        df_s["owner_display_name"] = df_s["owner_display_name"].fillna("").apply(clean_owner_display_name)
        df_s["sent_at_dt"] = pd.to_datetime(df_s["sent_at"], errors="coerce")
        df_s["success"] = pd.to_numeric(df_s["success"], errors="coerce").fillna(0).astype(int)

        payload_rows = []
        for _, r in df_s.iterrows():
            p = safe_json_loads(r["payload_json"])
            if not p:
                p = {"raw": r["payload_json"]}

            if not p.get("owner_display_name"):
                p["owner_display_name"] = r.get("owner_display_name", "") or p.get("cliente", "") or "Sin cliente"
            if not p.get("owner_username"):
                p["owner_username"] = r.get("owner_username", "")
            if not p.get("created_by_role"):
                p["created_by_role"] = r.get("created_by_role", "")

            payload_rows.append({
                "sent_at": r["sent_at"],
                "sent_at_dt": r["sent_at_dt"],
                "to_email": r["to_email"],
                "subject": r["subject"],
                "quote_id": int(r["quote_id"] or 0),
                "success": bool(r["success"]),
                "error": r["error"],
                "owner_display_name": p.get("owner_display_name", "Sin cliente"),
                "marca": p.get("marca", ""),
                "campania": p.get("campania", ""),
                "talento": p.get("talento", ""),
                "total_final": p.get("total_final", 0.0),
                "payload": p,
            })

        df_s_viz = pd.DataFrame(payload_rows)
        df_s_viz["owner_display_name"] = df_s_viz["owner_display_name"].fillna("").apply(clean_owner_display_name)

        clientes_envios = sorted([c for c in df_s_viz["owner_display_name"].dropna().unique().tolist() if str(c).strip()])
        filtro_cliente_envio = st.selectbox("Filtrar envíos por cliente", ["Todos"] + clientes_envios)

        if filtro_cliente_envio != "Todos":
            df_s_viz = df_s_viz[df_s_viz["owner_display_name"] == filtro_cliente_envio]

        st.caption(f"Total de envíos mostrados: {len(df_s_viz)}")

        if df_s_viz.empty:
            st.warning("No hay envíos para ese cliente.")
        else:
            total_envios = len(df_s_viz)
            exitosos = int(df_s_viz["success"].sum())
            tasa_exito = (exitosos / total_envios * 100) if total_envios > 0 else 0.0

            s1, s2, s3 = st.columns(3)
            with s1:
                st.metric("Envíos", total_envios)
            with s2:
                st.metric("Envíos exitosos", exitosos)
            with s3:
                st.metric("Tasa de éxito", f"{tasa_exito:.1f}%")

            serie_s = (
                df_s_viz.dropna(subset=["sent_at_dt"])
                .assign(fecha=df_s_viz["sent_at_dt"].dt.date)
                .groupby("fecha", as_index=False)
                .agg(envios=("quote_id", "count"))
            )

            status_df = pd.DataFrame({
                "estatus": ["Exitoso", "Fallido"],
                "total": [
                    int(df_s_viz["success"].sum()),
                    int((~df_s_viz["success"].astype(bool)).sum())
                ]
            })

            top_marcas_envios = (
                df_s_viz.groupby("marca", as_index=False)
                .agg(envios=("quote_id", "count"))
                .sort_values("envios", ascending=False)
                .head(10)
            )

            e1, e2 = st.columns(2)

            with e1:
                st.markdown('<div class="dreamlab-section-label">Envíos por día</div>', unsafe_allow_html=True)
                if serie_s.empty:
                    st.info("No hay suficientes datos para esta gráfica.")
                else:
                    chart_envios_dia = alt.Chart(serie_s).mark_line(point=True).encode(
                        x=alt.X("fecha:T", title="Fecha"),
                        y=alt.Y("envios:Q", title="Envíos")
                    ).properties(height=300)
                    st.altair_chart(chart_envios_dia, use_container_width=True)

            with e2:
                st.markdown('<div class="dreamlab-section-label">Estatus de envíos</div>', unsafe_allow_html=True)
                if status_df["total"].sum() == 0:
                    st.info("No hay suficientes datos para esta gráfica.")
                else:
                    chart_status = alt.Chart(status_df).mark_bar().encode(
                        x=alt.X("estatus:N", title="Estatus"),
                        y=alt.Y("total:Q", title="Total")
                    ).properties(height=300)
                    st.altair_chart(chart_status, use_container_width=True)

            st.markdown('<div class="dreamlab-section-label">Envíos por marca</div>', unsafe_allow_html=True)
            if top_marcas_envios.empty:
                st.info("No hay suficientes datos para esta gráfica.")
            else:
                chart_marcas_envios = alt.Chart(top_marcas_envios).mark_bar().encode(
                    x=alt.X("envios:Q", title="Envíos"),
                    y=alt.Y("marca:N", sort="-x", title="Marca")
                ).properties(height=320)
                st.altair_chart(chart_marcas_envios, use_container_width=True)

            sent_payloads = [
                {
                    "sent_at": r["sent_at"],
                    "to_email": r["to_email"],
                    "subject": r["subject"],
                    "quote_id": int(r["quote_id"] or 0),
                    "success": bool(r["success"]),
                    "error": r["error"],
                    "marca": r["marca"],
                    "campania": r["campania"],
                    "talento": r["talento"],
                    "total_final": r["total_final"],
                    "payload": r["payload"],
                }
                for _, r in df_s_viz.iterrows()
            ]

            json_sent = json.dumps(sent_payloads, ensure_ascii=False, indent=2)
            col3, col4 = st.columns(2)
            with col3:
                st.download_button(
                    "📥 Descargar historial envíos (JSON)",
                    json_sent,
                    "historial_envios_dreamlab.json",
                    "application/json"
                )
            with col4:
                if not REPORTLAB_AVAILABLE:
                    st.warning("Instala reportlab para PDF: pip install reportlab")
                else:
                    pdf_sent = build_sent_history_pdf_bytes(sent_payloads)
                    st.download_button(
                        "📄 Descargar historial envíos (PDF)",
                        pdf_sent,
                        "historial_envios_dreamlab.pdf",
                        "application/pdf"
                    )

st.caption(f"Datos guardados en MongoDB Atlas ({MONGO_DB_NAME}).")