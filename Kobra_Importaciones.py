import sys
import time
import traceback
import unicodedata
from datetime import datetime
from pathlib import Path
import os

import imaplib
import email
import re

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from Conexion import connect_to_database_sqlserver
from Kobra_Enviar_Correo import enviar_alertas_importacion

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

EMAIL = os.getenv('Email')
PASSWORD = os.getenv('Password')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')

IMPORTACIONES_URL = "https://admin.kobra.red/importacion"
LOG_PATH = BASE_DIR / "Logs" / "Kobra_Importaciones_Log.txt"
SESSION_PATH = BASE_DIR / "Sesion" / "sesion_kobra.json" # <--- NUEVA LÍNEA

LIMITE_FILAS = 6
ESPERA_SEGUNDOS = 300
SQLSERVER_DATABASE = "VISITAS_KOBRA"

MANUAL_TRIGGER_REQUESTED = False
PROCESS_RUNNING = False

IMPORTACIONES_MAP = {
    "cliente": "cliente",
    "id lote": "id_lote",
    "tipo layout": "tipo_layout",
    "estatus": "estatus",
    "cuentas importadas": "cuentas_importadas",
    "errores": "errores",
    "usuario": "usuario",
    "creada": "fecha_creada",
    "fecha creada": "fecha_creada",
}

DETALLE_MAP = {
    "id cuenta": "id_cuenta",
    "id subcuenta": "id_subcuenta",
    "folio domicilio": "folio",
    "domicilio": "domicilio",
    "estatus": "estatus",
    "error": "error",
    "resultado": "resultado",
    "probabilidad de visita": "probabilidad_visita",
}

IMPORTACIONES_FIELDS = [
    "id_lote",
    "cliente",
    "tipo_layout",
    "estatus",
    "cuentas_importadas",
    "errores",
    "usuario",
    "fecha_creada",
]

DETALLE_FIELDS = [
    "id_lote",
    "id_cuenta",
    "id_subcuenta",
    "folio",
    "domicilio",
    "estatus",
    "error",
    "resultado",
    "probabilidad_visita",
]


def normalizar_texto(texto):
    texto = unicodedata.normalize("NFKD", texto or "")
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return " ".join(texto.lower().split())


def normalizar_encabezado(texto, equivalencias):
    return equivalencias.get(normalizar_texto(texto))


def obtener_textos_visibles(scope, selector):
    return scope.eval_on_selector_all(
        selector,
        """(elements) => elements
            .filter((el) => {
                const style = window.getComputedStyle(el);
                const rect = el.getBoundingClientRect();
                return style.display !== "none"
                    && style.visibility !== "hidden"
                    && el.getAttribute("aria-hidden") !== "true"
                    && rect.width > 0
                    && rect.height > 0;
            })
            .map((el) => el.innerText.replace(/\\u00a0/g, " ").trim())
        """,
    )


def valor_columna(columnas, indices, campo):
    idx = indices.get(campo, -1)
    if 0 <= idx < len(columnas):
        return columnas[idx]
    return ""


def limpiar_columnas_iniciales_vacias(columnas, encabezados):
    if encabezados and len(columnas) > len(encabezados):
        diferencia = len(columnas) - len(encabezados)
        if columnas[:diferencia] == [""] * diferencia:
            return columnas[diferencia:]
    return columnas


def convertir_entero(valor):
    limpio = (valor or "").strip().replace(",", "")
    if not limpio:
        return None

    try:
        return int(limpio)
    except ValueError:
        digitos = "".join(c for c in limpio if c.isdigit())
        return int(digitos) if digitos else None


def convertir_fecha_sql(valor):
    limpio = (valor or "").strip()
    if not limpio:
        return None

    limpio = limpio.split(" (")[0].strip()

    for formato in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"):
        try:
            fecha = datetime.strptime(limpio, formato)
            return fecha.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    return limpio


def extraer_tabla(page, header_map, expected_fields, row_limit=None, debug_nombre="tabla"):
    encabezados = obtener_textos_visibles(page, "table thead th")
    if not encabezados:
        encabezados = obtener_textos_visibles(page, "table tr th")

    #print(f"DEBUG {debug_nombre} encabezados:", encabezados)

    indices = {}
    for idx, encabezado in enumerate(encabezados):
        campo = normalizar_encabezado(encabezado, header_map)
        if campo and campo not in indices:
            indices[campo] = idx

    #print(f"DEBUG {debug_nombre} indices:", indices)

    filas = page.query_selector_all("table tbody tr")
    if row_limit is not None:
        filas = filas[:row_limit]

    print(f"🔍 {debug_nombre} filas encontradas: {len(filas)}")

    registros = []

    for i, fila in enumerate(filas, start=1):
        columnas = obtener_textos_visibles(fila, "td")
        columnas = limpiar_columnas_iniciales_vacias(columnas, encabezados)

        #print(f"DEBUG {debug_nombre} fila {i} columnas:", columnas)

        if not indices:
            continue

        registro = {
            campo: valor_columna(columnas, indices, campo)
            for campo in expected_fields
        }

        print(f"📦 {debug_nombre} fila {i}:", registro)
        registros.append(registro)

    return registros

def obtener_codigo_gmail(email_user, app_password, max_intentos=10):
    """Se conecta a Gmail vía IMAP, busca el correo de Kobra y extrae el código de 6 dígitos."""
    print("📧 Conectando a Gmail para buscar el código...")
    
    try:
        # Conectarse al servidor IMAP de Gmail
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(email_user, app_password)
        mail.select('inbox')
        
        for intento in range(max_intentos):
            print(f"🔎 Buscando correo de Kobra (Intento {intento+1}/{max_intentos})...")
            
            # Buscar correos no leídos enviados por Kobra
            status, mensajes = mail.search(None, '(UNSEEN FROM "no-reply@kobra.red")')
            
            ids_correos = mensajes[0].split()
            if ids_correos:
                # Tomar el correo más reciente
                ultimo_id = ids_correos[-1]
                res, msg_data = mail.fetch(ultimo_id, '(RFC822)')
                
                for response_part in msg_data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_bytes(response_part[1])
                        
                        # Extraer el cuerpo del correo
                        cuerpo = ""
                        if msg.is_multipart():
                            for part in msg.walk():
                                if part.get_content_type() == "text/plain":
                                    cuerpo = part.get_payload(decode=True).decode('utf-8')
                                    break
                        else:
                            cuerpo = msg.get_payload(decode=True).decode('utf-8')
                        
                        # Usar RegEx para encontrar exactamente 6 dígitos seguidos
                        match = re.search(r'\b\d{6}\b', cuerpo)
                        if match:
                            codigo = match.group(0)
                            print(f"✅ ¡Código encontrado en Gmail!: {codigo}")
                            mail.logout()
                            return codigo
                            
            # Si no encontró el correo, espera 3 segundos y vuelve a intentar
            time.sleep(3)
            
        print("⚠️ Se agotó el tiempo esperando el correo de Kobra.")
        mail.logout()
        return None
        
    except Exception as e:
        print(f"❌ Error al leer Gmail: {e}")
        return None

def login(page):
    print("🌐 Abriendo login...")
    page.goto("https://admin.kobra.red/auth")
    page.wait_for_selector("#username")

    print("🔐 Ingresando credenciales...")
    page.fill("#username", EMAIL)
    page.fill('input[formcontrolname="contrasena"]', PASSWORD)
    page.click('button:has-text("Iniciar")')

    # Le damos 5 segundos de gracia al servidor de Kobra para que envíe el correo
    print("⏳ Esperando 5 segundos a que llegue el correo nuevo...")
    time.sleep(5)

    # El sistema envía el correo en este momento. Llamamos a nuestra función:
    codigo = obtener_codigo_gmail(EMAIL, GMAIL_APP_PASSWORD)
    
    if codigo:
        print("⌨️ Escribiendo código de verificación...")
        
        # AQUÍ ESTÁ CORREGIDO: Usamos codigo2FA
        page.wait_for_selector('input[formcontrolname="codigo2FA"]', timeout=5000) 
        page.fill('input[formcontrolname="codigo2FA"]', codigo)
        
        # Haz clic en el botón con el texto exacto de tu HTML
        try:
            page.click('button:has-text("Verificar código")')
        except:
            page.keyboard.press("Enter")
            
        # --- EL CAMBIO ESTÁ AQUÍ ---
        print("⏳ Esperando a que el sistema valide el código...")
        
        # Esperamos a que la URL cambie y ya NO contenga "auth"
        page.wait_for_url(lambda url: "auth" not in url.lower(), timeout=15000)
        
        # Pausa estratégica de 3 segundos para que Angular termine de guardar la sesión
        page.wait_for_timeout(3000) 
        print("✅ Login exitoso 100% automatizado")

        # --- NUEVA LÍNEA: Guardar la sesión ---
        page.context.storage_state(path=SESSION_PATH)
        print("💾 Sesión guardada para futuras ejecuciones.")
        # --------------------------------------

    else:
        print("❌ No se pudo automatizar el código. Por favor, ingrésalo manualmente.")
        input("👉 Presiona ENTER cuando ya estés dentro...")


def obtener_importaciones(page, limite=LIMITE_FILAS):
    print("🚀 Entrando a Importaciones...")
    page.goto(IMPORTACIONES_URL)
    page.wait_for_selector("table")
    page.wait_for_timeout(3000)

    registros = extraer_tabla(
        page,
        header_map=IMPORTACIONES_MAP,
        expected_fields=[
            "cliente",
            "id_lote",
            "tipo_layout",
            "estatus",
            "cuentas_importadas",
            "errores",
            "usuario",
            "fecha_creada",
        ],
        row_limit=limite,
        debug_nombre="importaciones",
    )

    importaciones = []
    for registro in registros:
        registro_sql = {
            "id_lote": convertir_entero(registro.get("id_lote")),
            "cliente": registro.get("cliente") or None,
            "tipo_layout": registro.get("tipo_layout") or None,
            "estatus": registro.get("estatus") or None,
            "cuentas_importadas": convertir_entero(registro.get("cuentas_importadas")),
            "errores": convertir_entero(registro.get("errores")),
            "usuario": registro.get("usuario") or None,
            "fecha_creada": convertir_fecha_sql(registro.get("fecha_creada")),
        }
        #print("🧾 Importación lista para SQL:", registro_sql)
        importaciones.append(registro_sql)

    return importaciones


def abrir_importacion_por_lote(page, id_lote):
    print(f"➡️ Abriendo detalle del lote {id_lote}...")
    page.goto(IMPORTACIONES_URL)
    page.wait_for_selector("table")
    page.wait_for_timeout(2000)

    texto_lote = str(id_lote)
    selectores = [
        f'table tbody tr a:has-text("{texto_lote}")',
        f'table tbody tr td:has-text("{texto_lote}")',
        f'text="{texto_lote}"',
    ]

    for selector in selectores:
        locator = page.locator(selector).first
        if locator.count() == 0:
            continue

        try:
            locator.scroll_into_view_if_needed()
            locator.click(force=True)
            page.wait_for_timeout(2000)
            return True
        except Exception:
            continue

    print(f"⚠️ No se pudo abrir el lote {id_lote}")
    return False


def abrir_modal_total_cuentas(page, id_lote):
    print(f"🪟 Abriendo modal de Total de cuentas para lote {id_lote}...")

    candidatos = [
        'text="Total de cuentas"',
        'text="Detalle cuentas"',
    ]

    for selector in candidatos:
        locator = page.locator(selector).first
        if locator.count() == 0:
            continue

        try:
            locator.scroll_into_view_if_needed()
            locator.click(force=True)
            page.wait_for_timeout(1500)
            page.wait_for_selector("table tbody tr", timeout=10000)
            return True
        except PlaywrightTimeoutError:
            continue
        except Exception:
            continue

    print(f"⚠️ No se pudo abrir el modal de detalle para lote {id_lote}")
    return False


def cerrar_modal_detalle(page):
    candidatos = [
        'button:has-text("Cerrar")',
        'button:has-text("Cancel")',
        '[aria-label="Close"]',
        '[aria-label="Cerrar"]',
    ]

    for selector in candidatos:
        locator = page.locator(selector).first
        if locator.count() == 0:
            continue

        try:
            locator.click(force=True)
            page.wait_for_timeout(800)
            return
        except Exception:
            continue

    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(800)
    except Exception:
        pass


def obtener_detalle_por_lote(page, id_lote):
    if not abrir_importacion_por_lote(page, id_lote):
        return []

    if not abrir_modal_total_cuentas(page, id_lote):
        return []

    registros = extraer_tabla(
        page,
        header_map=DETALLE_MAP,
        expected_fields=[
            "id_cuenta",
            "id_subcuenta",
            "folio",
            "domicilio",
            "estatus",
            "error",
            "resultado",
            "probabilidad_visita",
        ],
        debug_nombre=f"detalle lote {id_lote}",
    )

    detalles = []
    for registro in registros:
        detalle_sql = {
            "id_lote": id_lote,
            "id_cuenta": registro.get("id_cuenta") or None,
            "id_subcuenta": registro.get("id_subcuenta") or None,
            "folio": registro.get("folio") or None,
            "domicilio": registro.get("domicilio") or None,
            "estatus": registro.get("estatus") or None,
            "error": registro.get("error") or None,
            "resultado": registro.get("resultado") or None,
            "probabilidad_visita": registro.get("probabilidad_visita") or None,
        }
        #print("🧾 Detalle listo para SQL:", detalle_sql)
        detalles.append(detalle_sql)

    cerrar_modal_detalle(page)
    return detalles


def construir_log(resultado):
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lineas = [
        "=" * 100,
        f"EJECUCION: {ahora}",
        f"IMPORTACIONES TOMADAS: {len(resultado['importaciones'])}",
        f"IMPORTACIONES INSERTADAS: {resultado['insertadas_importaciones']}",
        f"IMPORTACIONES OMITIDAS: {resultado['omitidas_importaciones']}",
        f"DETALLES INSERTADOS: {resultado['insertados_detalle']}",
        f"LOTES OMITIDOS EN DETALLE: {len(resultado['lotes_omitidos_detalle'])}",
        "=" * 100,
        "",
        "[Kobra_Importaciones]",
        "",
    ]

    for idx, registro in enumerate(resultado["importaciones"], start=1):
        lineas.append(f"Registro {idx}: {registro}")

    lineas.extend(["", "[Kobra_Importaciones_Detalle]", ""])

    for id_lote, detalles in resultado["detalles_por_lote"].items():
        estado = "omitido" if id_lote in resultado["lotes_omitidos_detalle"] else "procesado"
        lineas.append(f"ID_LOTE {id_lote} - {estado} - filas detalle: {len(detalles)}")
        for idx, registro in enumerate(detalles, start=1):
            lineas.append(f"Detalle {idx}: {registro}")
        lineas.append("")

    if resultado["errores"]:
        lineas.extend(["[Errores]", ""])
        for error in resultado["errores"]:
            lineas.append(error)
            lineas.append("")

    lineas.append("")
    return "\n".join(lineas)


def guardar_log(contenido):
    with LOG_PATH.open("a", encoding="utf-8") as archivo:
        archivo.write(contenido)
    print(f"📝 Log actualizado en: {LOG_PATH}")


def obtener_lotes_unicos(importaciones):
    vistos = set()
    lotes = []

    for registro in importaciones:
        id_lote = registro.get("id_lote")
        if id_lote is None or id_lote in vistos:
            continue
        vistos.add(id_lote)
        lotes.append(id_lote)

    return lotes


def conectar_sqlserver():
    print(f"🛢️ Conectando a SQL Server DB={SQLSERVER_DATABASE}...")
    conn = connect_to_database_sqlserver(SQLSERVER_DATABASE)
    if conn is None:
        raise RuntimeError("No se pudo abrir la conexión a SQL Server.")
    return conn


def importacion_existe(cursor, id_lote, estatus):
    cursor.execute(
        """
        SELECT 1
        FROM Kobra_Importaciones
        WHERE id_lote = ? AND estatus = ?
        """,
        id_lote,
        estatus,
    )
    return cursor.fetchone() is not None


def detalle_lote_existe(cursor, id_lote):
    cursor.execute(
        """
        SELECT TOP 1 1
        FROM Kobra_Importaciones_Detalle
        WHERE id_lote = ?
        """,
        id_lote,
    )
    return cursor.fetchone() is not None


def insertar_importaciones(conn, importaciones):
    cursor = conn.cursor()
    insertadas = 0
    omitidas = 0

    for registro in importaciones:
        if registro["id_lote"] is None or not registro["estatus"]:
            print("⚠️ Importación omitida por datos incompletos:", registro)
            omitidas += 1
            continue

        if importacion_existe(cursor, registro["id_lote"], registro["estatus"]):
            print(
                f"⏭️ Importación ya existe, se omite: lote={registro['id_lote']} estatus={registro['estatus']}"
            )
            omitidas += 1
            continue

        cursor.execute(
            """
            INSERT INTO Kobra_Importaciones (
                id_lote,
                cliente,
                tipo_layout,
                estatus,
                cuentas_importadas,
                errores,
                usuario,
                fecha_creada
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            registro["id_lote"],
            registro["cliente"],
            registro["tipo_layout"],
            registro["estatus"],
            registro["cuentas_importadas"],
            registro["errores"],
            registro["usuario"],
            registro["fecha_creada"],
        )
        insertadas += 1
        print(
            f"✅ Importación insertada: lote={registro['id_lote']} estatus={registro['estatus']}"
        )

    conn.commit()
    cursor.close()
    return insertadas, omitidas


def insertar_detalles_lote(conn, id_lote, detalles):
    cursor = conn.cursor()

    if detalle_lote_existe(cursor, id_lote):
        print(f"⏭️ Detalle ya existe para el lote {id_lote}, se omite.")
        cursor.close()
        return 0, True

    insertados = 0
    for registro in detalles:
        cursor.execute(
            """
            INSERT INTO Kobra_Importaciones_Detalle (
                id_lote,
                id_cuenta,
                id_subcuenta,
                folio,
                domicilio,
                estatus,
                error,
                resultado,
                probabilidad_visita
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            registro["id_lote"],
            registro["id_cuenta"],
            registro["id_subcuenta"],
            registro["folio"],
            registro["domicilio"],
            registro["estatus"],
            registro["error"],
            registro["resultado"],
            registro["probabilidad_visita"],
        )
        insertados += 1

    conn.commit()
    cursor.close()
    print(f"✅ Detalle insertado para lote {id_lote}: {insertados} filas")
    return insertados, False


def instalar_boton_manual(page):
    """Inserta un botón flotante circular con ícono de 'Play' y lo enlaza a Python."""
    print("🎨 Instalando botón flotante de ejecución manual...")
    
    # Exponemos la función a JS para que el botón pueda avisarle a Python
    try:
        page.expose_binding("ejecutarProcesoKobra", on_manual_button_click)
    except Exception:
        pass # Ignorar si ya estaba expuesta en la sesión reanudada
        
    js_code = """
    (() => {
        const BUTTON_ID = "codex-kobra-run-button";

        function crearBoton() {
            // Solo mostrar en la página de importaciones
            if (!window.location.href.startsWith("https://admin.kobra.red/importacion")) {
                return;
            }

            // Si ya existe, no duplicarlo
            if (document.getElementById(BUTTON_ID)) {
                return;
            }

            // Crear el botón flotante (FAB)
            const boton = document.createElement("button");
            boton.id = BUTTON_ID;
            boton.type = "button";
            boton.title = "Ejecutar Proceso Ahora"; // Tooltip nativo al pasar el mouse
            
            // Estilos CSS inyectados directamente
            boton.style.position = "fixed";
            boton.style.bottom = "30px";
            boton.style.right = "30px";
            boton.style.zIndex = "99999";
            boton.style.width = "60px";
            boton.style.height = "60px";
            boton.style.borderRadius = "50%";
            boton.style.backgroundColor = "#2196F3"; // Azul Material
            boton.style.color = "#ffffff";
            boton.style.border = "none";
            boton.style.boxShadow = "0 4px 10px rgba(0,0,0,0.3)";
            boton.style.cursor = "pointer";
            boton.style.display = "flex";
            boton.style.alignItems = "center";
            boton.style.justifyContent = "center";
            boton.style.transition = "transform 0.2s, background-color 0.2s";

            // Íconos SVG (Play y Cargando)
            const playIcon = `<svg style="width:32px;height:32px" viewBox="0 0 24 24"><path fill="currentColor" d="M8,5.14V19.14L19,12.14L8,5.14Z" /></svg>`;
            const waitIcon = `<svg style="width:32px;height:32px" viewBox="0 0 24 24"><path fill="currentColor" d="M12,4V2A10,10 0 0,0 2,12H4A8,8 0 0,1 12,4Z"><animateTransform attributeName="transform" type="rotate" from="0 12 12" to="360 12 12" dur="1s" repeatCount="indefinite"/></path></svg>`;

            boton.innerHTML = playIcon;

            // Efectos visuales al pasar el mouse
            boton.addEventListener("mouseover", () => {
                boton.style.backgroundColor = "#1976D2"; // Azul oscuro
                boton.style.transform = "scale(1.1)"; // Crece un poco
            });
            boton.addEventListener("mouseout", () => {
                boton.style.backgroundColor = "#2196F3";
                boton.style.transform = "scale(1)";
            });

            // Acción al hacer clic
            boton.addEventListener("click", async () => {
                boton.disabled = true;
                boton.innerHTML = waitIcon; // Muestra ícono girando
                boton.style.backgroundColor = "#4CAF50"; // Cambia a Verde

                try {
                    // Llama a la función de Python
                    if (window.ejecutarProcesoKobra) {
                        await window.ejecutarProcesoKobra();
                    }
                } catch (error) {
                    console.error("No se pudo solicitar la ejecución manual", error);
                } finally {
                    // Restaurar botón después de 2 segundos
                    setTimeout(() => {
                        boton.disabled = false;
                        boton.innerHTML = playIcon;
                        boton.style.backgroundColor = "#2196F3";
                    }, 2000);
                }
            });

            document.body.appendChild(boton);
        }

        // Ejecutar al cargar y revisar constantemente si sigue ahí
        window.addEventListener("load", () => setTimeout(crearBoton, 1200));
        setInterval(crearBoton, 1500);
    })();
    """
    
    # Inyectamos el script para que sobreviva a recargas de página
    page.add_init_script(js_code)

def on_manual_button_click(source):
    global MANUAL_TRIGGER_REQUESTED

    MANUAL_TRIGGER_REQUESTED = True
    url = getattr(source, "page", None).url if getattr(source, "page", None) else ""
    if PROCESS_RUNNING:
        print(f"🖱️ Ejecución manual en cola desde: {url}")
    else:
        print(f"🖱️ Ejecución manual solicitada desde: {url}")
    return "ok"


def navegar_a_importaciones(page):
    try:
        page.goto(IMPORTACIONES_URL)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(1000)
    except Exception as exc:
        print(f"⚠️ No se pudo regresar a Importaciones: {exc}")


def esperar_siguiente_ciclo(page, segundos):
    global MANUAL_TRIGGER_REQUESTED

    print(f"⏳ Esperando {segundos // 60} minutos para la siguiente ejecución...")
    for restante in range(segundos, 0, -1):
        if MANUAL_TRIGGER_REQUESTED:
            MANUAL_TRIGGER_REQUESTED = False
            print("🚀 Se disparó una ejecución manual.")
            return

        if restante in {240, 180, 120, 60, 30, 10}:
            print(f"⏱️ Próxima ejecución en {restante} segundos...")

        page.wait_for_timeout(1000)


def ejecutar_ciclo(page):
    global PROCESS_RUNNING

    PROCESS_RUNNING = True
    resultado = {
        "importaciones": [],
        "detalles_por_lote": {},
        "insertadas_importaciones": 0,
        "omitidas_importaciones": 0,
        "insertados_detalle": 0,
        "lotes_omitidos_detalle": [],
        "errores": [],
    }

    conn = None
    try:
        conn = conectar_sqlserver()
        importaciones = obtener_importaciones(page, limite=LIMITE_FILAS)
        resultado["importaciones"] = importaciones

        insertadas, omitidas = insertar_importaciones(conn, importaciones)
        resultado["insertadas_importaciones"] = insertadas
        resultado["omitidas_importaciones"] = omitidas

        lotes = obtener_lotes_unicos(importaciones)
        cursor_validacion = conn.cursor()
        lotes_pendientes = []

        for id_lote in lotes:
            if detalle_lote_existe(cursor_validacion, id_lote):
                print(f"⏭️ Lote {id_lote} ya existe en detalle, se omite scraping de detalle.")
                resultado["lotes_omitidos_detalle"].append(id_lote)
                resultado["detalles_por_lote"][id_lote] = []
            else:
                lotes_pendientes.append(id_lote)

        cursor_validacion.close()

        for id_lote in lotes_pendientes:
            detalles = obtener_detalle_por_lote(page, id_lote)
            resultado["detalles_por_lote"][id_lote] = detalles

            insertados, omitido = insertar_detalles_lote(conn, id_lote, detalles)
            resultado["insertados_detalle"] += insertados
            if omitido:
                resultado["lotes_omitidos_detalle"].append(id_lote)

        # ------------- NUEVA FUNCIONALIDAD DE CORREOS -------------
        try:
            enviar_alertas_importacion(conn)
        except Exception as e:
            mensaje = f"Error al procesar notificaciones por correo: {e}"
            print(f"❌ {mensaje}")
            resultado["errores"].append(mensaje)
        # ----------------------------------------------------------

    except Exception as exc:
        mensaje = f"{type(exc).__name__}: {exc}"
        print(f"❌ Error en ciclo: {mensaje}")
        resultado["errores"].append(mensaje)
        resultado["errores"].append(traceback.format_exc())
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        PROCESS_RUNNING = False

    guardar_log(construir_log(resultado))
    return resultado


if __name__ == "__main__":
    with sync_playwright() as p:
        # 1. Le decimos a Chrome que arranque maximizado
        browser = p.chromium.launch(
            headless=False,
            channel="chrome",
            args=["--start-maximized"] 
        )

        # 2. Comprobar si existe un archivo de sesión guardado
        if SESSION_PATH.exists():
            print("🔄 Sesión previa encontrada. Intentando reanudar...")
            # 3. Agregamos no_viewport=True para que use todo el tamaño de la ventana
            context = browser.new_context(storage_state=SESSION_PATH, no_viewport=True)
            page = context.new_page()
            
            instalar_boton_manual(page)
            
            # Intentamos ir directo a Importaciones
            page.goto(IMPORTACIONES_URL)
            
            try:
                # Si la tabla carga, la sesión sigue viva
                page.wait_for_selector("table", timeout=10000)
                print("✅ Sesión reanudada con éxito. Saltando login.")
            except:
                print("⚠️ La sesión expiró o es inválida. Iniciando login desde cero...")
                SESSION_PATH.unlink(missing_ok=True)  # Borramos el archivo viejo
                context.close()  # Cerramos este contexto fallido
                
                # Empezamos desde cero con el viewport desactivado
                context = browser.new_context(no_viewport=True)
                page = context.new_page()
                instalar_boton_manual(page)
                login(page)
                
        else:
            # No hay sesión previa, empezamos desde cero con el viewport desactivado
            context = browser.new_context(no_viewport=True)
            page = context.new_page()
            instalar_boton_manual(page)
            login(page)

        # 3. Bucle principal de scraping
        while True:
            resultado = ejecutar_ciclo(page)
            print(f"\n🎯 Importaciones leídas: {len(resultado['importaciones'])}")
            print(f"🎯 Importaciones insertadas: {resultado['insertadas_importaciones']}")
            print(f"🎯 Detalles insertados: {resultado['insertados_detalle']}")

            navegar_a_importaciones(page)
            esperar_siguiente_ciclo(page, ESPERA_SEGUNDOS)