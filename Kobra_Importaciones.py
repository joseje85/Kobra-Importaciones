import sys
import time
import traceback
import unicodedata
from datetime import datetime
from pathlib import Path
import os

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

sys.path.append(r"C:\Progamas Compartido\utils")
from Conexion_Hana import connect_to_database_sqlserver

from dotenv import load_dotenv
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# EMAIL = "jespinosav@guanajuato.gob.mx"
# PASSWORD = "uUE0C3oaqRC1"

EMAIL = os.getenv('Email')
PASSWORD = os.getenv('Password')

IMPORTACIONES_URL = "https://admin.kobra.red/importacion"
LOG_PATH = Path(__file__).with_name("Kobra_Importaciones_Log.txt")
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

    print(f"DEBUG {debug_nombre} encabezados:", encabezados)

    indices = {}
    for idx, encabezado in enumerate(encabezados):
        campo = normalizar_encabezado(encabezado, header_map)
        if campo and campo not in indices:
            indices[campo] = idx

    print(f"DEBUG {debug_nombre} indices:", indices)

    filas = page.query_selector_all("table tbody tr")
    if row_limit is not None:
        filas = filas[:row_limit]

    print(f"🔍 {debug_nombre} filas encontradas: {len(filas)}")

    registros = []

    for i, fila in enumerate(filas, start=1):
        columnas = obtener_textos_visibles(fila, "td")
        columnas = limpiar_columnas_iniciales_vacias(columnas, encabezados)

        print(f"DEBUG {debug_nombre} fila {i} columnas:", columnas)

        if not indices:
            continue

        registro = {
            campo: valor_columna(columnas, indices, campo)
            for campo in expected_fields
        }

        print(f"📦 {debug_nombre} fila {i}:", registro)
        registros.append(registro)

    return registros


def login(page):
    print("🌐 Abriendo login...")
    page.goto("https://admin.kobra.red/auth")
    page.wait_for_selector("#username")

    print("🔐 Ingresando credenciales...")
    page.fill("#username", EMAIL)
    page.fill('input[formcontrolname="contrasena"]', PASSWORD)
    page.click('button:has-text("Iniciar")')

    print("📩 Ingresa el código en la página...")
    input("👉 Presiona ENTER cuando ya estés dentro...")

    page.wait_for_url("https://admin.kobra.red/**")
    print("✅ Login exitoso")


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
        print("🧾 Importación lista para SQL:", registro_sql)
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
        print("🧾 Detalle listo para SQL:", detalle_sql)
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
    page.expose_binding("ejecutarProcesoKobra", on_manual_button_click)
    page.add_init_script(
        """
        (() => {
            const BUTTON_ID = "codex-kobra-run-button";

            function crearBoton() {
                if (!window.location.href.startsWith("https://admin.kobra.red/importacion")) {
                    return;
                }

                if (window.location.pathname !== "/importacion") {
                    return;
                }

                if (document.getElementById(BUTTON_ID)) {
                    return;
                }

                const boton = document.createElement("button");
                boton.id = BUTTON_ID;
                boton.type = "button";
                boton.textContent = "Ejecutar Proceso";
                boton.style.position = "fixed";
                boton.style.top = "10px";
                boton.style.right = "330px";
                boton.style.zIndex = "99999";
                boton.style.padding = "12px 18px";
                boton.style.border = "none";
                boton.style.borderRadius = "10px";
                boton.style.background = "#0f766e";
                boton.style.color = "#fff";
                boton.style.fontWeight = "700";
                boton.style.fontSize = "14px";
                boton.style.boxShadow = "0 10px 24px rgba(15, 118, 110, 0.28)";
                boton.style.cursor = "pointer";

                boton.addEventListener("click", async () => {
                    boton.disabled = true;
                    const textoOriginal = boton.textContent;
                    boton.textContent = "Solicitado...";

                    try {
                        if (window.ejecutarProcesoKobra) {
                            await window.ejecutarProcesoKobra();
                        }
                    } catch (error) {
                        console.error("No se pudo solicitar la ejecución manual", error);
                    } finally {
                        setTimeout(() => {
                            boton.disabled = false;
                            boton.textContent = textoOriginal;
                        }, 2000);
                    }
                });

                document.body.appendChild(boton);
            }

            window.addEventListener("load", () => setTimeout(crearBoton, 1200));
            setInterval(crearBoton, 1500);
        })();
        """
    )


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
        browser = p.chromium.launch(
            headless=False,
            channel="chrome",
        )

        context = browser.new_context()
        page = context.new_page()

        instalar_boton_manual(page)
        login(page)

        while True:
            resultado = ejecutar_ciclo(page)
            print(f"\n🎯 Importaciones leídas: {len(resultado['importaciones'])}")
            print(f"🎯 Importaciones insertadas: {resultado['insertadas_importaciones']}")
            print(f"🎯 Detalles insertados: {resultado['insertados_detalle']}")

            navegar_a_importaciones(page)
            esperar_siguiente_ciclo(page, ESPERA_SEGUNDOS)
