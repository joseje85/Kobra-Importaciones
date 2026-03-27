import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import defaultdict
from dotenv import load_dotenv
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Cargar variables de entorno
load_dotenv()

EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER') # Destinatario por defecto
EMAIL_CC = os.getenv('EMAIL_CC')

def generar_html(id_lote, total_cuentas, importadas, errores, registros):
    """Genera el cuerpo del correo en HTML imitando el diseño del dashboard."""
    
    # Filas de la tabla
    filas_html = ""
    for r in registros:
        filas_html += f"""
        <tr style="border-bottom: 1px solid #e2e8f0;">
            <td style="padding: 12px; color: #475569; font-size: 14px;">{r['id_cuenta']}</td>
            <td style="padding: 12px; color: #475569; font-size: 14px;">{r['id_subcuenta']}</td>
            <td style="padding: 12px; color: #ef4444; font-size: 14px;">{r['error']}</td>
            <td style="padding: 12px; color: #475569; font-size: 14px;">{r['resultado']}</td>
            <td style="padding: 12px; color: #475569; font-size: 14px;">{r['usuario']}</td>
            <td style="padding: 12px; color: #475569; font-size: 14px;">{r['fecha_creada']}</td>
        </tr>
        """

    # Plantilla HTML completa (con estilos en línea por compatibilidad con gestores de correo)
    html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f8fafc; margin: 0; padding: 20px;">
        <div style="max-width: 1000px; margin: 0 auto;">
            
            <h2 style="color: #1e293b; margin-bottom: 24px;">Importación terminada - Lote {id_lote}</h2>
            <p style="color: #64748b; font-size: 14px;">Hemos procesado la importación y estos son los resultados con error:</p>
            
            <table width="100%" cellspacing="0" cellpadding="0" style="margin-bottom: 24px;">
                <tr>
                    <td width="32%" style="background-color: #ffffff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-left: 4px solid #3b82f6;">
                        <div style="color: #64748b; font-size: 12px; font-weight: bold; text-transform: uppercase; margin-bottom: 8px;">Total de cuentas</div>
                        <div style="color: #3b82f6; font-size: 28px; font-weight: bold;">{total_cuentas}</div>
                    </td>
                    <td width="2%"></td> <td width="32%" style="background-color: #ffffff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-left: 4px solid #ef4444;">
                        <div style="color: #64748b; font-size: 12px; font-weight: bold; text-transform: uppercase; margin-bottom: 8px;">Errores</div>
                        <div style="color: #ef4444; font-size: 28px; font-weight: bold;">{errores}</div>
                    </td>
                    <td width="2%"></td> <td width="32%" style="background-color: #ffffff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-left: 4px solid #10b981;">
                        <div style="color: #64748b; font-size: 12px; font-weight: bold; text-transform: uppercase; margin-bottom: 8px;">Cuentas importadas</div>
                        <div style="color: #10b981; font-size: 28px; font-weight: bold;">{importadas}</div>
                    </td>
                </tr>
            </table>

            <div style="background-color: #ffffff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <h3 style="color: #1e293b; margin-top: 0; margin-bottom: 16px; font-size: 16px;">Detalle de registros</h3>
                <table width="100%" cellspacing="0" cellpadding="0" style="text-align: left; border-collapse: collapse;">
                    <thead>
                        <tr style="background-color: #f1f5f9;">
                            <th style="padding: 12px; color: #475569; font-size: 13px; font-weight: bold; border-bottom: 2px solid #e2e8f0;">ID Cuenta</th>
                            <th style="padding: 12px; color: #475569; font-size: 13px; font-weight: bold; border-bottom: 2px solid #e2e8f0;">ID Subcuenta</th>
                            <th style="padding: 12px; color: #475569; font-size: 13px; font-weight: bold; border-bottom: 2px solid #e2e8f0;">Error</th>
                            <th style="padding: 12px; color: #475569; font-size: 13px; font-weight: bold; border-bottom: 2px solid #e2e8f0;">Resultado</th>
                            <th style="padding: 12px; color: #475569; font-size: 13px; font-weight: bold; border-bottom: 2px solid #e2e8f0;">Usuario</th>
                            <th style="padding: 12px; color: #475569; font-size: 13px; font-weight: bold; border-bottom: 2px solid #e2e8f0;">Fecha Creada</th>
                        </tr>
                    </thead>
                    <tbody>
                        {filas_html}
                    </tbody>
                </table>
            </div>

        </div>
    </body>
    </html>
    """
    return html

def enviar_correo_sql(conn, asunto, html_content, destinatario, con_copia_a=None):
    """Envía el correo utilizando Database Mail de SQL Server."""
    cursor = conn.cursor()
    try:
        # Asegúrate de poner el nombre exacto de tu perfil de Database Mail aquí
        perfil_dbmail = 'AlertasSQL' 
        
        query = """
        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = ?,
            @recipients = ?,
            @copy_recipients = ?,
            @subject = ?,
            @body = ?,
            @body_format = 'HTML';
        """
        # Ejecutamos el SP nativo de SQL Server
        cursor.execute(query, (perfil_dbmail, destinatario, con_copia_a, asunto, html_content))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Error al solicitar envío a SQL Server: {e}")
        return False

def enviar_alertas_importacion(conn):
    """Consulta la DB, agrupa por lote, envía correos y actualiza el estatus."""
    print("📧 Iniciando proceso de envío de correos...")
    cursor = conn.cursor()
    
    query = """
    SELECT  ki.id_lote,
            ISNULL(ki.cuentas_importadas, 0) as cuentas_importadas, 
            ISNULL(ki.errores, 0) as errores, 
            ki.fecha_creada,
            kid.id_cuenta,
            kid.id_subcuenta,
            kid.error,
            kid.resultado,
            pg.usuario
    FROM Kobra_Importaciones AS ki
    LEFT JOIN Kobra_Importaciones_Detalle AS kid ON ki.id_lote = kid.id_lote
    INNER JOIN 
    (
        SELECT  *
        FROM OPENQUERY(PG_Recaudacion, 
        'SELECT enpd.id, 
                enpd.rfc, 
                enpd.nombre, 
                enpd.no_guia, 
                uu.nombre || '' '' || uu.primer_ape || '' '' || uu.segundo_ape as usuario
        FROM ejecucion_notificaciones_programa_documento enpd 
        INNER JOIN users_usuarios uu ON uu.id = enpd.usuario_asigna_zona_id 
        WHERE enpd.is_active = true
            AND enpd.no_guia is not null;')
    ) AS pg ON trim(pg.no_guia) = trim(kid.id_subcuenta)
    WHERE 
        kid.error = 'Sí' 
        AND ki.usuario = 'LUIS ALCARAZ'
        AND ki.estatus = 'Terminado'
        AND ISNULL(ki.correo_enviado, 0) = 0
        AND UPPER(resultado) NOT IN ('|Esta cuenta ya se encuentra inactiva.', '|Registro repetido.') 
    ORDER BY ki.id_lote
    """
    
    try:
        cursor.execute(query)
        # Obtenemos los nombres de las columnas para armar diccionarios
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        if not rows:
            print("📭 No hay correos pendientes por enviar.")
            return

        # Agrupar los resultados por id_lote
        lotes = defaultdict(list)
        for row in rows:
            lotes[row['id_lote']].append(row)

        for id_lote, registros in lotes.items():
            # Extraemos los datos generales del primer registro del lote
            importadas = registros[0]['cuentas_importadas']
            errores = registros[0]['errores']
            total_cuentas = importadas + errores
            
            # Generamos el HTML
            html = generar_html(id_lote, total_cuentas, importadas, errores, registros)
            asunto = f"Resultados de Importación Kobra - Lote {id_lote}"
            
            # Enviamos el correo
            if enviar_correo_sql(conn, asunto, html, EMAIL_RECEIVER, EMAIL_CC):
                print(f"✅ Correo enviado con éxito para el lote: {id_lote}")
                
                # Actualizamos la base de datos
                update_query = """
                UPDATE Kobra_Importaciones 
                SET correo_enviado = 1 
                WHERE id_lote = ? AND estatus = 'Terminado'
                """
                cursor.execute(update_query, (id_lote,))
                conn.commit()
                print(f"🔄 Base de datos actualizada (correo_enviado=1) para lote {id_lote}")
                
    except Exception as e:
        print(f"❌ Error en el proceso de notificaciones: {e}")
    finally:
        cursor.close()

