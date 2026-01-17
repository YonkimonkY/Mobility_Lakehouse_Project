"""
Script para descargar datos de movilidad MITMA del año 2022 completo.
Descarga archivos diarios de Viajes en formato CSV.gz

Fuente: https://movilidad-opendata.mitma.es/RSS.xml
"""
import requests
import os
from datetime import datetime, timedelta
from pathlib import Path
import time
from tqdm import tqdm

# Configuración
OUTPUT_DIR = Path("data/raw/mitma2")
YEAR = 2022

# Tipos de archivos a descargar (SOLO VIAJES DISTRITOS)
TIPOS_ARCHIVOS = [
    ("Viajes_distritos", "distritos")  
]

def generar_urls_2022():
    """
    Genera todas las URLs de descarga para el año 2022.
    URL correcta del RSS: https://movilidad-opendata.mitma.es/
    """
    urls = []
    start_date = datetime(YEAR, 1, 1)
    end_date = datetime(YEAR, 12, 31)
    
    current_date = start_date
    while current_date <= end_date:
        fecha_str = current_date.strftime("%Y%m%d")
        
        for tipo_archivo, nivel in TIPOS_ARCHIVOS:
            filename = f"{fecha_str}_{tipo_archivo}.csv.gz"
            
           
            year_month = current_date.strftime("%Y-%m")  # Formato: 2022-01
            
            # Construir URL
            url = f"https://movilidad-opendata.mitma.es/estudios_basicos/por-distritos/viajes/ficheros-diarios/{year_month}/{filename}"
            
            urls.append({
                'url': url,
                'filename': filename,
                'fecha': current_date,
                'tipo': tipo_archivo
            })
        
        current_date += timedelta(days=1)
    
    return urls

def descargar_archivo(url, output_path, max_retries=3):
    """
    Descarga un archivo con reintentos si falla.
    """
    for intento in range(max_retries):
        try:
            # Headers para simular navegador
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=60, stream=True)
            
            if response.status_code == 200:
                # Descargar en chunks para archivos grandes
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Verificar tamaño
                size_mb = output_path.stat().st_size / (1024 * 1024)
                return True
                
            elif response.status_code == 404:
                return False
                
            else:
                time.sleep(2)  # Esperar antes de reintentar
                
        except Exception as e:
            time.sleep(2)
    
    return False

def main():
    print("=" * 70)
    print("DESCARGA DE DATOS MITMA - AÑO 2022 COMPLETO")
    print("=" * 70)
    
    # Crear directorio de salida
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generar URLs
    urls = generar_urls_2022()
    total_archivos = len(urls)
    
    print(f"\n Total de archivos a descargar: {total_archivos:,}")
    print(f" Periodo: 01/01/{YEAR} - 31/12/{YEAR}")
    print(f" Destino: {OUTPUT_DIR.absolute()}")
    print(f"\n{'=' * 70}\n")
    
    # Estadísticas
    descargados = 0
    saltados = 0
    errores = 0
    total_bytes = 0
    start_time = time.time()
    
    # Barra de progreso
    with tqdm(total=total_archivos, 
              desc="Descargando", 
              unit="archivo",
              bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
        
        for i, url_info in enumerate(urls, 1):
            output_path = OUTPUT_DIR / url_info['filename']
            
            # Saltar si ya existe
            if output_path.exists():
                saltados += 1
                total_bytes += output_path.stat().st_size
                pbar.update(1)
                pbar.set_postfix({
                    'Actual': url_info['filename'][:30],
                    'OK': descargados,
                    'Skip': saltados,
                    'Err': errores,
                    'GB': f"{total_bytes/(1024**3):.2f}"
                })
                continue
            
            # Descargar
            exito = descargar_archivo(url_info['url'], output_path)
            
            if exito:
                descargados += 1
                try:
                    total_bytes += output_path.stat().st_size
                except:
                    pass
            else:
                errores += 1
            
            # Actualizar barra de progreso
            pbar.update(1)
            pbar.set_postfix({
                'Actual': url_info['filename'][:30],
                'OK': descargados,
                'Skip': saltados,
                'Err': errores,
                'GB': f"{total_bytes/(1024**3):.2f}"
            })
            
            # Rate limiting
            time.sleep(0.5)
    
    # Tiempo total
    elapsed = time.time() - start_time
    
    # Resumen final
    print("\n" + "=" * 70)
    print("RESUMEN DE DESCARGA")
    print("=" * 70)
    print(f" Descargados: {descargados:,} archivos")
    print(f" Saltados (ya existían): {saltados:,} archivos")
    print(f" Errores: {errores:,} archivos")
    print(f" Total procesado: {descargados + saltados + errores:,} / {total_archivos:,}")
    print(f" Tiempo total: {elapsed/60:.1f} minutos")
    
    # Calcular espacio usado
    total_gb = total_bytes / (1024 ** 3)
    print(f"💾 Espacio descargado: {total_gb:.2f} GB")
    
    if descargados > 0:
        avg_size = total_bytes / (descargados + saltados) if (descargados + saltados) > 0 else 0
        print(f" Tamaño promedio: {avg_size/(1024**2):.1f} MB/archivo")
        print(f"  Velocidad promedio: {total_gb/(elapsed/60):.2f} GB/min")
    
    print("=" * 70)

if __name__ == "__main__":
    main()
