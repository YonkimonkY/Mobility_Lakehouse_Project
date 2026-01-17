"""
Pipeline completo de ingesta Bronze → Silver → Gold
Ejecuta los 3 pasos secuencialmente SIN necesidad de Airflow
"""
import subprocess
import sys
import time

def run_step(name, script_path):
    """Ejecuta un paso del pipeline y reporta éxito/fallo"""
    print("\n" + "=" * 70)
    print(f"▶️  EJECUTANDO: {name}")
    print("=" * 70)
    
    start = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=False,  # Mostrar output en tiempo real
            text=True
        )
        
        elapsed = time.time() - start
        print(f"\n✅ {name} completado en {elapsed/60:.1f} minutos")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {name} falló con código {e.returncode}")
        return False
    except Exception as e:
        print(f"\n❌ Error ejecutando {name}: {e}")
        return False

def main():
    print("=" * 70)
    print("PIPELINE COMPLETO: BRONZE → SILVER → GOLD")
    print("=" * 70)
    print("⏱️  Tiempo estimado total: 2-4 horas")
    print()
    
    steps = [
        ("PASO 1: Bronze (Ingesta)", "scripts/ingest_bronze_s3.py"),
        ("PASO 2: Silver (Transformación)", "scripts/process_silver_s3.py"),
        ("PASO 3: Gold (Métricas)", "scripts/process_gold.py"),
    ]
    
    total_start = time.time()
    results = []
    
    for name, script in steps:
        success = run_step(name, script)
        results.append((name, success))
        
        if not success:
            print("\n⚠️  PIPELINE DETENIDO POR ERROR")
            print("¿Deseas continuar con el siguiente paso de todos modos? (s/n)")
            # En modo automatizado, detener aquí
            break
    
    # Resumen final
    print("\n" + "=" * 70)
    print("RESUMEN DEL PIPELINE")
    print("=" * 70)
    
    for name, success in results:
        status = "✅" if success else "❌"
        print(f"  {status} {name}")
    
    total_elapsed = time.time() - total_start
    print(f"\n⏱️  Tiempo total: {total_elapsed/60:.1f} minutos")
    
    if all(success for _, success in results):
        print("\n🎉 ¡PIPELINE COMPLETADO EXITOSAMENTE!")
    else:
        print("\n⚠️  Pipeline completado con errores")
    
    print("=" * 70)

if __name__ == "__main__":
    main()
