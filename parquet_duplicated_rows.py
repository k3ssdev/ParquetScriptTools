import sys, importlib
import pandas, pyarrow

# Comprobar arugemnto filename
if len(sys.argv) != 2:
        print("[*] Run:\n\tscript.py <filename.parquet>")
        sys.exit(1)


# Lista de bibliotecas necesarias
libraries = ['pandas', 'pyarrow']

# Comprobar si cada biblioteca está instalada
for lib in libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"{lib} no está instalada. Por favor, instálala antes de ejecutar este script.")


# Cargar el archivo Parquet en un DataFrame

filename = sys.argv[1]

df = pandas.read_parquet(filename)


# Verificar duplicados basados en todas las columnas

duplicados = df[df.duplicated()]


# Mostrar los duplicados, si los hay

if not duplicados.empty:
    print("Filas duplicadas encontradas:")
    print(duplicados)

else:
    print("No se encontraron filas duplicadas.")
