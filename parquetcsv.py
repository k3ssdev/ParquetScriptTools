import sys, importlib
import pandas, pyarrow

# Lista de bibliotecas que necesitas
libraries = ['pandas', 'pyarrow']

# Comprobar si cada biblioteca está instalada
for lib in libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"{lib} no está instalada. Por favor, instálala antes de ejecutar este script.")

# Comprobar arugemnto filename
if len(sys.argv) != 2:
        print("[*] Run:\n\tparquetcsv.py <filename.parquet>")
        sys.exit(1)

filename = sys.argv[1]

# Leer el fichero y convertirlo
df = pandas.read_parquet(filename)
df.to_csv('output.csv')




