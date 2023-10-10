import sys
import importlib
import pandas as pd
import pyarrow.parquet as pq

# Comprobar argumento filename
if len(sys.argv) < 2:
    print("[*] Run:\n\tscript.py <filename1.parquet> [filename2.parquet ...]")
    sys.exit(1)

# Lista de bibliotecas necesarias
libraries = ['pandas', 'pyarrow']

# Comprobar si cada biblioteca está instalada
for lib in libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"{lib} no está instalada. Por favor, instálala antes de ejecutar este script.")
        sys.exit(1)

# Cargar todos los archivos Parquet en un solo DataFrame

filenames = sys.argv[1:]
dfs = []

for filename in filenames:
    # Cargar el archivo Parquet en un DataFrame
    table = pq.read_table(filename)
    df = table.to_pandas()
    dfs.append(df)

# Combinar los DataFrames en uno solo
combined_df = pd.concat(dfs, ignore_index=True)

# Verificar duplicados basados en todas las columnas
duplicates = combined_df[combined_df.duplicated()]

# Mostrar los duplicados, si los hay
if not duplicates.empty:
    print("Filas duplicadas encontradas:")
    print(duplicates)
else:
    print("No se encontraron filas duplicadas.")
