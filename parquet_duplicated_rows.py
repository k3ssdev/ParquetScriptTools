import sys
import importlib
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# Comprobar argumento
if len(sys.argv) < 2:
    print("[*] Run:\n\tscript.py <filename1.parquet> [filename2.parquet ...]")
    sys.exit(1)

# Lista bibliotecas
libraries = ['pandas', 'pyarrow']

# Comprobar bibliotecas
for lib in libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"{lib} no está instalada. Por favor, instálala antes de ejecutar este script.")
        sys.exit(1)

# Cargar archivos parquet en dataframes
filenames = sys.argv[1:]
dfs = []

for filename in filenames:
    # memory map para leer el parquet
    with pa.memory_map(filename, 'r') as source:
        table = pq.read_table(source)
        df = table.to_pandas()
        dfs.append(df)

# Combinar dataframes
combined_df = pd.concat(dfs, ignore_index=True)

# Verificar duplicados con todas las columnas
duplicates = combined_df[combined_df.duplicated()]

# Resultado y duplicados si hay
if not duplicates.empty:
    print("Filas duplicadas encontradas:")
    print(duplicates)
else:
    print("No se encontraron filas duplicadas.")
