import sys
import importlib
import pandas as pd
import pyarrow as pa

# Lista de bibliotecas que necesitas
libraries = ['pandas', 'pyarrow']

# Comprobar si cada biblioteca está instalada
for lib in libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"{lib} no está instalada. Por favor, instálala antes de ejecutar este script.")
        sys.exit(1)

# Comprobar argumentos
if len(sys.argv) < 4 or sys.argv[1] != '-f':
    print("Uso: python script.py -f <filename1.parquet> [filename2.parquet ...] -o <output.csv>")
    sys.exit(1)

# Manejar argumentos
input_files = []
output_file = 'output.csv'

for i in range(2, len(sys.argv)):
    if sys.argv[i] == '-o':
        try:
            output_file = sys.argv[i + 1]
        except IndexError:
            print("Error: Se esperaba un nombre de archivo después de -o.")
            sys.exit(1)
    elif sys.argv[i] == '-f':
        try:
            input_files.append(sys.argv[i + 1])
        except IndexError:
            print("Error: Se esperaba al menos un nombre de archivo después de -f.")
            sys.exit(1)

# Leer los archivos Parquet utilizando memory map
dfs = []

for filename in input_files:
    with pa.memory_map(filename, 'r') as source:
        df = pa.parquet.read_table(source).to_pandas()
        dfs.append(df)

# Combinar dataframes
combined_df = pd.concat(dfs, ignore_index=True)

# Guardar el resultado en un archivo CSV
combined_df.to_csv(output_file, index=False)



