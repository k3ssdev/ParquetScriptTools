import sys
import pyarrow.parquet as pq

# Comprobar arugemnto filename
if len(sys.argv) != 2:
        print("[*] Run:\n\tparquetcsv.py <filename.parquet>")
        sys.exit(1)

filename = sys.argv[1]

# Cargar el archivo Parquet
tabla_parquet = pq.read_table(filename)

# Obtener el esquema
esquema = tabla_parquet.schema

# Imprimir información sobre las columnas
for campo in esquema:
    print(f"Campo: {campo.name}, Tipo: {campo.type}")
