import sys
import pyarrow as pa
import pyarrow.parquet as pq

# Comprobar arugemnto filename
if len(sys.argv) != 3 or sys.argv[1] != '-f':
    print("[*] Run:\n\tparquet_schema.py -f <filename.parquet>")
    sys.exit(1)

filename = sys.argv[2]

# Cargar el archivo Parquet utilizando memory map
with pa.memory_map(filename, 'r') as source:
    tabla_parquet = pq.read_table(source)

# Obtener el esquema
esquema = tabla_parquet.schema

# Imprimir información sobre las columnas
for campo in esquema:
    print(f"Campo: {campo.name}, Tipo: {campo.type}")
