# Parquet Script Tools [Python]

Colección de scripts de Python con PyArrow y Pandas para tratar ficheros parquet de manera rápida: ver el esquema del parquet, convertir a CSV, comprobar filas duplicadas y fusionar archivos parquet.

## Requisitos previos
Asegúrate de tener las siguientes bibliotecas instaladas en tu entorno de Python:

- pandas
- pyarrow

Puedes instalar estas bibliotecas utilizando el siguiente comando:

```bash
pip install pandas pyarrow
```

## Uso

### Parquet Schema
Ejecuta el script para ver el esquema de un archivo Parquet:

```bash
python parquet_schema.py <filename.parquet>
```

Donde `<filename.parquet>` es la ruta del archivo en formato Parquet.

### Conversión de Parquet a CSV
Ejecuta el script para convertir un archivo Parquet a CSV de la siguiente manera:

```bash
python parquetcsv.py <filename.parquet>
```

Donde `<filename.parquet>` es la ruta del archivo en formato Parquet que deseas convertir a CSV.

### Verificación de filas duplicadas
Además, el script verifica la presencia de filas duplicadas en el conjunto de datos y las muestra en la salida estándar si se encuentran.

Ejecuta el script de la siguiente manera para verificar y mostrar filas duplicadas:

```bash
python parquetcsv.py <filename.parquet>
```

### Fusión de archivos Parquet
El script `parquet_merge.py` permite fusionar archivos Parquet. Puede combinar dos o más archivos Parquet en uno solo, utilizando una columna común para la unión. Aquí tienes un ejemplo de uso:

```bash
python parquet_merge.py -f file1.parquet file2.parquet [file3.parquet ...] [-o merged_result.parquet]
```

- `-f`: Especifica que los nombres de archivo Parquet seguirán a esta opción.
- `-o`: Opcional. Especifica el nombre del archivo de salida. Si no se proporciona, el nombre predeterminado es `merged_result.parquet`.

**Nota:** Asegúrate de que las bibliotecas necesarias estén instaladas antes de ejecutar el script. En caso de que alguna biblioteca no esté instalada, el script imprimirá un mensaje indicando cuál biblioteca falta.

## Contribuciones
Si encuentras problemas o tienes sugerencias de mejora, no dudes en abrir un problema o enviar una solicitud de extracción.
