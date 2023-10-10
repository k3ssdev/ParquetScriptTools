# Parquet Script Tools [Python]

Colección de scripts de Python para tratar ficheros parquet de manera rápida: ver el esquema del parquet, convertir a csv, comprobar filas duplicadas...

## Requisitos previos
Asegúrate de tener las siguientes bibliotecas instaladas en tu entorno de Python:

- pandas
- pyarrow

Puedes instalar estas bibliotecas utilizando el siguiente comando:

```bash
pip install pandas pyarrow
```

## Uso

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

**Nota:** Asegúrate de que las bibliotecas necesarias estén instaladas antes de ejecutar el script. En caso de que alguna biblioteca no esté instalada, el script imprimirá un mensaje indicando cuál biblioteca falta.

## Contribuciones
Si encuentras problemas o tienes sugerencias de mejora, no dudes en abrir un problema o enviar una solicitud de extracción.

¡Disfruta utilizando este script para convertir archivos Parquet a CSV y verificar duplicados en tus datos!
