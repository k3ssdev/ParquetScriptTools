import sys
import pandas as pd
import pyarrow as pa

def merge_parquet_files(file_paths):
    # Necesita al menos 2 ficheros para unir
    if len(file_paths) < 2:
        print("Se requieren al menos dos archivos Parquet para la unión.")
        return

    # Leer parquet 1 utilizando memory map
    with pa.memory_map(file_paths[0], 'r') as source:
        merged_df = pa.parquet.read_table(source).to_pandas()

    # Recorre los demás ficheros para encontrar columna común
    for path in file_paths[1:]:
        # Leer parquet utilizando memory map
        with pa.memory_map(path, 'r') as source:
            df = pa.parquet.read_table(source).to_pandas()

        common_columns = set(merged_df.columns) & set(df.columns)
        if not common_columns:
            print(f"No se encontraron columnas comunes para la unión con {path}.")
            return
        common_column = common_columns.pop()
        
        print(f"Fusionando con {path} usando la columna común: {common_column}")

        try:
            # Unir ficheros
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        except Exception as e:
            print(f"Error durante la unión con {path}: {str(e)}")
            return

    return merged_df

def main():
    # Método principal, comprueba que recibe argumentos
    if len(sys.argv) < 4 or sys.argv[1] != '-f':
        print("Uso: python script.py -f file1.parquet file2.parquet [file3.parquet ...] [-o archivo_salida.parquet]")
        sys.exit(1)

    # Manejar el argumento opcional -o
    output_file = 'merged_result.parquet'
    if '-o' in sys.argv:
        try:
            output_index = sys.argv.index('-o')
            output_file = sys.argv[output_index + 1]
            del sys.argv[output_index:output_index + 2]  # Eliminar -o y el nombre del archivo de sys.argv
        except IndexError:
            print("Error: Se esperaba un nombre de archivo después de -o.")
            sys.exit(1)

    # Nombres de ficheros desde el tercer argumento (después de -f)
    file_paths = sys.argv[2:]

    # Llama a la función merge_parquet_files y guarda el resultado
    result_df = merge_parquet_files(file_paths)

    # Guarda el resultado fusionado en el archivo especificado
    result_df.to_parquet(output_file, index=False)

if __name__ == "__main__":
    main()
