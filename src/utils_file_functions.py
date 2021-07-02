import json
import os

def get_last_period_updated():
    
    '''
        Me fijo en el ultimo log para ver cual fue el ultimo periodo actualizado y lo traigo
        input:
        output:
            last_period_updated: (str) AAAAMMM ultimo periodo actualizado
    '''
    last_log = max([file for file in os.listdir('logs') if file[-4:] == 'json'])
    
    with open(f'./logs/{last_log}') as file:
            log = json.load(file)
            if log['descargas']['status'] != 'ERROR':
                last_period_updated = str(log['ultimo_periodo_actualizado'])
            else:
                last_period_updated = log['periodo_actual_en_mysql']
    return last_period_updated

def check_local_files_to_update(ultimo_periodo_actualizado):
    
    '''
        Me fijo si ya tengo el ultimo .csv en la carpeta data/zip para actualizar
        input:
            last_period_updated: (str) AAAAMMM ultimo periodo actualizado
        output:
            files_to_use: (list) Archivos ya descargados para actualizar
    '''
    files_already_downloaded = [file for file in os.listdir('data/zip') if file[-3:] == 'csv']
    
    files_to_use = [file for file in files_already_downloaded if file[-10:-4] > ultimo_periodo_actualizado]
    
    return files_to_use

def proceso_string(str_var):

    '''
        Proceso campos string -> Todo to lowercase, saco espacios, etc.
        
        input: Pandas Series en string
        output: Pandas Series de la string procesada     
    '''
    
    str_var = str_var.str.lower()
    str_var = str_var.str.strip()
    str_var = str_var.str.replace('\s+', ' ')
    
    return str_var

def cuit_validation(cuit):
    
    '''
        Valido la estructura de los CUITs
        
        input: Pandas Series en string
        output: True para los casos en que esta bien la estructura del CUIT
    '''
    
    valid_regex = r'3[034]\d{9}'
    
    return cuit.astype('string').str.match(valid_regex)

def check_headers(file):
    
    '''
        Me fijo si el archivo viene con Headers o no
        
        input: File path
        output: 
            -> (skip,headers) (tuple)
                    -> skip (int) es la cantidad de filas a skipear (1 si tiene headers, 0 sino). 
                    -> headers (list) Headers del file
            -> Error si tiene Headers y no son las esparadas
    '''
    
    with open('./data/headers.csv', 'r', encoding= 'utf-8-sig') as f:
        headers = f.readline().split(',')
        
    with open(file, 'r', encoding= 'utf-8-sig') as f:
        first_line = f.readline().split(',')
    
    if (first_line[0][:2] == '30') or (first_line[0][:3] == '"30'):
        return (0,headers)
    elif (first_line[0] == 'cuit') or (first_line[0] == '"cuit"'):
        return (1,headers)
    else:
        raise ValueError('The file does not have the right header´s names')
    
def get_chunks(n, chunk= 10e3):
    
    '''
       Funcion para crear rangos de chunk para luego subir los datos a MySql
        
        input: 
            -> n: Cantidad de registros a subir
            -> chunk: Cantidad de registros por chunk
        output: 
            -> Lista con el tope superior de cada chunk         
    '''
    
    n = int(n)
    chunk = int(chunk)
     
    if n % chunk > 0:
        chunks = list(range(chunk, n, chunk)) + [n]
    else:
        chunks = list(range(chunk, n, chunk))
    
    return chunks

def resumen_nas(data):
    '''
        Saco los % de NAs por columna que importa para luego dejarlos en el log
        
        input:
            data: (pandas.DataFrame) Base de datos a procesar
        output:
            nulos: (pandas.Series) % de NAs por columna
    '''

    # Validacion NAs
    cols = ['cuit','razon_social','fh_contrato_social','tipo_societario','fh_actualizacion','dom_fiscal_provincia','dom_fiscal_localidad','dom_fiscal_calle','dom_fiscal_numero','dom_fiscal_piso','dom_fiscal_departamento','dom_fiscal_cp','dom_fiscal_estado_domicilio','dom_legal_provincia','dom_legal_localidad','dom_legal_calle','dom_legal_numero','dom_legal_piso','dom_legal_departamento','dom_legal_cp','dom_legal_estado_domicilio']
    
    nulos = data[cols].isnull().mean()
    
    return nulos