import pandas as pd
import numpy as np
import utils_file_functions as uf
from artifacts.connections import make_mysql_connection

def proceso_columnas(data, file_period):

    '''
        Proceso el archivo:
            1) Saco espacios de mas en los nombres como razon social, domicilios, etc.
            2) Creo variables fechas y corrijo algunos casos en donde la empresa se creo despues del archivo actual
            3) Paso a numerico los codigos postales
        
        input:
            data: (pandas.DataFrame) Base a procesar
            file_period: (int) Periodo que de la base que se esta procesando
        output: 
            data: (pandas.DataFrame) Base procesada
    '''

    data = data.copy()
    file_next_date = pd.date_range(pd.to_datetime(str(file_period*100+1)), periods=1, freq= 'M')[0]
    
    # Procesamiento

    string_cols = ['razon_social', 'tipo_societario', 'dom_fiscal_provincia',
                   'dom_fiscal_localidad', 'dom_fiscal_calle', 'dom_fiscal_piso',
                   'dom_fiscal_departamento', 'dom_fiscal_estado_domicilio', 'dom_legal_provincia',
                   'dom_legal_localidad', 'dom_legal_calle', 'dom_legal_piso', 'dom_legal_departamento',
                   'dom_legal_estado_domicilio']

    # Lower todos los tipos strings
    data.loc[:,string_cols] = data[string_cols].apply(lambda x: uf.proceso_string(x))

    # Corrijo la fecha de constitucion para ser YYYY-MM-DD
    data['fh_contrato_social'] = data['fecha_contrato_social'].str[:10] 
    data['fh_contrato_social'] = pd.to_datetime(data['fh_contrato_social'], errors= 'coerce')
    data.loc[data['fh_contrato_social'] >= file_next_date, 'fh_contrato_social'] = np.NaN

    # Lo mismo hago con la fecha de la ultima actualizacion
    data['fh_actualizacion'] = data['fecha_actualizacion'].str[:10] 
    data['fh_actualizacion'] = pd.to_datetime(data['fh_actualizacion'], errors= 'coerce')
    data.loc[data['fh_actualizacion'] >= file_next_date, 'fh_actualizacion'] = np.NaN

    # Pongo en Integer el Codigo Postal y el domicilio 
    data['dom_fiscal_cp'] = pd.to_numeric(data['dom_fiscal_cp'], errors= 'coerce')
    data['dom_legal_cp'] = pd.to_numeric(data['dom_legal_cp'], errors= 'coerce')
    data['dom_fiscal_numero'] = pd.to_numeric(data['dom_fiscal_numero'], errors= 'coerce')
    data['dom_legal_numero'] = pd.to_numeric(data['dom_legal_numero'], errors= 'coerce')

    # Filtro un poco los pisos y deptos de los domicilios
    data.loc[data['dom_legal_piso'].str.len() > 20, 'dom_legal_piso'] = np.NaN
    data.loc[data['dom_fiscal_piso'].str.len() > 20, 'dom_fiscal_piso'] = np.NaN
    data.loc[data['dom_fiscal_departamento'].str.len() > 20, 'dom_fiscal_departamento'] = np.NaN
    data.loc[data['dom_legal_departamento'].str.len() > 20, 'dom_legal_departamento'] = np.NaN

    # Valido la estructura de los CUITs
    data['cuit_valido'] = uf.cuit_validation(data['cuit'])
    
    # Cambio de string a int
    data['cuit'] = pd.to_numeric(data['cuit'], errors= 'coerce')
    
    return data

def codifico_columnas(data):

    '''
        Codifico las columnas: dom_fiscal_provincia, dom_legal_provincia, dom_fiscal_estado_domicilio, dom_legal_estado_domicilio y tipo_societario.
        
        input:
            data: (pandas.DataFrame) Base a codificar
        output: 
            data: (pandas.DataFrame) Base con columnas codificadas
    '''
    
    mysql = make_mysql_connection('empresas')
    
    with mysql.cursor() as cur:
        cur.execute('select * from look_provincia')
        
        look_provincia = {prov: cod for cod, prov in cur.fetchall()}
        
        cur.execute('select * from look_tipo_societario')
        
        look_tipo_societario = {soc: cod for cod, soc in cur.fetchall()}    

        cur.execute('select cd_estado_domicilio, nb_estado_domicilio from look_estado_domicilio')
        
        look_estado_domicilio = {est: cod for cod, est in cur.fetchall()}
    mysql.close()
    
    data['cd_provincia_dom_fiscal'] = data['dom_fiscal_provincia'].apply(lambda x: look_provincia.get(x, -2))
    data['cd_provincia_dom_legal'] = data['dom_legal_provincia'].apply(lambda x: look_provincia.get(x, -2))
    data['cd_estado_dom_fiscal'] = data['dom_fiscal_estado_domicilio'].apply(lambda x: look_estado_domicilio.get(x, -2))
    data['cd_estado_dom_legal'] = data['dom_legal_estado_domicilio'].apply(lambda x: look_estado_domicilio.get(x, -2))
    data['cd_tipo_societario'] = data['tipo_societario'].apply(lambda x: look_tipo_societario.get(x, -2))
    
    valores_no_registrados = {}
    cols = {'dom_fiscal_provincia': 'cd_provincia_dom_fiscal', 
            'dom_legal_provincia': 'cd_provincia_dom_legal',
            'dom_fiscal_estado_domicilio': 'cd_estado_dom_fiscal',
            'dom_legal_estado_domicilio': 'cd_estado_dom_legal',
            'tipo_societario': 'cd_tipo_societario'}
    for col in cols.keys():
        
        valores_no_registrados[col] = list(data.query(f'{cols[col]} == -2 and {col}.notnull()', engine='python')[col].unique())
        
        # Reemplazo los valores de -2 por -1 para ser validos en la base de datos
        data.loc[data[cols[col]] == -2, cols[col]] = -1
    
    return data, valores_no_registrados


def base_para_comprar(data):
    
    '''
        Seteo la base final para comparar con la actual en MySql.
        
        input:
            data: (pandas.DataFrame) Base a modificar
        output:
            data: (pandas.DataFrame) Base a para comparar
    '''
    
    data = data.copy()
    
    data.rename(columns= {'cuit': 'nu_cuit',
                          'razon_social': 'nb_razon_social',
                          'dom_fiscal_localidad': 'nb_localidad_dom_fiscal',
                          'dom_fiscal_cp': 'cd_postal_dom_fiscal',
                          'dom_fiscal_calle': 'nb_calle_dom_fiscal',
                          'dom_fiscal_numero': 'nu_calle_dom_fiscal',
                          'dom_fiscal_piso': 'tx_piso_dom_fiscal',
                          'dom_fiscal_departamento': 'tx_depto_dom_fiscal',
                          'dom_legal_localidad': 'nb_localidad_dom_legal',
                          'dom_legal_cp': 'cd_postal_dom_legal',
                          'dom_legal_calle': 'nb_calle_dom_legal',
                          'dom_legal_numero': 'nu_calle_dom_legal',
                          'dom_legal_piso': 'tx_piso_dom_legal',
                          'dom_legal_departamento': 'tx_depto_dom_legal'},
                inplace= True)
        
    variables_finales = ['nu_cuit', 'nb_razon_social', 'cd_tipo_societario', 'fh_contrato_social', 'fh_actualizacion',
                         'cd_provincia_dom_fiscal', 'nb_localidad_dom_fiscal', 'cd_postal_dom_fiscal',
                         'nb_calle_dom_fiscal', 'nu_calle_dom_fiscal', 'tx_piso_dom_fiscal', 'tx_depto_dom_fiscal','cd_estado_dom_fiscal',
                         'cd_provincia_dom_legal', 'nb_localidad_dom_legal', 'cd_postal_dom_legal',
                         'nb_calle_dom_legal', 'nu_calle_dom_legal', 'tx_piso_dom_legal', 'tx_depto_dom_legal','cd_estado_dom_legal']
    
    data['fh_contrato_social'] = np.where(data['fh_contrato_social'].isnull(), None, data['fh_contrato_social'].astype('str'))
    data['fh_actualizacion'] = np.where(data['fh_actualizacion'].isnull(), None, data['fh_actualizacion'].astype('str'))
    data = data.where(pd.notnull, None)

    return data[variables_finales]
    
    
def descargo_base_mysql():

    '''
        Descargo la base actualizada que tengo en MySql
        
        input:
            
        output:
            registro_sociedades_actual: (pandas.DataFrame) Base actual en MySql
    '''
    
    mysql = make_mysql_connection('empresas')

    variables = ['nu_cuit','nb_razon_social','cd_tipo_societario','fh_contrato_social',
                 'fh_actualizacion','cd_provincia_dom_fiscal','nb_localidad_dom_fiscal',
                 'cd_postal_dom_fiscal','nb_calle_dom_fiscal','nu_calle_dom_fiscal',
                 'tx_piso_dom_fiscal','tx_depto_dom_fiscal','cd_estado_dom_fiscal',
                 'cd_provincia_dom_legal','nb_localidad_dom_legal','cd_postal_dom_legal',
                 'nb_calle_dom_legal','nu_calle_dom_legal','tx_piso_dom_legal',
                 'tx_depto_dom_legal','cd_estado_dom_legal','fh_inicio_registro','fh_fin_registro']
    
    with mysql.cursor() as cur:

        sql = 'select %s from registro_sociedades where fh_fin_registro = "2100-12-31"' % ','.join(variables)

        cur.execute(sql)

        registro_sociedades_actual = cur.fetchall()
        
    registro_sociedades_actual = pd.DataFrame(registro_sociedades_actual, columns= variables)
    
    registro_sociedades_actual['fh_contrato_social'] = np.where(registro_sociedades_actual['fh_contrato_social'].isnull(), None, registro_sociedades_actual['fh_contrato_social'].astype('str'))
    registro_sociedades_actual['fh_actualizacion'] = np.where(registro_sociedades_actual['fh_actualizacion'].isnull(), None, registro_sociedades_actual['fh_actualizacion'].astype('str'))
    registro_sociedades_actual = registro_sociedades_actual.where(pd.notnull, None)
    
    mysql.close()
    
    return registro_sociedades_actual


def base_final_para_actualizar(base_para_comparar, base_actual, cd_log, file_period):

    '''
        Comparo la base en MySql que descargue con la que tiene la novedad y saco 2 bases: 
            1) Los CUITs que no tenia y por lo tanto voy a insertar
            2) Los CUITs que cambiaron y por lo tanto voy a updatear
        
        input:
            base_para_comparar: (pandas.DataFrame) Base con la novedad
            base_actual: (pandas.DataFrame) Base actual en MySql
            cd_log: (int) Codigo log del proceso que esta corriendo
            file_period: (int) Periodo AAAAMM de .csv que esta corriendo
            
        output:
            base_insert: (pandas.DataFrame) Base que voy a insertar a la base actual en MySql
            base_update: (pandas.DataFrame) Base que voy a updatear a la base actual en MySql
    '''
    
    base_para_comparar = base_para_comparar.copy()
    base_actual = base_actual.copy()
    
    base_para_comparar.set_index('nu_cuit', inplace=True)
    base_actual.set_index('nu_cuit', inplace=True)
    
    # Me fijo CUITs que no tenia en la base y voy a insertar
    base_insert = base_para_comparar.drop(base_actual.index, errors='ignore')

    base_insert['fh_inicio_registro'] ='1900-01-01'
    base_insert['fh_fin_registro'] ='2100-12-31'
    base_insert['cd_log_proceso'] = cd_log
    base_insert['cd_periodo_proceso'] = file_period
    
    # Distringo CUITs que ya tenia en la base y voy a updatear
    cuits_actual = pd.merge(base_para_comparar, base_actual, how='inner', on='nu_cuit', suffixes=['_novedad','_vigente'])

    cuits_novedad = cuits_actual.filter(regex='novedad')
    cuits_vigente = cuits_actual.filter(regex='vigente')

    registros_iguales = (cuits_novedad.to_numpy() == cuits_vigente.to_numpy()).all(axis=1)
    base_update = cuits_actual.loc[~registros_iguales,:].filter(regex='novedad').rename(lambda x: x.replace('_novedad',''), axis=1)
    
    file_next_date = pd.to_datetime(str(file_period*100+1)).strftime('%Y-%m-%d')
    
    base_update['fh_inicio_registro'] = file_next_date
    base_update['fh_fin_registro'] ='2100-12-31'
    base_update['cd_log_proceso'] = cd_log
    base_update['cd_periodo_proceso'] = file_period

    return base_insert.reset_index(),base_update.reset_index()


def inserto_cuits(data, database):
    '''
        Inserto nuevos registros a la tabla "database.registro_sociedades"
        
        input:
            data: (pandas.DataFrame) Base con CUITs a insertar
        output:        
           -1: (int) No hay registros para insertar
            0: (int) No corrio el proceso
            1: (int) Corrio bien el proceso
    '''
    
    if data.shape[0] == 0:
        return -1
    try:
        mysql = make_mysql_connection(database)

        with mysql.cursor() as cur:

            cols = data.columns.values.tolist()
            n_cols = len(cols)
            sql = 'insert into `registro_sociedades` (`{}`) values ({}%s)'.format('`,`'.join(cols), '%s,'*(n_cols-1))

            n = data.shape[0]
            i = 0
            chunks = uf.get_chunks(n)
            while chunks:
                chunk = chunks.pop(0)
                list_input = data.iloc[i:chunk,:].to_numpy().tolist()

                cur.executemany(sql, list_input)
                mysql.commit()    

                i = chunk
                
        mysql.close()
        return 1
    except:
        if mysql.open:
            mysql.close()
        return 0
    

def update_cuits(base_update):
    '''
        Inserto nuevos registros a la tabla "step.registro_sociedades", cambio la fh_fin_registro de los CUITs
        que estoy updateando como un dia antes a la fh_inicio_registro de los CUITs que subi anteriomente.
        Finalmente, inserto los CUITs de "step.registro_sociedades" en la tabla final.
        
        input:
            base_update: (pandas.DataFrame) Base con CUITs para updatear
        output:
           -1: (int) No hay registros para actualizar
            0: (int) No corrio el proceso
            1: (int) Corrio bien el proceso
    '''
    
    try:
        
        mysql = make_mysql_connection('empresas')

        # Trunco tabla de step
        with mysql.cursor() as cur:  
            sql = '''
                    truncate table step.registro_sociedades
                  '''
            cur.execute(sql)
            mysql.commit()
            
        # Iserto registro en step
        resultado = inserto_cuits(base_update, 'step')
        if resultado == -1:
            return -1
        elif resultado == 0:
            raise ValueError(f'No se pudieron isertar los registros en la base step')

        with mysql.cursor() as cur:

            sql = '''
                    update
                        empresas.registro_sociedades as a,
                        step.registro_sociedades as b
                    set
                        a.fh_fin_registro = date_sub(b.fh_inicio_registro, interval 1 day)
                    where
                        a.nu_cuit = b.nu_cuit and
                        a.fh_fin_registro = "2100-12-31"
            '''
            cur.execute(sql)
            mysql.commit()
            
            sql = '''
                    insert into empresas.registro_sociedades
                        select * from step.registro_sociedades
            '''
            cur.execute(sql)
            mysql.commit()
            
        mysql.close()
        return 1
    
    except:            
        if 'mysql' in locals():
            if mysql.open:
                mysql.close()
        return 0