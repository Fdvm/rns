import pandas as pd
import datetime
import os
import json
from artifacts.logs_func import log_mysql
import sys
sys.path.append(os.getcwd()+'\\src')
import download_files_functions as dw 
import utils_file_functions as uf
import process_file_functions as pf

# Parametros Globales -> Se tienen que definir en todos los procesos
fecha_corrida = datetime.datetime.now()
last_period_updated = uf.get_last_period_updated()
url = 'http://datos.jus.gob.ar/dataset/ee83de85-4305-4c53-9a9f-fd3d15e42c36'

# Parametros locales

## Cargo parametrios de logs para MySql
log_base = log_mysql(nb_proceso= 'Registro_Nacional_Sociedades', database= 'empresas')

## Cargo parametros para logs Local
log_local = {'periodo_actual_en_mysql': last_period_updated}

local_log_file = open(f'./logs/log_{fecha_corrida.strftime("%Y%m%d_%H%M%S")}.json','w')

# Corro el proceso, si no hay archivos a actualizar finaliza
try:
    ## Veo si hay un archivo a actualizar y lo actualizo
    files_downloaded = uf.check_local_files_to_update(last_period_updated)
    
    if len(files_downloaded) == 0:
    
        ## Como no tengo files descargados, busco en la pagina para descargarlos
        files_downloaded = dw.download_file(url, last_period_updated)
    
    if len(files_downloaded) == 0:
        log_local['descargas'] = {'archivos_para_actualizar': files_downloaded, 'status': 'Sin actualizaciones'}
        log_local['ultimo_periodo_actualizado'] = last_period_updated
        json.dump(log_local, local_log_file)
        local_log_file.close()
    else:
        files_downloaded.sort()
        log_local['descargas'] = {'archivos_para_actualizar': files_downloaded, 'status': 'OK'}
except:
    log_local['descargas'] = {'archivos_para_actualizar': '', 'status': 'ERROR'}
    log_local['ultimo_periodo_actualizado'] = last_period_updated
    json.dump(log_local, local_log_file)
    local_log_file.close()    
    sys.exit()

# Cargo cada uno de los archivos en la base MySql

while files_downloaded:
    
    log_local['archivo_procesado'] = {}
    log_base.update_log_procesos() # Creo el primer registro de inicio de carga de las tablas
    
    file_path = './data/zip/' + files_downloaded.pop(0)
    
    file_period = int(file_path[-10:-4])
    log_local['archivo_procesado'][file_period] = {}
    
    try:
        
        ## ---------------- LEVANTO ARCHIVO ----------------------
        
        log_base.log_step('Inicio: Leo archivo %s' % file_period)
        
        skip, headers = uf.check_headers(file_path)
        rns = pd.read_csv(file_path, header= None, names= headers, skiprows= skip, dtype= 'object').drop(columns= 'numero_inscripcion')

        log_base.update_log_step(step_status='OK', registros_procesados=rns.shape[0])
        
        log_local['archivo_procesado'].update({file_period: {'inicio': {'registros_iniciales': rns.shape[0], 'load_status': 'OK'}}})

        
        ## ---------------- PROCESO COLUMNAS ---------------------
        
        log_base.log_step('Proceso Columnas')
        
        rns_p = pf.proceso_columnas(rns, file_period)
        
        log_base.update_log_step(step_status='OK', registros_procesados=rns_p.shape[0])
        
        log_local['archivo_procesado'][file_period].update({'resumen': {'nulos': uf.resumen_nas(rns_p).to_dict(),
                                                            'p_cuits_invalidos': (~rns_p['cuit_valido']).mean(), 
                                                            'load_status': 'OK'}})
        
        ## ----------- FILTRO REGISTROS INVALIDOS ----------------
        
        log_base.log_step('Filtro registros invalidos')
        
        invalid_rows = rns_p[['cuit','razon_social','fh_contrato_social']].isnull().any(axis= 1)
        invalid_rows = invalid_rows | ~rns_p['cuit_valido']
        
        rns_p = rns_p.loc[~invalid_rows,:]

        log_base.update_log_step(step_status='OK', registros_procesados=rns_p.shape[0])
        
        log_local['archivo_procesado'][file_period].update({'registros_validos': rns_p.shape[0], 'load_status': 'OK'})
        
        ## ----------- REEMPLAZO TEXTO POR CODIGOS ---------------

        log_base.log_step('Reemplazo texto por codigos en cols con provincias, tipos societarios y estados del domicilios')
        
        rns_p, valores_no_registrados = pf.codifico_columnas(rns_p)
        
        log_base.update_log_step(step_status='OK', registros_procesados=rns_p.shape[0])
        log_local['archivo_procesado'][file_period].update({'valores_no_registrados': valores_no_registrados, 'load_status': 'OK'})
        
        ## --- CREO LAS BASES A INSERTAR Y UPDATEAR BASE FINAL ---
        
        log_base.log_step('Creo bases a insertar y updatear')
        
        rns_para_comprar = pf.base_para_comprar(rns_p)
        rns_actual = pf.descargo_base_mysql()
        rns_insert, rns_update = pf.base_final_para_actualizar(rns_para_comprar, rns_actual, log_base.cd_log, file_period)
        
        log_base.update_log_step(step_status='OK', registros_procesados=rns_insert.shape[0]+rns_update.shape[0])
        log_local['archivo_procesado'][file_period].update({'registros_a_actualizar': rns_insert.shape[0]+rns_update.shape[0], 'load_status': 'OK'})
        
        ## ----------------- INSERTO NUEVOS CUITS ----------------

        log_base.log_step('Inserto CUITs nuevos')
        
        resultado = pf.inserto_cuits(rns_insert, 'empresas')
        if resultado == 0:
            raise ValueError(f'pf.insert_cuits valor {resultado}')
        
        log_base.update_log_step(step_status='OK', registros_procesados=rns_insert.shape[0])
        log_local['archivo_procesado'][file_period].update({'registros_insertados': rns_insert.shape[0], 'load_status': 'OK'})

        ## ------------------ UPDATEO CUITS VIEJOS ---------------
     
        log_base.log_step('Updateo CUITs viejos')
        
        resultado = pf.update_cuits(rns_update)
        if resultado == 0:
            raise ValueError(f'pf.update_cuits valor {resultado}')
        
        log_base.update_log_step(step_status='OK', registros_procesados=rns_update.shape[0])
        log_local['archivo_procesado'][file_period].update({'registros_updateados': rns_update.shape[0], 'load_status': 'OK'})
      
        ## ------------------ FIN ACTUALIZACION ---------------
        
        log_base.log_step('Fin: Actualizacion desde archivo %s' % file_period, final_step=True)        
        log_base.update_log_step(step_status='OK')
        log_local['archivo_procesado'][file_period].update({'fin_proceso_archivo': 'OK'})
        
        ## Borro el archivo cargado correctamente
        os.system(f'del data\\zip\\*{str(file_period)}.csv')
        
    except Exception as e:
        log_base.update_log_step(step_status='ERROR')
        log_local['archivo_procesado'][file_period].update({'load_status': 'ERROR', 'step_name': log_base.step_name, 'step_num': log_base.step_num, 'error': str(e)})
        log_local['ultimo_periodo_actualizado'] = file_period-1 if str(file_period)[-2:] != '01' else (int(str(file_period)[:4])-1)*100+12
        json.dump(log_local, local_log_file)
        local_log_file.close()
        
        break

## ----------------------- FIN PROCESO -------------------

if log_base.log_procesos_loaded:
    if log_local['archivo_procesado'][file_period].get('fin_proceso_archivo', 'ERROR') == 'OK':
        log_local['ultimo_periodo_actualizado'] = file_period
        json.dump(log_local, local_log_file)
        local_log_file.close()
        
        ## Acomodo los archivos en su carpeta correspondiente
        os.system(f'move data\\zip\\*.zip data\\{str(file_period)[:4]}')
