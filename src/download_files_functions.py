import re
import urllib.request, urllib.parse
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
    
def download_file(url, last_period_updated):
    '''
        Se fija que archivos estan disponibles en la pagina y actualiza el mes siguiente a last_period_updated
        
        input:
            url: Web de donde se descargan los datos
            last_period_updated: string, AAAAMM fecha de la ultima actualizacion
        output:
            files_downloaded: list, Archivos descargados
    '''

    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')
    
    zips = [tag['href'] for tag in soup.find_all('a') if re.match('.*registro-nacional-sociedades-\d{4}.*\.zip', tag['href']) is not None]
    
    last_zip_url, next_period_to_update = get_file_path_to_update(zips, last_period_updated)
    
    files_downloaded = []
    if last_zip_url != None:
        r = requests.get(last_zip_url, stream= True)
        zip_file = 'data/zip/' + re.findall('registro.*', last_zip_url)[0]

        with open(zip_file, 'wb') as zipf:
            for chunk in r.iter_content(chunk_size= 1024 * 1024 * 10):
                zipf.write(chunk)

        # Exraigo unicamente los archivos que voy a usar
        with ZipFile(zip_file, 'r') as zipf:
            for file in zipf.namelist():
                if re.findall('\d{6}', file)[0] >= next_period_to_update:
                    zipf.extract(file, 'data/zip')
                    files_downloaded.append(file)
            
    return files_downloaded

def get_file_path_to_update(zips, last_period_updated):

    '''
        Busco si hay archivos nuevos para actualizar y si hay devuelvo el path.
        input:
            zips: Lista de los paths de los archivos en la pagina
            last_period_updated: AAAAMM del ultimo archivo actualizado
        output:
            url_zip: string, path del archivo para actualizar o None si no hay archivos para actualizar
            next_period_to_update: string, AAAAMM del proximo periodo a actualizar
    '''
        
    if last_period_updated[-2:] < '12':
        next_period_to_update = str(int(last_period_updated)+1)
    else:
        next_period_to_update = str((int(last_period_updated[:4])+1)*100+1)
        
    url_zips = [url_zip for url_zip in zips if re.match(f'.*registro-nacional-sociedades-{next_period_to_update[:4]}.*', url_zip) is not None]
        
    if len(url_zips) == 0:
        url_zip = None
    elif len(url_zips) == 1:
        url_zip = url_zips[0]
    elif len(url_zips) == 2:
        if next_period_to_update[-2:] <= '05':
            url_zip = [url for url in url_zips if re.match('.*semestre.1.*', url)][0]
        else:
            url_zip = [url for url in url_zips if re.match('.*semestre.2.*', url)][0]
    else:
        raise ValueError('Hay mas archivos de los que corresponden.')
        
    return url_zip,next_period_to_update