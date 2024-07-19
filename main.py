import json
import pandas as pd
from datetime import datetime
import subprocess
import os
from io import BytesIO
from zipfile import ZipFile
import requests
import time

# Atualização do caminho do log e caminho da tabela de anos
UPDATE_LOG_PATH = 'data/logs/update_log.json'
YEARS_TABLE_PATH = 'data/raw'

def check_data_update():
    with open(UPDATE_LOG_PATH, 'r') as log_file:
        update_log = json.load(log_file)

    last_updated_month = int(update_log['LAST_UPDATED']['MONTH'].lstrip('0'))
    last_updated_year = int(update_log['LAST_UPDATED']['YEAR'])

    file_path = f'{YEARS_TABLE_PATH}/{last_updated_year}_exponm{str(last_updated_year)[-2:]}.csv'
    try:
        df = pd.read_csv(file_path, sep=';', encoding='latin1')

        # Assegurar que 'Mes' é tratado como inteiro, lidando com casos onde pode ser lido como string
        df['Mes'] = df['Mes'].apply(lambda x: int(x.lstrip('0')) if isinstance(x, str) else x)

        if not df[(df['Año'] == last_updated_year) & (df['Mes'] == last_updated_month + 1)].empty:
            print("Novos dados disponíveis para o próximo mês. Procedendo com a atualização.")
            return True
        else:
            print("Dados já atualizados para o mês vigente.")
            return False
    except FileNotFoundError:
        print(f"Arquivo {file_path} não encontrado.")
        return False

def update_log_file():
    with open(UPDATE_LOG_PATH, 'r+') as log_file:
        update_log = json.load(log_file)
        last_updated_month = int(update_log['LAST_UPDATED']['MONTH'].lstrip('0'))
        last_updated_month += 1
        update_log['LAST_UPDATED']['MONTH'] = f"{last_updated_month:02}"
        update_log['LAST_UPDATED']['YEAR'] = str(datetime.now().year)
        log_file.seek(0)
        json.dump(update_log, log_file, indent=2)
        log_file.truncate()

def run_pipeline_scripts():
    # Ajustar esses caminhos de script de acordo com a estrutura do seu diretório
    scripts = [
        'scripts/download_and_extract.py',
        'scripts/generate_ipvs.py',
    ]

    success = True
    for script in scripts:
        try:
            subprocess.run(['python', script], check=True)
            print(f"Script {script} executado com sucesso.")
        except subprocess.CalledProcessError as e:
            print(f"Erro ao executar script {script}: {e}")
            success = False
            break
    return success

def download_and_extract(year, output_dir):
    print(f"Tentando baixar dados para o ano: {year}")
    url = f"https://comex.indec.gob.ar/files/zips/exports_{year}_M.zip"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with ZipFile(BytesIO(response.content)) as zip_file:
                print(f"Conteúdo do arquivo ZIP: {zip_file.namelist()}")
                for file in zip_file.namelist():
                    file_year_suffix = str(year)[-2:]
                    if (year < 2018 and file.endswith(f'expom{file_year_suffix}.csv')) or \
                       (year >= 2018 and (file.endswith(f'exponm{file_year_suffix}.csv') or file.endswith(f'expopm{file_year_suffix}.csv'))):
                        print(f"Arquivo correspondente encontrado para os critérios dados: {file}, extraindo...")
                        zip_file.extract(file, output_dir)
                        new_file_path = os.path.join(output_dir, f"{year}_{file}")
                        os.rename(os.path.join(output_dir, file), new_file_path)
                        print(f"Arquivo extraído e salvo em: {new_file_path}")
                        return new_file_path
        else:
            print(f"Falha ao baixar o arquivo. Código de status: {response.status_code}")
    except requests.RequestException as e:
        print(f"Falha na requisição: {e}")
    except ZipFile.BadZipFile as e:
        print(f"Arquivo ZIP corrompido: {e}")

    print("Nenhum arquivo adequado encontrado no arquivo ZIP.")
    return None


import shutil

def clear_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Falha ao deletar {file_path}. Motivo: {e}')

start_time = time.time()
if __name__ == '__main__':
    current_year = datetime.now().year
    output_dir = 'data/raw'

    # Limpar o diretório YEARS_TABLES antes de tentar baixar e extrair dados.
    clear_directory(YEARS_TABLE_PATH)

    # Tentativa de baixar e extrair dados para o ano corrente.
    extracted_file_path = download_and_extract(current_year, output_dir)

    # Verificar se esses dados recém extraídos contêm informações novas.
    if extracted_file_path and check_data_update():
        # Executar os scripts do pipeline apenas após limpar o diretório e verificar os novos dados.
        if run_pipeline_scripts():
            update_log_file()
        else:
            print("Execução do pipeline interrompida.")
    else:
        print("Nenhuns dados novos disponíveis ou falha na extração. Encerrando o script com sucesso.")
end_time = time.time()
print(f"Script executado em {end_time - start_time:.2f} segundos.")