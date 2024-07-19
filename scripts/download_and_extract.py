import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile
import os
from datetime import datetime

# Função para baixar e extrair arquivos zip dos dados de exportação por ano
def download_and_extract(year, output_dir):
    url = f"https://comex.indec.gob.ar/files/zips/exports_{year}_M.zip"
    response = requests.get(url)
    response.raise_for_status()

    with ZipFile(BytesIO(response.content)) as zip_file:
        for file in zip_file.namelist():
            file_year_suffix = str(year)[-2:]
            if (year < 2018 and file.endswith(f'expom{file_year_suffix}.csv')) or \
               (year >= 2018 and (file.endswith(f'exponm{file_year_suffix}.csv') or file.endswith(f'expopm{file_year_suffix}.csv'))):
                zip_file.extract(file, output_dir)
                new_file_path = os.path.join(output_dir, f"{year}_{file}")
                os.rename(os.path.join(output_dir, file), new_file_path)
                print(f"Arquivo '{file}' extraído e salvo em: {new_file_path}")
                return new_file_path
    return None

# Função para processar os arquivos CSV baixados, ajustando e filtrando dados
def process_file(file_path, ncm_cod_comm_mapping):
    if file_path:
        df = pd.read_csv(file_path, delimiter=';', encoding='latin1', header=None, skiprows=1, skipinitialspace=True)
        df = df.apply(lambda x: x.strip() if isinstance(x, str) else x)
        df.columns = ['ANO', 'MES', 'NCM', 'ARG_PAIS_CO', 'PNET', 'FOB']

        df['NCM'] = df['NCM'].astype(str).apply(lambda x: x.zfill(8))
        df = df[df['NCM'].isin(ncm_cod_comm_mapping.keys())]
        df['COD_COMM'] = df['NCM'].apply(lambda x: ncm_cod_comm_mapping.get(x, 'Unknown'))
        df.drop('NCM', axis=1, inplace=True)
        df = df[~df['PNET'].astype(str).str.contains('s') & ~df['FOB'].astype(str).str.contains('s')]
        df['FOB'] = df['FOB'].astype(str).apply(lambda x: x.strip())

        return df

# Função para substituir códigos de país no DataFrame final
def replace_country_codes(df, aux_file):
    aux_country = pd.read_csv(aux_file, sep=';', encoding='latin1')
    aux_country = aux_country[['ARG_PAIS_CO', 'COD_COUNTRY']]
    df = df.merge(aux_country, on='ARG_PAIS_CO', how='left')
    df.drop('ARG_PAIS_CO', axis=1, inplace=True)
    df.rename(columns={'COD_COUNTRY': 'ARG_PAIS_CO'}, inplace=True)
    return df

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, "..", "data","raw")
    consolidated_file_dir = os.path.join(current_dir, "..", "data", "processed")
    consolidated_file = os.path.join(consolidated_file_dir, "consolidated_exp_table.csv")
    final_file = os.path.join(consolidated_file_dir, "final_exp_table.csv")
    aux_file_dir = os.path.join(current_dir, "..", "data", "auxiliar")
    aux_file = os.path.join(aux_file_dir, "aux_17.csv")

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(consolidated_file_dir, exist_ok=True)
    os.makedirs(aux_file_dir, exist_ok=True)

    for f in os.listdir(output_dir):
        os.remove(os.path.join(output_dir, f))

    ncm_cod_comm_mapping = {
        '01012100': 'COS',
        '23040010': 'SBM',
        '12019000': 'SBS',
        '15071000': 'SBO',
        '11010010': 'WHM',
        '10019900': 'WHS',
    }

    all_data = []
    for year in range(datetime.now().year -3, datetime.now().year + 1):  # Changed 2024 to fetch current year dynamically
        file_path = download_and_extract(year, output_dir)
        df = process_file(file_path, ncm_cod_comm_mapping)
        if df is not None and not df.empty:
            all_data.append(df)

    master_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    master_df.to_csv(consolidated_file, index=False, sep=';')

    if not master_df.empty:
        final_df = replace_country_codes(master_df, aux_file)
        final_df.rename(columns={'ARG_PAIS_CO': 'COD_COUNTRY'}, inplace=True)
        final_df.to_csv(final_file, index=False, sep=';')
        print(f"Processamento completo. Tabela consolidada salva em {final_file}.")
    else:
        print("Nenhum dado processado.")

if __name__ == "__main__":
    pass