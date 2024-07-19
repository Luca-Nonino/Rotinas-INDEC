import pandas as pd
import json

# Função para aplicar mudanças no formato de cod
def adjust_cod_format(cod, append_wo=False):
    # Substitui 'EXP' por 'EX', adiciona '_AR' e prefixo 'INDEC:'
    if append_wo:
        return f"INDEC:{cod}_WO_AR"
    else:
        return f"INDEC:{cod}_AR"

def main():
    # Passo 1: Ler o arquivo CSV
    df = pd.read_csv('data/processed/final_exp_table.csv', sep=';')

    # Passo 2: Concatenar colunas para `cod`
    df['cod'] = df['COD_COMM'] + "_" + df['COD_COUNTRY']

    # Passo 3: Ler a lista de valores `cod` válidos de AUX_TABLES\series_list.csv
    valid_cod_df = pd.read_csv('data/auxiliar/series_list.csv', header=None)
    valid_cod_list = valid_cod_df.squeeze().tolist()

    # Passo 4: Filtrar o DataFrame para incluir apenas as linhas onde o valor de `cod` está na lista de valores válidos
    df = df[df['cod'].isin(valid_cod_list)]

    # Passo 5: Formatar a coluna `data`
    df['data'] = df.apply(lambda row: f"{row['ANO']}-{str(row['MES']).zfill(2)}-01", axis=1)

    # Passo 6: Converter <KGL> e <FOB> para float e garantir seu formato
    df['PNET'] = pd.to_numeric(df['PNET'].str.replace(',', '.'), errors='coerce')
    df['FOB'] = pd.to_numeric(df['FOB'].str.replace(',', '.'), errors='coerce')

    # Passo 7: Agregar os dados e salvar os dados IPV iniciais
    df_agg = df.groupby(['cod', 'data']).agg({'PNET': 'sum', 'FOB': 'sum'}).rename(columns={'PNET': 'KGL'}).reset_index()
    df_agg['cod'] = df_agg['cod'].apply(adjust_cod_format)
    df_agg.columns = ['<'+col+'>' if col != '<cod>' else col for col in df_agg.columns]
    df_agg.to_csv('data/ipvs/historical_indec_exp.ipv', index=False, sep=',', float_format='%.3f')

    # Passo 8: Reler os dados para processamento dos códigos mundiais
    df_world = pd.read_csv('data/ipvs/historical_indec_exp.ipv', sep=',')
    df_world['<KGL>'] = pd.to_numeric(df_world['<KGL>'], errors='coerce')
    df_world['<FOB>'] = pd.to_numeric(df_world['<FOB>'], errors='coerce')

    # Passo 9: Extrair o código de commodity (COD_COMM) da coluna 'cod' e criar códigos mundiais
    df_world['COD_COMM'] = df_world['<cod>'].apply(lambda x: x.split(':')[1].split('_')[0])

    # Passo 10: Agregar os dados para cada commodity e data, criar os tickers "_WO"
    agg_df = df_world.groupby(['COD_COMM', '<data>']).agg({'<KGL>': 'sum', '<FOB>': 'sum'}).reset_index()
    agg_df['<cod>'] = agg_df['COD_COMM'].apply(lambda cod: adjust_cod_format(cod, append_wo=True))
    agg_df = agg_df[['<cod>', '<data>', '<KGL>', '<FOB>']]

    # Passo 11: Anexar os dados agregados ao DataFrame original e ordenar
    final_df = pd.concat([df_world, agg_df]).sort_values(by=['<cod>', '<data>'])
    final_df = final_df.drop(columns=['COD_COMM'])

    # Passo 12: Salvar o DataFrame final atualizado com data incorporada
    with open('data/logs/update_log.json', 'r') as file:
        update_data = json.load(file)
        last_updated = update_data['LAST_UPDATED']
        month = int(last_updated['MONTH']) + 1
        year = int(last_updated['YEAR'])
        if month > 12:
            month = 1
            year += 1
        year_month = f"_{year}_{str(month).zfill(2)}"
        final_df.to_csv(f"data/ipvs/historical_indec_exp{year_month}.ipv", index=False, sep=',', float_format='%.3f')
import os

if __name__ == "__main__":
    main()
    os.remove('data/ipvs/historical_indec_exp.ipv')