import os
import pandas as pd
import json

# Load province data from 
df_kode_provinsi = pd.read_csv('kode_provinsi.csv')

df_daftar_lolos = pd.read_csv('daftar_lolos.csv')
df_daftar_lolos = df_daftar_lolos.rename(columns={
    'Nama Lengkap (tanpa gelar dan tanda baca)': 'name',
    'E-mail Aktif': 'email',
    'Nomor WhatsApp': 'whatsapp',
    'Asal Instansi': 'institution',
    'Kabupaten/Kota Asal Instansi ': 'regency',
    'Provinsi Asal Instansi': 'province',
    'Skor Total': 'score',
    'Pembagian Group': 'group',
    'Provinsi (Transformed)': 'transformed_province'
})

df_merged = pd.merge(df_daftar_lolos, df_kode_provinsi, on='transformed_province', how='left')
df_merged['transformed_regency'] = df_merged['regency'].str.upper() \
    .str.replace(r'^KAB ', 'KAB. ', regex=True) \
    .str.replace(r'^KABUPATEN ', 'KAB. ', regex=True) \
    .str.strip()


folder_path = 'area/kabupaten_kota'
dfs = []
for file_name in os.listdir(folder_path):
    if file_name.endswith('.json'):
        province_code = file_name.split('-')[1].split('.')[0]
        with open(os.path.join(folder_path, file_name), 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(list(data.items()), columns=['regency_code', 'regency'])
        df['province_code'] = province_code
        dfs.append(df)
df_regency = pd.concat(dfs, ignore_index=True)

def transform_regency(row):
    transformed_regency = row['transformed_regency'].lower()
    matches = df_regency[df_regency['regency'].str.lower().str.contains(transformed_regency, regex=False)]
    if len(matches) == 1:
        # Case 1: Exactly one match
        return matches['regency'].iloc[0]
    elif len(matches) > 1:
        # Case 2: More than one match
        return "-2"
    else:
        # Case 3: No matches
        return "-1"
df_merged['transformed_regency_2'] = df_merged.apply(lambda row: transform_regency(row), axis=1)

problematic_df = pd.read_csv('problematic_regencies_fixed.csv')
regency_mapping = problematic_df[problematic_df['true_regency'] != ''].set_index('transformed_regency')['true_regency'].to_dict()
def update_regency(row):
    if row['transformed_regency_2'] in ['-1', '-2']:
        return regency_mapping.get(row['transformed_regency'], row['transformed_regency_2'])
    return row['transformed_regency_2']
df_merged['transformed_regency_2'] = df_merged.apply(update_regency, axis=1)

transformed_regencies = df_merged[['transformed_regency_2']]
output_filename = 'all_transformed_regencies.csv'
transformed_regencies.to_csv(output_filename, index=False)

all_regency = df_regency[['regency']]
output_filename = 'all_regency.csv'
all_regency.to_csv(output_filename, index=False)
