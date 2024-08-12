# %%
#IMPORTACION DE LIBRERIAS
import pandas as pd
import datetime
import os
import numpy as np
# hoy = datetime.datetime.today() dejar esta linea cuadno se haga el calculo real
hoy = datetime.datetime.today()
#LECTURA DE DFS


# %%
#DDP
dtypes = {'Año de llegada':'int'}
ddp = "C:/Users/lravlic/Inchcape/Planificación y Compras Chile - Documentos/Planificación y Compras OEM/Planificación/Plan de Compras/2024/05 Mayo 24/1-DDP Mayo 2024 v2.xlsx"
df_ddp_1 = pd.read_excel(ddp, sheet_name='SOQ')

# %%
#COD_ACTUAL
maestro = "C:/Users/lravlic/Inchcape/Planificación y Compras Chile - Documentos/Planificación y Compras Maestros"
dir_maestro = os.listdir(maestro)
for c_año in dir_maestro:
    if str(hoy.year) in c_año:
        c_carpeta = os.path.join(maestro, c_año)
        c_mes = os.listdir(c_carpeta)
        c_arch = os.path.join(c_carpeta, c_mes[-1])
        print(c_arch)
        archivos = os.listdir(c_arch)
        for a in archivos:
            if 'COD_ACTUAL_R3' in a:
                ruta_cad = os.path.join(c_arch, a)
                cadena_de_remplazo = pd.read_excel(ruta_cad, usecols= ['Nro_pieza_fabricante_1',	'Cod_Actual_1'] )
                print(ruta_cad)


# %%
#MARA
columnas_mara = ['Material_R3','Part_number','Material_dsc','Modelo','Familia', 'Subfamilia', 'Categoría', 'Subcatgería','Sector_dsc']
df_mara = pd.read_excel("C:/Users/lravlic/Inchcape/Planificación y Compras Chile - Documentos/Planificación y Compras Maestros/2024/2024-05/MARA_R3_20240501.xlsx", sheet_name='Sheet1' ,usecols = columnas_mara)


# %%


# %%

#OBSOLECENCIA
columnas = ['ZFI_INNV1_T','ZFI_INNV2_T','ZFI_INNV3_T','ZFI_INNV4_T','ZFI_INNV5_T','ZFI_INNV6_T','ZFI_INNV7_T','sociedad_orig','Último Eslabón','Centro','obso_inchcape']
ruta_obs = "C:/Users/lravlic/Inchcape/Planificación y Compras Chile - Documentos/Planificación y Compras Maestros/2024/2024-05/new_obso_repuestos_cl_inchcape_202404.xlsx"
df_obs_1 = pd.read_excel(ruta_obs, sheet_name='Base Obs Cierre Abr-24', usecols=columnas)

# %%

#FC
df_fc = pd.read_excel("C:/Users/lravlic/Inchcape/Planificación y Compras Chile - Documentos/Planificación y Compras OEM/Demanda/Forecast Inbound/2024/2024-04 Abril/04.2024 S&OP Demanda Sin Restricciones OEM_Inbound.xlsx", sheet_name='Inbound', header=4)



#LT
ruta_lt = "LT Actuales Mar-24.xlsx"
df_lt = pd.read_excel(ruta_lt, header=1)
#STOCK
dtypes = {'Almacén':'str'}
df_stock = pd.read_excel("C:/Users/lravlic/Inchcape/Planificación y Compras Chile - Documentos/Planificación y Compras KPI-Reportes/Tubo Semanal/2024-05-07/2024-06-05 - Stock R3.xlsx",usecols= ['Ult. Eslabon','Libre utilización','Centro','Almacén','Trans./Trasl.','En control calidad'] ,sheet_name = 'Sheet1' , dtype = dtypes)


#TRANSITO
df_tr = pd.read_excel("C:/Users/lravlic/Inchcape/Planificación y Compras Chile - Documentos/Planificación y Compras KPI-Reportes/Tubo Semanal/2024-05-07/2024-06-05 TR FINAL R3 - Consolidado.xlsx",sheet_name = 'Sheet1')



# %%


# %%
#Respaldo DDP para no cargar de nuevo el df
df_ddp = df_ddp_1
df_obs = df_obs_1

# %%
df_obs.shape

# %%
df_obs_1 = df_obs_1.rename(columns={'Último Eslabón': 'Ultimo Eslabon'}, inplace = True)

# %% [markdown]
# LECTURA CAD REMPLAZO

# %%


# %%
df_ddp = df_ddp.merge(cadena_de_remplazo, left_on='Material', right_on='Nro_pieza_fabricante_1', how='left')
df_ddp['Cod_Actual_1'] = df_ddp['Cod_Actual_1'].fillna(df_ddp['Material'])
df_ddp = df_ddp.drop('Nro_pieza_fabricante_1', axis=1)

# %%


df_ddp.rename(columns={'Precio ':'Precio'}, inplace= True)

# %%
#traer el valor desde el material r3 y los casos que no crucen hacer lo mismo con cod_actual
#ddp_precio_moneda = df_ddp.groupby(['Cod_Actual_1']).agg({'Precio': 'max', 'Moneda':'first'})

ddp_precio_moneda = df_ddp[['Material','Precio','Moneda']]

# %%
#aplicar lo mismo
#para precio, moneda, origen, proveedor regular, costo, leadtime
#ddp_origen = df_ddp.groupby(['Cod_Actual_1'])['Origen'].first()
ddp_origen = df_ddp[['Material','Origen']]

# %%
ddp_filtro_origen = df_ddp.groupby('Cod_Actual_1').agg({'Marca':'first', 'Origen':'first'})

# %%
segmentacion = ['AA','AB','AC','BA','BB','BC','CA','CB','CC']
ddp_segmentacion = df_ddp[df_ddp['Segmentacion'].isin(segmentacion)]['Cod_Actual_1'].reset_index()

#campo parque puede sustituir el campo apertura parque en el "o"
ddp_estrategico = df_ddp[~df_ddp['Segmentacion'].isin(segmentacion) & ((df_ddp['Estratégico'] == 1) & ((df_ddp['Apertura Parque'] == 'Vigente') | (df_ddp['Apertura Parque'] == 'Nuevo')))]['Cod_Actual_1'].reset_index()
#aplicar logica anterior

df_codigo = pd.concat([ddp_estrategico,ddp_segmentacion],axis=0).reset_index(drop=True)
df_codigo = df_codigo.drop('index', axis=1).reset_index(drop=True)
df_codigo = df_codigo.reset_index(drop=True)
df_codigo.drop_duplicates(inplace = True)

# %%


# %%
df_mara.drop_duplicates(subset='Material_R3', inplace=True)

# %%
df_base = pd.merge(df_codigo, df_mara, left_on = 'Cod_Actual_1', right_on='Material_R3', how='left')
df_base['Part_number'] = df_base['Part_number'].str.replace(r'\[\#\]', '', regex=True)

# %%
df_base.shape

# %%
#hacerlo a traves de la logica anterior
#df_ddp_marca_origen = df_ddp[df_ddp['En dispo']==1].groupby('Cod_Actual_1').agg({'Marca': 'first', 'Origen': 'first'}).reset_index()
df_ddp_marca_origen = df_ddp[['Material','Cod_Actual_1','Marca','Origen']]

# %%
df_ddp_marca_origen.drop_duplicates(subset=['Material'],keep='first', inplace=True)

# %%
df_base = df_base.merge(df_ddp_marca_origen[['Material','Marca','Origen']], left_on='Material_R3', right_on='Material', how= 'left')
df_ddp_marca_origen.drop_duplicates(subset=['Cod_Actual_1'], keep='first', inplace=True)
df_base = df_base.merge(df_ddp_marca_origen[['Cod_Actual_1','Marca','Origen']], left_on='Cod_Actual_1', right_on='Cod_Actual_1', how='left')

df_base['Marca_x'] = df_base['Marca_x'].fillna(df_base['Marca_y'])
df_base['Origen_x'] = df_base['Origen_x'].fillna(df_base['Origen_y'])
df_base.drop(['Marca_y','Origen_y'], inplace = True, axis=1)
df_base = df_base.rename(columns = {'Marca_x':'Marca','Origen_x':'Origen'})

# %%
# Suponiendo que df_base es tu DataFrame

# Filtrar las filas que cumplen con la condición de la columna 'Marca'
filt = df_base['Marca'].isin(['Jac', 'Great Wall', 'Changan'])

# Reemplazar los valores de la columna 'Origen' correspondientes a las filas filtradas
df_base.loc[filt, 'Origen'] = 'China'

# %%
df_base['Origen'][df_base['Marca'].isin(['Jac', 'Great Wall', 'Changan'])].value_counts()

# %%
print(df_fc.columns,  '\n',  df_fc.shape)

# %%
df_fc = df_fc.merge(cadena_de_remplazo, left_on='Último Eslabón', right_on='Nro_pieza_fabricante_1', how ='left')
df_fc['Cod_Actual_1'] = df_fc['Cod_Actual_1'].fillna(df_fc['Último Eslabón'])

# %%
df_fc_prom = df_fc

# %%
#df_base['Faltante AP'] = 0

# %%
columnas_prom = [col for col in df_fc_prom.columns if 'FC' in col and 'Prom' not in col][:10]
df_fc_prom['Promedio FC'] = df_fc_prom[columnas_prom[:3]].mean(axis=1)

# %%
columnas_prom

# %%
columnas_seleccionadas = ['Cod_Actual_1'] + [col for col in df_fc_prom.columns if 'FC' in col and 'Prom' not in col][:10]

nuevo_df_fc_prom = df_fc_prom[columnas_seleccionadas].copy()

# %%
nuevo_df_fc_prom = nuevo_df_fc_prom.groupby('Cod_Actual_1').sum()/4.33

# %%
nuevo_df_fc_prom = nuevo_df_fc_prom.reset_index()

# %%


# %%
df_fc_venta = df_fc
columnas_venta = [col for col in df_fc_venta.columns if 'Vta R' in col]
df_fc_venta['Promedio Venta'] = df_fc_venta[columnas_venta].mean(axis=1)

# %%
df_fc_venta = df_fc_venta.groupby(['Cod_Actual_1'])['Promedio Venta'].sum().reset_index()

# %%
df_fc_venta['Promedio Venta'].sum()

# %%
df_fc = df_fc[['Cod_Actual_1', 'Segmentación Inchcape']].sort_values(by='Segmentación Inchcape')
df_fc = df_fc.groupby('Cod_Actual_1').first().reset_index()  

# %%
df_plan_mantencion = df_ddp[['Cod_Actual_1', 'Plan mantención']].sort_values(by=['Cod_Actual_1','Plan mantención'])

# %%
df_plan_mantencion = df_plan_mantencion.groupby('Cod_Actual_1').max('Plan mantención').reset_index()

# %%
df_estrategicos  = df_ddp[['Cod_Actual_1', 'Estratégico']].sort_values(by=['Cod_Actual_1','Estratégico'])

# %%
df_estrategicos = df_estrategicos.groupby('Cod_Actual_1').max('Estratégico').reset_index()

# %%
#df_base = df_base.drop('Material_R3', axis=1)

# %%
df_base = df_base.merge(df_fc, left_on='Cod_Actual_1', right_on = 'Cod_Actual_1', how='left')

# %%
df_base['Segmentación Inchcape'] = df_base['Segmentación Inchcape'].fillna('OO')

# %%
df_base['Segm. Planf']  = df_base['Segmentación Inchcape'].apply(lambda x: 1 if x in ['AA', 'AB', 'AC','BA','BB','BC','CA','CB','CC'] else 0)

# %%
df_base = df_base.merge(df_fc_venta, left_on='Cod_Actual_1', right_on='Cod_Actual_1', how='left')

# %%
df_base = df_base.merge(df_plan_mantencion, left_on='Cod_Actual_1', right_on='Cod_Actual_1', how='left')

# %%
df_base = df_base.merge(df_estrategicos, left_on='Cod_Actual_1', right_on='Cod_Actual_1', how='left')

# %%
nuevo_df_fc_prom['Cod_Actual_1'].count()

# %%
df_base = df_base.merge(nuevo_df_fc_prom, left_on='Cod_Actual_1',right_on='Cod_Actual_1', how='left')

# %%
columnas_fc = [col for col in df_base.columns if 'FC' in col][:3]

# Crear la nueva columna 'fc promedio' que contiene el promedio de las primeras tres columnas
df_base['fc promedio'] = df_base[columnas_fc].mean(axis=1)*4.33

# %%
#df_base['Cobertura de stock'] = df.apply(lambda row: 0 if row['AI5'] == 0 else row['AI5']/row['R5'] if pd.notnull(row['AI5']) and pd.notnull(row['R5']) else 12, axis=1)

# %%
#material y luego codigo actual

df_ddp_costo = df_ddp[['Cod_Actual_1','Costo UN CLP']].groupby('Cod_Actual_1').max('Costo UN CLP').reset_index()

# %%
#material y luego codigo actual

df_ddp_descont = df_ddp[['Cod_Actual_1','Mateiales descontinuados']].groupby('Cod_Actual_1').max('Mateiales descontinuados').reset_index()

# %%
df_ddp_costo['Cod_Actual_1'].nunique() == df_ddp_costo['Cod_Actual_1'].count()

# %%


# %%
df_base = df_base.merge(df_ddp_costo, left_on='Cod_Actual_1',right_on='Cod_Actual_1', how='left')

# %%
df_base = df_base.merge(df_ddp_descont, left_on='Cod_Actual_1',right_on='Cod_Actual_1', how='left')

# %%
df_base['Marca/Origen'] = df_base['Marca'] + df_base['Origen']

# %%
df_ddp['Parque'].fillna(0, inplace=True)

# %%
idx_max_parque = df_ddp.groupby('Cod_Actual_1')['Parque'].idxmax()

# Seleccionar las filas correspondientes a los índices encontrados
df_ddp_parque = df_ddp.loc[idx_max_parque]

# Restablecer los índices si es necesario
df_ddp_parque.reset_index(drop=True, inplace=True)

df_ddp_parque = df_ddp_parque[['Cod_Actual_1','Parque','Apertura Parque']]

# %%
df_ddp_parque['Cod_Actual_1'].count()

# %%
df_base = df_base.merge(df_ddp_parque, left_on='Cod_Actual_1',right_on='Cod_Actual_1', how='left')

# %%
df_base

# %%
df_ddp_precio = df_ddp[['Material','Cod_Actual_1','Precio','Moneda']]

# %%
df_ddp_precio.shape

# %% [markdown]
# STOP!

# %%
df_ddp_precio.drop_duplicates(subset=['Material'], inplace=True)

# %%
df_ddp_precio.shape

# %%
df_base = df_base.merge(df_ddp_precio[['Material','Precio', 'Moneda']], left_on='Material', right_on='Material', how= 'left')


# %%
df_ddp_precio.drop_duplicates(subset=['Cod_Actual_1'], keep='first', inplace=True)
df_base = df_base.merge(df_ddp_precio[['Cod_Actual_1','Precio', 'Moneda']], left_on='Cod_Actual_1', right_on='Cod_Actual_1', how='left')


# %%

df_base['Precio_x'] = df_base['Precio_x'].fillna(df_base['Precio_y'])
df_base['Moneda_x'] = df_base['Moneda_x'].fillna(df_base['Moneda_y'])
df_base.drop(['Precio_y','Moneda_y'], inplace = True, axis=1)
df_base = df_base.rename(columns = {'Precio_x':'Precio','Moneda_x':'Moneda'})

# %%
columnas_fc = df_base.filter(like='FC')

# Sumar las columnas
suma_fc = columnas_fc.sum()

# Mostrar el resultado
print(suma_fc)

# %%
df_lt = df_lt[['Marca.1', 'Origen.1',
       'Marca&Origen.1', 'Proveedor', 'LT', 'Sem LT']]

# %%
columnas = {'Marca.1':'Marca', 'Origen.1':'Origen',
       'Marca&Origen.1':'Marca&Origen'}

df_lt.rename(columns=columnas, inplace=True)

# %%
df_lt['Marca&Origen'].nunique() == df_lt['Marca&Origen'].count()

# %%
din_obs = df_obs[(df_obs['ZFI_INNV1_T'] == 'CHILE') & 
                 (df_obs['ZFI_INNV2_T'] == 'BACK OFFICE PAISES') & 
                 (df_obs['ZFI_INNV3_T'] == 'SOPORTE PAIS') & 
                 (df_obs['ZFI_INNV4_T'] == 'SOPORTE PAIS') & 
                 (df_obs['ZFI_INNV5_T'] == 'SOPORTE PAIS') & 
                 (df_obs['ZFI_INNV6_T'] == 'OPERACIONES Y LOGIST') & 
                 (df_obs['ZFI_INNV7_T'] == 'PLANIFICACIN Y ABAST') & 
                 (df_obs['sociedad_orig'] == 'CL02') &
                 (df_obs['Centro'] == '0201')]


# %%
din_obs_final = din_obs.groupby('Ultimo Eslabon').sum(['obso_inchcape']).reset_index()

# %% [markdown]
# NO SE ENCUENTRAN LOS ARCHIVOS NI DE LT NI OBSOLECENCIA

# %%
df_base = df_base.merge(din_obs_final,left_on='Cod_Actual_1', right_on='Ultimo Eslabon', how='left')

# %%
df_base.fillna(0, inplace=True)

# %%
df_base['Obsolescencia'] = np.where(df_base['obso_inchcape'].notna() & (df_base['obso_inchcape'] > 0), 1, 0)

# %% [markdown]
# diferencias con obs

# %%
df_lt

# %%
df_base = df_base.merge(df_lt[['Marca&Origen', 'LT']], left_on='Marca/Origen', right_on='Marca&Origen', how='left')

# %% [markdown]
# suzukuindia = 120 vs 0 
# 
# changanchine = 130 vs 0
# 
# renault corea = N vs 120
# 
# suzuki&hungria = 87 vs 0 
# 
# gwchina = 126 vs 0
# 
# mazdajapon = 106 vs 0
# 

# %%
hoy_datetime = datetime.datetime.combine(hoy, datetime.datetime.min.time())

# Adding the 'LT' values to hoy


# %%
df_base['LT Semana'] = (hoy_datetime + pd.to_timedelta(df_base['LT'], unit='D')).dt.isocalendar().week

# %%
df_base['Mes'] = (hoy_datetime + pd.to_timedelta(df_base['LT'], unit='D')).dt.month

# %% [markdown]
# OBTENCION TRANSITO Y TUBO

# %%
df_stock['Almacén'].value_counts()

# %%
#Cambiar nombres para que tome los correctos
df_stock['Centro'] = df_stock['Centro'].astype(str)
df_stock['Almacén'] = df_stock['Almacén'].astype(str)
df_stock['Total'] = df_stock['Libre utilización'] + df_stock['Trans./Trasl.'] + df_stock['En control calidad']
columns_to_drop = ['Libre utilización', 'Trans./Trasl.', 'En control calidad']


df_stock = df_stock.drop(columns=columns_to_drop)

df_stock_cd = df_stock[df_stock['Almacén'] == '1100'].groupby(['Ult. Eslabon']).agg({'Total': 'sum'}).reset_index()
#df_stock_entrante = df_stock[(df_stock['Centro'].isin(['711', '0711'])) & (df_stock['Almacén'] == '1100')].groupby(['Ult. Eslabon']).agg({'Total': 'sum'}).reset_index()

# %%
df_stock['Almacén']

# %%
df_base = df_base.merge(df_stock_cd, left_on='Cod_Actual_1', right_on='Ult. Eslabon', how='left')
#df_base = df_base.merge(df_stock_entrante, left_on='Cod_Actual_1', right_on='Ult. Eslabon', how='left')


# %%
df_base['Stock_711'] = 0

# %%
df_base = df_base.fillna(0)

# %%
df_base['Cobertura Stock'] = np.where((df_base['Total'] == 0) | (df_base['fc promedio'] == 0),
                                      0,
                                      df_base['Total'] / df_base['fc promedio'])

# Reemplazar inf con un valor específico (por ejemplo, 9999)
df_base.replace([np.inf, -np.inf], 9999, inplace=True)

# %%
df_base['Total'].sum()

# %%
cl_doc = ['ZIPL','ZSTO','ZSPT']
# Assuming your DataFrame is named df_tr
# Assuming 'año' and 'semanas' are already present in the DataFrame

# Apply filters to the DataFrame if needed


# Create a pivot table with 'year' and 'week' as index columns



filtered_df = df_tr[df_tr['Cl.documento compras'].isin(cl_doc)]
filtered_df = filtered_df[['Material','Cantidad','Fecha']]
filtered_df.reset_index(drop=True)

df_base['Cobertura Stock'].sum()

# %%
df_base_2 =df_base

# %%
from datetime import timedelta

# %%
current_date = datetime.date(2024,5,8)
print(current_date.isocalendar())
# Crear las columnas en base a las próximas 39 semanas en la base de datos 'b'
for i in range(39):
    week_start = current_date + timedelta(weeks=i)
    year = week_start.year

    nombre_meses = {1: 'jan',2: 'feb',3: 'mar',4: 'apr',5: 'may',6: 'jun',7: 'jul',8: 'aug',9: 'sep',10: 'oct',11: 'nov',12: 'dec'}
    week_number = str(week_start.isocalendar()[1]).zfill(2)
    def nombrar_mes(mes):
        nombre_mes = nombre_meses.get(mes)
        return nombre_mes
    
        
    month_name = nombrar_mes(week_start.month)
    column_name = f"{year}-{month_name}-{week_number}"
    
 
    df_base[column_name] = 0  # Inicializar todas las columnas con 0


# %%


# %%
filtered_df[filtered_df['Material']=='1109130P33M0']

# %%
filtered_df['Año'] = filtered_df['Fecha'].dt.year
filtered_df['Month'] = filtered_df['Fecha'].dt.strftime('%B').str.lower().str[:3]
filtered_df['Semana'] = filtered_df['Fecha'].dt.isocalendar().week



# %%
import pandas as pd

# Supongamos que df_base es tu DataFrame base
# y filtered_df es el DataFrame con las ventas filtradas

# Primero, agrupamos las ventas por material, año, mes y semana
grouped_sales = filtered_df.groupby(['Material', 'Año', 'Month', 'Semana'])['Cantidad'].sum().reset_index()

# Luego, cruzamos los datos de ventas en df_base
for index, row in grouped_sales.iterrows():
    product_code = row['Material']
    month = row['Month']
    week_number = row['Semana']
    year = row['Año']
    column_name = f"{year}-{month}-{week_number}"
    if column_name in df_base.columns:
        df_base.loc[df_base['Material'] == product_code, column_name] = row['Cantidad']

# Ahora df_base debe tener las ventas cruzadas en las columnas correspondientes


# %%
columnas = ['Ult. Eslabon','Ultimo Eslabon']
df_base = df_base.drop(columns=columnas)
df_base = df_base.rename({'Total_x':'Stock CD', 'Total_y':'Stock Entrante'})


# %%
df_base['Faltante AP'] = 0

# %%
df_base = df_base.fillna(0)

# %%
meses_ingles_español = {
    "jan": "ene",
    "feb": "feb",
    "mar": "mar",
    "apr": "abr",
    "may": "may",
    "jun": "jun",
    "jul": "jul",
    "aug": "ago",
    "sep": "sep",
    "oct": "oct",
    "nov": "nov",
    "dec": "dic"
}
def obtener_mes_español(mes):
    mes_español = meses_ingles_español.get(mes)
    if mes_español:
        return mes_español.lower()
    else:
        return None

# %%
year_columns = [col for col in df_base.columns if col.split('-')[0].isdigit() and 'POS-STOCK' not in col]

df_base['Qty Filial'] = 0
year_columns
nueva_columna = f'POS-STOCK-{year_columns[0]}'
df_base[nueva_columna] = df_base.apply(lambda row: 0 if row['Total'] - row['Faltante AP'] - row['Qty Filial']<= 0 else row['Total'] - row['Faltante AP'] - row['Qty Filial'], axis=1)


# %%
nueva_columna_2 = f'POS-STOCK-{year_columns[1]}'
first_fc_column = df_base.filter(like='FC').columns[0]

mes = year_columns[1][5:8]
año = year_columns[1][2:4]

mes_español = obtener_mes_español(mes)
if mes_español is None:
    print(f"Could not find Spanish equivalent for month: {mes}")


columna_fc = f'FC {mes_español}-{año}'


df_base[nueva_columna_2] = np.where((df_base[nueva_columna] + df_base[year_columns[0]] - df_base[columna_fc]) < 0, 0, df_base[nueva_columna] + df_base[year_columns[0]] - df_base[columna_fc])

# %%
nueva_columna_3 = f'POS-STOCK-{year_columns[2]}'

mes = year_columns[2][5:8]
año = year_columns[2][2:4]

mes_español = obtener_mes_español(mes)
if mes_español is None:
    print(f"Could not find Spanish equivalent for month: {mes}")
    



columna_fc = f'FC {mes_español}-{año}'


df_base[nueva_columna_3] = np.where((df_base[nueva_columna_2] + df_base[year_columns[1]] + df_base['Stock_711'] - df_base[columna_fc]) < 0, 0, df_base[nueva_columna_2] + df_base[year_columns[1]] + df_base['Stock_711'] - df_base[columna_fc])


# %%
for col in year_columns[3:]:
    column_name = f'POS-STOCK-{col}'
    
    last_column_name = df_base.columns[-1]
    year_month = last_column_name[-11:]
    
    mes = col[5:8]
    año = col[2:4]

   

    mes_español = obtener_mes_español(mes)
    if mes_español is None:
        print(f"Could not find Spanish equivalent for month: {mes}")
        continue

    columna_fc = f'FC {mes_español}-{año}'
    columna_tr = year_month

    



    calculo_columna = np.where((df_base[last_column_name] + df_base[columna_tr] - df_base[columna_fc]) < 0, 0, df_base[last_column_name] + df_base[columna_tr] - df_base[columna_fc])
    
    df_base[column_name] = calculo_columna
    print(column_name)



# %%
#cobertura
df_base_aux = df_base



pos_columns = [col for col in df_base_aux.columns if 'POS-STOCK' in col]

pos_columns[0][15:18]
pos_columns[0][12:14]
mes = pos_columns[0][15:18]
año = pos_columns[0][12:14]

mes_español = obtener_mes_español(mes)
if mes_español is None:
    print(f"Could not find Spanish equivalent for month: {mes}")
    



columna_fc = f'FC {mes_español}-{año}'
df_base_aux[f'COBERTURA-{pos_columns[0][10:]}']= (df_base_aux[f'POS-STOCK-{pos_columns[0][10:]}']/((df_base_aux[columna_fc]/2)))
df_base_aux[f'COBERTURA-{pos_columns[0][10:]}'].replace([np.inf, -np.inf, np.nan], 0, inplace=True)
mes = pos_columns[1][15:18]
año = pos_columns[1][12:14]

mes_español = obtener_mes_español(mes)
if mes_español is None:
    print(f"Could not find Spanish equivalent for month: {mes}")
    



columna_fc = f'FC {mes_español}-{año}'
df_base_aux[f'COBERTURA-{pos_columns[1][10:]}']= (df_base_aux[f'POS-STOCK-{pos_columns[1][10:]}']/df_base_aux[columna_fc])
df_base_aux[f'COBERTURA-{pos_columns[1][10:]}'].replace([np.inf, -np.inf, np.nan], 0, inplace=True)

for col in pos_columns[2:]:
    column_name = f'COBERTURA-{col[10:]}'
    
   
    mes = col[15:18]
    año = col[12:14]

    mes_español = obtener_mes_español(mes)
    if mes_español is None:
        print(f"Could not find Spanish equivalent for month: {mes}")
        



    columna_fc = f'FC {mes_español}-{año}'
    
    df_base_aux[column_name]= (df_base_aux[f'POS-STOCK-{column_name[10:]}']/df_base_aux[columna_fc])
    df_base_aux[column_name].replace([np.inf, -np.inf, np.nan], 0, inplace=True)
df_base_aux['transito'] = df_base[year_columns].sum(axis=1)
    

df_base_aux['pos_stock'] = df_base_aux['Total'] + df_base_aux['Stock_711'] + df_base_aux['transito']
cob_columns = [col for col in df_base_aux.columns if 'COBERTURA' in col]
for c in cob_columns:
    print(c[10:])

for col in cob_columns:
    nombre_columna = f'CUMPLIMIENTO-{col[10:]}'

    def calculate_value(row):
        vta_prom = row['Promedio Venta']
        pos_stock = row['pos_stock']
        cobertura = row[col]

        if vta_prom < 1 and pos_stock > 0:
            return 1
        elif cobertura > 1:
            return 1
        elif cobertura < 0:
            return 0
        else:
            return cobertura

    # Apply the function row-wise using apply() and axis=1
    df_base_aux[nombre_columna] = df_base_aux.apply(calculate_value, axis=1)

import pandas as pd

# Set display options to show all columns and rows without truncation

# Display the DataFrame without column truncation
df_base_aux[df_base_aux['Material']=='ZZJ118110']


# %%
cump_cols = [col for col in df_base_aux.columns if 'CUMPLIMIENTO' in col]

# %%


# %%
for col in cump_cols:
    nombre_columna = f'NNSS_P - {col[13:]}'
    mes = col[18:21]
    año = col[15:17]

    mes_español = obtener_mes_español(mes)
    if mes_español is None:
        print(f"Could not find Spanish equivalent for month: {mes}")
        



    columna_fc = f'FC {mes_español}-{año}'

    df_base_aux[nombre_columna] = df_base[col] * df_base_aux[columna_fc]

    


# %%


# %%
ns_cols = [col for col in df_base_aux.columns if 'NNSS_P' in col]

# %%


# %%
df_base_aux.head()

# %%
for col in ns_cols:

    mes = col[14:17]
    año = col[11:13]
    #print(nombre_columna)

    

    mes_español = obtener_mes_español(mes)
    if mes_español is None:
        print(f"Could not find Spanish equivalent for month: {mes}")


    nombre_columna = f'forecast - {col[9:]}'
    
    
    columna_fc = f'FC {mes_español}-{año}'
    print(columna_fc)
    

    df_base_aux[nombre_columna] = df_base_aux[columna_fc]


        



    # columna_fc = f'FC {mes_español}-{año}'

    # df_base_aux[nombre_columna] = df_base[col] * df_base_aux[columna_fc]

    


# %%
df_base_aux.head()

# %%
df_base_aux.to_excel('Base_Final.xlsx')

# %%
sub_df = df_base_aux.filter(regex='^Cod_Actual_1$|^NNSS_P - ')
sub_df_2 = df_base_aux.filter(regex = '^Cod_Actual_1$|^forecast - ')

# %%
sub_df.columns

# %%


#declarar id
id_vars = ['Cod_Actual_1']



# Luego, usamos melt para transformar el DataFrame
df_transformado = pd.melt(sub_df, id_vars=id_vars, var_name='NNSS - AÑO-MES-SEM', value_name='Cumplimiento')

df_transformado_2 = pd.melt(sub_df_2, id_vars=id_vars, var_name='FC SEM', value_name='Forecast')


# Puedes resetear los índices si lo deseas
df_transformado.reset_index(drop=True, inplace=True)
#f_transformado_2.reset_index(drop=True, inplace=True)





# Ahora df_transformado contiene el DataFrame transformado como lo necesitas


# %%
df_transformado_2['FC SEM'] = df_transformado_2['FC SEM'].str[11:]

# %%
df_transformado_2['ID'] = df_transformado_2['Cod_Actual_1'] + df_transformado_2['FC SEM']

# %%
df_transformado_2

# %%
df_transformado

# %%
df_transformado['ID_AUX'] = df_transformado['NNSS - AÑO-MES-SEM'].str[9:]


# %%
df_transformado

# %%
df_transformado['ID'] = df_transformado['Cod_Actual_1'] + df_transformado['ID_AUX']

# %%
df_transformado.nunique()

# %%
df_transformado = df_transformado.merge(df_transformado_2, left_on='ID',right_on='ID', how='left')

# %%
df_transformado.columns

# %%
df_transformado

# %%
rename_cols = {'Cod_Actual_1_x':'Cod_Actual_1'}
df_transformado.drop('Cod_Actual_1_y', inplace = True, axis=1)
df_transformado.rename(columns=rename_cols, inplace = True)

# %%
reducir_cols = ['Cod_Actual_1','NNSS - AÑO-MES-SEM','Cumplimiento','Forecast']

# %%
df_transformado = df_transformado[reducir_cols]

# %%
df_transformado[df_transformado['Cod_Actual_1']=='ZZJ118110']

# %%
df_transformado.to_csv('base_pbi.csv')

# %%
df_mara.dropna(subset=['Material_R3'], inplace=True)

# Assuming 'df_mara' is your DataFrame


# %%
# Eliminar duplicados basados en la columna 'Material_R3'
df_mara.drop_duplicates(subset=['Material_R3'], inplace=True)

# %%
df_mara.dtypes

# %%
df_mara.to_csv('mara_tratada.csv')

# %%


# %%


# %%


# %%


# %%


# %%



