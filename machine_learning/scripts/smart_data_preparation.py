#!/usr/bin/env python
# coding: utf-8

# ***********************************************************************************************
# # SMART DATA PREPARATION
# ***********************************************************************************************

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Imports
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
import pandas as pd
import numpy  as np

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Auxiliary functions
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Lambda para limpiar datos:
extrae_id = lambda x: (x.strip('ObjectId').strip('(")'))

# Función para imputar outliers:
def remove_outliers_6sigma(X):
    return X[abs(X - np.mean(X)) < 3 * np.std(X)]

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Constants
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
DAYLIST = ["all_data.csv", "20200124.csv", "20200201.csv", "20200202.csv", "20200203.csv", "20200204.csv"]
filename_input = DAYLIST[0]
ruta = "./dataset/" + filename_input
filename_output = "./dataset/log_{}".format(filename_input)
variables = ['diff_posx_m', 'diff_time_sec', 'LinearSpeed', 'Acceleration']

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Main block
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if __name__ == "__main__":
    df = pd.read_csv(ruta, converters={'_id': extrae_id, 'DrillID': extrae_id})
    print("[INFO] smart_data_preparation | Read path {}".format(ruta))

    df.sort_values(['TagID', 'DrillID', 'TimeStamp'], ascending=[True, True, True], inplace=True)
    
    if df.duplicated().sum() > 0:
        df = df.drop_duplicates()
    
    df.drop(list(df.columns[df.apply(pd.Series.nunique)==1]), axis=1, inplace=True)

    if df.isnull().sum().sum() > 0:
        df = df.fillna(0)

    df['PositionX'] = pd.to_numeric(df['Position'].str.strip('[]').str.split(pat=",", expand=True)[0], downcast='float')
    df['TimeStampDT'] = pd.to_datetime(df['TimeStamp'], unit='ms')
    df.drop(['Position'], axis=1, inplace=True)

    df["diff_posx_m"] = df["PositionX"].diff().abs()
    df["diff_time_sec"] = df["TimeStamp"].diff().abs() / 1000
    df["LinearSpeed"] = df["diff_posx_m"].div(df["diff_time_sec"])
    df["Acceleration"] = df["diff_posx_m"].div(df["diff_time_sec"]**2)

    if df.isnull().sum().sum() > 0:
        df = df.fillna(0)

    df = df.drop((df[df['LinearSpeed'] > 1.56].index))
    df = df.drop((df[df['Acceleration'] > 0.03].index))

    datos1 = pd.merge(df, pd.DataFrame(remove_outliers_6sigma(df[variables[0]].values)), how='left', left_index=True,
                      right_index=True).rename(columns={0: variables[0] + '_6sigma'})
    datos2 = pd.merge(datos1, pd.DataFrame(remove_outliers_6sigma(df[variables[1]].values)), how='left', left_index=True,
                      right_index=True).rename(columns={0: variables[1] + '_6sigma'})
    datos3 = pd.merge(datos2, pd.DataFrame(remove_outliers_6sigma(df[variables[2]].values)), how='left', left_index=True,
                      right_index=True).rename(columns={0: variables[2] + '_6sigma'})
    datos4 = pd.merge(datos3, pd.DataFrame(remove_outliers_6sigma(df[variables[3]].values)), how='left', left_index=True,
                      right_index=True).rename(columns={0: variables[3] + '_6sigma'})

    datos4.dropna(inplace=True)
    datos4.drop(variables, axis=1, inplace=True)
    datos4.rename(columns={"diff_posx_m_6sigma": "diff_pos",
                           "diff_time_sec_6sigma": "diff_time",
                           "LinearSpeed_6sigma": "Speed",
                           "Acceleration_6sigma": "Accel"}, inplace=True)
    
    df_agg = datos4.groupby(['TagID', 'DrillID']).agg({'TimeStamp': ['min', 'max'],
                                                       'TimeStampDT': ['min', 'max'],
                                                       'PositionX': ['min', 'max', 'mean'],
                                                       'diff_pos': ['min', 'max', 'mean'],
                                                       'diff_time': ['min', 'max', 'mean'],
                                                       'Speed': ['min', 'max', 'mean'],
                                                       'Accel': ['min', 'max', 'mean']}).reset_index()

    df_agg['Outlier_PositionX'] = df_agg['PositionX']['max'] * df_agg['PositionX']['min'] >= 0
    datos_mask = df_agg['Outlier_PositionX']==False
    filtered_datos = df_agg[datos_mask]
    df_agg = filtered_datos.drop('Outlier_PositionX', axis=1)
    df_agg.columns = ['_'.join(col_).strip() for col_ in df_agg.columns.values]
    df_agg['classifier_'] = np.log( (1 + df_agg['Speed_mean']) / ( 1 + (df_agg['Accel_mean'] * df_agg['diff_time_mean']) ) )

    df_agg.drop(['diff_time_max', 'Speed_mean'], axis=1, inplace=True)
    df_agg.to_csv(filename_output, index=False)

    print("[INFO] smart_data_preparation | Output filename written {}".format(filename_output))
