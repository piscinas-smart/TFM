#!/usr/bin/env python
# coding: utf-8

# ***********************************************************************************************
#  SMART MODEL TRAINING
# ***********************************************************************************************

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Imports
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Auxiliary Functions
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
extrae_id = lambda x : (x.strip('ObjectId').strip('(")'))

def remove_outliers_6sigma(X):
    return X[abs(X - np.mean(X)) < 3 * np.std(X)]

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Constants
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
ruta = "./dataset/all_data.csv"
variables = ["diff_posx_m", "diff_time_sec", "LinearSpeed", "Acceleration"]
number_of_clusters = 3
output = "smart_new_model.pkl"

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Main block
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if __name__ == "__main__":
    
    df = pd.read_csv(ruta, converters = {'_id':extrae_id , 'DrillID':extrae_id})
    print("[INFO] smart_model_training | Read path {}".format(ruta))

    df.sort_values(['TagID', 'DrillID', 'TimeStamp'], ascending=[True, True, True], inplace=True)

    if df.duplicated().sum() > 0:
        df = df.drop_duplicates()

    df.drop(list(df.columns[df.apply(pd.Series.nunique)==1]), axis=1, inplace=True)

    if df.isnull().sum().sum() > 0:
        df = df.fillna(0)
    
    df['PositionX'] = pd.to_numeric(df['Position'].str.strip('[]').str.split(pat="," , expand = True)[0], downcast='float')
    df['TimeStampDT'] = pd.to_datetime(df['TimeStamp'], unit='ms')
    df.drop(['Position'], axis=1, inplace=True)

    df = df[['_id', 'TagID', 'DrillID', 'TimeStamp', 'TimeStampDT', 'PositionX']]

    df["diff_posx_m"]   = df["PositionX"].diff().abs()
    df["diff_time_sec"] = df["TimeStamp"].diff().abs() / 1000
    df["LinearSpeed"]   = df["diff_posx_m"].div(df["diff_time_sec"])
    df["Acceleration"]  = df["diff_posx_m"].div(df["diff_time_sec"]**2)

    if df.isnull().sum().sum() > 0:
        df = df.fillna(0)

    df = df.drop( (df[df['LinearSpeed']  > 1.56].index ) )
    df = df.drop( (df[df['Acceleration'] > 0.03].index ) )

    df_aux1     = pd.merge(df, pd.DataFrame(remove_outliers_6sigma(df[variables[0]].values)), how = 'left', left_index = True, 
                           right_index = True).rename(columns={0: variables[0] + '_no_outlier'})
    df_aux2     = pd.merge(df_aux1, pd.DataFrame(remove_outliers_6sigma(df[variables[1]].values)), how = 'left', left_index = True, 
                           right_index = True).rename(columns={0: variables[1] + '_no_outlier'})
    df_aux3     = pd.merge(df_aux2, pd.DataFrame(remove_outliers_6sigma(df[variables[2]].values)), how = 'left', left_index = True, 
                           right_index = True).rename(columns={0: variables[2] + '_no_outlier'})
    df_filtered = pd.merge(df_aux3, pd.DataFrame(remove_outliers_6sigma(df[variables[3]].values)), how = 'left', left_index = True, 
                           right_index = True).rename(columns={0: variables[3] + '_no_outlier'})

    df_filtered.dropna(inplace=True)
    df_filtered.drop(variables, axis=1, inplace=True)
    df_filtered.rename(columns={"diff_posx_m_no_outlier"  : "diff_pos",
                                "diff_time_sec_no_outlier": "diff_time",
                                "LinearSpeed_no_outlier"  : "Speed", 
                                "Acceleration_no_outlier" : "Accel"}, inplace=True)

    df_agg = df_filtered.groupby(['TagID', 'DrillID']).agg({'TimeStamp'  : ['min','max'],
                                                            'TimeStampDT': ['min','max'], 
                                                            'PositionX'  : ['min','max', 'mean'], 
                                                            'diff_pos'   : ['min','max', 'mean'],
                                                            'diff_time'  : ['min','max', 'mean'],
                                                            'Speed'      : ['min','max', 'mean'],
                                                            'Accel'      : ['min','max', 'mean']}).reset_index()
    df_agg = df_agg.drop(['TagID', 'DrillID', 'TimeStamp', 'TimeStampDT'], axis = 1)
    df_agg['Outlier_PositionX'] = ( df_agg['PositionX']['max'] * df_agg['PositionX']['min'] ) >= 0

    df_mask_X = df_agg['Outlier_PositionX']==False
    filtered_X = df_agg[df_mask_X]

    df_ul = filtered_X.drop('Outlier_PositionX', axis=1)
    df_ul.columns = ['_'.join(col_).strip() for col_ in df_ul.columns.values]
    df_ul['classifier_'] = np.log( (1 + df_ul['Speed_mean']) / ( 1 + (df_ul['Accel_mean'] * df_ul['diff_time_mean']) ) )
    df_ul.drop(['diff_time_max', 'Speed_mean'], axis=1, inplace=True)

    min_max = MinMaxScaler()
    df_scaled = pd.DataFrame(min_max.fit_transform(df_ul.astype("float64")))
    df_scaled.columns = df_ul.columns

    kmeans = KMeans(n_clusters = number_of_clusters)
    kmeans.fit(df_scaled)

    joblib.dump(kmeans, 'smart_new_model.pkl', compress=9)
    print("[INFO] smart_model_training | Output filename written {}".format(output))
