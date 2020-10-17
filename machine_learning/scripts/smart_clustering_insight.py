#!/usr/bin/env python
# coding: utf-8

# ***********************************************************************************************
#  SMART CLUSTERING: INSIGHT
# ***********************************************************************************************

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Imports
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
import pandas as pd
import joblib
import re
from sklearn.preprocessing import MinMaxScaler

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Constants
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
logs = ["log_all_data.csv", "log_20200124.csv", "log_20200201.csv", "log_20200202.csv", "log_20200203.csv", "log_20200204.csv"]
filename_orig = logs[0]
ruta = "./dataset/" + filename_orig
fecha = re.search('log_(.*)\.csv', filename_orig, re.IGNORECASE).group(1)
filename_new = "smart_predicted_style_{}.csv".format(fecha)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Main block
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if __name__ == "__main__":
    datos = pd.read_csv(ruta)
    print("[INFO] smart_clustering_insight | Read path {}".format(ruta))

    df_ul = datos.drop(['TagID_', 'DrillID_', 'TimeStamp_min', 'TimeStamp_max', 'TimeStampDT_min', 'TimeStampDT_max'], axis = 1)

    min_max = MinMaxScaler()
    df_scaled = pd.DataFrame(min_max.fit_transform(df_ul.astype("float64")))
    df_scaled.columns = df_ul.columns

    model_clone = joblib.load('smart_model.pkl')
    model_clone.fit_predict(df_scaled)
    datos_labels = model_clone.labels_
    datos_centroids = model_clone.cluster_centers_

    number_of_clusters = 3
    col_classif = list(df_scaled.columns).index("classifier_")
    centroids_classif = [cc[col_classif] for cc in datos_centroids]
    index_min = centroids_classif.index(min(centroids_classif))
    index_max = centroids_classif.index(max(centroids_classif))
    lista_labels = ["Others"] * number_of_clusters
    lista_labels[index_min] = 'Breaststroke'
    lista_labels[index_max] = 'Front Crawl/Freestyle'

    df_scaled['Prediction'] = datos_labels
    df_scaled['Prediction'] = df_scaled['Prediction'].replace({0: lista_labels[0],
                                                               1: lista_labels[1],
                                                               2: lista_labels[2]})
    df_scaled.groupby(['Prediction']).count().iloc[:, -1]

    insight = pd.DataFrame(df_scaled.iloc[:, -1])
    insight[['TagID', 'DrillID']] = datos[['TagID_', 'DrillID_']]
    insight = insight[['TagID', 'DrillID', 'Prediction']]
    insight.set_index(['TagID', 'DrillID'], inplace=True)
    insight.to_csv(filename_new)

    print("[INFO] smart_clustering_insight | Output filename written {}".format(filename_new))
