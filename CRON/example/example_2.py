#!/usr/bin/python
# -*- coding: utf8 -*-

"""
    Loading new data,
        Data sudah di cleansing,
        user dengan minimal 10klik/hari baru dimasukkan kedalam perhitungan
"""

import os
import glob
import pandas as pd
import numpy as np
from time import time

from .. src.googlenews.BayesTopicRecommender import GBayesTopicRecommender

t0 = time()
print "Begin loading data..."
root_path = os.path.abspath(os.path.join(os.path.dirname('__file__')))
dataset_path = '/topic-recommender/src/datasets/reco_topic_click_distribution/'
allFiles = glob.glob(os.path.join(root_path + dataset_path, "*.csv"))

np_array_list = []
for file_ in allFiles:
    df = pd.read_csv(file_, index_col=None, header=0)
    np_array_list.append(df.as_matrix())

comb_np_array = np.vstack(np_array_list)
big_frame = pd.DataFrame(comb_np_array)
big_frame.columns = ['date', 'user_id', 'topic_id', 'topic_name',
                     'is_global', 'num', 'Nt', 'proba']

load_time = time() - t0
print ("Total loading time: %0.3fs" % load_time)

print big_frame.head(5)
print big_frame.as_matrix().shape, "\n"

# clarify some data format
df_input = big_frame[['date', 'user_id', 'topic_id', 'num']].copy(deep=True)
df_input['user_id'] = df_input['user_id'].astype(str)
df_input['num'] = df_input['num'].fillna(0.0).astype(float)
df_input['date'] = pd.to_datetime(df_input['date'], format='%Y-%m-%d', errors='coerce')

# clear un-used object
big_frame = None

# ~ set current date for p0, later should be
#   date + 4-8 hours windows range,
#   ex: '2018-03-14 01:00' - '2018-03-14 09:00'
current_date = '2018-04-05'

# validating data
test_uid = '029671d3-61d3-4c5e-b049-b1f750b0d62c'

# ~ Data Preprocessing ~
# split data train, untuk menggambarkan data berasal dari 2 table
# D(u, t)
df_dut = df_input.loc[df_input['date'] < current_date]
df_dut['date'] = pd.to_datetime(df_dut['date'], format='%Y-%m-%d', errors='coerce')

# D(t)
df_dt = df_input.loc[df_input['date'] == current_date]
df_dt['date'] = pd.to_datetime(df_dt['date'], format='%Y-%m-%d', errors='coerce')

# just for see some detail on training data
time_slide = df_dut.loc[df_dut['date'] < current_date].groupby('date').size().to_frame().reset_index()
time_slide['date'] = time_slide['date'].dt.date
print "\nTotal data per-date:"
print time_slide

# ~~~~~~ Begin train ~~~~~~
t0 = time()
print "\n"
print "~" * 75
print "train on: %d total history data -- ( D(u, t) )" % len(df_dut)
print "transform on: %d total current data -- ( D(t) )" % len(df_dt)
# instantiace class
BR = GBayesTopicRecommender(current_date, G=10)

# ~~ Fit ~~
#   handling genuine news interest < current date
NB = BR.processX(df_dut)
result = pd.merge(df_dut, NB, on=['date', 'topic_id'])
"""
     num_y = total global click for category=ci on periode t
     num_x = total click from user_U for category=ci on periode t
"""
df_input_X = result[['date', 'user_id',
                     'topic_id', 'num_x', 'num_y']]
model_fit = BR.fit(df_dut, df_input_X,
                   full_bayes=False, use_sigmant=False,
                   verbose=False)
print "\nTest view on user id: ", test_uid
print "\nFitted Model:"
print model_fit[model_fit['user_id'] == test_uid].head(10)
# ~~ and Transform ~~
#   handling current news interest == current date
NB = BR.processX(df_dt)
result = pd.merge(df_dt, NB, on=['date', 'topic_id'])

df_input_X = result[['date', 'user_id', 'topic_id',
                     'num_x', 'num_y']]
model_transform = BR.transform(df1=df_dt, df2=df_input_X,
                               df3=df_dut, fitted_model=model_fit,
                               verbose=False)
print "\nTransformed Model:"
print model_transform[model_transform['user_id'] == test_uid].head(10)
train_time = time() - t0
print model_transform.head(5)
print ("\nTotal train time: %0.3fs" % train_time)
print "~" * 75