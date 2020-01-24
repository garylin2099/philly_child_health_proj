import numpy as np
import pandas as pd
import pyreadstat

data_cbcl = pd.read_spss("cbc parent items.sav")
data_ysr = pd.read_spss("CBC YSR child report items.sav")
data_master = pd.read_spss("MASTER file June 2015.sav")
data_omega3 = pd.read_spss("N3.sav")

###a function to identify outliers###
##any item score should be not higher than 2
def fun_look_outlier(data):
    # excluding SubjectID column when looking for outliers
    df = data.drop(data.columns[0], axis = 1)
    ind = []
    for i in range(len(df)):
        if any(df.iloc[i] == "NA") or any(df.iloc[i] > 2) or any(np.isnan(df.iloc[i])):
            ind.append(i)
    return ind

###a function to change illegitimate item scores (usually 9) into nan###
def fun_change_outlier(data):
    ncol = len(data.columns)
    for i in range(len(data)):
        # starting from column of index 1, excluding SubjectID column when looking for outliers
        if any(data.iloc[i,1:ncol] == "NA") or any(data.iloc[i,1:ncol] > 2):
            data.loc[i, [False] + ((data.iloc[i,1:ncol] == "NA") | (data.iloc[i,1:ncol] > 2)).tolist()] = np.nan

###functions to change outlier###
##if more than half of the items under sleep total score have missing values,
##exclude the entries (do nothing), otherwise, replace the missing values with the median score
##of that item at each time point
def fun_many_na(data):
    return sum(data.isna()) > (len(data)-1)/2
def fun_has_na(data):
    return any(data.isna())
def fun_rplc_median(data):
    median = data.median(axis = 0)
    data_to_rplc = data.loc[data.apply(fun_has_na, axis = 1)]
    for i in data_to_rplc.index:
        na_col_ind = data_to_rplc.loc[i].isna()
        data_to_rplc.loc[i,na_col_ind] = median[na_col_ind]
    data.loc[data.apply(fun_has_na, axis = 1)] = data_to_rplc
    return data

def fun_excl_rplc(data):
    data = data[~data.apply(fun_many_na, axis = 1)]
    return fun_rplc_median(data)

###the ultimate function to deal with missing values###
def fun_handle_na(data):
    fun_change_outlier(data)
    return fun_excl_rplc(data)


##create seperate datasets recording all variables of the subjects
col_sleep_cbcl = ["SubjectID","X47","X54","X76","X77","X92","X100","X108"]
data_cbcl["SubjectID"] = data_cbcl["SubjectID"].astype(int).astype(str)
data_cbcl_base = fun_handle_na(data_cbcl.loc[data_cbcl["SubjectID"].str.startswith("1"),col_sleep_cbcl])
data_cbcl_3month = fun_handle_na(data_cbcl.loc[data_cbcl["SubjectID"].str.startswith("2"),col_sleep_cbcl])
data_cbcl_6month = fun_handle_na(data_cbcl.loc[data_cbcl["SubjectID"].str.startswith("3"),col_sleep_cbcl])
data_cbcl_12month = fun_handle_na(data_cbcl.loc[data_cbcl["SubjectID"].str.startswith("4"),col_sleep_cbcl])
data_cbcl_new = data_cbcl_base.append(data_cbcl_3month).\
append(data_cbcl_6month).append(data_cbcl_12month)

col_sleep_ysr = ["id","X47","X54","X76","X77","X100"]
data_ysr["id"] = data_ysr["id"].astype(int).astype(str)
data_ysr_base = fun_handle_na(data_ysr.loc[data_ysr["id"].str.startswith("1"),col_sleep_ysr])
data_ysr_3month = fun_handle_na(data_ysr.loc[data_ysr["id"].str.startswith("2"),col_sleep_ysr])
data_ysr_6month = fun_handle_na(data_ysr.loc[data_ysr["id"].str.startswith("3"),col_sleep_ysr])
data_ysr_12month = fun_handle_na(data_ysr.loc[data_ysr["id"].str.startswith("4"),col_sleep_ysr])
data_ysr_new = data_ysr_base.append(data_ysr_3month).\
append(data_ysr_6month).append(data_ysr_12month)

##compute sleep total score
sleep_item_cbcl = col_sleep_cbcl[1:len(col_sleep_cbcl)]
sleep_item_ysr = col_sleep_ysr[1:len(col_sleep_ysr)]

def fun_sleep_total(data, items):
    data["total_score"] = data.apply(lambda x: sum(x[items]), axis = 1)

fun_sleep_total(data_cbcl_new, sleep_item_cbcl)
fun_sleep_total(data_ysr_new, sleep_item_ysr)

##write dataframe to csv
data_cbcl_new.to_csv("sleep_cbcl.csv", index = False)
data_ysr_new.to_csv("sleep_ysr.csv", index = False)


