import warnings
warnings.filterwarnings("ignore")


import pandas as pd
import numpy as np
import os
import time


ST_DETAILS = pd.read_csv('ST_DETAILS.csv', skip_blank_lines=True, engine='pyarrow')
MAJCAT_LIST = pd.read_csv('MAJCAT_LIST.csv', skip_blank_lines=True, engine='pyarrow')
ST_L_DETAILS = pd.read_csv('ST_L_DETAILS.csv', skip_blank_lines=True, engine='pyarrow')
BGT_MAJCAT_I = pd.read_csv('BGT_MAJCAT.csv', skip_blank_lines=True, engine='pyarrow')
BGT_MERCHCAT_I = pd.read_csv('BGT_MERCHCAT.csv', skip_blank_lines=True, engine='pyarrow')
ST_STK_I = pd.read_csv('ST_STK.csv', skip_blank_lines=True, engine='pyarrow')
DC_STK_I = pd.read_csv('DC_STK.csv', skip_blank_lines=True, engine='pyarrow')
MC_AUTO_ART_I = pd.read_csv('MC_AUTO_ART.csv', skip_blank_lines=True, engine='pyarrow')
MC_BODY_CONT_I = pd.read_csv('MC_BODY_CONT.csv', skip_blank_lines=True, engine='pyarrow')
MC_CLR_CONT_I = pd.read_csv('MC_CLR_CONT.csv', skip_blank_lines=True, engine='pyarrow')
MC_FABRIC_CONT_I = pd.read_csv('MC_FABRIC_CONT.csv', skip_blank_lines=True, engine='pyarrow')
MC_M_MVGR_CONT_I = pd.read_csv('MC_M_MVGR_CONT.csv', skip_blank_lines=True, engine='pyarrow')
MC_MVGR_CONT_I = pd.read_csv('MC_MVGR_CONT.csv', skip_blank_lines=True, engine='pyarrow')
MC_SEG_CONT_I = pd.read_csv('MC_SEG_CONT.csv', skip_blank_lines=True, engine='pyarrow')
MC_VEND_CONT_I = pd.read_csv('MC_VEND_CONT.csv', skip_blank_lines=True, engine='pyarrow')
ST_OPT_I = pd.read_csv('ST_OPT.csv', skip_blank_lines=True, engine='pyarrow')
OPT_DISP_Q_I = pd.read_csv('OPT_DISP_Q.csv', skip_blank_lines=True, engine='pyarrow')
ST_OPT_DISP_Q_I = pd.read_csv('ST_OPT_DISP_Q.csv', skip_blank_lines=True, engine='pyarrow')
ST_MC_CONS_I = pd.read_csv('ST_MC_CONS.csv', skip_blank_lines=True, engine='pyarrow')
ST_SSN_CONS_I = pd.read_csv('ST_SSN_CONS.csv', skip_blank_lines=True, engine='pyarrow')


CAT = "M_JEANS"
BGT_MAJCAT = BGT_MAJCAT_I[BGT_MAJCAT_I['MAJCAT']==CAT].reset_index(drop=True)
BGT_MERCHCAT = BGT_MERCHCAT_I[BGT_MERCHCAT_I['MAJCAT']==CAT].reset_index(drop=True)
ST_STK = ST_STK_I[ST_STK_I['MAJCAT']==CAT].reset_index(drop=True)
DC_STK = DC_STK_I[DC_STK_I['MAJCAT']==CAT].reset_index(drop=True)
MC_AUTO_ART = MC_AUTO_ART_I[MC_AUTO_ART_I['MAJCAT']==CAT].reset_index(drop=True)
MC_BODY_CONT = MC_BODY_CONT_I[MC_BODY_CONT_I['MAJCAT']==CAT].reset_index(drop=True)
MC_CLR_CONT = MC_CLR_CONT_I[MC_CLR_CONT_I['MAJCAT']==CAT].reset_index(drop=True)
MC_FABRIC_CONT = MC_FABRIC_CONT_I[MC_FABRIC_CONT_I['MAJCAT']==CAT].reset_index(drop=True)
MC_M_MVGR_CONT = MC_M_MVGR_CONT_I[MC_M_MVGR_CONT_I['MAJCAT']==CAT].reset_index(drop=True)
MC_MVGR_CONT = MC_MVGR_CONT_I[MC_MVGR_CONT_I['MAJCAT']==CAT].reset_index(drop=True)
MC_SEG_CONT = MC_SEG_CONT_I[MC_SEG_CONT_I['MAJCAT']==CAT].reset_index(drop=True)
MC_VEND_CONT = MC_VEND_CONT_I[MC_VEND_CONT_I['MAJCAT']==CAT].reset_index(drop=True)
ST_OPT = ST_OPT_I[ST_OPT_I['MAJCAT']==CAT].reset_index(drop=True)
OPT_DISP_Q = OPT_DISP_Q_I[OPT_DISP_Q_I['MAJCAT']==CAT].reset_index(drop=True)
ST_OPT_DISP_Q = ST_OPT_DISP_Q_I[ST_OPT_DISP_Q_I['MAJCAT']==CAT].reset_index(drop=True)
ST_MC_CONS = ST_MC_CONS_I[ST_MC_CONS_I['MAJCAT']==CAT].reset_index(drop=True)
# ST_SSN_CONS = ST_SSN_CONS_I[ST_SSN_CONS_I['MAJCAT']==CAT].reset_index(drop=True)

# converting column to object
ST_OPT_DISP_Q['ST_CD']=ST_OPT_DISP_Q['ST_CD'].astype(object)
ST_DETAILS = ST_DETAILS.sort_values(by='PRIORITY_LIST', ascending=True)


MERCHCAT_MBQ = BGT_MERCHCAT
MAJCAT_MBQ = BGT_MAJCAT


MAJCAT_MBQ = pd.merge(MAJCAT_MBQ, ST_DETAILS, how="left",on=["ST_CD"],suffixes=('_left', '_right'))
MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, ST_DETAILS, how="left",on=["ST_CD"],suffixes=('_left', '_right'))
MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, ST_L_DETAILS, how="left",on=["ST_CD"],suffixes=('_left', '_right'))
MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, OPT_DISP_Q, how="left",on=["MERCHCAT"],suffixes=('_left', '_right'))
MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, ST_OPT_DISP_Q, how="left",on=["ST_CD","MERCHCAT"],suffixes=('_left', '_right'))


# Calculate REV_MJ_SALES_PD_1
MAJCAT_MBQ['REV_MJ_SALES_PD_1'] = (MAJCAT_MBQ['SALES_Q_1'] * MAJCAT_MBQ['CM_RMN_SALES_CONS'] + MAJCAT_MBQ['SALES_Q_2'] * MAJCAT_MBQ['NM_RMN_SALES_CONS']) * (1 / MAJCAT_MBQ['BGT_DAYS'])

# Calculate MJ_MBQ_A
MAJCAT_MBQ['MJ_MBQ_A'] = MAJCAT_MBQ['LISTING_M_1'] * (MAJCAT_MBQ['ALLOC_DISP'] + MAJCAT_MBQ['REV_MJ_SALES_PD_1'] * MAJCAT_MBQ['ALLOC_DAY_COVER'])

# Calculate MJ_MBQ_30_1
MAJCAT_MBQ['MJ_MBQ_30_1'] = MAJCAT_MBQ['LISTING_M_1'] * (MAJCAT_MBQ['DISP_Q_1'] + MAJCAT_MBQ['REV_MJ_SALES_PD_1'] * MAJCAT_MBQ['BGT_DAYS'])

# Calculate REM_SALES_1
MAJCAT_MBQ['REM_SALES_1'] = MAJCAT_MBQ['SALES_Q_1'] * MAJCAT_MBQ['CM_RMN_SALES_CONS'] * (1 / MAJCAT_MBQ['RMN_BGT_DAYS'])

# Calculate MJ_MBQ_30_2 to MJ_MBQ_30_12
for i in range(2, 13):
    MAJCAT_MBQ[f'MJ_MBQ_30_{i}'] = MAJCAT_MBQ[f'LISTING_M_{i}'] * (MAJCAT_MBQ[f'DISP_Q_{i}'] + MAJCAT_MBQ[f'SALES_Q_{i}'])



for i in range(1, 13):
    MERCHCAT_MBQ[f'F_OPT_DISP_Q_{i}'] = MERCHCAT_MBQ[f'ST_OPT_DISP_Q_{i}'].where(MERCHCAT_MBQ[f'ST_OPT_DISP_Q_{i}'] > 0, MERCHCAT_MBQ[f'OPT_DISP_Q_{i}'])


MERCHCAT_MBQ['OPT_CNT_A'] = (MERCHCAT_MBQ['ALLOC_DISP'] / MERCHCAT_MBQ['F_OPT_DISP_Q_1']).clip(lower=0).round()

for i in range(1, 13):
    MERCHCAT_MBQ[f'OPT_CNT_{i}'] = (MERCHCAT_MBQ[f'DISP_Q_{i}'] / MERCHCAT_MBQ[f'F_OPT_DISP_Q_{i}']).clip(lower=0).round()


MERCHCAT_MBQ['OPT_SALES_A'] = np.where(
    (MERCHCAT_MBQ['SALES_Q_1'] + 0 == 0) | (MERCHCAT_MBQ['OPT_CNT_A'] + 0 == 0),
    0,
    np.round(MERCHCAT_MBQ['SALES_Q_1'] / MERCHCAT_MBQ['OPT_CNT_A'], 2)
)

# Calculate 'OPT_SALES_{i}' using vectorized operations in a loop
for i in range(1, 13):
    MERCHCAT_MBQ[f'OPT_SALES_{i}'] = np.where(
        (MERCHCAT_MBQ[f'SALES_Q_{i}'] == 0) | (MERCHCAT_MBQ[f'OPT_CNT_{i}'] == 0),
        0,
        np.round(MERCHCAT_MBQ[f'SALES_Q_{i}'] / MERCHCAT_MBQ[f'OPT_CNT_{i}'], 2)
    )



# Calculation for REV_OPT_SALES_PD_A and OPT_MBQ_A
MERCHCAT_MBQ['REV_OPT_SALES_PD_A'] = (MERCHCAT_MBQ['OPT_SALES_A'] * MERCHCAT_MBQ['CM_RMN_SALES_CONS'] +
                                      MERCHCAT_MBQ['OPT_SALES_2'] * MERCHCAT_MBQ['NM_RMN_SALES_CONS']) * (1 / MERCHCAT_MBQ['BGT_DAYS'])
MERCHCAT_MBQ['OPT_MBQ_A'] = MERCHCAT_MBQ['LISTING_M_1'] * (MERCHCAT_MBQ['F_OPT_DISP_Q_1'] + MERCHCAT_MBQ['REV_OPT_SALES_PD_A'] * MERCHCAT_MBQ['ALLOC_DAY_COVER'])

# Calculation for REV_OPT_SALES_PD_1 and OPT_MBQ_30_1
MERCHCAT_MBQ['REV_OPT_SALES_PD_1'] = (MERCHCAT_MBQ['OPT_SALES_1'] * MERCHCAT_MBQ['CM_RMN_SALES_CONS'] +
                                       MERCHCAT_MBQ['OPT_SALES_2'] * MERCHCAT_MBQ['NM_RMN_SALES_CONS']) * (1 / MERCHCAT_MBQ['BGT_DAYS'])
MERCHCAT_MBQ['OPT_MBQ_30_1'] = MERCHCAT_MBQ['LISTING_M_1'] * (MERCHCAT_MBQ['F_OPT_DISP_Q_1'] + MERCHCAT_MBQ['REV_OPT_SALES_PD_1'] * MERCHCAT_MBQ['BGT_DAYS'])

# Calculation for REM_OPT_SALES_PD_1 and OPTION MBQ 30 QTY for each month (OPT_MBQ_30_2 to OPT_MBQ_30_12)
for i in range(2, 13):
    MERCHCAT_MBQ[f'OPT_MBQ_30_{i}'] = MERCHCAT_MBQ[f'LISTING_M_{i}'] * (MERCHCAT_MBQ[f'F_OPT_DISP_Q_{i}'] + MERCHCAT_MBQ[f'OPT_SALES_{i}'])



# Vectorized calculation for OPT_MBQ_L1_A and OPT_MBQ_L1_1
MERCHCAT_MBQ['OPT_MBQ_L1_A'] = MERCHCAT_MBQ['LISTING_M_1'] * (MERCHCAT_MBQ['F_OPT_DISP_Q_1'] + (MERCHCAT_MBQ['REV_OPT_SALES_PD_A'] * MERCHCAT_MBQ['L1_SALES_COVER']))
MERCHCAT_MBQ['OPT_MBQ_L1_1'] = MERCHCAT_MBQ['LISTING_M_1'] * (MERCHCAT_MBQ['F_OPT_DISP_Q_1'] + (MERCHCAT_MBQ['REV_OPT_SALES_PD_1'] * MERCHCAT_MBQ['L1_SALES_COVER']))

# Vectorized calculation for the rest of the columns
for i in range(2, 13):
    MERCHCAT_MBQ[f'OPT_MBQ_L1_{i}'] = (
        MERCHCAT_MBQ[f'LISTING_M_{i}'] *
        (MERCHCAT_MBQ[f'F_OPT_DISP_Q_{i}'] +
         (MERCHCAT_MBQ[f'OPT_SALES_{i}'] * (MERCHCAT_MBQ['L1_SALES_COVER'] / 30)))
)



# Vectorized calculation for OPT_MBQ_30_A and OPT_MBQ_30_1
MERCHCAT_MBQ['OPT_MBQ_30_A'] = MERCHCAT_MBQ['LISTING_M_1'] * (
    MERCHCAT_MBQ['F_OPT_DISP_Q_1'] + 
    (MERCHCAT_MBQ['REV_OPT_SALES_PD_A'] * MERCHCAT_MBQ['ALLOC_DAY_COVER'] * MERCHCAT_MBQ['L_OPT_SALES_GR_M_1'])
)

MERCHCAT_MBQ['OPT_MBQ_30_1'] = MERCHCAT_MBQ['LISTING_M_1'] * (
    MERCHCAT_MBQ['F_OPT_DISP_Q_1'] + 
    (MERCHCAT_MBQ['REV_OPT_SALES_PD_1'] * MERCHCAT_MBQ['BGT_DAYS'] * MERCHCAT_MBQ['L_OPT_SALES_GR_M_1'])
)

# Vectorized calculation for the rest of the columns
for i in range(2, 13):
    MERCHCAT_MBQ[f'OPT_MBQ_30_{i}'] = MERCHCAT_MBQ[f'LISTING_M_{i}'] * (
        MERCHCAT_MBQ[f'F_OPT_DISP_Q_{i}'] + 
        (MERCHCAT_MBQ[f'OPT_SALES_{i}'] * MERCHCAT_MBQ[f'L_OPT_SALES_GR_M_{i-1}'])
    )




# Vectorized calculation for SSN_MBQ_A and SSN_MBQ_1
MERCHCAT_MBQ['SSN_MBQ_A'] = MERCHCAT_MBQ['LISTING_M_1'] * (
    MERCHCAT_MBQ['F_OPT_DISP_Q_1'] + 
    (MERCHCAT_MBQ['REV_OPT_SALES_PD_A'] * MERCHCAT_MBQ['ALLOC_DAY_COVER'] * MERCHCAT_MBQ['L_OPT_SALES_GR_M_1'])
)

MERCHCAT_MBQ['SSN_MBQ_1'] = MERCHCAT_MBQ['LISTING_M_1'] * (
    MERCHCAT_MBQ['F_OPT_DISP_Q_1'] + 
    (MERCHCAT_MBQ['REV_OPT_SALES_PD_1'] * MERCHCAT_MBQ['BGT_DAYS'] * MERCHCAT_MBQ['L_OPT_SALES_GR_M_1'])
)

# Vectorized calculation for the rest of the SSN_MBQ columns
for i in range(2, 13):
    MERCHCAT_MBQ[f'SSN_MBQ_{i}'] = MERCHCAT_MBQ[f'LISTING_M_{i}'] * (
        MERCHCAT_MBQ[f'F_OPT_DISP_Q_{i}'] + 
        (MERCHCAT_MBQ[f'OPT_SALES_{i}'] * MERCHCAT_MBQ[f'L_OPT_SALES_GR_M_{i}'])
    )



# Vectorized calculation for REV_MC_SALES_PD_1
MERCHCAT_MBQ['REV_MC_SALES_PD_1'] = (
    (MERCHCAT_MBQ['SALES_Q_1'] * MERCHCAT_MBQ['CM_RMN_SALES_CONS'] + MERCHCAT_MBQ['SALES_Q_2'] * MERCHCAT_MBQ['NM_RMN_SALES_CONS']) * 
    (1 / MERCHCAT_MBQ['BGT_DAYS'])
)

# Vectorized calculation for MC_MBQ_A
MERCHCAT_MBQ['MC_MBQ_A'] = (
    MERCHCAT_MBQ['LISTING_M_1'] * 
    (MERCHCAT_MBQ['ALLOC_DISP'] + (MERCHCAT_MBQ['REV_MC_SALES_PD_1'] * MERCHCAT_MBQ['ALLOC_DAY_COVER']))
)

# Vectorized calculation for MC_MBQ_30_1
MERCHCAT_MBQ['MC_MBQ_30_1'] = (
    MERCHCAT_MBQ['LISTING_M_1'] * 
    (MERCHCAT_MBQ['DISP_Q_1'] + (MERCHCAT_MBQ['REV_MC_SALES_PD_1'] * MERCHCAT_MBQ['BGT_DAYS']))
)

# Vectorized calculation for REM_SALES_1
MERCHCAT_MBQ['REM_SALES_1'] = (
    MERCHCAT_MBQ['SALES_Q_1'] * MERCHCAT_MBQ['CM_RMN_SALES_CONS'] * 
    (1 / MERCHCAT_MBQ['RMN_BGT_DAYS'])
)





# Vectorized calculation for MC 30-MBQ Quantity
for i in range(2, 13):
    MERCHCAT_MBQ[f'MC_MBQ_30_{i}'] = (
        MERCHCAT_MBQ[f'LISTING_M_{i}'] * 
        (MERCHCAT_MBQ[f'DISP_Q_{i}'] + MERCHCAT_MBQ[f'SALES_Q_{i}'])
    )



MERCHCAT_MBQ.drop(['OPT_DISP_Q_1','OPT_DISP_Q_2','OPT_DISP_Q_3','OPT_DISP_Q_4','OPT_DISP_Q_5','OPT_DISP_Q_6','OPT_DISP_Q_7','OPT_DISP_Q_8','OPT_DISP_Q_9','OPT_DISP_Q_10','OPT_DISP_Q_11','OPT_DISP_Q_12','ST_OPT_DISP_Q_1','ST_OPT_DISP_Q_2','ST_OPT_DISP_Q_3','ST_OPT_DISP_Q_4','ST_OPT_DISP_Q_5','ST_OPT_DISP_Q_6','ST_OPT_DISP_Q_7','ST_OPT_DISP_Q_8','ST_OPT_DISP_Q_9','ST_OPT_DISP_Q_10','ST_OPT_DISP_Q_11','ST_OPT_DISP_Q_12'],inplace=True,axis=1)




MERCHCAT_MBQ['DISP_Q_1']=MERCHCAT_MBQ['DISP_Q_1'].astype(float)


MAJCAT_MBQ_A = MAJCAT_MBQ[['ST_CD','MAJCAT','MJ_MBQ_A','MJ_MBQ_30_1','MJ_MBQ_30_2','MJ_MBQ_30_3','MJ_MBQ_30_4','MJ_MBQ_30_5',
                          'MJ_MBQ_30_6','MJ_MBQ_30_7','MJ_MBQ_30_8','MJ_MBQ_30_9','MJ_MBQ_30_10','MJ_MBQ_30_11','MJ_MBQ_30_12']]


MERCHCAT_MBQ_A = MERCHCAT_MBQ[['ST_CD','MAJCAT','MERCHCAT','MC_MBQ_A','MC_MBQ_30_1','MC_MBQ_30_2','MC_MBQ_30_3','MC_MBQ_30_4','MC_MBQ_30_5',
                          'MC_MBQ_30_6','MC_MBQ_30_7','MC_MBQ_30_8','MC_MBQ_30_9','MC_MBQ_30_10','MC_MBQ_30_11','MC_MBQ_30_12']]


MERCHCAT_MBQ_TEMP = MERCHCAT_MBQ[['ST_CD','MERCHCAT','OPT_MBQ_A','OPT_MBQ_L1_A','OPT_MBQ_30_A','SSN_MBQ_A']]


#download data to test
# path = r"C:\Users\pardeep.jajoria\Desktop\LISTING AUTOMATION"
path = r".\Testing"
# MAJCAT_MBQ.to_csv(path+r"\MJ_MBQ_file.csv")
# MERCHCAT_MBQ.to_csv(path+r"\MC_MBQ_file.csv")
# MERCHCAT_MBQ_A.to_csv(path+r"\MC_MBQ_B_file.csv")
# MAJCAT_MBQ_A.to_csv(path+r"\MJ_MBQ_B_file.csv")


MJ_MBQ = MAJCAT_MBQ_A
MC_MBQ = MERCHCAT_MBQ_A


ST_STK_DATA = ST_STK
ST_STK_DATA = pd.merge(ST_STK_DATA, MERCHCAT_MBQ_TEMP, how="left",on=["ST_CD","MERCHCAT"],suffixes=('_left', '_right'))




# Vectorized calculation for REV_ST_STK
ST_STK_DATA['REV_ST_STK'] = np.where(
    ST_STK_DATA['OPT_MBQ_A'] + 0 == 0,
    ST_STK_DATA['ST_STK'],
    np.minimum(ST_STK_DATA['ST_STK'], ST_STK_DATA['OPT_MBQ_A'])
)



ST_STK_DATA_TEMP = ST_STK_DATA[['ST_CD','MERCHCAT','GEN_ART','COLOR','ST_STK','REV_ST_STK']]


#REV STK BTM-UP AT ALL HIER LEVEL
ST_STK_MJ = ST_STK_DATA.groupby(['ST_CD','MAJCAT'], as_index=False).agg(MJ_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC = ST_STK_DATA.groupby(['ST_CD','MERCHCAT'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_M_MVGR = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MACRO_MVGR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_MVGR_1 = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MVGR_1'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_FABRIC = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','FABRIC'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_BODY = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','BODY'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_COLOR = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_M_MVGR_COLOR = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MACRO_MVGR','COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_MVGR_1_COLOR = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MVGR_1','COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_FABRIC_COLOR = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','FABRIC','COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_BODY_COLOR = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','BODY','COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_M_MVGR_MVGR_1 = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MACRO_MVGR','MVGR_1'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_M_MVGR_FABRIC = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MACRO_MVGR','FABRIC'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_M_MVGR_BODY = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MACRO_MVGR','BODY'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_M_MVGR_MVGR_1_COLOR = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MACRO_MVGR','MVGR_1','COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_M_MVGR_FABRIC_COLOR = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MACRO_MVGR','FABRIC','COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))
ST_STK_MC_M_MVGR_BODY_COLOR = ST_STK_DATA.groupby(['ST_CD','MERCHCAT','MACRO_MVGR','BODY','COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK','sum'))



#download data ST_STK_DATA
temp=ST_STK_DATA
temp.to_csv(path+r"\ST_STK_temp_file.csv")


DC_STK_DATA = DC_STK
ST_DC=DC_STK_DATA[['SEG','DIV','SUB_DIV','MAJCAT','MERCHCAT','MRP','RNG_SEG','SSN','GEN_ART','GEN_ART_DESC','COLOR','VAR_ART','SZ','M_VND_CD',
              'M_VND_NM','CO_AGING','MACRO_MVGR','MVGR_1','FABRIC','BODY','DISCOUNT_STATUS']]
ST_DC.insert(0,"ST_NO",0)
ST_DC['ST_NO']=0
ST_TEST = ST_DC
ST_DATA = ST_DC


for k in range(0,200):
    ST_TEST['ST_NO']=ST_TEST['ST_NO'].replace(k,k+1)
    ST_DATA = pd.concat([ST_DATA,ST_TEST],ignore_index=True)

# Replace values in the 'ST_NO' column using vectorized operations




DATACHECK = ST_DATA[(ST_DATA['ST_NO']==1)].index
ST_DATA.drop(DATACHECK, inplace=True)


ST_DC['ST_NO']=1
ST_DATA = pd.concat([ST_DATA,ST_DC],ignore_index=True)


ST_DETAILS_R = ST_DETAILS[['ST_CD','PRIORITY_LIST']]
ST_DETAILS_R['ST_NO']=ST_DETAILS_R['PRIORITY_LIST']
ST_DETAILS_R = ST_DETAILS[['ST_CD','ST_NO']]


ST_DATA = pd.merge(ST_DATA, ST_DETAILS_R, how="left",on=["ST_NO"])


ST_DATA['ST_CD'] = ST_DATA['ST_CD'].fillna("xxx")
DATACHECK = ST_DATA[(ST_DATA['ST_CD']=='xxx')].index
ST_DATA.drop(DATACHECK, inplace=True)


ST_DATA = pd.merge(ST_DATA, MERCHCAT_MBQ_TEMP, how="left",on=["ST_CD","MERCHCAT"])
ST_DATA = pd.merge(ST_DATA, ST_STK_DATA_TEMP, how="left",on=["ST_CD","MERCHCAT","GEN_ART","COLOR"])
ST_DATA['ST_STK'] = ST_DATA['ST_STK'].fillna(0)
ST_DATA['REV_ST_STK'] = ST_DATA['REV_ST_STK'].fillna(0)


temp=ST_DATA.head(10000)
temp.to_csv(path+r"\ST_DATA_temp_file.csv")


#comb
DC_STK_DATA['REV_DC_STK']=DC_STK_DATA['CURR_DC_STK']
DC_STK_DATA['GEN_CLR'] = DC_STK_DATA['GEN_ART'].astype(str)+DC_STK_DATA['COLOR'].astype(str)
DC_STK_DATA['MC_GEN_CLR'] = DC_STK_DATA['MERCHCAT'].astype(str)+DC_STK_DATA['GEN_ART'].astype(str)+DC_STK_DATA['COLOR'].astype(str)


ST_DATA['GEN_CLR'] = ST_DATA['GEN_ART'].astype(str)+ST_DATA['COLOR'].astype(str)
ST_DATA['COMB'] = ST_DATA['ST_CD']+ST_DATA['GEN_ART'].astype(str)+ST_DATA['COLOR'].astype(str)
ST_DATA['ART_REQ'] = np.where(ST_DATA['ST_STK'] + 0 == 0,
                              ST_DATA['OPT_MBQ_A'],
                              np.maximum(ST_DATA['OPT_MBQ_A'] - (ST_DATA['ST_STK'] + 0), 0))

# Calculate 'SSN_ART_REQ' using vectorized operations
ST_DATA['SSN_ART_REQ'] = np.where(ST_DATA['ST_STK'] + 0 == 0,
                                  ST_DATA['SSN_MBQ_A'],
                                  np.maximum(ST_DATA['SSN_MBQ_A'] - (ST_DATA['ST_STK'] + 0), 0))



DC_STK_DATA['ALLOC']=0
ST_DATA['ALLOC']=0

ST_STK_MJ['MJ_ALLOC']=0
ST_STK_MC['MC_ALLOC']=0
ST_STK_MC_M_MVGR['ALLOC'] = 0
ST_STK_MC_MVGR_1['ALLOC'] = 0
ST_STK_MC_FABRIC['ALLOC'] = 0
ST_STK_MC_BODY['ALLOC'] = 0
ST_STK_MC_COLOR['ALLOC'] = 0
ST_STK_MC_M_MVGR_COLOR['ALLOC'] = 0
ST_STK_MC_MVGR_1_COLOR['ALLOC'] = 0
ST_STK_MC_FABRIC_COLOR['ALLOC'] = 0
ST_STK_MC_BODY_COLOR['ALLOC'] = 0
ST_STK_MC_M_MVGR_MVGR_1['ALLOC'] = 0
ST_STK_MC_M_MVGR_FABRIC['ALLOC'] = 0
ST_STK_MC_M_MVGR_BODY['ALLOC'] = 0
ST_STK_MC_M_MVGR_MVGR_1_COLOR['ALLOC'] = 0
ST_STK_MC_M_MVGR_FABRIC_COLOR['ALLOC'] = 0
ST_STK_MC_M_MVGR_BODY_COLOR['ALLOC'] = 0
MJ_MBQ['ALLOC'] = 0


MJ_ALGO = BGT_MAJCAT[['ST_CD','MAJCAT']]
MC_ALGO = BGT_MERCHCAT[['ST_CD','MAJCAT','MERCHCAT']]
MC_M_MVGR = MC_M_MVGR_CONT[['ST_CD','MERCHCAT','MACRO_MVGR']]
MC_MVGR = MC_MVGR_CONT[['ST_CD','MERCHCAT','MVGR']]
MC_BODY = MC_BODY_CONT[['ST_CD','MERCHCAT','BODY']]
MC_FABRIC = MC_FABRIC_CONT[['ST_CD','MERCHCAT','FABRIC']]
MC_CLR = MC_CLR_CONT[['ST_CD','MERCHCAT','CLR']]
MC_VEND = MC_VEND_CONT[['ST_CD','MERCHCAT','VEND_CD']]



MJ_MBQ['ALLOC']=MJ_MBQ['ALLOC'].astype(float)



#MAJCAT_MBQ_A['COMB']= MAJCAT_MBQ_A['ST_CD']+MAJCAT_MBQ_A['MAJCAT']
MJ_MBQ['COMB'] = MJ_MBQ['ST_CD']+MJ_MBQ['MAJCAT']
MC_MBQ['COMB'] = MC_MBQ['ST_CD']+MC_MBQ['MERCHCAT']
ST_STK_MJ['COMB'] = ST_STK_MJ['ST_CD']+ST_STK_MJ['MAJCAT']
ST_STK_MC['COMB'] = ST_STK_MC['ST_CD']+ST_STK_MC['MERCHCAT']
MC_M_MVGR['COMB'] = MC_M_MVGR['ST_CD']+MC_M_MVGR['MERCHCAT']+MC_M_MVGR['MACRO_MVGR']
MC_MVGR['COMB'] = MC_MVGR['ST_CD']+MC_MVGR['MERCHCAT']+MC_MVGR['MVGR']
MC_BODY['COMB'] = MC_BODY['ST_CD']+MC_BODY['MERCHCAT']+MC_BODY['BODY']
MC_FABRIC['COMB'] = MC_FABRIC['ST_CD']+MC_FABRIC['MERCHCAT']+MC_FABRIC['FABRIC']
MC_CLR['COMB'] = MC_CLR['ST_CD']+MC_CLR['MERCHCAT']+MC_CLR['CLR']
MC_VEND['COMB'] = MC_VEND['ST_CD']+MC_VEND['MERCHCAT']+str(MC_VEND['VEND_CD'])


MJ_MBQ['MJ_MBQ']=MJ_MBQ['MJ_MBQ_A']
MC_MBQ['MC_MBQ']=MC_MBQ['MC_MBQ_A']
# print(MJ_MBQ.columns.to_list())

MJ_MBQ = pd.merge(MJ_MBQ, ST_STK_MJ, how="left",on=["ST_CD","MAJCAT"],suffixes=("_left","_right"))
MC_MBQ = pd.merge(MC_MBQ, ST_STK_MC, how="left",on=["ST_CD","MERCHCAT"],suffixes=("_left","_right"))
MJ_MBQ = MJ_MBQ.rename(columns={'COMB_left':'COMB'})
MC_MBQ = MC_MBQ.rename(columns={'COMB_left':'COMB'})
# MJ_MBQ = MJ_MBQ.columns.str.replace('COMB_left','COMB')
# MC_MBQ = MC_MBQ.columns.str.replace('COMB_left','COMB')
# print(MC_MBQ.columns.to_list())



# print(MJ_MBQ.columns.to_list())
# print(MC_MBQ.columns.to_list())
MJ_MBQ['MJ_REQ'] = MJ_MBQ['MJ_MBQ']-(0.0+MJ_MBQ['MJ_REV_STK'])
MC_MBQ['MC_REQ'] = MC_MBQ['MC_MBQ']-(0.0+MC_MBQ['MC_REV_STK'])


MJ_MBQ.to_csv(path+r"\MJ_MBQ.csv")
MC_MBQ.to_csv(path+r"\MC_MBQ.csv")
ST_STK_MJ.to_csv(path+r"\MJ_STK.csv")
ST_STK_MC.to_csv(path+r"\MC_STK.csv")
DC_STK_DATA.to_csv(path+r"\DC_STK.csv")


A1=ST_DATA.columns
ST_DATA_NEW = pd.DataFrame(columns=A1)
A2=DC_STK_DATA.columns
DC_DATA_NEW = pd.DataFrame(columns=A2)
A3=MJ_MBQ.columns
MJ_MBQ_NEW = pd.DataFrame(columns=A3)
A4=MC_MBQ.columns
MC_MBQ_NEW = pd.DataFrame(columns=A4)



DC_STK_DATA['ALLOC']=0
ST_DATA['ALLOC']=0
MJ_MBQ['ALLOC'] = 0
MC_MBQ['ALLOC'] = 0
MC_M_MVGR['ALLOC'] = 0
MC_MVGR['ALLOC'] = 0
MC_BODY['ALLOC'] = 0
MC_FABRIC['ALLOC'] = 0
MC_CLR['ALLOC'] = 0
MC_VEND['ALLOC'] = 0



MC_M_MVGR_CONT['MC_M_MVGR_CONT_1'] = MC_M_MVGR_CONT['MC_M_MVGR_CONT_1'].str.rstrip('%').astype(float) / 100
merged_df = pd.merge(MC_M_MVGR_CONT, MC_MBQ, on=['ST_CD', 'MERCHCAT'],how="left",suffixes=("_left","_right"))
merged_df['MBQ'] = merged_df['MC_M_MVGR_CONT_1'] * merged_df['MC_MBQ']
MC_M_MVGR = MC_M_MVGR.merge(merged_df[['ST_CD', 'MERCHCAT', 'MBQ']], on=['ST_CD', 'MERCHCAT'], how='left')
print(MC_M_MVGR['MBQ'])
MC_MVGR['MBQ'] = 0
MC_BODY['MBQ'] = 0
MC_FABRIC['MBQ'] = 0
MC_CLR['MBQ'] = 0
MC_VEND['MBQ'] = 0

starttime = time.time()
i = CAT
ST_DATA_TEMP = ST_DATA
DC_STK_TEMP = DC_STK_DATA.loc[(DC_STK_DATA['REV_DC_STK']>0)].reset_index(drop=True)
cnt=0
gen_clr_to_merchcat = DC_STK_TEMP.set_index('GEN_CLR')['MERCHCAT'].to_dict()


st_cd_to_mc_m_mvgr = {row['ST_CD']: str(row['MERCHCAT']) + str(row['MACRO_MVGR']) for _,row in MC_M_MVGR.iterrows()}
st_cd_to_mc_mvgr = {row['ST_CD']: str(row['MERCHCAT']) + str(row['MVGR']) for _,row in MC_MVGR.iterrows()}
st_cd_to_mc_body = {row['ST_CD']: str(row['MERCHCAT']) + str(row['BODY']) for _,row in MC_BODY.iterrows()}
st_cd_to_mc_fabric = {row['ST_CD']: str(row['MERCHCAT']) + str(row['FABRIC']) for _,row in MC_FABRIC.iterrows()}
st_cd_to_mc_clr = {row['ST_CD']: str(row['MERCHCAT']) + str(row['CLR']) for _,row in MC_CLR.iterrows()}
st_cd_to_mc_vend = {row['ST_CD']: str(row['MERCHCAT']) + str(row['VEND_CD']) for _,row in MC_VEND.iterrows()}

comb_to_artreq = ST_DATA_TEMP.groupby('COMB')['ART_REQ'].first().to_dict()
# concatenated_combinations = {str(j) + str(k): (j, k) for j in ST_DETAILS['ST_CD'] for k in DC_STK_TEMP['GEN_CLR']}
concatenated_combinations = {str(j) + str(k): (j, k) for j in ST_DETAILS['ST_CD'] for k in DC_STK_TEMP['GEN_CLR'] if j=='HA10'}

art_req_sums = ST_DATA_TEMP.groupby('COMB')['ART_REQ'].sum().to_dict()
rev_dc_stk_sums = DC_STK_TEMP.groupby('GEN_CLR')['REV_DC_STK'].sum().to_dict()
mj_req_sums = MJ_MBQ.groupby('COMB')['MJ_REQ'].sum().to_dict()
mc_req_sums = MC_MBQ.groupby('COMB')['MC_REQ'].sum().to_dict()
for comb, (j, k) in concatenated_combinations.items():
    l = gen_clr_to_merchcat[k]
    ART_REQ = art_req_sums.get(str(j)+str(k), 0)
    # cnt+=1
    # print(ART_REQ)
    if rev_dc_stk_sums.get(k) >= ART_REQ:
        if comb_to_artreq.get(comb)>0:
            if mj_req_sums.get(str(j)+str(i)) >= ART_REQ:
                if (MC_MBQ.loc[MC_MBQ['COMB'] == str(j)+str(l),'MC_REQ'].sum()) >= ART_REQ:
                    ALLOC = art_req_sums.get(comb,0)

                    # Update ALLOC
                    ST_DATA_TEMP.loc[ST_DATA_TEMP['COMB'] == comb, 'ALLOC'] = ALLOC
                    DC_STK_TEMP.loc[DC_STK_TEMP['GEN_CLR'] == k, 'ALLOC'] += ALLOC
                    MJ_MBQ.loc[MJ_MBQ['COMB'] == str(j)+str(i),'ALLOC'] += ALLOC
                    MC_MBQ.loc[MC_MBQ['COMB'] == str(j)+str(l),'ALLOC'] += ALLOC
                    
                    # Update Requirement
                    ST_DATA_TEMP.loc[ST_DATA_TEMP['COMB'] == comb, 'ART_REQ'] -= ALLOC
                    DC_STK_TEMP.loc[DC_STK_TEMP['GEN_CLR'] == k, 'REV_DC_STK'] -= ALLOC
                    MJ_MBQ.loc[MJ_MBQ['COMB'] == str(j)+str(i),'MJ_REQ'] -= ALLOC
                    MC_MBQ.loc[MC_MBQ['COMB'] == str(j)+str(l),'MC_REQ'] -= ALLOC

                    # m = st_cd_to_mc_m_mvgr.get(j)
                    # MC_M_MVGR.loc[MC_M_MVGR['COMB'] == str(j)+str(m),'ALLOC'] += ALLOC
                    # m = st_cd_to_mc_mvgr.get(j)
                    # MC_MVGR.loc[MC_MVGR['COMB'] == str(j)+str(m),'ALLOC'] += ALLOC
                    # m = st_cd_to_mc_body.get(j)
                    # MC_BODY.loc[MC_BODY['COMB'] == str(j)+str(m),'ALLOC'] += ALLOC
                    # m = st_cd_to_mc_fabric.get(j)
                    # MC_FABRIC.loc[MC_FABRIC['COMB'] == str(j)+str(m),'ALLOC'] += ALLOC
                    # m = st_cd_to_mc_clr.get(j)
                    # MC_CLR.loc[MC_CLR['COMB'] == str(j)+str(m),'ALLOC'] += ALLOC
                    # m = st_cd_to_mc_vend.get(j)
                    # MC_VEND.loc[MC_VEND['COMB'] == str(j)+str(m),'ALLOC'] += ALLOC
                    
endtime = time.time()
                
# print(cnt)
if ST_DATA_NEW is None:
    ST_DATA_NEW = ST_DATA_TEMP
else:
    ST_DATA_NEW = pd.concat([ST_DATA_NEW,ST_DATA_TEMP],ignore_index=True)
if DC_DATA_NEW is None:  
    DC_DATA_NEW = DC_STK_TEMP
else:
    DC_DATA_NEW = pd.concat([DC_DATA_NEW,DC_STK_TEMP],ignore_index=True)

# print(MC_MBQ[['MC_MBQ','MC_REV_STK','ALLOC']])


print(MC_MBQ[['MC_MBQ','MC_REV_STK','ALLOC']])


path += r'\Ishu'
if not os.path.exists(path):
    os.makedirs(path)
ST_DATA_TEMP.to_csv(path+r"\ST_DATA_TEMP_Final.csv")
DC_STK_TEMP.to_csv(path+r"\DC_STK_TEMP_Final.csv")
MJ_MBQ.to_csv(path+r"\MJ_MBQ_Final.csv")
MC_MBQ.to_csv(path+r"\MC_MBQ_Final.csv")

MC_M_MVGR.to_csv(path+r"\MC_M_MVGR_Final.csv")
MC_MVGR.to_csv(path+r"\MC_MVGR_Final.csv")
MC_BODY.to_csv(path+r"\MC_BODY_Final.csv")
MC_FABRIC.to_csv(path+r"\MC_FABRIC_Final.csv")
MC_CLR.to_csv(path+r"\MC_CLR_Final.csv")
MC_VEND.to_csv(path+r"\MC_VEND_Final.csv")



seconds = endtime-starttime
hours = seconds // 3600
minutes = (seconds % 3600) // 60
seconds = seconds % 60
print(f"Loop Takes {str(int(hours)).rjust(2,'0')}:{str(int(minutes)).rjust(2,'0')}:{str(int(seconds)).rjust(2,'0')}")