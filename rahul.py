# %%
import warnings
warnings.filterwarnings("ignore")

# %%
import pandas as pd
import time
start_time = time.time()
#import datetime
#from datetime import timedelta, datetime
#import os


# %% [markdown]
# 1. Reading DAta from files

# %%
ST_DETAILS = pd.read_csv('ST_DETAILS.csv',skip_blank_lines=True)
MAJCAT_LIST = pd.read_csv('MAJCAT_LIST.csv',skip_blank_lines=True)
ST_L_DETAILS = pd.read_csv('ST_L_DETAILS.csv',skip_blank_lines=True)
BGT_MAJCAT_I = pd.read_csv('BGT_MAJCAT.csv',skip_blank_lines=True)
BGT_MERCHCAT_I = pd.read_csv('BGT_MERCHCAT.csv',skip_blank_lines=True)
ST_STK_I = pd.read_csv('ST_STK.csv',skip_blank_lines=True)
DC_STK_I = pd.read_csv('DC_STK.csv',skip_blank_lines=True)
MC_AUTO_ART_I = pd.read_csv('MC_AUTO_ART.csv',skip_blank_lines=True)
MC_BODY_CONT_I = pd.read_csv('MC_BODY_CONT.csv',skip_blank_lines=True)
MC_CLR_CONT_I = pd.read_csv('MC_CLR_CONT.csv',skip_blank_lines=True)
MC_FABRIC_CONT_I = pd.read_csv('MC_FABRIC_CONT.csv',skip_blank_lines=True)
MC_M_MVGR_CONT_I = pd.read_csv('MC_M_MVGR_CONT.csv',skip_blank_lines=True)
MC_MVGR_CONT_I = pd.read_csv('MC_MVGR_CONT.csv',skip_blank_lines=True)
MC_SEG_CONT_I = pd.read_csv('MC_SEG_CONT.csv',skip_blank_lines=True)
MC_VEND_CONT_I = pd.read_csv('MC_VEND_CONT.csv',skip_blank_lines=True)
ST_OPT_I = pd.read_csv('ST_OPT.csv',skip_blank_lines=True)
OPT_DISP_Q_I = pd.read_csv('OPT_DISP_Q.csv',skip_blank_lines=True)
ST_OPT_DISP_Q_I = pd.read_csv('ST_OPT_DISP_Q.csv',skip_blank_lines=True)
ST_MC_CONS_I = pd.read_csv('ST_MC_CONS.csv',skip_blank_lines=True)
ST_SSN_CONS_I = pd.read_csv('ST_SSN_CONS.csv',skip_blank_lines=True)

# %% [markdown]
# 2. Filtering rows on the basis of MAJCAT

# %%
CAT = "M_JEANS"
# print(BGT_MAJCAT_I[BGT_MAJCAT_I['MAJCAT']==CAT].reset_index(drop=True).columns.to_list())
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

# %%
# converting column to object
ST_OPT_DISP_Q['ST_CD']=ST_OPT_DISP_Q['ST_CD'].astype(object)
ST_DETAILS = ST_DETAILS.sort_values(by='PRIORITY_LIST', ascending=True)



# %%


# %%


# %%
MERCHCAT_MBQ = BGT_MERCHCAT
MAJCAT_MBQ = BGT_MAJCAT


MAJCAT_MBQ = pd.merge(MAJCAT_MBQ, ST_DETAILS, how="left",on=["ST_CD"],suffixes=('_left', '_right'))
# print(MAJCAT_MBQ.columns.to_list())
MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, ST_DETAILS, how="left",on=["ST_CD"],suffixes=('_left', '_right'))
MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, ST_L_DETAILS, how="left",on=["ST_CD"],suffixes=('_left', '_right'))
MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, OPT_DISP_Q, how="left",on=["MERCHCAT"],suffixes=('_left', '_right'))
MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, ST_OPT_DISP_Q, how="left",on=["ST_CD","MERCHCAT"],suffixes=('_left', '_right'))

# %%
#MAJCAT ALLOC MBQ
# print(MAJCAT_MBQ['REV_MJ_SALES_PD_1'])
MAJCAT_MBQ['REV_MJ_SALES_PD_1'] = MAJCAT_MBQ.apply(lambda x: (x['SALES_Q_1']*x['CM_RMN_SALES_CONS']+x['SALES_Q_2']*x['NM_RMN_SALES_CONS'])
                                                                           *(1/x['BGT_DAYS']), axis=1)
MAJCAT_MBQ['MJ_MBQ_A'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['ALLOC_DISP']+(x['REV_MJ_SALES_PD_1']*x['ALLOC_DAY_COVER']))
                                               , axis=1)
#MAJCAT MTH-1 REV-MBQ
MAJCAT_MBQ['MJ_MBQ_30_1'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['DISP_Q_1']+(x['REV_MJ_SALES_PD_1']*x['BGT_DAYS']))
                                               , axis=1)
#MAJCAT MTH-1 RMN PD SALES
MAJCAT_MBQ['REM_SALES_1'] = MAJCAT_MBQ.apply(lambda x: (x['SALES_Q_1']*x['CM_RMN_SALES_CONS'])
                                                                           *(1/x['RMN_BGT_DAYS']), axis=1)
#MAJCAT 30-MBQ QTY
MAJCAT_MBQ['MJ_MBQ_30_2'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_2']*(x['DISP_Q_2']+x['SALES_Q_2']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_3'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_3']*(x['DISP_Q_3']+x['SALES_Q_3']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_4'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_4']*(x['DISP_Q_4']+x['SALES_Q_4']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_5'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_5']*(x['DISP_Q_5']+x['SALES_Q_5']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_6'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_6']*(x['DISP_Q_6']+x['SALES_Q_6']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_7'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_7']*(x['DISP_Q_7']+x['SALES_Q_7']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_8'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_8']*(x['DISP_Q_8']+x['SALES_Q_8']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_9'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_9']*(x['DISP_Q_9']+x['SALES_Q_9']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_10'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_10']*(x['DISP_Q_10']+x['SALES_Q_10']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_11'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_11']*(x['DISP_Q_11']+x['SALES_Q_11']), axis=1)
MAJCAT_MBQ['MJ_MBQ_30_12'] = MAJCAT_MBQ.apply(lambda x: x['LISTING_M_12']*(x['DISP_Q_12']+x['SALES_Q_12']), axis=1)

# %%
#OPT_DISP_Q FINALISATION
MERCHCAT_MBQ['F_OPT_DISP_Q_1'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_1'] if x['ST_OPT_DISP_Q_1']+0>0
                                                        else x['OPT_DISP_Q_1'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_2'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_2'] if x['ST_OPT_DISP_Q_2']+0>0
                                                        else x['OPT_DISP_Q_2'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_3'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_3'] if x['ST_OPT_DISP_Q_3']+0>0
                                                        else x['OPT_DISP_Q_3'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_4'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_4'] if x['ST_OPT_DISP_Q_4']+0>0
                                                        else x['OPT_DISP_Q_4'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_5'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_5'] if x['ST_OPT_DISP_Q_5']+0>0
                                                        else x['OPT_DISP_Q_5'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_6'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_6'] if x['ST_OPT_DISP_Q_6']+0>0
                                                        else x['OPT_DISP_Q_6'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_7'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_7'] if x['ST_OPT_DISP_Q_7']+0>0
                                                        else x['OPT_DISP_Q_7'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_8'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_8'] if x['ST_OPT_DISP_Q_8']+0>0
                                                        else x['OPT_DISP_Q_8'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_9'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_9'] if x['ST_OPT_DISP_Q_9']+0>0
                                                        else x['OPT_DISP_Q_9'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_10'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_10'] if x['ST_OPT_DISP_Q_10']+0>0
                                                        else x['OPT_DISP_Q_10'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_11'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_11'] if x['ST_OPT_DISP_Q_11']+0>0
                                                        else x['OPT_DISP_Q_11'], axis=1)
MERCHCAT_MBQ['F_OPT_DISP_Q_12'] = MERCHCAT_MBQ.apply(lambda x: 
                                                    x['ST_OPT_DISP_Q_12'] if x['ST_OPT_DISP_Q_12']+0>0
                                                        else x['OPT_DISP_Q_12'], axis=1)
# print(MERCHCAT_MBQ[f'F_OPT_DISP_Q_2'])


# %%
#OPT_CNT FINALISATION
MERCHCAT_MBQ['OPT_CNT_A'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['ALLOC_DISP']/x['F_OPT_DISP_Q_1']) if (x['ALLOC_DISP']/x['F_OPT_DISP_Q_1'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_1'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_1']/x['F_OPT_DISP_Q_1']) if (x['DISP_Q_1']/x['F_OPT_DISP_Q_1'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_2'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_2']/x['F_OPT_DISP_Q_2']) if (x['DISP_Q_2']/x['F_OPT_DISP_Q_2'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_3'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_3']/x['F_OPT_DISP_Q_3']) if (x['DISP_Q_3']/x['F_OPT_DISP_Q_3'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_4'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_4']/x['F_OPT_DISP_Q_4']) if (x['DISP_Q_4']/x['F_OPT_DISP_Q_4'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_5'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_5']/x['F_OPT_DISP_Q_5']) if (x['DISP_Q_5']/x['F_OPT_DISP_Q_5'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_6'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_6']/x['F_OPT_DISP_Q_6']) if (x['DISP_Q_6']/x['F_OPT_DISP_Q_6'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_7'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_7']/x['F_OPT_DISP_Q_7']) if (x['DISP_Q_7']/x['F_OPT_DISP_Q_7'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_8'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_8']/x['F_OPT_DISP_Q_8']) if (x['DISP_Q_8']/x['F_OPT_DISP_Q_8'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_9'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_9']/x['F_OPT_DISP_Q_9']) if (x['DISP_Q_9']/x['F_OPT_DISP_Q_9'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_10'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_10']/x['F_OPT_DISP_Q_10']) if (x['DISP_Q_10']/x['F_OPT_DISP_Q_10'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_11'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_11']/x['F_OPT_DISP_Q_11']) if (x['DISP_Q_11']/x['F_OPT_DISP_Q_11'])+0>0
                                               else 0, axis=1)
MERCHCAT_MBQ['OPT_CNT_12'] = MERCHCAT_MBQ.apply(lambda x: 
                                               round(x['DISP_Q_12']/x['F_OPT_DISP_Q_12']) if (x['DISP_Q_12']/x['F_OPT_DISP_Q_12'])+0>0
                                               else 0, axis=1)

# %%
#OPTION SALES QTY
MERCHCAT_MBQ['OPT_SALES_A'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_1']+0==0 else 0 if x['OPT_CNT_A']+0==0
                                               else round(x['SALES_Q_1']/x['OPT_CNT_A'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_1'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_1']+0==0 else 0 if x['OPT_CNT_1']+0==0
                                               else round(x['SALES_Q_1']/x['OPT_CNT_1'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_2'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_2']+0==0 else 0 if x['OPT_CNT_2']+0==0
                                               else round(x['SALES_Q_2']/x['OPT_CNT_2'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_3'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_3']+0==0 else 0 if x['OPT_CNT_3']+0==0
                                               else round(x['SALES_Q_3']/x['OPT_CNT_3'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_4'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_4']+0==0 else 0 if x['OPT_CNT_4']+0==0
                                               else round(x['SALES_Q_4']/x['OPT_CNT_4'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_5'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_5']+0==0 else 0 if x['OPT_CNT_5']+0==0
                                               else round(x['SALES_Q_5']/x['OPT_CNT_5'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_6'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_6']+0==0 else 0 if x['OPT_CNT_6']+0==0
                                               else round(x['SALES_Q_6']/x['OPT_CNT_6'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_7'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_7']+0==0 else 0 if x['OPT_CNT_7']+0==0
                                               else round(x['SALES_Q_7']/x['OPT_CNT_7'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_8'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_8']+0==0 else 0 if x['OPT_CNT_8']+0==0
                                               else round(x['SALES_Q_8']/x['OPT_CNT_8'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_9'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_9']+0==0 else 0 if x['OPT_CNT_9']+0==0
                                               else round(x['SALES_Q_9']/x['OPT_CNT_9'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_10'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_10']+0==0 else 0 if x['OPT_CNT_10']+0==0
                                               else round(x['SALES_Q_10']/x['OPT_CNT_10'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_11'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_11']+0==0 else 0 if x['OPT_CNT_11']+0==0
                                               else round(x['SALES_Q_11']/x['OPT_CNT_11'],2), axis=1)
MERCHCAT_MBQ['OPT_SALES_12'] = MERCHCAT_MBQ.apply(lambda x: 
                                               0 if x['SALES_Q_12']+0==0 else 0 if x['OPT_CNT_12']+0==0
                                               else round(x['SALES_Q_12']/x['OPT_CNT_12'],2), axis=1)


# %%
#ALLOC MBQ
MERCHCAT_MBQ['REV_OPT_SALES_PD_A'] = MERCHCAT_MBQ.apply(lambda x: (x['OPT_SALES_A']*x['CM_RMN_SALES_CONS']+x['OPT_SALES_2']*x['NM_RMN_SALES_CONS'])
                                                                           *(1/x['BGT_DAYS']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_A'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['F_OPT_DISP_Q_1']+(x['REV_OPT_SALES_PD_A']*x['ALLOC_DAY_COVER']))
                                               , axis=1)
#MTH-1 REV-MBQ
MERCHCAT_MBQ['REV_OPT_SALES_PD_1'] = MERCHCAT_MBQ.apply(lambda x: (x['OPT_SALES_1']*x['CM_RMN_SALES_CONS']+x['OPT_SALES_2']*x['NM_RMN_SALES_CONS'])
                                                                           *(1/x['BGT_DAYS']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_1'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['F_OPT_DISP_Q_1']+(x['REV_OPT_SALES_PD_1']*x['BGT_DAYS']))
                                               , axis=1)
#MTH-1 RMN PD SALES
MERCHCAT_MBQ['REM_OPT_SALES_PD_1'] = MERCHCAT_MBQ.apply(lambda x: (x['OPT_SALES_1']*x['CM_RMN_SALES_CONS'])
                                                                           *(1/x['RMN_BGT_DAYS']), axis=1)


# %%
#OPTION 30-MBQ QTY
MERCHCAT_MBQ['OPT_MBQ_30_2'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_2']*(x['F_OPT_DISP_Q_2']+x['OPT_SALES_2']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_3'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_3']*(x['F_OPT_DISP_Q_3']+x['OPT_SALES_3']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_4'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_4']*(x['F_OPT_DISP_Q_4']+x['OPT_SALES_4']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_5'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_5']*(x['F_OPT_DISP_Q_5']+x['OPT_SALES_5']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_6'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_6']*(x['F_OPT_DISP_Q_6']+x['OPT_SALES_6']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_7'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_7']*(x['F_OPT_DISP_Q_7']+x['OPT_SALES_7']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_8'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_8']*(x['F_OPT_DISP_Q_8']+x['OPT_SALES_8']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_9'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_9']*(x['F_OPT_DISP_Q_9']+x['OPT_SALES_9']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_10'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_10']*(x['F_OPT_DISP_Q_10']+x['OPT_SALES_10']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_11'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_11']*(x['F_OPT_DISP_Q_11']+x['OPT_SALES_11']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_12'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_12']*(x['F_OPT_DISP_Q_12']+x['OPT_SALES_12']), axis=1)

# %%
#OPTION L1-MBQ QTY
MERCHCAT_MBQ['OPT_MBQ_L1_A'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['F_OPT_DISP_Q_1']+(x['REV_OPT_SALES_PD_A']*x['L1_SALES_COVER'])), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_1'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['F_OPT_DISP_Q_1']+(x['REV_OPT_SALES_PD_1']*x['L1_SALES_COVER'])), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_2'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_2']*(x['F_OPT_DISP_Q_2']+x['OPT_SALES_2']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_3'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_3']*(x['F_OPT_DISP_Q_3']+x['OPT_SALES_3']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_4'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_4']*(x['F_OPT_DISP_Q_4']+x['OPT_SALES_4']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_5'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_5']*(x['F_OPT_DISP_Q_5']+x['OPT_SALES_5']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_6'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_6']*(x['F_OPT_DISP_Q_6']+x['OPT_SALES_6']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_7'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_7']*(x['F_OPT_DISP_Q_7']+x['OPT_SALES_7']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_8'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_8']*(x['F_OPT_DISP_Q_8']+x['OPT_SALES_8']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_9'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_9']*(x['F_OPT_DISP_Q_9']+x['OPT_SALES_9']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_10'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_10']*(x['F_OPT_DISP_Q_10']+x['OPT_SALES_10']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_11'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_11']*(x['F_OPT_DISP_Q_11']+x['OPT_SALES_11']*(x['L1_SALES_COVER']/30)), axis=1)
MERCHCAT_MBQ['OPT_MBQ_L1_12'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_12']*(x['F_OPT_DISP_Q_12']+x['OPT_SALES_12']*(x['L1_SALES_COVER']/30)), axis=1)

# %%
#L-OPTION 30-MBQ QTY
MERCHCAT_MBQ['OPT_MBQ_30_A'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['F_OPT_DISP_Q_1']+(x['REV_OPT_SALES_PD_A']*x['ALLOC_DAY_COVER']*x['L_OPT_SALES_GR_M_1']))
                                               , axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_1'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['F_OPT_DISP_Q_1']+(x['REV_OPT_SALES_PD_1']*x['BGT_DAYS']*x['L_OPT_SALES_GR_M_1']))
                                               , axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_2'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_2']*(x['F_OPT_DISP_Q_2']+x['OPT_SALES_2']*x['L_OPT_SALES_GR_M_2']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_3'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_3']*(x['F_OPT_DISP_Q_3']+x['OPT_SALES_3']*x['L_OPT_SALES_GR_M_3']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_4'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_4']*(x['F_OPT_DISP_Q_4']+x['OPT_SALES_4']*x['L_OPT_SALES_GR_M_4']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_5'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_5']*(x['F_OPT_DISP_Q_5']+x['OPT_SALES_5']*x['L_OPT_SALES_GR_M_5']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_6'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_6']*(x['F_OPT_DISP_Q_6']+x['OPT_SALES_6']*x['L_OPT_SALES_GR_M_6']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_7'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_7']*(x['F_OPT_DISP_Q_7']+x['OPT_SALES_7']*x['L_OPT_SALES_GR_M_7']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_8'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_8']*(x['F_OPT_DISP_Q_8']+x['OPT_SALES_8']*x['L_OPT_SALES_GR_M_8']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_9'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_9']*(x['F_OPT_DISP_Q_9']+x['OPT_SALES_9']*x['L_OPT_SALES_GR_M_9']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_10'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_10']*(x['F_OPT_DISP_Q_10']+x['OPT_SALES_10']*x['L_OPT_SALES_GR_M_10']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_11'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_11']*(x['F_OPT_DISP_Q_11']+x['OPT_SALES_11']*x['L_OPT_SALES_GR_M_11']), axis=1)
MERCHCAT_MBQ['OPT_MBQ_30_12'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_12']*(x['F_OPT_DISP_Q_12']+x['OPT_SALES_12']*x['L_OPT_SALES_GR_M_12']), axis=1)

# %%
#L-OPTION SSN-MBQ QTY
MERCHCAT_MBQ['SSN_MBQ_A'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['F_OPT_DISP_Q_1']+(x['REV_OPT_SALES_PD_A']*x['ALLOC_DAY_COVER']*x['L_OPT_SALES_GR_M_1']))
                                               , axis=1)
MERCHCAT_MBQ['SSN_MBQ_1'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['F_OPT_DISP_Q_1']+(x['REV_OPT_SALES_PD_1']*x['BGT_DAYS']*x['L_OPT_SALES_GR_M_1']))
                                               , axis=1)
MERCHCAT_MBQ['SSN_MBQ_2'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_2']*(x['F_OPT_DISP_Q_2']+x['OPT_SALES_2']*x['L_OPT_SALES_GR_M_2']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_3'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_3']*(x['F_OPT_DISP_Q_3']+x['OPT_SALES_3']*x['L_OPT_SALES_GR_M_3']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_4'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_4']*(x['F_OPT_DISP_Q_4']+x['OPT_SALES_4']*x['L_OPT_SALES_GR_M_4']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_5'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_5']*(x['F_OPT_DISP_Q_5']+x['OPT_SALES_5']*x['L_OPT_SALES_GR_M_5']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_6'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_6']*(x['F_OPT_DISP_Q_6']+x['OPT_SALES_6']*x['L_OPT_SALES_GR_M_6']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_7'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_7']*(x['F_OPT_DISP_Q_7']+x['OPT_SALES_7']*x['L_OPT_SALES_GR_M_7']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_8'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_8']*(x['F_OPT_DISP_Q_8']+x['OPT_SALES_8']*x['L_OPT_SALES_GR_M_8']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_9'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_9']*(x['F_OPT_DISP_Q_9']+x['OPT_SALES_9']*x['L_OPT_SALES_GR_M_9']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_10'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_10']*(x['F_OPT_DISP_Q_10']+x['OPT_SALES_10']*x['L_OPT_SALES_GR_M_10']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_11'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_11']*(x['F_OPT_DISP_Q_11']+x['OPT_SALES_11']*x['L_OPT_SALES_GR_M_11']), axis=1)
MERCHCAT_MBQ['SSN_MBQ_12'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_12']*(x['F_OPT_DISP_Q_12']+x['OPT_SALES_12']*x['L_OPT_SALES_GR_M_12']), axis=1)

# %%
#MC ALLOC MBQ
MERCHCAT_MBQ['REV_MC_SALES_PD_1'] = MERCHCAT_MBQ.apply(lambda x: (x['SALES_Q_1']*x['CM_RMN_SALES_CONS']+x['SALES_Q_2']*x['NM_RMN_SALES_CONS'])
                                                                           *(1/x['BGT_DAYS']), axis=1)

MERCHCAT_MBQ['MC_MBQ_A'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['ALLOC_DISP']+(x['REV_MC_SALES_PD_1']*x['ALLOC_DAY_COVER']))
                                               , axis=1)
#MC MTH-1 REV-MBQ
MERCHCAT_MBQ['MC_MBQ_30_1'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_1']*(x['DISP_Q_1']+(x['REV_MC_SALES_PD_1']*x['BGT_DAYS']))
                                               , axis=1)
#MC MTH-1 RMN PD SALES
MERCHCAT_MBQ['REM_SALES_1'] = MERCHCAT_MBQ.apply(lambda x: (x['SALES_Q_1']*x['CM_RMN_SALES_CONS'])
                                                                           *(1/x['RMN_BGT_DAYS']), axis=1)


# %%
#MC 30-MBQ QTY
MERCHCAT_MBQ['MC_MBQ_30_2'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_2']*(x['DISP_Q_2']+x['SALES_Q_2']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_3'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_3']*(x['DISP_Q_3']+x['SALES_Q_3']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_4'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_4']*(x['DISP_Q_4']+x['SALES_Q_4']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_5'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_5']*(x['DISP_Q_5']+x['SALES_Q_5']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_6'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_6']*(x['DISP_Q_6']+x['SALES_Q_6']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_7'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_7']*(x['DISP_Q_7']+x['SALES_Q_7']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_8'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_8']*(x['DISP_Q_8']+x['SALES_Q_8']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_9'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_9']*(x['DISP_Q_9']+x['SALES_Q_9']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_10'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_10']*(x['DISP_Q_10']+x['SALES_Q_10']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_11'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_11']*(x['DISP_Q_11']+x['SALES_Q_11']), axis=1)
MERCHCAT_MBQ['MC_MBQ_30_12'] = MERCHCAT_MBQ.apply(lambda x: x['LISTING_M_12']*(x['DISP_Q_12']+x['SALES_Q_12']), axis=1)

# %%
MERCHCAT_MBQ.drop(['OPT_DISP_Q_1','OPT_DISP_Q_2','OPT_DISP_Q_3','OPT_DISP_Q_4','OPT_DISP_Q_5','OPT_DISP_Q_6'],inplace=True,axis=1)
MERCHCAT_MBQ.drop(['OPT_DISP_Q_7','OPT_DISP_Q_8','OPT_DISP_Q_9','OPT_DISP_Q_10','OPT_DISP_Q_11','OPT_DISP_Q_12'],inplace=True,axis=1)
MERCHCAT_MBQ.drop(['ST_OPT_DISP_Q_1','ST_OPT_DISP_Q_2','ST_OPT_DISP_Q_3','ST_OPT_DISP_Q_4','ST_OPT_DISP_Q_5','ST_OPT_DISP_Q_6'],inplace=True,axis=1)
MERCHCAT_MBQ.drop(['ST_OPT_DISP_Q_7','ST_OPT_DISP_Q_8','ST_OPT_DISP_Q_9','ST_OPT_DISP_Q_10','ST_OPT_DISP_Q_11','ST_OPT_DISP_Q_12'],inplace=True,axis=1)


# %%
MERCHCAT_MBQ['DISP_Q_1']=MERCHCAT_MBQ['DISP_Q_1'].astype(float)

# %%
MAJCAT_MBQ_A = MAJCAT_MBQ[['ST_CD','MAJCAT','MJ_MBQ_A','MJ_MBQ_30_1','MJ_MBQ_30_2','MJ_MBQ_30_3','MJ_MBQ_30_4','MJ_MBQ_30_5',
                          'MJ_MBQ_30_6','MJ_MBQ_30_7','MJ_MBQ_30_8','MJ_MBQ_30_9','MJ_MBQ_30_10','MJ_MBQ_30_11','MJ_MBQ_30_12']]

# %%
MERCHCAT_MBQ_A = MERCHCAT_MBQ[['ST_CD','MAJCAT','MERCHCAT','MC_MBQ_A','MC_MBQ_30_1','MC_MBQ_30_2','MC_MBQ_30_3','MC_MBQ_30_4','MC_MBQ_30_5',
                          'MC_MBQ_30_6','MC_MBQ_30_7','MC_MBQ_30_8','MC_MBQ_30_9','MC_MBQ_30_10','MC_MBQ_30_11','MC_MBQ_30_12']]

# %%
MERCHCAT_MBQ_TEMP = MERCHCAT_MBQ[['ST_CD','MERCHCAT','OPT_MBQ_A','OPT_MBQ_L1_A','OPT_MBQ_30_A','SSN_MBQ_A']]

# %%
#MERCHCAT_MBQ[['ST_CD','MERCHCAT','LISTING_M_1','ALLOC_DAY_COVER','CM_RMN_DAYS','ALLOC_DISP',
#                               'SALES_Q_1','REM_SALES_1','REV_MC_SALES_PD_1','MC_MBQ_A','F_OPT_DISP_Q_1','OPT_CNT_A','OPT_SALES_A',
#                               'REV_OPT_SALES_PD_A','OPT_MBQ_A','OPT_MBQ_L1_A','OPT_MBQ_30_A','SSN_MBQ_A']]

# %%
#download data to test
# path = r"C:\Users\pardeep.jajoria\Desktop\LISTING AUTOMATION"
path = r"C:\Users\Administrator\Desktop\drive-download-20240503T040120Z-001\Testing"
MAJCAT_MBQ.to_csv(path+r"\MJ_MBQ_file.csv")
MERCHCAT_MBQ.to_csv(path+r"\MC_MBQ_file.csv")
MERCHCAT_MBQ_A.to_csv(path+r"\MC_MBQ_B_file.csv")
MAJCAT_MBQ_A.to_csv(path+r"\MJ_MBQ_B_file.csv")

# %%
#MERCHCAT_MBQ_TEMP =MERCHCAT_MBQ_A
#MAJCAT_MBQ_TEMP = MAJCAT_MBQ_A


# %%
MJ_MBQ = MAJCAT_MBQ_A
MC_MBQ = MERCHCAT_MBQ_A

# %%
ST_STK_DATA = ST_STK
ST_STK_DATA = pd.merge(ST_STK_DATA, MERCHCAT_MBQ_TEMP, how="left",on=["ST_CD","MERCHCAT"])

# %%
#EXCESS STOCK REMOVAL
ST_STK_DATA['REV_ST_STK'] = ST_STK_DATA.apply(lambda x: x['ST_STK'] if x['OPT_MBQ_A']+0==0 
                                              else min(x['ST_STK'],x['OPT_MBQ_A']), axis=1)

# %%
ST_STK_DATA_TEMP = ST_STK_DATA[['ST_CD','MERCHCAT','GEN_ART','COLOR','ST_STK','REV_ST_STK']]

# %%
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


# %%
#download data ST_STK_DATA
temp=ST_STK_DATA
temp.to_csv(path+r"\ST_STK_temp_file.csv")

# %%


# %%
#CONVERTING DC-DATA INTO ST MULTIPLE DC-DATA

# %%
DC_STK_DATA = DC_STK

# %%
ST_DC=DC_STK_DATA[['SEG','DIV','SUB_DIV','MAJCAT','MERCHCAT','MRP','RNG_SEG','SSN','GEN_ART','GEN_ART_DESC','COLOR','VAR_ART','SZ','M_VND_CD',
              'M_VND_NM','CO_AGING','MACRO_MVGR','MVGR_1','FABRIC','BODY','DISCOUNT_STATUS']]
ST_DC.insert(0,"ST_NO",0)
ST_DC['ST_NO']=0
ST_TEST = ST_DC
ST_DATA = ST_DC

# %%
for k in range(0,200):
    #print(k)
    ST_TEST['ST_NO']=ST_TEST['ST_NO'].replace(k,k+1)
    ST_DATA = pd.concat([ST_DATA,ST_TEST],ignore_index=True)

# %%
DATACHECK = ST_DATA[(ST_DATA['ST_NO']==1)].index
ST_DATA.drop(DATACHECK, inplace=True)

# %%
ST_DC['ST_NO']=1
ST_DATA = pd.concat([ST_DATA,ST_DC],ignore_index=True)

# %%


# %%
ST_DETAILS_R = ST_DETAILS[['ST_CD','PRIORITY_LIST']]
ST_DETAILS_R['ST_NO']=ST_DETAILS_R['PRIORITY_LIST']
ST_DETAILS_R = ST_DETAILS[['ST_CD','ST_NO']]

# %%
ST_DATA = pd.merge(ST_DATA, ST_DETAILS_R, how="left",on=["ST_NO"])

# %%
ST_DATA['ST_CD'] = ST_DATA['ST_CD'].fillna("xxx")
DATACHECK = ST_DATA[(ST_DATA['ST_CD']=='xxx')].index
ST_DATA.drop(DATACHECK, inplace=True)

# %%
#ST_DATA = pd.merge(ST_DATA, DC_STK_DATA_TEMP, how="left",on=["MAJCAT","MERCHCAT","GEN_ART","COLOR"])
#ST_DATA = pd.merge(ST_DATA, ST_STK_MC, how="left",on=["ST_CD","MERCHCAT"])
#ST_DATA = pd.merge(ST_DATA, ST_STK_MJ, how="left",on=["ST_CD","MAJCAT"])


# %%
ST_DATA = pd.merge(ST_DATA, MERCHCAT_MBQ_TEMP, how="left",on=["ST_CD","MERCHCAT"])
ST_DATA = pd.merge(ST_DATA, ST_STK_DATA_TEMP, how="left",on=["ST_CD","MERCHCAT","GEN_ART","COLOR"])
ST_DATA['ST_STK'] = ST_DATA['ST_STK'].fillna(0)
ST_DATA['REV_ST_STK'] = ST_DATA['REV_ST_STK'].fillna(0)

# %%
temp=ST_DATA.head(10000)
temp.to_csv(path+r"\ST_DATA_temp_file.csv")

# %%


# %%
#comb
DC_STK_DATA['REV_DC_STK']=DC_STK_DATA['CURR_DC_STK']
DC_STK_DATA['GEN_CLR'] = DC_STK_DATA['GEN_ART'].astype(str)+DC_STK_DATA['COLOR'].astype(str)
DC_STK_DATA['MC_GEN_CLR'] = DC_STK_DATA['MERCHCAT'].astype(str)+DC_STK_DATA['GEN_ART'].astype(str)+DC_STK_DATA['COLOR'].astype(str)

# %%
ST_DATA['GEN_CLR'] = ST_DATA['GEN_ART'].astype(str)+ST_DATA['COLOR'].astype(str)
ST_DATA['COMB'] = ST_DATA['ST_CD']+ST_DATA['GEN_ART'].astype(str)+ST_DATA['COLOR'].astype(str)
ST_DATA['ART_REQ'] = ST_DATA.apply(lambda x: x['OPT_MBQ_A'] if (x['ST_STK']+0) ==0
                                   else max(x['OPT_MBQ_A']-(x['ST_STK']+0),0), axis=1)
ST_DATA['SSN_ART_REQ'] = ST_DATA.apply(lambda x: x['SSN_MBQ_A'] if (x['ST_STK']+0) == 0
                                       else max(x['SSN_MBQ_A']-(x['ST_STK']+0),0), axis=1)


# %%
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

# %%
MJ_ALGO = BGT_MAJCAT[['ST_CD','MAJCAT']]
MC_ALGO = BGT_MERCHCAT[['ST_CD','MAJCAT','MERCHCAT']]
MC_M_MVGR = MC_M_MVGR_CONT[['ST_CD','MERCHCAT','MACRO_MVGR']]
MC_MVGR = MC_MVGR_CONT[['ST_CD','MERCHCAT','MVGR']]
MC_BODY = MC_BODY_CONT[['ST_CD','MERCHCAT','BODY']]
MC_FABRIC = MC_FABRIC_CONT[['ST_CD','MERCHCAT','FABRIC']]
MC_CLR = MC_CLR_CONT[['ST_CD','MERCHCAT','CLR']]
MC_VEND = MC_VEND_CONT[['ST_CD','MERCHCAT','VEND_CD']]


# %%
MJ_MBQ['ALLOC']=MJ_MBQ['ALLOC'].astype(float)

# %%

#MAJCAT_MBQ_A['COMB']= MAJCAT_MBQ_A['ST_CD']+MAJCAT_MBQ_A['MAJCAT']
MJ_MBQ['COMB'] = MJ_MBQ['ST_CD']+MJ_MBQ['MAJCAT']
MC_MBQ['COMB'] = MC_MBQ['ST_CD']+MC_MBQ['MERCHCAT']
#ST_STK_MJ['COMB'] = ST_STK_MJ['ST_CD']+ST_STK_MJ['MAJCAT']
#ST_STK_MC['COMB'] = ST_STK_MC['ST_CD']+ST_STK_MC['MERCHCAT']
MC_M_MVGR['COMB'] = MC_M_MVGR['ST_CD']+MC_M_MVGR['MERCHCAT']+MC_M_MVGR['MACRO_MVGR']
MC_MVGR['COMB'] = MC_MVGR['ST_CD']+MC_MVGR['MERCHCAT']+MC_MVGR['MVGR']
MC_BODY['COMB'] = MC_BODY['ST_CD']+MC_BODY['MERCHCAT']+MC_BODY['BODY']
MC_FABRIC['COMB'] = MC_FABRIC['ST_CD']+MC_FABRIC['MERCHCAT']+MC_FABRIC['FABRIC']
MC_CLR['COMB'] = MC_CLR['ST_CD']+MC_CLR['MERCHCAT']+MC_CLR['CLR']
MC_VEND['COMB'] = MC_VEND['ST_CD']+MC_VEND['MERCHCAT']+MC_VEND['VEND_CD']


# %%
MJ_MBQ['MJ_MBQ']=MJ_MBQ['MJ_MBQ_A']
MC_MBQ['MC_MBQ']=MC_MBQ['MC_MBQ_A']
MJ_MBQ = pd.merge(MJ_MBQ, ST_STK_MJ, how="left",on=["ST_CD","MAJCAT"])
MC_MBQ = pd.merge(MC_MBQ, ST_STK_MC, how="left",on=["ST_CD","MERCHCAT"])


# %%
MJ_MBQ['MJ_REQ'] = MJ_MBQ['MJ_MBQ']-(0.0+MJ_MBQ['MJ_REV_STK'])
MC_MBQ['MC_REQ'] = MC_MBQ['MC_MBQ']-(0.0+MC_MBQ['MC_REV_STK'])

# %%
MJ_MBQ.to_csv(path+r"\MJ_MBQ.csv")
MC_MBQ.to_csv(path+r"\MC_MBQ.csv")
ST_STK_MJ.to_csv(path+r"\MJ_STK.csv")
ST_STK_MC.to_csv(path+r"\MC_STK.csv")
DC_STK_DATA.to_csv(path+r"\DC_STK.csv")

# %%
#MAJCAT_LIST1 = list(MAJCAT_LIST['MAJCAT'].unique())

# %%
#ST_LIST=['HB05','HB06','HA10']
#MAJCAT_LIST2 = MAJCAT_LIST1['M_JEANS']

# %%
A1=ST_DATA.columns
ST_DATA_NEW = pd.DataFrame(columns=A1)
A2=DC_STK_DATA.columns
DC_DATA_NEW = pd.DataFrame(columns=A2)
A3=MJ_MBQ.columns
MJ_MBQ_NEW = pd.DataFrame(columns=A3)
A4=MC_MBQ.columns
MC_MBQ_NEW = pd.DataFrame(columns=A4)


# %%
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

# %%
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

# %%
#for i in MAJCAT_LIST1:
i = CAT
cnt=0
ST_DATA_TEMP = ST_DATA
# print(datetime.now())
DC_STK_TEMP = DC_STK_DATA[(DC_STK_DATA['REV_DC_STK']>0)].reset_index(drop=True)

# print(DC_STK_TEMP[['GEN_CLR','MERCHCAT']])
for j in ST_DETAILS['ST_CD']:
# j='HA10'
# # MERCHCAT LEVEL LISTING
    for k in DC_STK_TEMP['GEN_CLR']:
        l = DC_STK_TEMP.loc[DC_STK_TEMP['GEN_CLR']==k,'MERCHCAT'].item()
        ART_REQ = ST_DATA_TEMP.loc[ST_DATA_TEMP['COMB'] == str(j)+str(k),'ART_REQ'].sum()
        # print(j,' ',k,' ',l,' ',ART_REQ)
        #ART EXISTENCE IN ST-DATA & THERE SHOULD BE NO ALLOC IN PREVIOUS
        # i1 = DC_STK_TEMP.loc[DC_STK_TEMP['GEN_CLR']==k,'REV_DC_STK'].sum()
        # i2 = ST_DATA_TEMP.loc[ST_DATA_TEMP['COMB'] == str(j)+str(k),'ART_REQ'].item()
        # i3 = MJ_MBQ.loc[MJ_MBQ['COMB'] == str(j)+str(i),'MJ_REQ'].sum()
        # i4 = MC_MBQ.loc[MC_MBQ['COMB'] == str(j)+str(l),'MC_REQ'].sum()
        # # print(j,' ',k,' ',l,' ',ART_REQ,' ',i1,' ',i2,' ',i3,' ',i4)
        if DC_STK_TEMP.loc[DC_STK_TEMP['GEN_CLR']==k,'REV_DC_STK'].sum() >= ART_REQ:
            if ST_DATA_TEMP.loc[ST_DATA_TEMP['COMB'] == str(j)+str(k),'ART_REQ'].item()>0:
                if (MJ_MBQ.loc[MJ_MBQ['COMB'] == str(j)+str(i),'MJ_REQ'].sum()) >= ART_REQ:
                    if (MC_MBQ.loc[MC_MBQ['COMB'] == str(j)+str(l),'MC_REQ'].sum()) >= ART_REQ:
    #                     #IF CONDITION TRUE THEN EXECUTE THE BELOW STEPS
                        ALLOC=0
                        ALLOC = ST_DATA_TEMP.loc[ST_DATA_TEMP['COMB'] == str(j)+str(k),'ART_REQ'].sum()
                        # print(ALLOC)
                        ST_DATA_TEMP.loc[ST_DATA_TEMP['COMB'] == str(j)+str(k),'ALLOC'] = ALLOC
                        ST_DATA_TEMP.loc[ST_DATA_TEMP['COMB'] == str(j)+str(k),'ART_REQ'] -= ALLOC
                        DC_STK_TEMP.loc[DC_STK_TEMP['GEN_CLR']==k,'ALLOC'] += ALLOC
                        DC_STK_TEMP.loc[DC_STK_TEMP['GEN_CLR']==k,'REV_DC_STK'] -= ALLOC
                        MJ_MBQ.loc[MJ_MBQ['COMB'] == str(j)+str(i),'ALLOC'] += ALLOC
                        MC_MBQ.loc[MC_MBQ['COMB'] == str(j)+str(l),'ALLOC'] += ALLOC
                        MJ_MBQ.loc[MJ_MBQ['COMB'] == str(j)+str(i),'MJ_REQ'] -= ALLOC
                        MC_MBQ.loc[MC_MBQ['COMB'] == str(j)+str(l),'MC_REQ'] -= ALLOC
    # # cnt+=1
    #     # print(j)
    #     # print(datetime.now())

if ST_DATA_NEW is None:  
    ST_DATA_NEW = ST_DATA_TEMP
else:
    ST_DATA_NEW = pd.concat([ST_DATA_NEW,ST_DATA_TEMP],ignore_index=True)
if DC_DATA_NEW is None:  
    DC_DATA_NEW = DC_STK_TEMP
else:
    DC_DATA_NEW = pd.concat([DC_DATA_NEW,DC_STK_TEMP],ignore_index=True)
# print(datetime.now())

# %%
print(MC_MBQ[['MC_MBQ','MC_REV_STK','ALLOC']])
end_time = time.time()
execution_time = end_time - start_time
print("Execution time:", execution_time, "seconds")


# %%



