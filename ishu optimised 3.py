import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import polars as pl
import numpy as np
import os
import time
import multiprocessing as mp
from functools import partial

def load_data(file_name):
    return pd.read_csv(file_name, skip_blank_lines=True, engine='pyarrow')

def process_combination(comb, comb_values, gen_clr_to_merchcat, unique_macro_mvgr, unique_mvgr, unique_fabric, unique_clr, unique_vend, i, st_polar, dc_polar, mj_polar, mc_polar):
    j, k = comb_values
    l = gen_clr_to_merchcat.get(k)
    comb_jk = str(j) + str(k)
    comb_jl = str(j) + str(l)
    comb_ji = str(j) + str(i)

    ART_REQ_sum = st_polar.filter(pl.col('COMB') == comb_jk)['ART_REQ'].sum()
    rev_dc_stk = dc_polar.filter(pl.col('GEN_CLR') == k)['REV_DC_STK'].sum()
    first_art_req = st_polar.filter(pl.col('COMB') == comb_jk).select(pl.col('ART_REQ').first().fill_nan(0).fill_null(0)).item()
    mj_req = mj_polar.filter(pl.col('COMB') == comb_ji)['MJ_REQ'].sum()
    mc_req = mc_polar.filter(pl.col('COMB') == comb_jl)['MC_REQ'].sum()

    if rev_dc_stk >= ART_REQ_sum and first_art_req > 0 and mj_req >= ART_REQ_sum and mc_req >= ART_REQ_sum:
        ALLOC = ART_REQ_sum

        st_polar_updates = [(comb_jk, ALLOC)]
        dc_polar_updates = [(k, ALLOC)]
        mj_polar_updates = [(comb_ji, ALLOC)]
        mc_polar_updates = [(comb_jl, ALLOC)]

        mc_m_mvgr_polar_updates = []
        mc_mvgr_polar_updates = []
        mc_fabric_polar_updates = []
        mc_clr_polar_updates = []
        mc_vend_polar_updates = []

        for walk in unique_macro_mvgr:
            comb_jlm = str(j) + str(l) + str(walk)
            alloc = st_polar.filter(
                (pl.col('ST_CD') == str(j)) &
                (pl.col('MERCHCAT') == str(l)) &
                (pl.col('MACRO_MVGR') == str(walk))
            )['ART_REQ'].sum()
            mc_m_mvgr_polar_updates.append((comb_jlm, alloc))

        for walk in unique_mvgr:
            comb_jlm = str(j) + str(l) + str(walk)
            alloc = st_polar.filter(
                (pl.col('ST_CD') == str(j)) &
                (pl.col('MERCHCAT') == str(l)) &
                (pl.col('MVGR_1') == str(walk))
            )['ART_REQ'].sum()
            mc_mvgr_polar_updates.append((comb_jlm, alloc))

        for walk in unique_fabric:
            comb_jlm = str(j) + str(l) + str(walk)
            alloc = st_polar.filter(
                (pl.col('ST_CD') == str(j)) &
                (pl.col('MERCHCAT') == str(l)) &
                (pl.col('FABRIC') == str(walk))
            )['ART_REQ'].sum()
            mc_fabric_polar_updates.append((comb_jlm, alloc))

        for walk in unique_clr:
            comb_jlm = str(j) + str(l) + str(walk)
            alloc = st_polar.filter(
                (pl.col('ST_CD') == str(j)) &
                (pl.col('MERCHCAT') == str(l)) &
                (pl.col('COLOR') == str(walk))
            )['ART_REQ'].sum()
            mc_clr_polar_updates.append((comb_jlm, alloc))

        # for walk in unique_vend:
        #     comb_jlm = str(j) + str(l) + str(walk)
        #     alloc = st_polar.filter(
        #         (pl.col('ST_CD') == str(j)) &
        #         (pl.col('MERCHCAT') == str(l)) &
        #         (pl.col('M_VND_CD') == str(walk))
        #     )['ART_REQ'].sum()
        #     mc_fabric_polar_updates.append((comb_jlm, alloc))

        return {
            'st_polar_updates': st_polar_updates,
            'dc_polar_updates': dc_polar_updates,
            'mj_polar_updates': mj_polar_updates,
            'mc_polar_updates': mc_polar_updates,
            'mc_m_mvgr_polar_updates': mc_m_mvgr_polar_updates,
            'mc_mvgr_polar_updates': mc_mvgr_polar_updates,
            'mc_fabric_polar_updates': mc_fabric_polar_updates,
            'mc_clr_polar_updates': mc_clr_polar_updates
        }

    return None

def update_dataframe(df, updates):
    for comb, alloc in updates:
        df = df.with_columns([
            pl.when(pl.col('COMB') == comb).then(alloc).otherwise(pl.col('ALLOC')).alias('ALLOC'),
            pl.when(pl.col('COMB') == comb).then(pl.col('ART_REQ') - alloc).otherwise(pl.col('ART_REQ')).alias('ART_REQ')
        ])
    return df

if __name__ == '__main__':
    starttime1 = time.time()

    # Load data and perform initial processing
    ST_DETAILS = load_data('ST_DETAILS.csv')
    MAJCAT_LIST = load_data('MAJCAT_LIST.csv')
    ST_L_DETAILS = load_data('ST_L_DETAILS.csv')
    BGT_MAJCAT_I = load_data('BGT_MAJCAT.csv')
    BGT_MERCHCAT_I = load_data('BGT_MERCHCAT.csv')
    ST_STK_I = load_data('ST_STK.csv')
    DC_STK_I = load_data('DC_STK.csv')
    MC_AUTO_ART_I = load_data('MC_AUTO_ART.csv')
    MC_BODY_CONT_I = load_data('MC_BODY_CONT.csv')
    MC_CLR_CONT_I = load_data('MC_CLR_CONT.csv')
    MC_FABRIC_CONT_I = load_data('MC_FABRIC_CONT.csv')
    MC_M_MVGR_CONT_I = load_data('MC_M_MVGR_CONT.csv')
    MC_MVGR_CONT_I = load_data('MC_MVGR_CONT.csv')
    MC_SEG_CONT_I = load_data('MC_SEG_CONT.csv')
    MC_VEND_CONT_I = load_data('MC_VEND_CONT.csv')
    ST_OPT_I = load_data('ST_OPT.csv')
    OPT_DISP_Q_I = load_data('OPT_DISP_Q.csv')
    ST_OPT_DISP_Q_I = load_data('ST_OPT_DISP_Q.csv')
    ST_MC_CONS_I = load_data('ST_MC_CONS.csv')
    ST_SSN_CONS_I = load_data('ST_SSN_CONS.csv')

    # Data preparation steps...
    CAT = "M_JEANS"
    BGT_MAJCAT = BGT_MAJCAT_I[BGT_MAJCAT_I['MAJCAT'] == CAT].reset_index(drop=True)
    BGT_MERCHCAT = BGT_MERCHCAT_I[BGT_MERCHCAT_I['MAJCAT'] == CAT].reset_index(drop=True)
    ST_STK = ST_STK_I[ST_STK_I['MAJCAT'] == CAT].reset_index(drop=True)
    DC_STK = DC_STK_I[DC_STK_I['MAJCAT'] == CAT].reset_index(drop=True)
    MC_AUTO_ART = MC_AUTO_ART_I[MC_AUTO_ART_I['MAJCAT'] == CAT].reset_index(drop=True)
    MC_BODY_CONT = MC_BODY_CONT_I[MC_BODY_CONT_I['MAJCAT'] == CAT].reset_index(drop=True)
    MC_CLR_CONT = MC_CLR_CONT_I[MC_CLR_CONT_I['MAJCAT'] == CAT].reset_index(drop=True)
    MC_FABRIC_CONT = MC_FABRIC_CONT_I[MC_FABRIC_CONT_I['MAJCAT'] == CAT].reset_index(drop=True)
    MC_M_MVGR_CONT = MC_M_MVGR_CONT_I[MC_M_MVGR_CONT_I['MAJCAT'] == CAT].reset_index(drop=True)
    MC_MVGR_CONT = MC_MVGR_CONT_I[MC_MVGR_CONT_I['MAJCAT'] == CAT].reset_index(drop=True)
    MC_SEG_CONT = MC_SEG_CONT_I[MC_SEG_CONT_I['MAJCAT'] == CAT].reset_index(drop=True)
    MC_VEND_CONT = MC_VEND_CONT_I[MC_VEND_CONT_I['MAJCAT'] == CAT].reset_index(drop=True)
    ST_OPT = ST_OPT_I[ST_OPT_I['MAJCAT'] == CAT].reset_index(drop=True)
    OPT_DISP_Q = OPT_DISP_Q_I[OPT_DISP_Q_I['MAJCAT'] == CAT].reset_index(drop=True)
    ST_OPT_DISP_Q = ST_OPT_DISP_Q_I[ST_OPT_DISP_Q_I['MAJCAT'] == CAT].reset_index(drop=True)
    ST_MC_CONS = ST_MC_CONS_I[ST_MC_CONS_I['MAJCAT'] == CAT].reset_index(drop=True)
    # ST_SSN_CONS = ST_SSN_CONS_I[ST_SSN_CONS_I['MAJCAT'] == CAT].reset_index(drop=True)

    # Converting column to object
    ST_OPT_DISP_Q['ST_CD'] = ST_OPT_DISP_Q['ST_CD'].astype(object)
    ST_DETAILS = ST_DETAILS.sort_values(by='PRIORITY_LIST', ascending=True)

    MERCHCAT_MBQ = BGT_MERCHCAT
    MAJCAT_MBQ = BGT_MAJCAT

    MAJCAT_MBQ = pd.merge(MAJCAT_MBQ, ST_DETAILS, how="left", on=["ST_CD"], suffixes=('_left', '_right'))
    MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, ST_DETAILS, how="left", on=["ST_CD"], suffixes=('_left', '_right'))
    MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, ST_L_DETAILS, how="left", on=["ST_CD"], suffixes=('_left', '_right'))
    MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, OPT_DISP_Q, how="left", on=["MERCHCAT"], suffixes=('_left', '_right'))
    MERCHCAT_MBQ = pd.merge(MERCHCAT_MBQ, ST_OPT_DISP_Q, how="left", on=["ST_CD", "MERCHCAT"], suffixes=('_left', '_right'))

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

    MERCHCAT_MBQ.drop(['OPT_DISP_Q_1', 'OPT_DISP_Q_2', 'OPT_DISP_Q_3', 'OPT_DISP_Q_4', 'OPT_DISP_Q_5', 'OPT_DISP_Q_6', 'OPT_DISP_Q_7', 'OPT_DISP_Q_8', 'OPT_DISP_Q_9', 'OPT_DISP_Q_10', 'OPT_DISP_Q_11', 'OPT_DISP_Q_12',
                      'ST_OPT_DISP_Q_1', 'ST_OPT_DISP_Q_2', 'ST_OPT_DISP_Q_3', 'ST_OPT_DISP_Q_4', 'ST_OPT_DISP_Q_5', 'ST_OPT_DISP_Q_6', 'ST_OPT_DISP_Q_7', 'ST_OPT_DISP_Q_8', 'ST_OPT_DISP_Q_9', 'ST_OPT_DISP_Q_10', 'ST_OPT_DISP_Q_11', 'ST_OPT_DISP_Q_12'], inplace=True, axis=1)

    MERCHCAT_MBQ['DISP_Q_1'] = MERCHCAT_MBQ['DISP_Q_1'].astype(float)

    MAJCAT_MBQ_A = MAJCAT_MBQ[['ST_CD', 'MAJCAT', 'MJ_MBQ_A', 'MJ_MBQ_30_1', 'MJ_MBQ_30_2', 'MJ_MBQ_30_3', 'MJ_MBQ_30_4', 'MJ_MBQ_30_5',
                               'MJ_MBQ_30_6', 'MJ_MBQ_30_7', 'MJ_MBQ_30_8', 'MJ_MBQ_30_9', 'MJ_MBQ_30_10', 'MJ_MBQ_30_11', 'MJ_MBQ_30_12']]

    MERCHCAT_MBQ_A = MERCHCAT_MBQ[['ST_CD', 'MAJCAT', 'MERCHCAT', 'MC_MBQ_A', 'MC_MBQ_30_1', 'MC_MBQ_30_2', 'MC_MBQ_30_3', 'MC_MBQ_30_4', 'MC_MBQ_30_5',
                                   'MC_MBQ_30_6', 'MC_MBQ_30_7', 'MC_MBQ_30_8', 'MC_MBQ_30_9', 'MC_MBQ_30_10', 'MC_MBQ_30_11', 'MC_MBQ_30_12']]

    MERCHCAT_MBQ_TEMP = MERCHCAT_MBQ[['ST_CD', 'MERCHCAT', 'OPT_MBQ_A', 'OPT_MBQ_L1_A', 'OPT_MBQ_30_A', 'SSN_MBQ_A']]

    # Download data to test
    # path = r"C:\Users\pardeep.jajoria\Desktop\LISTING AUTOMATION"
    path = r".\Testing"
    # MAJCAT_MBQ.to_csv(path+r"\MJ_MBQ_file.csv")
    # MERCHCAT_MBQ.to_csv(path+r"\MC_MBQ_file.csv")
    # MERCHCAT_MBQ_A.to_csv(path+r"\MC_MBQ_B_file.csv")
    # MAJCAT_MBQ_A.to_csv(path+r"\MJ_MBQ_B_file.csv")

    MJ_MBQ = MAJCAT_MBQ_A
    MC_MBQ = MERCHCAT_MBQ_A

    ST_STK_DATA = ST_STK
    ST_STK_DATA = pd.merge(ST_STK_DATA, MERCHCAT_MBQ_TEMP, how="left", on=["ST_CD", "MERCHCAT"], suffixes=('_left', '_right'))

    # Vectorized calculation for REV_ST_STK
    ST_STK_DATA['REV_ST_STK'] = np.where(
        ST_STK_DATA['OPT_MBQ_A'] + 0 == 0,
        ST_STK_DATA['ST_STK'],
        np.minimum(ST_STK_DATA['ST_STK'], ST_STK_DATA['OPT_MBQ_A'])
    )

    ST_STK_DATA_TEMP = ST_STK_DATA[['ST_CD', 'MERCHCAT', 'GEN_ART', 'COLOR', 'ST_STK', 'REV_ST_STK']]

    # REV STK BTM-UP AT ALL HIER LEVEL
    ST_STK_MJ = ST_STK_DATA.groupby(['ST_CD', 'MAJCAT'], as_index=False).agg(MJ_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_M_MVGR = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_MVGR_1 = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MVGR_1'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_FABRIC = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'FABRIC'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_BODY = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'BODY'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_COLOR = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_M_MVGR_COLOR = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR', 'COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_MVGR_1_COLOR = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MVGR_1', 'COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_FABRIC_COLOR = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'FABRIC', 'COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_BODY_COLOR = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'BODY', 'COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_M_MVGR_MVGR_1 = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR', 'MVGR_1'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_M_MVGR_FABRIC = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR', 'FABRIC'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_M_MVGR_BODY = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR', 'BODY'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_M_MVGR_MVGR_1_COLOR = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR', 'MVGR_1', 'COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_M_MVGR_FABRIC_COLOR = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR', 'FABRIC', 'COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))
    ST_STK_MC_M_MVGR_BODY_COLOR = ST_STK_DATA.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR', 'BODY', 'COLOR'], as_index=False).agg(MC_REV_STK=('REV_ST_STK', 'sum'))

    # Download data ST_STK_DATA
    temp = ST_STK_DATA
    temp.to_csv(path+r"\ST_STK_temp_file.csv")

    DC_STK_DATA = DC_STK
    ST_DC = DC_STK_DATA[['SEG', 'DIV', 'SUB_DIV', 'MAJCAT', 'MERCHCAT', 'MRP', 'RNG_SEG', 'SSN', 'GEN_ART', 'GEN_ART_DESC', 'COLOR', 'VAR_ART', 'SZ', 'M_VND_CD',
                         'M_VND_NM', 'CO_AGING', 'MACRO_MVGR', 'MVGR_1', 'FABRIC', 'BODY', 'DISCOUNT_STATUS']]
    ST_DC.insert(0, "ST_NO", 0)
    ST_DC['ST_NO'] = 0
    ST_TEST = ST_DC
    ST_DATA = ST_DC

    for k in range(0, 200):
        ST_TEST['ST_NO'] = ST_TEST['ST_NO'].replace(k, k+1)
        ST_DATA = pd.concat([ST_DATA, ST_TEST], ignore_index=True)

    # Replace values in the 'ST_NO' column using vectorized operations

    DATACHECK = ST_DATA[(ST_DATA['ST_NO'] == 1)].index
    ST_DATA.drop(DATACHECK, inplace=True)

    ST_DC['ST_NO'] = 1
    ST_DATA = pd.concat([ST_DATA, ST_DC], ignore_index=True)

    ST_DETAILS_R = ST_DETAILS[['ST_CD', 'PRIORITY_LIST']]
    ST_DETAILS_R['ST_NO'] = ST_DETAILS_R['PRIORITY_LIST']
    ST_DETAILS_R = ST_DETAILS[['ST_CD', 'ST_NO']]

    ST_DATA = pd.merge(ST_DATA, ST_DETAILS_R, how="left", on=["ST_NO"])

    ST_DATA['ST_CD'] = ST_DATA['ST_CD'].fillna("xxx")
    DATACHECK = ST_DATA[(ST_DATA['ST_CD'] == 'xxx')].index
    ST_DATA.drop(DATACHECK, inplace=True)

    ST_DATA = pd.merge(ST_DATA, MERCHCAT_MBQ_TEMP, how="left", on=["ST_CD", "MERCHCAT"])
    ST_DATA = pd.merge(ST_DATA, ST_STK_DATA_TEMP, how="left", on=["ST_CD", "MERCHCAT", "GEN_ART", "COLOR"])
    ST_DATA['ST_STK'] = ST_DATA['ST_STK'].fillna(0)

    ST_DATA['REV_ST_STK'] = ST_DATA['REV_ST_STK'].fillna(0)

    temp = ST_DATA.head(10000)
    temp.to_csv(path+r"\ST_DATA_temp_file.csv")

    # comb
    DC_STK_DATA['REV_DC_STK'] = DC_STK_DATA['CURR_DC_STK']
    DC_STK_DATA['GEN_CLR'] = DC_STK_DATA['GEN_ART'].astype(str) + DC_STK_DATA['COLOR'].astype(str)
    DC_STK_DATA['MC_GEN_CLR'] = DC_STK_DATA['MERCHCAT'].astype(str) + DC_STK_DATA['GEN_ART'].astype(str) + DC_STK_DATA['COLOR'].astype(str)

    ST_DATA['GEN_CLR'] = ST_DATA['GEN_ART'].astype(str) + ST_DATA['COLOR'].astype(str)
    ST_DATA['COMB'] = ST_DATA['ST_CD'] + ST_DATA['GEN_ART'].astype(str) + ST_DATA['COLOR'].astype(str)
    ST_DATA['ART_REQ'] = np.where(ST_DATA['ST_STK'] + 0 == 0,
                                  ST_DATA['OPT_MBQ_A'],
                                  np.maximum(ST_DATA['OPT_MBQ_A'] - (ST_DATA['ST_STK'] + 0), 0))

    # Calculate 'SSN_ART_REQ' using vectorized operations
    ST_DATA['SSN_ART_REQ'] = np.where(ST_DATA['ST_STK'] + 0 == 0,
                                      ST_DATA['SSN_MBQ_A'],
                                      np.maximum(ST_DATA['SSN_MBQ_A'] - (ST_DATA['ST_STK'] + 0), 0))

    DC_STK_DATA['ALLOC'] = 0
    ST_DATA['ALLOC'] = 0

    ST_STK_MJ['MJ_ALLOC'] = 0
    ST_STK_MC['MC_ALLOC'] = 0
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

    MJ_ALGO = BGT_MAJCAT[['ST_CD', 'MAJCAT']]
    MC_ALGO = BGT_MERCHCAT[['ST_CD', 'MAJCAT', 'MERCHCAT']]
    MC_M_MVGR = MC_M_MVGR_CONT[['ST_CD', 'MERCHCAT', 'MACRO_MVGR']]
    MC_MVGR = MC_MVGR_CONT[['ST_CD', 'MERCHCAT', 'MVGR']]
    MC_BODY = MC_BODY_CONT[['ST_CD', 'MERCHCAT', 'BODY']]
    MC_FABRIC = MC_FABRIC_CONT[['ST_CD', 'MERCHCAT', 'FABRIC']]
    MC_CLR = MC_CLR_CONT[['ST_CD', 'MERCHCAT', 'CLR']]
    MC_VEND = MC_VEND_CONT[['ST_CD', 'MERCHCAT', 'VEND_CD']]

    MJ_MBQ['ALLOC'] = MJ_MBQ['ALLOC'].astype(float)

    MJ_MBQ['COMB'] = MJ_MBQ['ST_CD'] + MJ_MBQ['MAJCAT']
    MC_MBQ['COMB'] = MC_MBQ['ST_CD'] + MC_MBQ['MERCHCAT']
    ST_STK_MJ['COMB'] = ST_STK_MJ['ST_CD'] + ST_STK_MJ['MAJCAT']
    ST_STK_MC['COMB'] = ST_STK_MC['ST_CD'] + ST_STK_MC['MERCHCAT']
    MC_M_MVGR['COMB'] = MC_M_MVGR['ST_CD'] + MC_M_MVGR['MERCHCAT'] + MC_M_MVGR['MACRO_MVGR']
    MC_MVGR['COMB'] = MC_MVGR['ST_CD'] + MC_MVGR['MERCHCAT'] + MC_MVGR['MVGR']
    MC_BODY['COMB'] = MC_BODY['ST_CD'] + MC_BODY['MERCHCAT'] + MC_BODY['BODY']
    MC_FABRIC['COMB'] = MC_FABRIC['ST_CD'] + MC_FABRIC['MERCHCAT'] + MC_FABRIC['FABRIC']
    MC_CLR['COMB'] = MC_CLR['ST_CD'] + MC_CLR['MERCHCAT'] + MC_CLR['CLR']
    MC_VEND['COMB'] = MC_VEND['ST_CD'] + MC_VEND['MERCHCAT'] + str(MC_VEND['VEND_CD'])

    MJ_MBQ['MJ_MBQ'] = MJ_MBQ['MJ_MBQ_A']
    MC_MBQ['MC_MBQ'] = MC_MBQ['MC_MBQ_A']

    MJ_MBQ = pd.merge(MJ_MBQ, ST_STK_MJ, how="left", on=["ST_CD", "MAJCAT"], suffixes=("_left", "_right"))
    MC_MBQ = pd.merge(MC_MBQ, ST_STK_MC, how="left", on=["ST_CD", "MERCHCAT"], suffixes=("_left", "_right"))
    MJ_MBQ = MJ_MBQ.rename(columns={'COMB_left': 'COMB'})
    MC_MBQ = MC_MBQ.rename(columns={'COMB_left': 'COMB'})

    MJ_MBQ['MJ_REQ'] = MJ_MBQ['MJ_MBQ'] - (0.0 + MJ_MBQ['MJ_REV_STK'])
    MC_MBQ['MC_REQ'] = MC_MBQ['MC_MBQ'] - (0.0 + MC_MBQ['MC_REV_STK'])

    MJ_MBQ.to_csv(path+r"\MJ_MBQ.csv")
    MC_MBQ.to_csv(path+r"\MC_MBQ.csv")
    ST_STK_MJ.to_csv(path+r"\MJ_STK.csv")
    ST_STK_MC.to_csv(path+r"\MC_STK.csv")
    DC_STK_DATA.to_csv(path+r"\DC_STK.csv")

    A1 = ST_DATA.columns
    ST_DATA_NEW = pd.DataFrame(columns=A1)
    A2 = DC_STK_DATA.columns
    DC_DATA_NEW = pd.DataFrame(columns=A2)
    A3 = MJ_MBQ.columns
    MJ_MBQ_NEW = pd.DataFrame(columns=A3)
    A4 = MC_MBQ.columns
    MC_MBQ_NEW = pd.DataFrame(columns=A4)

    DC_STK_DATA['ALLOC'] = 0
    ST_DATA['ALLOC'] = 0
    MJ_MBQ['ALLOC'] = 0
    MC_MBQ['ALLOC'] = 0
    MC_M_MVGR['ALLOC'] = 0
    MC_M_MVGR['REQ'] = 0
    MC_MVGR['ALLOC'] = 0
    MC_MVGR['REQ'] = 0
    MC_BODY['ALLOC'] = 0
    MC_FABRIC['ALLOC'] = 0
    MC_FABRIC['REQ'] = 0
    MC_CLR['ALLOC'] = 0
    MC_CLR['REQ'] = 0
    MC_VEND['ALLOC'] = 0
    MC_VEND['REQ'] = 0
    # Calculating MBQ and St-STK for all MC tables
    ############################################## MC_M_MVGR ####################################################
    MC_M_MVGR_CONT['MC_M_MVGR_CONT_1'] = MC_M_MVGR_CONT['MC_M_MVGR_CONT_1'].str.rstrip('%').astype(float) / 100
    merged_df = pd.merge(MC_M_MVGR_CONT, MC_MBQ, on=['ST_CD', 'MERCHCAT'], how="left", suffixes=("_left", "_right")).fillna(0)
    merged_df['MBQ'] = merged_df['MC_M_MVGR_CONT_1'] * merged_df['MC_MBQ'].fillna(0)
    grouped = merged_df.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR'], as_index=False).agg({
        'MBQ': 'sum'
    }).fillna(0)
    MC_M_MVGR = MC_M_MVGR.merge(grouped, on=['ST_CD', 'MERCHCAT', 'MACRO_MVGR'], how="left", suffixes=("_left", "_right")).fillna(0)
    grouped = ST_DATA.groupby(['ST_CD', 'MERCHCAT', 'MACRO_MVGR'], as_index=False).agg({
        'ST_STK': 'sum'
    }).fillna(0)

    MC_M_MVGR = MC_M_MVGR.merge(grouped, on=['ST_CD', 'MERCHCAT', 'MACRO_MVGR'], how="left", suffixes=("_left", "_right")).fillna(0)
    ############################################# MC_MVGR #########################################################
    MC_MVGR_CONT['MC_MVGR_CONT_1'] = MC_MVGR_CONT['MC_MVGR_CONT_1'].str.rstrip('%').astype(float) / 100
    merged_df_1 = pd.merge(MC_MVGR_CONT, MC_MBQ, on=['ST_CD', 'MERCHCAT'], how="left", suffixes=("_left", "_right")).fillna(0)
    merged_df_1['MBQ'] = merged_df_1['MC_MVGR_CONT_1'] * merged_df_1['MC_MBQ'].fillna(0)
    grouped = merged_df_1.groupby(['ST_CD', 'MERCHCAT', 'MVGR'], as_index=False).agg({'MBQ': 'sum'}).fillna(0)
    MC_MVGR = MC_MVGR.merge(grouped, on=['ST_CD', 'MERCHCAT', 'MVGR'], how="left", suffixes=("_left", "_right")).fillna(0)
    grouped = ST_DATA.groupby(['ST_CD', 'MERCHCAT', 'MVGR_1'], as_index=False).agg({'ST_STK': 'sum'}).fillna(0)

    MC_MVGR = MC_MVGR.merge(grouped, left_on=['ST_CD', 'MERCHCAT', 'MVGR'], right_on=['ST_CD', 'MERCHCAT', 'MVGR_1'], how="left", suffixes=("_left", "_right")).fillna(0)

    ############################################### MC_FABRIC #####################################################
    MC_FABRIC_CONT['MC_FABRIC_CONT_1'] = MC_FABRIC_CONT['MC_FABRIC_CONT_1'].str.rstrip('%').astype(float) / 100
    merged_df_3 = pd.merge(MC_FABRIC_CONT, MC_MBQ, on=['ST_CD', 'MERCHCAT'], how="left", suffixes=("_left", "_right"))
    merged_df_3['MBQ'] = merged_df_3['MC_FABRIC_CONT_1'] * merged_df_3['MC_MBQ']

    grouped = merged_df_3.groupby(['ST_CD', 'MERCHCAT', 'FABRIC'], as_index=False).agg({'MBQ': 'sum'}).fillna(0)
    MC_FABRIC = MC_FABRIC.merge(grouped, on=['ST_CD', 'MERCHCAT', 'FABRIC'], how="left", suffixes=("_left", "_right")).fillna(0)
    grouped = ST_DATA.groupby(['ST_CD', 'MERCHCAT', 'FABRIC'], as_index=False).agg({'ST_STK': 'sum'}).fillna(0)

    MC_FABRIC = MC_FABRIC.merge(grouped, on=['ST_CD', 'MERCHCAT', 'FABRIC'], how="left", suffixes=("_left", "_right")).fillna(0)
    ############################################### MC_CLR #####################################################
    MC_CLR_CONT['MC_CLR_CONT_1'] = MC_CLR_CONT['MC_CLR_CONT_1'].str.rstrip('%').astype(float) / 100
    merged_df_4 = pd.merge(MC_CLR_CONT, MC_MBQ, on=['ST_CD', 'MERCHCAT'], how="left", suffixes=("_left", "_right"))
    merged_df_4['MBQ'] = merged_df_4['MC_CLR_CONT_1'] * merged_df_4['MC_MBQ']

    grouped = merged_df_4.groupby(['ST_CD', 'MERCHCAT', 'CLR'], as_index=False).agg({'MBQ': 'sum'}).fillna(0)
    MC_CLR = MC_CLR.merge(grouped, on=['ST_CD', 'MERCHCAT', 'CLR'], how="left", suffixes=("_left", "_right")).fillna(0)
    grouped = ST_DATA.groupby(['ST_CD', 'MERCHCAT', 'COLOR'], as_index=False).agg({'ST_STK': 'sum'}).fillna(0)

    MC_CLR = MC_CLR.merge(grouped, left_on=['ST_CD', 'MERCHCAT', 'CLR'], right_on=['ST_CD', 'MERCHCAT', 'COLOR'], how="left", suffixes=("_left", "_right")).fillna(0)

    ############################################### MC_VEND #####################################################
    MC_VEND_CONT['MC_VEND_CONT_1'] = MC_VEND_CONT['MC_VEND_CONT_1'].str.rstrip('%').astype(float) / 100
    merged_df_5 = pd.merge(MC_VEND_CONT, MC_MBQ, on=['ST_CD', 'MERCHCAT'], how="left", suffixes=("_left", "_right"))
    merged_df_5['MBQ'] = merged_df_5['MC_VEND_CONT_1'] * merged_df_5['MC_MBQ']

    grouped = merged_df_5.groupby(['ST_CD', 'MERCHCAT', 'VEND_CD'], as_index=False).agg({'MBQ': 'sum'}).fillna(0)
    MC_VEND = MC_VEND.merge(grouped, on=['ST_CD', 'MERCHCAT', 'VEND_CD'], how="left", suffixes=("_left", "_right")).fillna(0)
    grouped = ST_DATA.groupby(['ST_CD', 'MERCHCAT', 'M_VND_CD'], as_index=False).agg({'ST_STK': 'sum'}).fillna(0)

    MC_VEND = MC_VEND.merge(grouped, left_on=['ST_CD', 'MERCHCAT', 'VEND_CD'], right_on=['ST_CD', 'MERCHCAT', 'M_VND_CD'], how="left", suffixes=("_left", "_right")).fillna(0)

    # MBQ and St-STK calculation ended
    mc_m_mvgr_polar = pl.DataFrame(MC_M_MVGR)
    mc_mvgr_polar = pl.DataFrame(MC_MVGR)
    mc_fabric_polar = pl.DataFrame(MC_FABRIC)
    mc_clr_polar = pl.DataFrame(MC_CLR)
    mc_vend_polar = pl.DataFrame(MC_VEND)

    ST_DATA_TEMP = ST_DATA
    DC_STK_TEMP = DC_STK_DATA.loc[(DC_STK_DATA['REV_DC_STK'] > 0)].reset_index(drop=True)

    st_polar = pl.DataFrame(ST_DATA_TEMP)
    dc_polar = pl.DataFrame(DC_STK_TEMP)
    mc_polar = pl.DataFrame(MC_MBQ)
    mj_polar = pl.DataFrame(MJ_MBQ)
    mc_m_mvgr_polar = pl.DataFrame(MC_M_MVGR)

    i = CAT
    cnt = 0
    gen_clr_to_merchcat = DC_STK_TEMP.set_index('GEN_CLR')['MERCHCAT'].to_dict()

    unique_macro_mvgr = MC_M_MVGR['MACRO_MVGR'].unique()
    unique_mvgr = MC_MVGR['MVGR'].unique()
    unique_body = MC_BODY['BODY'].unique()
    unique_fabric = MC_FABRIC['FABRIC'].unique()
    unique_clr = MC_CLR['CLR'].unique()
    unique_vend = MC_VEND['VEND_CD'].unique()

    unique_macro_mvgr_list = unique_macro_mvgr.tolist()
    unique_mvgr_list = unique_mvgr.tolist()
    unique_fabric_list = unique_fabric.tolist()
    unique_clr_list = unique_clr.tolist()
    unique_vend_list = unique_vend.tolist()

    st_polar_comb_cache = st_polar['COMB'].to_list()
    dc_polar_gen_clr_cache = dc_polar['GEN_CLR'].to_list()
    mj_polar_comb_cache = mj_polar['COMB'].to_list()
    mc_polar_comb_cache = mc_polar['COMB'].to_list()

    concatenated_combinations = [(str(j) + str(k), (j, k)) for j in ST_DETAILS['ST_CD'] for k in DC_STK_TEMP['GEN_CLR']]

    starttime = time.time()

    process_combination_partial = partial(
        process_combination,
        gen_clr_to_merchcat=gen_clr_to_merchcat,
        unique_macro_mvgr=unique_macro_mvgr,
        unique_mvgr=unique_mvgr,
        unique_fabric=unique_fabric,
        unique_clr=unique_clr,
        unique_vend=unique_vend,
        i=i,
        st_polar=st_polar,
        dc_polar=dc_polar,
        mj_polar=mj_polar,
        mc_polar=mc_polar
    )

    with mp.Pool(mp.cpu_count()) as pool:
        results = pool.starmap(process_combination_partial, concatenated_combinations)

        st_polar_updates = []
        dc_polar_updates = []
        mj_polar_updates = []
        mc_polar_updates = []
        mc_m_mvgr_polar_updates = []
        mc_mvgr_polar_updates = []
        mc_fabric_polar_updates = []
        mc_clr_polar_updates = []

        for result in results:
            if result:
                st_polar_updates.extend(result['st_polar_updates'])
                dc_polar_updates.extend(result['dc_polar_updates'])
                mj_polar_updates.extend(result['mj_polar_updates'])
                mc_polar_updates.extend(result['mc_polar_updates'])
                mc_m_mvgr_polar_updates.extend(result['mc_m_mvgr_polar_updates'])
                mc_mvgr_polar_updates.extend(result['mc_mvgr_polar_updates'])
                mc_fabric_polar_updates.extend(result['mc_fabric_polar_updates'])
                mc_clr_polar_updates.extend(result['mc_clr_polar_updates'])

        st_polar = update_dataframe(st_polar, st_polar_updates)
        dc_polar = update_dataframe(dc_polar, dc_polar_updates)
        mj_polar = update_dataframe(mj_polar, mj_polar_updates)
        mc_polar = update_dataframe(mc_polar, mc_polar_updates)
        mc_m_mvgr_polar = update_dataframe(mc_m_mvgr_polar, mc_m_mvgr_polar_updates)
        mc_mvgr_polar = update_dataframe(mc_mvgr_polar, mc_mvgr_polar_updates)
        mc_fabric_polar = update_dataframe(mc_fabric_polar, mc_fabric_polar_updates)
        mc_clr_polar = update_dataframe(mc_clr_polar, mc_clr_polar_updates)

    print(mc_m_mvgr_polar.filter(
        (mc_m_mvgr_polar['ALLOC'] > 0) |
        (mc_m_mvgr_polar['REQ'] > 0) |
        (mc_m_mvgr_polar['MBQ'] > 0) |
        (mc_m_mvgr_polar['ST_STK'] > 0)
    ))
    print(mc_mvgr_polar.filter(
        (mc_mvgr_polar['ALLOC'] > 0) |
        (mc_mvgr_polar['REQ'] > 0) |
        (mc_mvgr_polar['MBQ'] > 0) |
        (mc_mvgr_polar['ST_STK'] > 0)
    ))
    print(mc_fabric_polar.filter(
        (mc_fabric_polar['ALLOC'] > 0) |
        (mc_fabric_polar['REQ'] > 0) |
        (mc_fabric_polar['MBQ'] > 0) |
        (mc_fabric_polar['ST_STK'] > 0)
    ))
    print(mc_clr_polar.filter(
        (mc_clr_polar['ALLOC'] > 0) |
        (mc_clr_polar['REQ'] > 0) |
        (mc_clr_polar['MBQ'] > 0) |
        (mc_clr_polar['ST_STK'] > 0)
    ))
    print(mc_vend_polar.filter(
        (mc_vend_polar['ALLOC'] > 0) |
        (mc_vend_polar['REQ'] > 0) |
        (mc_vend_polar['MBQ'] > 0) |
        (mc_vend_polar['ST_STK'] > 0)
    ))

    print(mc_polar[['MC_MBQ', 'MC_REV_STK', 'ALLOC']])

    path += r'\Polar'
    if not os.path.exists(path):
        os.makedirs(path)

    st_polar.write_csv(file=path+'ST_POLAR.csv', include_header=True, separator=',', line_terminator='\n')
    dc_polar.write_csv(file=path+'DC_POLAR.csv', include_header=True, separator=',', line_terminator='\n')
    mc_polar.write_csv(file=path+'MC_POLAR.csv', include_header=True, separator=',', line_terminator='\n')
    mj_polar.write_csv(file=path+'MJ_POLAR.csv', include_header=True, separator=',', line_terminator='\n')
    mc_m_mvgr_polar.write_csv(file=path+'MC_M_MVGR_POLAR.csv', include_header=True, separator=',', line_terminator='\n')
    mc_mvgr_polar.write_csv(file=path+'MC_MVGR.csv', include_header=True, separator=',', line_terminator='\n')
    mc_clr_polar.write_csv(file=path+'MC_CLR.csv', include_header=True, separator=',', line_terminator='\n')
    mc_fabric_polar.write_csv(file=path+'MC_FABRIC.csv', include_header=True, separator=',', line_terminator='\n')
    mc_vend_polar.write_csv(file=path+'MC_VEND.csv', include_header=True, separator=',', line_terminator='\n')

    endtime = time.time()
    print('ended 1st loop', (endtime - starttime))
    print('ended total loop', (endtime - starttime1))
