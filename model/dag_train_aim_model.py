# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import pendulum
from datetime import datetime as dt
import psycopg2 # 필요시 alchemy 사용
import psycopg2.extras as extras
import numpy as np
import pandas as pd
import os
import json
import glob
import pickle
import string
import shutil
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")


MLFLOW_HOST = os.getenv("MLFLOW_HOST")
MLFLOW_PORT = os.getenv("MLFLOW_PORT")
MLFLOW_TRACKING_URI= f"http://{MLFLOW_HOST}:{MLFLOW_PORT}"
MLFLOW_EXP_NAME = os.getenv("MLFLOW_EXP_NAME")
MLFLOW_MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME")

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")

# 기본 args 생성
default_args = {
    #'owner' : 'Hello World',
    #'email' : ['airflow@airflow.com'],
    'email_on_failure': False,
}

db_connect = psycopg2.connect(
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT
)

# params
# outlier_value = None

#sql query
query_drop_train_table = """
    DROP TABLE IF EXISTS TRAIN_AIM_DATA
;"""

query_drop_median_table = """
    DROP TABLE IF EXISTS TRAIN_MEDIAN_DATA
;"""

query_create_train_table = """
    CREATE TABLE IF NOT EXISTS TRAIN_AIM_DATA (
        WONBU_NO VARCHAR(10),
        YOYANG_ILSU_CUM INT,
        JINRYO_COUNT_CUM INT,
        AGE_GROUP VARCHAR(10),
        GENDER VARCHAR(10),
        SANGBYEONG_JONGRYU_CD VARCHAR(10),
        JAEHAEBALSAENG_HYEONGTAE_CD VARCHAR(10),
        JIKEOPBYEONG_CD VARCHAR(10),
        JIKEOPBYEONG_JUNGBUNRYU_CD VARCHAR(10),
        GYOTONG_SAGO_YN VARCHAR(10),
        HANBANG_YN VARCHAR(10),
        HOSPITAL_FG VARCHAR(10),
        FIRST_HOSPITAL_JONGRYU_CD VARCHAR(10),
        FIRST_HOSPITAL_FG VARCHAR(10),
        SANGBYEONG_CD_CNT INT,
        SANGBYEONG_CD VARCHAR,
        SANGHAE_BUWI_CD VARCHAR,
        SANGSE_SANGBYEONG_NM VARCHAR,
        JAEHAE_GYEONGWI VARCHAR,
        JUYO_GEOMSA_INFO VARCHAR,
        TRANSFER_COUNT_CUM INT,
        HOSPITAL_JONGRYU_CD_1 FLOAT,
        HOSPITAL_JONGRYU_CD_2 FLOAT,
        HOSPITAL_JONGRYU_CD_3 FLOAT,
        HOSPITAL_JONGRYU_CD_4 FLOAT,
        HOSPITAL_JONGRYU_CD_5 FLOAT,
        HOSPITAL_JONGRYU_CD_6 FLOAT,
        HOSPITAL_JONGRYU_CD_7 FLOAT,
        HOSPITAL_JONGRYU_CD_8 FLOAT,
        HOSPITAL_JONGRYU_CD_9 FLOAT,
        HOSPITAL_JONGRYU_CD_10 FLOAT,
        HOSPITAL_JONGRYU_CD_11 FLOAT,
        HOSPITAL_JONGRYU_CD_99 FLOAT,
        IPWON_PERCENT_CUM FLOAT,
        SANGBYEONG_CD_DAE_A INT,
        SANGBYEONG_CD_DAE_B INT,
        SANGBYEONG_CD_DAE_C INT,
        SANGBYEONG_CD_DAE_D INT,
        SANGBYEONG_CD_DAE_E INT,
        SANGBYEONG_CD_DAE_F INT,
        SANGBYEONG_CD_DAE_G INT,
        SANGBYEONG_CD_DAE_H INT,
        SANGBYEONG_CD_DAE_I INT,
        SANGBYEONG_CD_DAE_J INT,
        SANGBYEONG_CD_DAE_K INT,
        SANGBYEONG_CD_DAE_L INT,
        SANGBYEONG_CD_DAE_M INT,
        SANGBYEONG_CD_DAE_N INT,
        SANGBYEONG_CD_DAE_O INT,
        SANGBYEONG_CD_DAE_P INT,
        SANGBYEONG_CD_DAE_Q INT,
        SANGBYEONG_CD_DAE_R INT,
        SANGBYEONG_CD_DAE_S INT,
        SANGBYEONG_CD_DAE_T INT,
        SANGBYEONG_CD_DAE_U INT,
        SANGBYEONG_CD_DAE_V INT,
        SANGBYEONG_CD_DAE_W INT,
        SANGBYEONG_CD_DAE_X INT,
        SANGBYEONG_CD_DAE_Y INT,
        SANGBYEONG_CD_DAE_Z INT,
        MEDIAN FLOAT,
        FIRST_INPUTJA_ID VARCHAR,
        LAST_CHANGEJA_ID VARCHAR,
        FIRST_INPUT_ILSI DATE,
        LAST_CHANGE_ILSI DATE,
        FIRST_INPUTJA_IP VARCHAR,
        LAST_CHANGEJA_IP VARCHAR,
        PRIMARY KEY(WONBU_NO, JINRYO_COUNT_CUM)
);"""

query_create_median_table = """
    CREATE TABLE IF NOT EXISTS TRAIN_MEDIAN_DATA (
        SANGBYEONG_CD VARCHAR,
        AGE_GROUP VARCHAR(10),
        GENDER VARCHAR(10),
        MEDIAN FLOAT,
        FIRST_INPUTJA_ID VARCHAR,
        LAST_CHANGEJA_ID VARCHAR,
        FIRST_INPUT_ILSI DATE,
        LAST_CHANGE_ILSI DATE,
        FIRST_INPUTJA_IP VARCHAR,
        LAST_CHANGEJA_IP VARCHAR
);"""


def print_text(text):
    print(text)


def insert_data(db_connect):
    '''
    #CSV 버전
    AAA010MT_SAMPLE = pd.read_csv('/usr/app/smp_data/AAA010MT_SAMPLE.csv', dtype={'SAMPLE_ID':object}, sep='\t')
    AAA020MT_SAMPLE = pd.read_csv('/usr/app/smp_data/AAA020MT_SAMPLE.csv', dtype={'SAMPLE_ID':object}, sep='\t')
    AAA050DT_SAMPLE = pd.read_csv('/usr/app/smp_data/AAA050DT_SAMPLE.csv', dtype={'SAMPLE_ID':object}, sep='\t')
    AAA260MT_SAMPLE = pd.read_csv('/usr/app/smp_data/AAA260MT_SAMPLE.csv', dtype={'SAMPLE_ID':object}, sep='\t')
    AAA240MT_SAMPLE = pd.read_csv('/usr/app/smp_data/AAA240MT_SAMPLE.csv', dtype={'SAMPLE_ID':object}, sep='\t')
    ADA320MT_SAMPLE = pd.read_csv('/usr/app/smp_data/ADA320MT_SAMPLE.csv', dtype={'SAMPLE_ID':object}, sep='\t')
    AXA010MT_SAMPLE = pd.read_csv('/usr/app/smp_data/AXA010MT_SAMPLE.csv', dtype={'SAMPLE_ID':object}, sep='\t')

    AAA010MT_SAMPLE['SAMPLE_ID'] = 'sep_' + AAA010MT_SAMPLE.SAMPLE_ID.str.zfill(6)
    AAA020MT_SAMPLE['SAMPLE_ID'] = 'sep_' + AAA020MT_SAMPLE.SAMPLE_ID.str.zfill(6)
    AAA050DT_SAMPLE['SAMPLE_ID'] = 'sep_' + AAA050DT_SAMPLE.SAMPLE_ID.str.zfill(6)
    AAA260MT_SAMPLE['SAMPLE_ID'] = 'sep_' + AAA260MT_SAMPLE.SAMPLE_ID.str.zfill(6)
    AAA240MT_SAMPLE['SAMPLE_ID'] = 'sep_' + AAA240MT_SAMPLE.SAMPLE_ID.str.zfill(6)
    ADA320MT_SAMPLE['SAMPLE_ID'] = 'sep_' + ADA320MT_SAMPLE.SAMPLE_ID.str.zfill(6)
    #AXA010MT_SAMPLE['SAMPLE_ID'] = 'sep_' + AXA010MT_SAMPLE.SAMPLE_ID.str.zfill(6)
    
    AAA010MT_SAMPLE.rename(columns={'SAMPLE_ID':'WONBU_NO'},inplace=True)
    AAA020MT_SAMPLE.rename(columns={'SAMPLE_ID':'WONBU_NO'},inplace=True)
    AAA050DT_SAMPLE.rename(columns={'SAMPLE_ID':'WONBU_NO'},inplace=True)
    AAA260MT_SAMPLE.rename(columns={'SAMPLE_ID':'WONBU_NO'},inplace=True)
    AAA240MT_SAMPLE.rename(columns={'SAMPLE_ID':'WONBU_NO'},inplace=True)
    ADA320MT_SAMPLE.rename(columns={'SAMPLE_ID':'WONBU_NO'},inplace=True)
    #AXA010MT_SAMPLE.rename(columns={'SAMPLE_ID':'WONBU_NO'},inplace=True)
    '''

    db_connect_raw_input_0 = psycopg2.connect(
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
        )

    #SQL 버전
    with db_connect_raw_input_0.cursor() as cur:
        AAA010MT_SAMPLE = pd.read_sql_query('SELECT * FROM AAA010MT_SAMPLE', db_connect_raw_input_0)
        AAA020MT_SAMPLE = pd.read_sql_query('SELECT * FROM AAA020MT_SAMPLE', db_connect_raw_input_0)
        AAA050DT_SAMPLE = pd.read_sql_query('SELECT * FROM AAA050DT_SAMPLE', db_connect_raw_input_0)
        AAA260MT_SAMPLE = pd.read_sql_query('SELECT * FROM AAA260MT_SAMPLE', db_connect_raw_input_0)
        AAA240MT_SAMPLE = pd.read_sql_query('SELECT * FROM AAA240MT_SAMPLE', db_connect_raw_input_0)
        ADA320MT_SAMPLE = pd.read_sql_query('SELECT * FROM ADA320MT_SAMPLE', db_connect_raw_input_0)
        AXA010MT_SAMPLE = pd.read_sql_query('SELECT * FROM AXA010MT_SAMPLE', db_connect_raw_input_0)

        AAA010MT_SAMPLE.columns = map(lambda x: str(x).upper(), AAA010MT_SAMPLE.columns)
        AAA020MT_SAMPLE.columns = map(lambda x: str(x).upper(), AAA020MT_SAMPLE.columns)
        AAA050DT_SAMPLE.columns = map(lambda x: str(x).upper(), AAA050DT_SAMPLE.columns)
        AAA260MT_SAMPLE.columns = map(lambda x: str(x).upper(), AAA260MT_SAMPLE.columns)
        AAA240MT_SAMPLE.columns = map(lambda x: str(x).upper(), AAA240MT_SAMPLE.columns)
        ADA320MT_SAMPLE.columns = map(lambda x: str(x).upper(), ADA320MT_SAMPLE.columns)
        AXA010MT_SAMPLE.columns = map(lambda x: str(x).upper(), AXA010MT_SAMPLE.columns)


    #재해정보 전처리
    AAA010MT_SAMPLE['JAEHAE_DT'] = AAA010MT_SAMPLE.JAEHAE_DT.astype('str').astype('datetime64[ns]')
    AAA010MT_SAMPLE[['SANGBYEONG_JONGRYU_CD','JAEHAEBALSAENG_HYEONGTAE_CD','JIKEOPBYEONG_CD','JIKEOPBYEONG_JUNGBUNRYU_CD']] = AAA010MT_SAMPLE[['SANGBYEONG_JONGRYU_CD','JAEHAEBALSAENG_HYEONGTAE_CD','JIKEOPBYEONG_CD','JIKEOPBYEONG_JUNGBUNRYU_CD']].fillna(9999).astype('int').astype('str')

    #급여원부 전처리  
    AAA260MT_SAMPLE['JAEHAE_DT'] = AAA260MT_SAMPLE.JAEHAE_DT.astype('str').astype('datetime64[ns]')
    AAA260MT_SAMPLE.loc[AAA260MT_SAMPLE.JAEHAEJA_RGNO.str[6].isin(['1','3','5','7','9']),'GENDER'] = 'M'
    AAA260MT_SAMPLE.loc[AAA260MT_SAMPLE.JAEHAEJA_RGNO.str[6].isin(['0','2','4','6','8']),'GENDER'] = 'F'
    AAA260MT_SAMPLE['BIRTH_DT'] = pd.to_datetime(AAA260MT_SAMPLE.JAEHAEJA_RGNO.apply(lambda x: '20'+x[:6] if x[:2] < str(dt.today().year)[2:] else '19'+x[:6]))
    AAA260MT_SAMPLE['AGE_GROUP'] = (((AAA260MT_SAMPLE['JAEHAE_DT'] - AAA260MT_SAMPLE['BIRTH_DT']).dt.days/365).astype('int')//10*10).astype('str') +'대'
    AAA260MT_SAMPLE = AAA260MT_SAMPLE.drop('JAEHAE_DT',axis=1)

    #최초요양 전처리
    AAA020MT_SAMPLE.loc[AAA020MT_SAMPLE['GYOTONG_SAGOJA_YUHYEONG_CD'].isna(),'GYOTONG_SAGO_YN'] = '0'
    AAA020MT_SAMPLE.loc[AAA020MT_SAMPLE['GYOTONG_SAGOJA_YUHYEONG_CD'].notna(),'GYOTONG_SAGO_YN'] = '1'
    AAA020MT_SAMPLE = AAA020MT_SAMPLE.sort_values(['WONBU_NO','GYOTONG_SAGOJA_YUHYEONG_CD'],ascending=[True,False]).drop_duplicates('WONBU_NO').reset_index(drop=True)

    #정렬요양 전처리 #상병구분값 확인!!
    AAA240MT_SAMPLE = AAA240MT_SAMPLE[(AAA240MT_SAMPLE.YOYANG_FG.isin(['1','2','3','5','8','10','11','13']))&(AAA240MT_SAMPLE.YOYANG_FG.notna())].reset_index(drop=True)
    AAA240MT_SAMPLE['YOYANG_FROM_DT'] = AAA240MT_SAMPLE.YOYANG_FROM_DT.astype('str').astype('datetime64[ns]')
    AAA240MT_SAMPLE['YOYANG_TO_DT'] = AAA240MT_SAMPLE.YOYANG_TO_DT.astype('str').astype('datetime64[ns]')
    AAA240MT_SAMPLE['YOYANG_ILSU'] = (AAA240MT_SAMPLE.YOYANG_TO_DT-AAA240MT_SAMPLE.YOYANG_FROM_DT).dt.days+1
    AAA240MT_SAMPLE = AAA240MT_SAMPLE[AAA240MT_SAMPLE.YOYANG_FROM_DT<=AAA240MT_SAMPLE.YOYANG_TO_DT].reset_index(drop=True)

    # 재해정보 전처리 2 : 원부번호 제거
    AAA010MT_SAMPLE = AAA010MT_SAMPLE[AAA010MT_SAMPLE.WONBU_NO.isin(AAA240MT_SAMPLE.WONBU_NO.unique())].reset_index(drop=True)
    JAEHAE_DT_ERROR = (AAA240MT_SAMPLE.groupby('WONBU_NO').YOYANG_FROM_DT.min()-AAA010MT_SAMPLE.set_index('WONBU_NO').JAEHAE_DT).dt.days
    AAA240MT_SAMPLE = AAA240MT_SAMPLE[~AAA240MT_SAMPLE.WONBU_NO.isin(JAEHAE_DT_ERROR[JAEHAE_DT_ERROR<0].index)].reset_index(drop=True)
    AAA240MT_SAMPLE = pd.merge(AAA240MT_SAMPLE,
                            AAA240MT_SAMPLE[AAA240MT_SAMPLE.YOYANG_FG.isin(['1','11'])].groupby('WONBU_NO').YOYANG_FROM_DT.max().rename('FISRT_MAX_DT').reset_index(),
                            left_on='WONBU_NO',right_on='WONBU_NO',how='left')
    AAA240MT_SAMPLE = pd.merge(AAA240MT_SAMPLE,
                            AAA240MT_SAMPLE[(AAA240MT_SAMPLE.YOYANG_FG.isin(['2','13']))&(AAA240MT_SAMPLE.YOYANG_FROM_DT>=AAA240MT_SAMPLE.FISRT_MAX_DT)].groupby('WONBU_NO').YOYANG_FROM_DT.min().rename('RE_MIN_DT').reset_index(),
                            left_on='WONBU_NO',right_on='WONBU_NO',how='left')
    AAA240MT_SAMPLE.loc[AAA240MT_SAMPLE.YOYANG_FROM_DT>=AAA240MT_SAMPLE.RE_MIN_DT,'FIRST_YN']=0
    AAA240MT_SAMPLE['FIRST_YN'] = AAA240MT_SAMPLE.FIRST_YN.fillna(1)
    AAA240MT_SAMPLE['HOSPITAL_NO'] = AAA240MT_SAMPLE['HOSPITAL_NO'].astype('int').astype('str')

    #상병코드 전처리
    AAA050DT_SAMPLE = AAA050DT_SAMPLE[AAA050DT_SAMPLE.WONBU_NO.isin(AAA240MT_SAMPLE.WONBU_NO.unique())].reset_index(drop=True)
    AAA050DT_SAMPLE['SANGBYEONG_CD'] = AAA050DT_SAMPLE.SANGBYEONG_CD.str.replace(' ','')    
    AAA050DT_SAMPLE = AAA050DT_SAMPLE[AAA050DT_SAMPLE.SANGBYEONG_CD.str.contains(r'[A-Z][0-9]{2,5}',na=False)].reset_index(drop=True)
    AAA050DT_SAMPLE.loc[AAA050DT_SAMPLE.SANGHAE_BUWI_CD.notna(),'SANGHAE_BUWI_CD'] = AAA050DT_SAMPLE[AAA050DT_SAMPLE.SANGHAE_BUWI_CD.notna()].SANGHAE_BUWI_CD.astype('int').astype('str')

    #의료기관 전처리
    AXA010MT_SAMPLE['HOSPITAL_FG'] = AXA010MT_SAMPLE.HOSPITAL_NO.astype('str').apply(lambda x: '2' if x.startswith('199') else '1')
    AXA010MT_SAMPLE['HOSPITAL_NO'] = AXA010MT_SAMPLE['HOSPITAL_NO'].astype('int').astype('str')

    #최초요양일수 산정
    AAA240MT_SAMPLE_FIRST = AAA240MT_SAMPLE[AAA240MT_SAMPLE.FIRST_YN==1]
    YOYANG_BE_ALL = (AAA240MT_SAMPLE_FIRST.groupby('WONBU_NO').YOYANG_FROM_DT.min()-AAA010MT_SAMPLE.set_index('WONBU_NO').JAEHAE_DT).dt.days
    EX_11 = AAA240MT_SAMPLE_FIRST.groupby('WONBU_NO').apply(lambda x: 0 if "11" in x.YOYANG_FG.unique() else YOYANG_BE_ALL[x.name])
    EX_11_df = pd.merge(EX_11[EX_11!=0].rename('YOYANG_ILSU'),AAA010MT_SAMPLE.set_index('WONBU_NO')['JAEHAE_DT'].rename('YOYANG_FROM_DT'),how='left',left_index=True,right_index=True).reset_index()
    EX_11_df['BEF_YOYANG'] = 1
    YOYANG_ILSU_FIRST = pd.concat([AAA240MT_SAMPLE_FIRST[['WONBU_NO','YOYANG_FROM_DT','HOSPITAL_NO','JINRYO_FG','YOYANG_ILSU']],EX_11_df])
    YOYANG_ILSU_FIRST['BEF_YOYANG'] = YOYANG_ILSU_FIRST['BEF_YOYANG'].fillna(0)
    YOYANG_ILSU_FIRST = YOYANG_ILSU_FIRST.sort_values(['WONBU_NO','YOYANG_FROM_DT'])
    YOYANG_ILSU_FIRST['JINRYO_COUNT_CUM'] = YOYANG_ILSU_FIRST.groupby('WONBU_NO').YOYANG_FROM_DT.cumcount() + 1
    YOYANG_ILSU_FIRST.loc[YOYANG_ILSU_FIRST.JINRYO_FG.notna(),'JINRYO_FG'] = YOYANG_ILSU_FIRST.loc[YOYANG_ILSU_FIRST.JINRYO_FG.notna(),'JINRYO_FG'].astype('int').astype('str')

    #통합 데이터셋
    DF = pd.merge(YOYANG_ILSU_FIRST,AAA260MT_SAMPLE,left_on='WONBU_NO',right_on='WONBU_NO',how='left')
    DF = pd.merge(DF,AAA010MT_SAMPLE,how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = DF[~DF.JIKEOPBYEONG_CD.isin(['6','14'])].reset_index(drop=True)
    DF = pd.merge(DF,AAA020MT_SAMPLE[['WONBU_NO','GYOTONG_SAGO_YN']],how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF,AXA010MT_SAMPLE.loc[AXA010MT_SAMPLE.HOSPITAL_JONGRYU_CD=="7",['HOSPITAL_NO','HOSPITAL_JONGRYU_CD']].rename(columns={'HOSPITAL_JONGRYU_CD':'HANBANG_YN'}),left_on='HOSPITAL_NO',right_on='HOSPITAL_NO',how='left')
    DF.loc[DF.HANBANG_YN.notna(),'HANBANG_YN'] = '1'
    DF.loc[DF.HANBANG_YN.isna(),'HANBANG_YN'] = '0'
    DF = pd.merge(DF,AXA010MT_SAMPLE[['HOSPITAL_NO','HOSPITAL_JONGRYU_CD','HOSPITAL_FG']],left_on='HOSPITAL_NO',right_on='HOSPITAL_NO',how='left')
    DF = pd.merge(DF,DF[DF.HOSPITAL_NO.notna()].groupby('WONBU_NO')[['WONBU_NO','HOSPITAL_JONGRYU_CD','HOSPITAL_FG']].head(1).rename(columns={'HOSPITAL_JONGRYU_CD':'FIRST_HOSPITAL_JONGRYU_CD','HOSPITAL_FG':'FIRST_HOSPITAL_FG'})
                    ,how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF,AAA050DT_SAMPLE.groupby('WONBU_NO').SANGBYEONG_CD.nunique().rename('SANGBYEONG_CD_CNT'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF,AAA050DT_SAMPLE.groupby('WONBU_NO').SANGBYEONG_CD.unique().apply(lambda x: ", ".join(sorted(x))),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = DF[DF.SANGBYEONG_CD.notna()]
    DF = pd.merge(DF,AAA050DT_SAMPLE[AAA050DT_SAMPLE.SANGHAE_BUWI_CD.notna()].groupby('WONBU_NO').SANGHAE_BUWI_CD.unique().apply(lambda x: ", ".join(sorted(x))),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF,AAA050DT_SAMPLE.sort_values(['WONBU_NO','SANGBYEONG_FG','SANGBYEONG_CNT','SER']).groupby('WONBU_NO').SANGSE_SANGBYEONG_NM.unique().str.join(', '),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF,ADA320MT_SAMPLE.sort_values(['WONBU_NO','JUCHIUI_SOGYEON_CNT']).groupby('WONBU_NO').JAEHAE_GYEONGWI.unique().str.join(', '),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF,ADA320MT_SAMPLE.sort_values(['WONBU_NO','JUCHIUI_SOGYEON_CNT']).groupby('WONBU_NO').JUYO_GEOMSA_INFO.unique().str.join(', '),how='left',left_on='WONBU_NO',right_on='WONBU_NO')

    DF['HOSPITAL_NO_BEF'] = DF.groupby('WONBU_NO').HOSPITAL_NO.shift()
    DF.loc[DF.JINRYO_COUNT_CUM==1,'HOSPITAL_NO_BEF'] = DF.loc[DF.JINRYO_COUNT_CUM==1,'HOSPITAL_NO']
    DF.loc[DF['HOSPITAL_NO_BEF']!= DF.HOSPITAL_NO,'TRANSFER_COUNT_CUM'] = 1
    DF.loc[DF['HOSPITAL_NO_BEF']== DF.HOSPITAL_NO,'TRANSFER_COUNT_CUM'] = 0
    DF.loc[(DF['HOSPITAL_NO_BEF'].isna())& (DF.HOSPITAL_NO.isna()),'TRANSFER_COUNT_CUM'] = 0
    DF['TRANSFER_COUNT_CUM'] = DF.groupby('WONBU_NO').TRANSFER_COUNT_CUM.cumsum()

    temp_df = pd.pivot_table(DF,'YOYANG_ILSU',index=['WONBU_NO','JINRYO_COUNT_CUM'],columns=['HOSPITAL_JONGRYU_CD'],aggfunc='sum',fill_value=0).reset_index()
    HOSPITAL_JONGRYU_CD_LIST = [str(i) for i in range(1,12)] + ['99']
    HOSPITAL_JONGRYU_CD_LIST = [i for i in HOSPITAL_JONGRYU_CD_LIST if i not in temp_df.filter(regex='[0-9]').columns.tolist()]
    temp_df.loc[:,HOSPITAL_JONGRYU_CD_LIST] = 0
    temp_df.columns = ['WONBU_NO','JINRYO_COUNT_CUM'] + ['HOSPITAL_JONGRYU_CD_' + str(int(i)) for i in temp_df.filter(regex='[0-9]').columns]
    temp_df.loc[:,temp_df.filter(like='HOSPITAL_JONGRYU_CD').columns] = temp_df.groupby('WONBU_NO').cumsum().loc[:,temp_df.filter(like='HOSPITAL_JONGRYU_CD').columns]
    DF = pd.merge(DF,temp_df,left_on=['WONBU_NO','JINRYO_COUNT_CUM'],right_on=['WONBU_NO','JINRYO_COUNT_CUM'],how='left')
    DF.loc[:,DF.filter(like='HOSPITAL_JONGRYU_CD_').columns] = DF.groupby('WONBU_NO').ffill().filter(like='HOSPITAL_JONGRYU_CD_')

    DF.loc[DF.JINRYO_FG=='1','IPWON_PERCENT_CUM'] = DF.loc[DF.JINRYO_FG=='1','YOYANG_ILSU']
    DF.loc[DF.JINRYO_FG!='1','IPWON_PERCENT_CUM'] = 0
    DF.loc[:,'IPWON_PERCENT_CUM'] = DF.groupby('WONBU_NO').IPWON_PERCENT_CUM.cumsum() / DF.groupby('WONBU_NO').YOYANG_ILSU.cumsum()

    DF['YOYANG_ILSU'] = DF.groupby('WONBU_NO').YOYANG_ILSU.transform("sum")
    DF.rename(columns={'YOYANG_ILSU':'YOYANG_ILSU_CUM'},inplace=True)

    DF = pd.concat([DF,DF.SANGBYEONG_CD.str.split(', ').apply(lambda x: ", ".join([i[0] for i in x])).str.get_dummies(', ')],axis=1)


    SANGBYEONG_CD_DAE_LIST = list(string.ascii_uppercase)
    SANGBYEONG_CD_DAE_LIST = [i for i in SANGBYEONG_CD_DAE_LIST if i not in [i for i in DF.columns if len(i)==1]]
    DF.loc[:,SANGBYEONG_CD_DAE_LIST] = 0
    DF.columns = ['SANGBYEONG_CD_DAE_' + i if len(i)==1 else i for i in DF.columns]

    DF.drop(['YOYANG_FROM_DT','HOSPITAL_NO','HOSPITAL_JONGRYU_CD','JINRYO_FG','BEF_YOYANG','JAEHAE_DT','HOSPITAL_NO_BEF'],axis=1,inplace=True)

    DF = DF.fillna(np.nan).replace([np.nan], [None])

    # 이상치 제거 및 중위값 변수 생성
    DESC = DF.groupby('WONBU_NO').tail(1).YOYANG_ILSU_CUM.astype(int).describe()
    #global outlier_value
    outlier_value = DESC['75%']+1.5*(DESC['75%']-DESC['25%'])
    DF = DF[~DF.WONBU_NO.isin(DF[DF['YOYANG_ILSU_CUM']>outlier_value].WONBU_NO)]
    median_df = DF.groupby('WONBU_NO').tail(1).groupby(['SANGBYEONG_CD','AGE_GROUP','GENDER']).YOYANG_ILSU_CUM.median().rename('MEDIAN').reset_index()

    DF = pd.merge(DF,median_df,left_on=['SANGBYEONG_CD','AGE_GROUP','GENDER'],right_on=['SANGBYEONG_CD','AGE_GROUP','GENDER'],how='left')

    #타입변경/확인
    var_col = ['AGE_GROUP', 'GENDER', 'HOSPITAL_FG', 'HANBANG_YN', 'SANGBYEONG_JONGRYU_CD','JAEHAEBALSAENG_HYEONGTAE_CD', 'JIKEOPBYEONG_CD',
        'JIKEOPBYEONG_JUNGBUNRYU_CD', 'GYOTONG_SAGO_YN','FIRST_HOSPITAL_JONGRYU_CD', 'FIRST_HOSPITAL_FG','SANGBYEONG_CD',
        'SANGHAE_BUWI_CD','SANGSE_SANGBYEONG_NM','JAEHAE_GYEONGWI','JUYO_GEOMSA_INFO']
    int_col = ['JINRYO_COUNT_CUM','YOYANG_ILSU_CUM','SANGBYEONG_CD_CNT','TRANSFER_COUNT_CUM',
        'SANGBYEONG_CD_DAE_A', 'SANGBYEONG_CD_DAE_B', 'SANGBYEONG_CD_DAE_C', 'SANGBYEONG_CD_DAE_D', 'SANGBYEONG_CD_DAE_E', 
        'SANGBYEONG_CD_DAE_F','SANGBYEONG_CD_DAE_G', 'SANGBYEONG_CD_DAE_H', 'SANGBYEONG_CD_DAE_I','SANGBYEONG_CD_DAE_J', 
        'SANGBYEONG_CD_DAE_K', 'SANGBYEONG_CD_DAE_L','SANGBYEONG_CD_DAE_M', 'SANGBYEONG_CD_DAE_N', 'SANGBYEONG_CD_DAE_O',
        'SANGBYEONG_CD_DAE_P', 'SANGBYEONG_CD_DAE_Q', 'SANGBYEONG_CD_DAE_R','SANGBYEONG_CD_DAE_S', 'SANGBYEONG_CD_DAE_T', 
        'SANGBYEONG_CD_DAE_U','SANGBYEONG_CD_DAE_V', 'SANGBYEONG_CD_DAE_W', 'SANGBYEONG_CD_DAE_X','SANGBYEONG_CD_DAE_Y', 
        'SANGBYEONG_CD_DAE_Z']
    float_col = ['HOSPITAL_JONGRYU_CD_1', 'HOSPITAL_JONGRYU_CD_2', 'HOSPITAL_JONGRYU_CD_3', 'HOSPITAL_JONGRYU_CD_4','HOSPITAL_JONGRYU_CD_5', 
            'HOSPITAL_JONGRYU_CD_6', 'HOSPITAL_JONGRYU_CD_7','HOSPITAL_JONGRYU_CD_8','HOSPITAL_JONGRYU_CD_9', 'HOSPITAL_JONGRYU_CD_10',
            'HOSPITAL_JONGRYU_CD_11', 'HOSPITAL_JONGRYU_CD_99','IPWON_PERCENT_CUM','MEDIAN']

    for i in DF.columns:
        if i in var_col:
            if DF[i].dtype == 'object':
                DF.loc[DF[i].notna(),i] = DF.loc[DF[i].notna(),i].astype('str')
            else:
                DF.loc[DF[i].notna(),i] = DF.loc[DF[i].notna(),i].astype('int').astype('str')
        elif i in int_col:
            DF.loc[DF[i].notna(),i] = DF.loc[DF[i].notna(),i].astype('int')
        elif i in float_col:
            DF.loc[DF[i].notna(),i] = DF.loc[DF[i].notna(),i].astype('float')

    DF = DF[['WONBU_NO']+var_col+int_col+float_col]


    def _exec_query(conn, query):
        print(query)
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

    def _execute_values(conn, df, table_name): 
        import socket

        df['FIRST_INPUTJA_ID'] = 'SYSTEM'
        df['LAST_CHANGEJA_ID'] = 'SYSTEM'
        df['FIRST_INPUT_ILSI'] = dt.today()
        df['LAST_CHANGE_ILSI'] = dt.today()
        df['FIRST_INPUTJA_IP'] = socket.gethostbyname(socket.gethostname())
        df['LAST_CHANGEJA_IP'] = socket.gethostbyname(socket.gethostname())

        tuples = [tuple(x) for x in df.to_numpy()] 
        cols = ','.join(list(df.columns)) 
        query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols) 
        extras.execute_values(cur, query, tuples) 
        conn.commit()

    with db_connect.cursor() as cur:
        #지우기
        _exec_query(conn=db_connect, query=query_drop_train_table)
        _exec_query(conn=db_connect, query=query_drop_median_table)
        #만들기
        _exec_query(conn=db_connect, query=query_create_train_table)
        _exec_query(conn=db_connect, query=query_create_median_table)
        #넣기        
        _execute_values(conn=db_connect, df=DF, table_name='TRAIN_AIM_DATA')
        _execute_values(conn=db_connect, df=median_df, table_name='TRAIN_MEDIAN_DATA')


def train_model(db_connect, exp_name, model_name):
    import mlflow
    from sklearn.metrics import mean_squared_error, r2_score
    from sklearn.preprocessing import MinMaxScaler
    from sklearn.model_selection import train_test_split
    from mlflow.tracking import MlflowClient
    from mlflow.models import infer_signature
    from autogluon.tabular import TabularPredictor

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    #client = MlflowClient()
    #experiment_id = client.create_experiment("test_aim")
    #client.set_experiment_tag(experiment_id, "test", "gb")
    mlflow.set_experiment(exp_name)

    class AutogluonModel(mlflow.pyfunc.PythonModel):
        def fit(self, save_path, train_first ,hyperparameters):
            self.predictor = TabularPredictor(label='YOYANG_ILSU_CUM',path=save_path)
            self.predictor.fit(train_first, 
                               hyperparameters=hyperparameters, #hyperparameters='light', #hyperparameters=hyperparameters,
                               num_gpus=0, #
                               time_limit=10800, #
                               infer_limit=1, #
                               presets=['optimize_for_deployment']) #presets=['good_quality','optimize_for_deployment'] #save_space={'remove_data':True}
            
        def load_context(self, context):
            self.predictor = TabularPredictor.load(context.artifacts.get("predictor_path"))

        def predict(self, context, model_input):
            return self.predictor.predict(model_input)
        
    def eval_metrics(scaler, actual, pred):
        rmse_real = mean_squared_error(scaler.inverse_transform(pd.DataFrame(actual)),
                                       scaler.inverse_transform(pd.DataFrame(pred)),squared=False)
        rmse = mean_squared_error(actual, pred,squared=False)
        r2 = r2_score(scaler.inverse_transform(pd.DataFrame(actual)), scaler.inverse_transform(pd.DataFrame(pred)))
        
        metrics = {"rmse": rmse, "rmse_real": rmse_real, "r2": r2}

        return metrics

    with db_connect.cursor() as cur:
        DF = pd.read_sql_query("SELECT * FROM TRAIN_AIM_DATA", db_connect) #DF = pd.read_sql_query("SELECT * FROM TRAIN_AIM_DATA LIMIT 1000", db_connect)
        DF.columns = map(lambda x: str(x).upper(), DF.columns)
        DF = DF.drop(['FIRST_INPUTJA_ID', 'LAST_CHANGEJA_ID', 'FIRST_INPUT_ILSI', 'LAST_CHANGE_ILSI', 'FIRST_INPUTJA_IP', 'LAST_CHANGEJA_IP'], axis=1)

    # Min-Max Scale
    scaler = MinMaxScaler()
    DF['YOYANG_ILSU_CUM'] = scaler.fit_transform(DF[['YOYANG_ILSU_CUM']])

    train, test = train_test_split(DF, train_size=0.7, random_state=1234)
    input_sample = train.iloc[:10]
    
    with mlflow.start_run() as run:
        exp=dict(mlflow.get_experiment_by_name(exp_name))
        exp_id=exp['experiment_id']
        print("current_exp_name : ", exp_name)
        print("current_exp_id : ", exp_id)

        #save_path = f'./artifacts/1/{run.info.run_id}/artifacts/{model_name}/artifacts/'
        save_path = f"./data/aim/model/{exp_id}/{run.info.run_id}/"
        print("save_path : ", save_path)

        artifacts = {"predictor_path": save_path}

        #hyperparameters 원본
        '''
        #original hyperparameters
        hyperparameters = {'GBM': [{},{'extra_trees': True, 'ag_args': {'name_suffix': 'XT'}},
                           'GBMLarge'],
                           'CAT': {},
                           'XGB': {}}
        '''
        '''
        #new hyperparameters 
        hyperparameters = {'GBM': [{},{'extra_trees': True, 'ag_args': {'name_suffix': 'XT'}}],
                           'XGB': {}}
        '''
        #new hyperparameters 
        hyperparameters = {'GBM': [{},{'extra_trees': True, 'ag_args': {'name_suffix': 'XT'}}]}                           


        model = AutogluonModel()
        model.fit(save_path, train.drop('WONBU_NO', axis=1), hyperparameters)
        y_pred = model.predict(save_path, test.drop('WONBU_NO', axis=1)) #model="XGBoost", model="WeightedEnsemble_L2", model="LightGBM", model="LightGBMXT"
        
        signature = infer_signature(test.drop('WONBU_NO', axis=1), y_pred) #signature = infer_signature(train, predictions)

        pickle.dump(scaler,open(save_path+'minmaxscaler.pkl','wb'))

        metrics = eval_metrics(scaler, test.YOYANG_ILSU_CUM, y_pred)
        mlflow.log_metrics(metrics)

        train_date = dt.today().strftime("%Y-%m-%d")
        custom_params = {"train_date":train_date}
        with open(save_path+'custom_params.json', 'w', encoding='utf-8') as file:
            json.dump(custom_params, file)


        mlflow.pyfunc.log_model(artifact_path=model_name, 
                                python_model=model,
                                artifacts=artifacts,
                                signature=signature,
                                input_example=input_sample,
                                registered_model_name="autogluonModel"
                           )
        #pickle.dump(scaler,open(save_path+'minmaxscaler.pkl','wb')) #mlflow.log_params({"scaler": scaler}) #데이터를 줄여서 테스트해볼것
        #지우기
        shutil.rmtree(f"./data/aim/model/{exp_id}/{run.info.run_id}/")


################################################################################################
with DAG(
    dag_id='train_aim_model',
    default_args=default_args,
    start_date=dt(2023, 6, 21, tzinfo=kst),
    description='train aim model',
    schedule_interval=None, #매월1일: '0 0 1 * *', 매일(자정): '0 0 * * *'
    catchup=False, #과거 작업 실행 X
    tags=['aim_train']
) as dag:
    
    t1 = PythonOperator(
        task_id='start_job',
        python_callable=print_text,
        op_kwargs={"text":"start train_aim_model"}
    )

    t2 = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
        op_kwargs={"db_connect":db_connect}
    )

    t3 = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        op_kwargs={"db_connect":db_connect,"exp_name":MLFLOW_EXP_NAME,"model_name":MLFLOW_MODEL_NAME} #"exp_name":"test_aim","model_name":"aim_model"}
    )

    t4 = BashOperator(
        task_id='restart_api',
        bash_command="sudo docker restart mlops_v06_api_1"
    )

    #오래된 모델은 삭제 (현재:365일이 지난 모델을 삭제) 
    t5 = BashOperator(
        task_id='delete_models',
        bash_command="sudo docker exec mlops_v06_mlflow_1 mc rm --recursive --force --older-than ${DELETE_MODEL_OLDER_THAN} ${MC_HOST}/${MINIO_BUCKET_NAME}"
        
    )

    t6 = PythonOperator(
        task_id='end_job',
        python_callable=print_text,
        op_kwargs={"text":"complete train_aim_model"}
    )

    #t1 >> t3 >> t4 >> t5 >> t6
    t1 >> t2 >> t3 >> t4 >> t5 >> t6

    
