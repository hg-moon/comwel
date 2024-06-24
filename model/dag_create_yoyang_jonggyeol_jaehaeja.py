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
import glob
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
'''
#postgres-server 연결
POSTGRES_HOST="postgres-server"
POSTGRES_USER="myuser"
POSTGRES_PASSWORD="mypassword"
POSTGRES_DB="mydatabase"
POSTGRES_PORT="5432"
'''
'''
FILE_PATH_YOYANG_JONGGYEOL_JAEHAEJA = os.getenv("FILE_PATH_YOYANG_JONGGYEOL_JAEHAEJA")
'''
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


query_drop_table = """
    DROP TABLE IF EXISTS YOYANG_JONGGYEOL_JAEHAEJA
;"""

query_create_table = """
    CREATE TABLE IF NOT EXISTS YOYANG_JONGGYEOL_JAEHAEJA (
        WONBU_NO VARCHAR(10) PRIMARY KEY,
        AGE_GROUP VARCHAR(10),
        GENDER VARCHAR(10),
        JAEHAE_DT VARCHAR(10),
        SANGBYEONG_CD_ALL VARCHAR,
        SANGBYEONG_CD_SO_ALL VARCHAR,
        SANGBYEONG_CD_SE_ALL VARCHAR,
        SANGBYEONG_CD_MAIN VARCHAR(10),
        SANGBYEONG_CD_MAIN_SANGSE VARCHAR,
        YOYANG_ILSU_FIRST INT,
        YOYANG_ILSU_FIRST_HOSP INT,
        YOYANG_ILSU_RE INT,
        YOYANG_ILSU_RE_HOSP INT,
        SAYU_CD VARCHAR(10),
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

    db_connect_raw_input_0 = psycopg2.connect(
        database="postgres",
        user="wesleyquest",
        password="Wqasdf01!",
        host="211.218.17.10",
        port="5432")

    with db_connect_raw_input_0.cursor() as cur:
        AAA010MT_SAMPLE = pd.read_sql_query(f"SELECT WONBU_NO, JAEHAE_DT FROM AAA010MT_SAMPLE",db_connect_raw_input_0)
        AAA050DT_SAMPLE = pd.read_sql_query(f"SELECT * FROM AAA050DT_SAMPLE",db_connect_raw_input_0)
        AAA240MT_SAMPLE = pd.read_sql_query(f"SELECT * FROM AAA240MT_SAMPLE",db_connect_raw_input_0)
        AAA260MT_SAMPLE = pd.read_sql_query(f"SELECT WONBU_NO, JAEHAEJA_RGNO, JAEHAE_DT FROM AAA260MT_SAMPLE",db_connect_raw_input_0)
        #AAA0300MT_SAMPLE = pd.read_sql_table(f"SELECT * FROM AAA030MT_SAMPLE WHERE WONBU_NO='{wonbu_no}'",db_connect_raw_input_0)
        AAA010MT_SAMPLE.columns = map(lambda x: str(x).upper(), AAA010MT_SAMPLE.columns)
        AAA050DT_SAMPLE.columns = map(lambda x: str(x).upper(), AAA050DT_SAMPLE.columns)
        AAA240MT_SAMPLE.columns = map(lambda x: str(x).upper(), AAA240MT_SAMPLE.columns)
        AAA260MT_SAMPLE.columns = map(lambda x: str(x).upper(), AAA260MT_SAMPLE.columns)


    #상병코드 전처리
    AAA050DT_SAMPLE = AAA050DT_SAMPLE.reset_index(drop=True)
    AAA050DT_SAMPLE['SANGBYEONG_CD'] = AAA050DT_SAMPLE.SANGBYEONG_CD.str.replace(' ','')    
    AAA050DT_SAMPLE = AAA050DT_SAMPLE[AAA050DT_SAMPLE.SANGBYEONG_CD.str.contains(r'[A-Z][0-9]{2,5}',na=False)].reset_index(drop=True)  

    #급여원부 전처리
    AAA260MT_SAMPLE['JAEHAE_DT'] = AAA260MT_SAMPLE.JAEHAE_DT.astype('str').astype('datetime64[ns]')
    AAA260MT_SAMPLE.loc[AAA260MT_SAMPLE.JAEHAEJA_RGNO.str[6].isin(['1','3','5','7','9']),'GENDER'] = 'M'
    AAA260MT_SAMPLE.loc[AAA260MT_SAMPLE.JAEHAEJA_RGNO.str[6].isin(['0','2','4','6','8']),'GENDER'] = 'F'
    AAA260MT_SAMPLE['BIRTH_DT'] = pd.to_datetime(AAA260MT_SAMPLE.JAEHAEJA_RGNO.apply(lambda x: '20'+x[:6] if x[:2] < str(dt.today().year)[2:] else '19'+x[:6]))
    AAA260MT_SAMPLE['AGE_GROUP'] = (((AAA260MT_SAMPLE['JAEHAE_DT'] - AAA260MT_SAMPLE['BIRTH_DT']).dt.days/365).astype('int')//10*10).astype('str') +'대'
    AAA260MT_SAMPLE = AAA260MT_SAMPLE.drop('JAEHAE_DT',axis=1)

    #통합 #상병구분값 확인!!
    DF_1 = AAA260MT_SAMPLE.reset_index(drop=True).set_index('WONBU_NO')
    DF_1 = pd.merge(DF_1,AAA010MT_SAMPLE[['WONBU_NO','JAEHAE_DT']].set_index('WONBU_NO'),how='left',left_index=True,right_index=True)
    DF_1 = pd.merge(DF_1,AAA050DT_SAMPLE.groupby('WONBU_NO').SANGBYEONG_CD.unique().apply(lambda x: ", ".join(sorted(x))).rename('SANGBYEONG_CD_ALL'),how='left',left_index=True,right_index=True)
    DF_1 = DF_1[DF_1.SANGBYEONG_CD_ALL.notna()]
    DF_1['SANGBYEONG_CD_SO_ALL'] = DF_1.SANGBYEONG_CD_ALL.str.split(', ').apply(lambda x: ", ".join(sorted(list(set([i[:3] for i in x]))))) ### 상병코드 셋째자리까지만 활용할 경우
    DF_1['SANGBYEONG_CD_SE_ALL'] = DF_1.SANGBYEONG_CD_ALL.str.split(', ').apply(lambda x: ", ".join(sorted(list(set([i[:4] for i in x]))))) ### 주상병코드 넷째자리까지만 활용할 경우

    DF_1 = pd.merge(DF_1,AAA050DT_SAMPLE[AAA050DT_SAMPLE.SANGBYEONG_FG=="1"].sort_values(['WONBU_NO','SANGBYEONG_CNT','SER']).groupby('WONBU_NO').head(1).set_index('WONBU_NO').SANGBYEONG_CD.rename('SANGBYEONG_CD_MAIN'),how='left',left_index=True,right_index=True)
    DF_1 = pd.merge(DF_1,AAA050DT_SAMPLE[AAA050DT_SAMPLE.SANGBYEONG_FG=="1"].sort_values(['WONBU_NO','SANGBYEONG_CNT','SER']).groupby('WONBU_NO').head(1).set_index('WONBU_NO').SANGSE_SANGBYEONG_NM.rename('SANGBYEONG_CD_MAIN_SANGSE'),how='left',left_index=True,right_index=True)

    #재요양 사유
    DF_1['SAYU_CD'] = None

    DF_1 = DF_1[['AGE_GROUP','GENDER','JAEHAE_DT','SANGBYEONG_CD_ALL','SANGBYEONG_CD_SO_ALL','SANGBYEONG_CD_SE_ALL','SANGBYEONG_CD_MAIN','SANGBYEONG_CD_MAIN_SANGSE', 'SAYU_CD']]
    DF_1 = DF_1.fillna(np.nan).replace([np.nan], [None])
    DF_1 = DF_1.reset_index()
    

    #재해정보 전처리
    AAA010MT_SAMPLE['JAEHAE_DT'] = AAA010MT_SAMPLE.JAEHAE_DT.astype('str').astype('datetime64[ns]')    

    #정렬요양 전처리 #요양구분값 확인!!
    AAA240MT_SAMPLE = AAA240MT_SAMPLE[(AAA240MT_SAMPLE.YOYANG_FG.isin(["1","2","3","5","8","10","11","13"]))&(AAA240MT_SAMPLE.YOYANG_FG.notna())].reset_index(drop=True)

    AAA240MT_SAMPLE['YOYANG_FROM_DT'] = AAA240MT_SAMPLE.YOYANG_FROM_DT.astype('str').astype('datetime64[ns]')
    AAA240MT_SAMPLE['YOYANG_TO_DT'] = AAA240MT_SAMPLE.YOYANG_TO_DT.astype('str').astype('datetime64[ns]')
    AAA240MT_SAMPLE['YOYANG_ILSU'] = (AAA240MT_SAMPLE.YOYANG_TO_DT-AAA240MT_SAMPLE.YOYANG_FROM_DT).dt.days+1
    AAA240MT_SAMPLE = AAA240MT_SAMPLE[AAA240MT_SAMPLE.YOYANG_FROM_DT<=AAA240MT_SAMPLE.YOYANG_TO_DT].reset_index(drop=True)

    # 재해정보 전처리 2 : 원부번호 제거
    AAA010MT_SAMPLE = AAA010MT_SAMPLE[AAA010MT_SAMPLE.WONBU_NO.isin(AAA240MT_SAMPLE.WONBU_NO.unique())].reset_index(drop=True)
    JAEHAE_DT_ERROR = (AAA240MT_SAMPLE.groupby('WONBU_NO').YOYANG_FROM_DT.min()-AAA010MT_SAMPLE.set_index('WONBU_NO').JAEHAE_DT).dt.days
    AAA240MT_SAMPLE = AAA240MT_SAMPLE[~AAA240MT_SAMPLE.WONBU_NO.isin(JAEHAE_DT_ERROR[JAEHAE_DT_ERROR<0].index)].reset_index(drop=True)
    AAA240MT_SAMPLE = pd.merge(AAA240MT_SAMPLE,
                            AAA240MT_SAMPLE[AAA240MT_SAMPLE.YOYANG_FG.isin(["1","11"])].groupby('WONBU_NO').YOYANG_FROM_DT.max().rename('FISRT_MAX_DT').reset_index(),
                            left_on='WONBU_NO',right_on='WONBU_NO',how='left')
    AAA240MT_SAMPLE = pd.merge(AAA240MT_SAMPLE,
                            AAA240MT_SAMPLE[(AAA240MT_SAMPLE.YOYANG_FG.isin(["2","13"]))&(AAA240MT_SAMPLE.YOYANG_FROM_DT>=AAA240MT_SAMPLE.FISRT_MAX_DT)].groupby('WONBU_NO').YOYANG_FROM_DT.min().rename('RE_MIN_DT').reset_index(),
                            left_on='WONBU_NO',right_on='WONBU_NO',how='left')
    AAA240MT_SAMPLE.loc[AAA240MT_SAMPLE.YOYANG_FROM_DT>=AAA240MT_SAMPLE.RE_MIN_DT,'FIRST_YN']=0
    AAA240MT_SAMPLE['FIRST_YN'] = AAA240MT_SAMPLE.FIRST_YN.fillna(1)    

    #최초요양일수 산정
    AAA240MT_SAMPLE_FIRST = AAA240MT_SAMPLE[AAA240MT_SAMPLE.FIRST_YN==1]
    YOYANG_BE_ALL = (AAA240MT_SAMPLE_FIRST.groupby('WONBU_NO').YOYANG_FROM_DT.min()-AAA010MT_SAMPLE.set_index('WONBU_NO').JAEHAE_DT).dt.days
    EX_11 = AAA240MT_SAMPLE_FIRST.groupby('WONBU_NO').apply(lambda x: 0 if "11" in x.YOYANG_FG.unique() else YOYANG_BE_ALL[x.name])
    EX_11_df = pd.merge(EX_11[EX_11!=0].rename('YOYANG_ILSU'),AAA010MT_SAMPLE.set_index('WONBU_NO')['JAEHAE_DT'].rename('YOYANG_FROM_DT'),how='left',left_index=True,right_index=True).reset_index()
    EX_11_df['BEF_YOYANG'] = 1
    YOYANG_ILSU_FIRST = pd.concat([AAA240MT_SAMPLE_FIRST[['WONBU_NO','YOYANG_FROM_DT','YOYANG_ILSU']],EX_11_df])
    YOYANG_ILSU_FIRST['BEF_YOYANG'] = YOYANG_ILSU_FIRST['BEF_YOYANG'].fillna(0)
    YOYANG_ILSU_FIRST = YOYANG_ILSU_FIRST.sort_values(['WONBU_NO','YOYANG_FROM_DT'])

    #재요양일수 산정
    AAA240MT_SAMPLE_RE = AAA240MT_SAMPLE[AAA240MT_SAMPLE.FIRST_YN==0]
    YOYANG_ILSU_RE = AAA240MT_SAMPLE_RE[['WONBU_NO','YOYANG_FROM_DT','YOYANG_ILSU']]
    YOYANG_ILSU_RE = YOYANG_ILSU_RE.sort_values(['WONBU_NO','YOYANG_FROM_DT'])

    #통합
    DF_2 = pd.merge(YOYANG_ILSU_FIRST.groupby('WONBU_NO').YOYANG_ILSU.sum().rename('YOYANG_ILSU_FIRST'),AAA240MT_SAMPLE_FIRST[AAA240MT_SAMPLE_FIRST.JINRYO_FG=="1"].groupby('WONBU_NO').YOYANG_ILSU.sum().rename('YOYANG_ILSU_FIRST_HOSP'),
            how='left',left_index=True,right_index=True)
    DF_2[['YOYANG_ILSU_FIRST','YOYANG_ILSU_FIRST_HOSP']] = DF_2[['YOYANG_ILSU_FIRST','YOYANG_ILSU_FIRST_HOSP']].fillna(0)
    DF_2 = pd.merge(DF_2,YOYANG_ILSU_RE.groupby('WONBU_NO').YOYANG_ILSU.sum().rename('YOYANG_ILSU_RE'),
            how='left',left_index=True,right_index=True)
    DF_2 = pd.merge(DF_2,AAA240MT_SAMPLE_RE[AAA240MT_SAMPLE_RE.JINRYO_FG=="1"].groupby('WONBU_NO').YOYANG_ILSU.sum().rename('YOYANG_ILSU_RE_HOSP'),
            how='left',left_index=True,right_index=True)
    DF_2[['YOYANG_ILSU_RE','YOYANG_ILSU_RE_HOSP']] = DF_2[['YOYANG_ILSU_RE','YOYANG_ILSU_RE_HOSP']].fillna(0)

    DF_2 = DF_2[['YOYANG_ILSU_FIRST','YOYANG_ILSU_FIRST_HOSP','YOYANG_ILSU_RE','YOYANG_ILSU_RE_HOSP']]
    DF_2 = DF_2.fillna(np.nan).replace([np.nan], [None])
    DF_2 = DF_2.reset_index()

    DF = pd.merge(DF_1, DF_2, on='WONBU_NO', how='inner')

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
        _exec_query(conn=db_connect, query=query_drop_table)
        #넣기
        _exec_query(conn=db_connect, query=query_create_table)
        _execute_values(conn=db_connect, df=DF, table_name='YOYANG_JONGGYEOL_JAEHAEJA')


def save_data_to_csv(drivername, username, password, host, database):
    url = URL.create(
        drivername = drivername,
        username=username,
        password=password,
        host=host,
        database=database
    )

    engine = create_engine(url)
    db_connect = engine.connect()

    query_select_table = """
        SELECT * FROM YOYANG_JONGGYEOL_JAEHAEJA; 
    ;"""

    DF = pd.read_sql(query_select_table, db_connect)
    DF.columns = map(lambda x: str(x).upper(), DF.columns)

    current_dt = pendulum.now("Asia/Seoul").strftime("%Y%m%d_%H%M") #dt.now().strftime("%Y%m%d_%H%M")


    #지우기
    [os.remove(f) for f in glob.glob(f"{FILE_PATH_YOYANG_JONGGYEOL_JAEHAEJA}/{os.path.split(FILE_PATH_YOYANG_JONGGYEOL_JAEHAEJA)[-1]}_*.csv")]
    #넣기
    DF.to_csv(f'{FILE_PATH_YOYANG_JONGGYEOL_JAEHAEJA}/{os.path.split(FILE_PATH_YOYANG_JONGGYEOL_JAEHAEJA)[-1]}_{current_dt}.csv', index=False, encoding='utf-8-sig')
    db_connect.close()


################################################################################################
with DAG(
    dag_id='create_yoyang_jonggyeol_jaehaeja',
    default_args=default_args,
    start_date=dt(2023, 6, 21, tzinfo=kst),
    description='create yoyang jonggyeol jaehaeja',
    schedule_interval=None, #매월1일: '0 0 1 * *', 매일(자정): '0 0 * * *', @once, #매분기: '0 2 1 1,4,7,10 *'
    catchup=False, #과거 작업 실행 X
    tags=['aim_create']
) as dag:

    t1 = PythonOperator(
        task_id='start_job',
        python_callable=print_text,
        op_kwargs={"text":"start create_yoyang_jonggyeol_jaehaeja"}
    )

    t2 = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
        op_kwargs={"db_connect":db_connect}
    )
    t3 = BashOperator(
        task_id='restart_api',
        bash_command="sudo docker restart mlops_v06_api_1"
        
    )

    t4 = PythonOperator(
        task_id='end_job',
        python_callable=print_text,
        op_kwargs={"text":"complete create_yoyang_jonggyeol_jaehaeja"}
    )

    t1 >> t2 >> t3 >> t4
