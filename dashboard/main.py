import streamlit as st
from st_utils.korean_pages import titles

import os
from dotenv import load_dotenv
from st_utils.load_data_from_api import load_similar_users_data_date, load_model_train_date

#page config
st.set_page_config(
    page_title="개요", page_icon="👋", 
    layout="wide",
    )
#st.title("Home")
titles()

load_dotenv()
NGINX_HOST = os.getenv("NGINX_HOST")
NGINX_PORT = os.getenv("NGINX_PORT")
API_HOST = os.getenv("API_HOST")

SIMILAR_USER_DATA_DATE = load_similar_users_data_date(host = NGINX_HOST, port = NGINX_PORT, location=API_HOST)
TRAIN_DATE = load_model_train_date(host = NGINX_HOST, port = NGINX_PORT, location=API_HOST)
#st.write("# Welcome to Streamlit! 👋")
st.markdown(
    f"""
    #### 요양기간 분석 페이지
    - 사이드바 화면 : 조회된 재해자의 기본적인 재해 정보, 승인 요양기간 정보, 신청 요양기간 정보 제공
    - 메인 화면 : 재해자와 유사한 종결된 재해자들의 요양기간 분석 정보 제공
    
    ##### 메인 화면
    - 메인 화면은 재해자 정보, 요양기간 분석, 재요양기간 분석으로 구성
    - 각 분석은 전체 요양기간(좌측) 분포와 입원 요양기간(우측) 분포로 구성

    #### 재해자 정보
    - 재해 정보 : 해당 재해자의 원부번호, 재해일자, 연령대, 성별, 주상병명, 재요양 사유 정보 제공
    - 승인 요양 정보 : 해당 재해자의 과거 승인된 요양기간, 요양기간(입원), 재요양기간, 재요양기간(입원) 정보 제공
    - 신청 요양 정보 : 해당 재해자가 현재 신청한 요양기간, 요양기간(입원), 재요양기간, 재요양기간(입원) 정보 제공

    ##### 분포 (차트)
    - 각 분포는 요약, 상세, 통계 탭으로 구성
    - 요약 : 요양기간에 따른 재해자의 단순 분포를 보여주며, 중앙값과 승인(과거 승인된 요양기간), 승인+신청(과거 승인된 요양기간과 현재 신청한 요양기간 합산) 제공
    - 상세 : 요양기간에 따른 재해자의 히스토그램을 보여주며, 추가로 전체 요양기간에 대해서는 AI예측 결과 제공
    - 통계 : 해당 분포의 기본적인 통계값(재해자수, 평균, 표준편차, 최소값, 1분위수, 중앙값, 3분위수, 최대값) 제공   

    #### 요양 기간 산정 방식
    - 요양기간은 재해일자부터 최초 요양시작일까지 기간 + 승인된 요양기간을 모두 합산 (입원은 입원인 경우만 합산)
    - 재요양기간은 승인된 재요양기간을 모두 합산 (입원은 입원인 경우만 합산)

    #### 유사 재해자 데이터
    - 유사 재해자 데이터는 최근 5년 종결자를 대상으로 매 분기 업데이트 :red[(데이터 업데이트 날짜: {SIMILAR_USER_DATA_DATE})]
    - 종결자 판단은 유사 재해자 선정 시점(매분기)에 요양 신청이 1년 동안 들어오지 않은 경우, 종결자로 판단  
    - 유사 재해자 데이터는 난청, 진폐 직업병을 가진 재해자는 미포함 

    #### 유사 재해자 선정 조건
    - 1) 재해자와 전체 상병코드, 연령대, 성별이 동일한 유사 재해자를 우선 파악
    - 2) 1번 조건에서 10명 미만 시, 재해자와 전체 상병코드(세분류), 연령대, 성별이 동일한 유사 재해자를 파악 
    - 3) 2번 조건에서 10명 미만 시, 재해자와 전체 상병코드(소분류), 연령대, 성별이 동일한 유사 재해자를 파악 
    - 4) 3번 조건에서 10명 미만 시, 재해자와 주상병코드, 연령대, 성별이 동일한 유사 재해자를 파악

    #### AI 예측
    - 1) AI 예측 모델은 매 분기 최근 5년 종결자를 대상으로 재학습된 모델이 생성되며, 해당 연도의 성능이 높은 모델을 활용 :red[(모델 업데이트 날짜: {TRAIN_DATE})]
    - 1) AI 예측 모델에 활용되는 데이터는 상병코드, 연령대, 성별, 병원종류코드, 지정병원여부 등 66개 변수 활용

"""
)













