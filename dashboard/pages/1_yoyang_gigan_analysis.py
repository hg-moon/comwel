import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
from plot.yoyang_gigan import fig_yoyang_gigan_by_wonbu_no_plus_pred_sincheong_gigan
from plot.yoyang_gigan import fig_yoyang_gigan_by_wonbu_no_sangse_plus_pred_sincheong_gigan

from st_utils.load_data_from_api import load_profile_main, load_profile_yoyang, load_similar_users_yoyang_ilsu_first, load_similar_users_yoyang_ilsu_re, load_yoyang_ilsu_pred
from st_utils.korean_pages import titles

load_dotenv()
NGINX_HOST = os.getenv("NGINX_HOST")
NGINX_PORT = os.getenv("NGINX_PORT")
API_HOST = os.getenv("API_HOST")

#page config
st.set_page_config(
    page_title="요양기간 분석", page_icon="📈", 
    layout="wide",
    )

titles()

st.markdown("""
        <style>
               .main > .block-container {
                    padding-top: 0rem;
                    padding-bottom: 0rem;
                    padding-left: 5rem;
                    padding-right: 5rem;
                }
        </style>
        """, unsafe_allow_html=True)

st.markdown("""
<style>
.custom-font-1 {
    font-size:16px;
    font-weight: bold;
    text-align: center;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
footer {visibility: hidden;}
#header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


try:
    #데이터 준비
    ##쿼리 파라미터
    query_params = st.experimental_get_query_params()
    wonbu_no = query_params['wonbu_no'][0]
    sincheong_yoyang_gigan = query_params['sincheong_yoyang_gigan'][0]
    sincheong_jaeyoyang_gigan = query_params['sincheong_jaeyoyang_gigan'][0]
    sincheong_yoyang_gigan_ipwon = query_params['sincheong_yoyang_gigan_ipwon'][0]
    sincheong_jaeyoyang_gigan_ipwon = query_params['sincheong_jaeyoyang_gigan_ipwon'][0]
    percentile = query_params['percentile'][0]

    ##로드 원천 데이터
    try:
        wonbu_no_data_main = load_profile_main(host = NGINX_HOST, port = NGINX_PORT, location=API_HOST, wonbu_no = wonbu_no)
        wonbu_no_data_yoyang = load_profile_yoyang(host = NGINX_HOST, port = NGINX_PORT, location=API_HOST, wonbu_no = wonbu_no)
        wonbu_no_data = pd.merge(wonbu_no_data_main, wonbu_no_data_yoyang, how='inner', on='WONBU_NO')

        ##유사재해자 그룹 데이터 
        try:
            SIMILAR_USER_GROUP_FIRST, SIMILAR_USER_GROUP_INFO_FIRST, YOYANG_ILSU_DESC_FIRST, YOYANG_ILSU_HOSP_DESC_FIRST, SIMILAR_USERS_FIRST = load_similar_users_yoyang_ilsu_first(host = NGINX_HOST, port = NGINX_PORT, location=API_HOST, wonbu_no = wonbu_no)
            SIMILAR_USER_GROUP_RE, SIMILAR_USER_GROUP_INFO_RE, YOYANG_ILSU_DESC_RE, YOYANG_ILSU_HOSP_DESC_RE, SIMILAR_USERS_RE = load_similar_users_yoyang_ilsu_re(host = NGINX_HOST, port = NGINX_PORT, location=API_HOST, wonbu_no = wonbu_no)

            desc_data = YOYANG_ILSU_DESC_FIRST
            desc_data_hosp = YOYANG_ILSU_HOSP_DESC_FIRST   
            desc_data_re = YOYANG_ILSU_DESC_RE
            desc_data_re_hosp = YOYANG_ILSU_HOSP_DESC_RE

            ##데이터 오브젝트
            wonbu_no_jaehae_dt = str(wonbu_no_data['JAEHAE_DT'].item())[:10]
            wonbu_no_age_group = str(wonbu_no_data['AGE_GROUP'].item())
            wonbu_no_gender = str(wonbu_no_data['GENDER'].item())
            wonbu_no_sangbyeong_cd = str(wonbu_no_data['SANGBYEONG_CD_ALL'].item())
            wonbu_no_sangbyeong_cd_main = str(wonbu_no_data['SANGBYEONG_CD_MAIN'].item())
            wonbu_no_sangbyeong_cd_main_sangse_sangbyeong_nm = str(wonbu_no_data['SANGBYEONG_CD_MAIN_SANGSE'].reset_index(drop=True)[0])
            wonbu_no_sayu_cd = "-" if str(wonbu_no_data['SAYU_CD'].item())=="None" else str(wonbu_no_data['SAYU_CD'].item())
            wonbu_no_sayu_nm = "-" if str(wonbu_no_data['SAYU_NM'].item())=="None" else str(wonbu_no_data['SAYU_NM'].item())

            wonbu_no_data_yoyang_ilsu = int(wonbu_no_data['YOYANG_ILSU_FIRST'].item()) #원부번호 요양기간
            wonbu_no_data_yoyang_ilsu_hosp = int(wonbu_no_data['YOYANG_ILSU_FIRST_HOSP'].item()) #원부번호 요양기간(입원)
            wonbu_no_data_yoyang_ilsu_re = int(wonbu_no_data['YOYANG_ILSU_RE'].item()) #원부번호 재요양기간
            wonbu_no_data_yoyang_ilsu_re_hosp = int(wonbu_no_data['YOYANG_ILSU_RE_HOSP'].item()) #원부번호 재요양기간(입원)

            jaehaeja_cnt = len(SIMILAR_USERS_FIRST) #유사재해자 수
            jaehaeja_yoyang_re_cnt = len(SIMILAR_USER_GROUP_RE) #재요양 유사재해자수

            #요양기간 예측값
            try:
                pred = load_yoyang_ilsu_pred(host = NGINX_HOST, port = NGINX_PORT, location=API_HOST, wonbu_no = wonbu_no)
            except:
                pred = "none"
            
            #pred = load_yoyang_ilsu_pred(host = NGINX_HOST, port = NGINX_PORT, location=API_HOST, wonbu_no = wonbu_no)


            if (sincheong_yoyang_gigan!="none" or sincheong_yoyang_gigan_ipwon!="none" or sincheong_jaeyoyang_gigan!="none" or sincheong_jaeyoyang_gigan_ipwon!="none"):
                try:
                    if sincheong_yoyang_gigan == "none":
                        sincheong_yoyang_gigan = 0
                    else:
                        sincheong_yoyang_gigan = int(sincheong_yoyang_gigan)

                    if sincheong_yoyang_gigan_ipwon == "none":
                        sincheong_yoyang_gigan_ipwon = 0
                    else:
                        sincheong_yoyang_gigan_ipwon = int(sincheong_yoyang_gigan_ipwon)

                    if sincheong_jaeyoyang_gigan == "none":
                        sincheong_jaeyoyang_gigan = 0
                    else:
                        sincheong_jaeyoyang_gigan = int(sincheong_jaeyoyang_gigan)

                    if sincheong_jaeyoyang_gigan_ipwon == "none":
                        sincheong_jaeyoyang_gigan_ipwon = 0
                    else:
                        sincheong_jaeyoyang_gigan_ipwon = int(sincheong_jaeyoyang_gigan_ipwon)

                    #신청 기간을 반영한 그래프

                    #메인
                    with st.spinner('please wait...'):
                        ####재해자 정보
                        with st.container():
                            st.info(f"👷‍♂️ 재해자 정보 : 원부번호 {wonbu_no} 조회되었습니다.")

                            st.markdown(f"""
                            <table style="width:100%; height:100%;"> 
                            <tr>
                            <td colspan="8" style="background-color:#F0F2F6; text-align:center; font-weight:bold; font-size:14px;"> 재해자 정보 </td>
                            </tr>
                            <tr>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">원부번호</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{wonbu_no}</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">재해일자</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{wonbu_no_jaehae_dt}</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">연령대</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{wonbu_no_age_group}</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">성별</td>
                            <td style="width:12%; text-align:center; font-size:14px;">남성</td>
                            </tr>
                            <tr>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">주상병명(상세)</td>
                            <td colspan="5" style="font-size:14px; max-width: 10px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{wonbu_no_sangbyeong_cd_main_sangse_sangbyeong_nm}</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">재요양사유</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{wonbu_no_sayu_nm}</td>
                            </tr>
                            <tr>
                            <td colspan="8" style="background-color:#F0F2F6; text-align:center; font-weight:bold; font-size:14px;"> 승인 요양 정보 </td>
                            </tr>
                            <tr>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">요양기간</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{wonbu_no_data_yoyang_ilsu} 일</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">요양기간(입원)</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{sincheong_yoyang_gigan_ipwon} 일</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">재요양기간</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{wonbu_no_data_yoyang_ilsu_re} 일</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">재요양기간(입원)</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{wonbu_no_data_yoyang_ilsu_re_hosp} 일</td>
                            </tr>
                            <tr>
                            <td colspan="8" style="background-color:#F0F2F6; text-align:center; font-weight:bold; font-size:14px;"> 신청 요양 정보 </td>
                            </tr>
                            <tr>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">요양기간</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{sincheong_yoyang_gigan} 일</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">요양기간(입원)</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{sincheong_yoyang_gigan_ipwon} 일</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">재요양기간</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{sincheong_jaeyoyang_gigan} 일</td>
                            <td style="width:13%; background-color: #F2F2F2; text-align:center; font-weight:bold; font-size:14px;">재요양기간(입원)</td>
                            <td style="width:12%; text-align:center; font-size:14px;">{sincheong_jaeyoyang_gigan_ipwon} 일</td>
                            </tr>
                            </table>
                                    """, unsafe_allow_html=True)

                        st.markdown(" ")
                        st.markdown(" ")

                        ####요양기간
                        with st.container():
                            if SIMILAR_USER_GROUP_FIRST == 'group_1_30':
                                st.info(f"📈 요양기간 분석 : 유사 재해자 총 {str(jaehaeja_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_FIRST})")
                            elif SIMILAR_USER_GROUP_FIRST == 'group_1_10':
                                st.info(f"📈 요양기간 분석 : 유사 재해자 총 {str(jaehaeja_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_FIRST})")
                            elif SIMILAR_USER_GROUP_FIRST == 'group_2_30':
                                st.info(f"📈 요양기간 분석 : 유사 재해자 총 {str(jaehaeja_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_FIRST})")
                            elif SIMILAR_USER_GROUP_FIRST == 'group_2_10':
                                st.info(f"📈 요양기간 분석 : 유사 재해자 총 {str(jaehaeja_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_FIRST})")
                            elif SIMILAR_USER_GROUP_FIRST == 'group_3_30':
                                st.info(f"📈 요양기간 분석 : 유사 재해자 총 {str(jaehaeja_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_FIRST})")
                            elif SIMILAR_USER_GROUP_FIRST == 'group_3_10':
                                st.info(f"📈 요양기간 분석 : 유사 재해자 총 {str(jaehaeja_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_FIRST})")
                            elif SIMILAR_USER_GROUP_FIRST == 'group_4_30':
                                st.info(f"📈 요양기간 분석 : 유사 재해자 총 {str(jaehaeja_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_FIRST})")
                            elif SIMILAR_USER_GROUP_FIRST == 'group_4_10':
                                st.info(f"📈 요양기간 분석 : 유사 재해자 총 {str(jaehaeja_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_FIRST})")
                            else: #SIMILAR_USER_GROUP_FIRST == 'no_group'
                                st.info(f"📈 요양기간 분석 : 유사 재해자를 찾을 수 없습니다.")           
                        
                        if SIMILAR_USER_GROUP_FIRST != 'no_group':
                            with st.container():
                                col1, col2 = st.columns((6,4), gap="large")

                                with col1:
                                    st.markdown('<div class="custom-font-1">요양 기간 분포</div>', unsafe_allow_html=True)
                                    tab1, tab2, tab3 = st.tabs(["요약","상세","통계"])
                                    with tab1:
                                        st.plotly_chart(fig_yoyang_gigan_by_wonbu_no_plus_pred_sincheong_gigan(wonbu_no_data = wonbu_no_data, data=SIMILAR_USERS_FIRST, col='YOYANG_ILSU_FIRST', sincheong_ilsu=sincheong_yoyang_gigan, pred=pred, percentile=percentile), config = {'staticPlot': True}, theme=None, use_container_width=True)
                                    with tab2:
                                        st.plotly_chart(fig_yoyang_gigan_by_wonbu_no_sangse_plus_pred_sincheong_gigan(wonbu_no_data = wonbu_no_data, data=SIMILAR_USERS_FIRST, col='YOYANG_ILSU_FIRST', sincheong_ilsu=sincheong_yoyang_gigan, pred=pred, percentile=percentile), config = {'displayModeBar': False}, theme=None, use_container_width=True)
                                    with tab3:
                                        st.dataframe(
                                                desc_data,
                                                hide_index=True,
                                                use_container_width=True,
                                            )
                                        
                                with col2:
                                    st.markdown('<div class="custom-font-1">요양 기간(입원) 분포</div>', unsafe_allow_html=True)
                                    tab1, tab2, tab3 = st.tabs(["요약","상세","통계"])
                                    with tab1:
                                        st.plotly_chart(fig_yoyang_gigan_by_wonbu_no_plus_pred_sincheong_gigan(wonbu_no_data = wonbu_no_data, data=SIMILAR_USERS_FIRST, col="YOYANG_ILSU_FIRST_HOSP", sincheong_ilsu=sincheong_yoyang_gigan_ipwon, pred="none", percentile=percentile), config = {'staticPlot': True}, theme=None, use_container_width=True)
                                    with tab2:
                                        st.plotly_chart(fig_yoyang_gigan_by_wonbu_no_sangse_plus_pred_sincheong_gigan(wonbu_no_data = wonbu_no_data, data=SIMILAR_USERS_FIRST, col="YOYANG_ILSU_FIRST_HOSP", sincheong_ilsu=sincheong_yoyang_gigan_ipwon, pred="none", percentile=percentile), config = {'displayModeBar': False}, theme=None, use_container_width=True)
                                    with tab3:
                                        st.dataframe(
                                            desc_data_hosp,
                                            hide_index=True,
                                            use_container_width=True,
                                        )


                        st.markdown(" ")

                        ####재요양기간
                        with st.container():
                            if SIMILAR_USER_GROUP_RE == 'group_1_30':
                                st.success(f"📈 재요양기간 분석 : 유사 재해자 총 {str(jaehaeja_yoyang_re_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_RE})")
                            elif SIMILAR_USER_GROUP_RE == 'group_1_10':
                                st.success(f"📈 재요양기간 분석 : 유사 재해자 총 {str(jaehaeja_yoyang_re_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_RE})")
                            elif SIMILAR_USER_GROUP_RE == 'group_2_30':
                                st.success(f"📈 재요양기간 분석 : 유사 재해자 총 {str(jaehaeja_yoyang_re_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_RE})")
                            elif SIMILAR_USER_GROUP_RE == 'group_2_10':
                                st.success(f"📈 재요양기간 분석 : 유사 재해자 총 {str(jaehaeja_yoyang_re_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_RE})")
                            elif SIMILAR_USER_GROUP_RE == 'group_3_30':
                                st.success(f"📈 재요양기간 분석 : 유사 재해자 총 {str(jaehaeja_yoyang_re_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_RE})")
                            elif SIMILAR_USER_GROUP_RE == 'group_3_10':
                                st.success(f"📈 재요양기간 분석 : 유사 재해자 총 {str(jaehaeja_yoyang_re_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_RE})")
                            elif SIMILAR_USER_GROUP_RE == 'group_4_30':
                                st.success(f"📈 재요양기간 분석 : 유사 재해자 총 {str(jaehaeja_yoyang_re_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_RE})")
                            elif SIMILAR_USER_GROUP_RE == 'group_4_10':
                                st.success(f"📈 재요양기간 분석 : 유사 재해자 총 {str(jaehaeja_yoyang_re_cnt)} 명이 조회되었습니다. (🧑‍🤝‍🧑 유사 재해자 : {SIMILAR_USER_GROUP_INFO_RE})")
                            else: #SIMILAR_USER_GROUP_RE == 'no_group'
                                st.success(f"📈 재요양기간 분석 : 재요양 사유가 없거나, 유사 재해자를 찾을 수 없습니다.")

                        if SIMILAR_USER_GROUP_RE != "no_group":
                            with st.container():

                                col1, col2 = st.columns((6,4), gap="large")                                 
                                with col1:
                                    st.markdown('<div class="custom-font-1">재요양 기간 분포</div>', unsafe_allow_html=True)
                                    tab1, tab2, tab3 = st.tabs(["요약","상세","통계"])
                                    with tab1:
                                        st.plotly_chart(fig_yoyang_gigan_by_wonbu_no_plus_pred_sincheong_gigan(wonbu_no_data = wonbu_no_data, data=SIMILAR_USERS_RE, col="YOYANG_ILSU_RE", sincheong_ilsu=sincheong_jaeyoyang_gigan, pred="none", percentile=percentile), config = {'staticPlot': True}, theme=None, use_container_width=True)
                                    with tab2:
                                        st.plotly_chart(fig_yoyang_gigan_by_wonbu_no_sangse_plus_pred_sincheong_gigan(wonbu_no_data = wonbu_no_data, data=SIMILAR_USERS_RE, col="YOYANG_ILSU_RE", sincheong_ilsu=sincheong_jaeyoyang_gigan, pred="none", percentile=percentile), config = {'displayModeBar': False}, theme=None, use_container_width=True)
                                    with tab3:
                                        st.dataframe(
                                            desc_data_re,
                                            hide_index=True,
                                            use_container_width=True,
                                        ) 

                                with col2:
                                    st.markdown('<div class="custom-font-1">재요양 기간(입원) 분포</div>', unsafe_allow_html=True)
                                    tab1, tab2, tab3 = st.tabs(["요약","상세","통계"])
                                    with tab1:
                                        st.plotly_chart(fig_yoyang_gigan_by_wonbu_no_plus_pred_sincheong_gigan(wonbu_no_data = wonbu_no_data, data=SIMILAR_USERS_RE, col="YOYANG_ILSU_RE_HOSP", sincheong_ilsu=sincheong_jaeyoyang_gigan_ipwon, pred="none", percentile=percentile), config = {'staticPlot': True}, theme=None, use_container_width=True)
                                    with tab2:
                                        st.plotly_chart(fig_yoyang_gigan_by_wonbu_no_sangse_plus_pred_sincheong_gigan(wonbu_no_data = wonbu_no_data, data=SIMILAR_USERS_RE, col="YOYANG_ILSU_RE_HOSP", sincheong_ilsu=sincheong_jaeyoyang_gigan_ipwon, pred="none", percentile=percentile), config = {'displayModeBar': False}, theme=None, use_container_width=True)
                                    with tab3:
                                        st.dataframe(
                                            desc_data_re_hosp,
                                            hide_index=True,
                                            use_container_width=True,
                                        )


                except Exception as e:
                    st.exception(RuntimeError('신청 기간을 조회할 수 없습니다.'))
                    st.exception(e)

            else:
                st.exception(RuntimeError('신청 기간을 조회할 수 없습니다.'))              


        except Exception as e:
            st.exception(RuntimeError('유사 재해자를 찾을 수 없습니다.'))
            st.exception(e)



    except Exception as e:
        st.exception(RuntimeError('원부번호를 조회할 수 없습니다.'))
        st.exception(e)


except Exception as e:
    st.exception(RuntimeError('원부번호 및 신청 요양기간을 입력하세요.'))
    st.exception(e)









