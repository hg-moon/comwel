# korean_pages.py
import streamlit as st
import os
from dotenv import load_dotenv

load_dotenv()
IP_MAIN = os.getenv("IP_MAIN")
NGINX_PORT = os.getenv("NGINX_PORT")
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST")


def titles():
    return st.markdown(
    "<style>:root {--text-color: #808495; /* Light text color */ --bg-color: transparent; /* Dark background color */} " +
    
    f"a[href='http://{IP_MAIN}:{NGINX_PORT}/{DASHBOARD_HOST}/'] span:first-child "+
    "{position: relative; z-index: 1;color: transparent;} " +
    f"a[href='http://{IP_MAIN}:{NGINX_PORT}/{DASHBOARD_HOST}/'] span:first-child::before "+ 
    "{content: '개요'; position: absolute; left: 0; z-index: 2; color: var(--text-color); background-color: var(--bg-color);} " +

    f"a[href='http://{IP_MAIN}:{NGINX_PORT}/{DASHBOARD_HOST}/yoyang_gigan_analysis'] span:first-child " + 
    "{position: relative; z-index: 1; color: transparent;} "+
    f"a[href='http://{IP_MAIN}:{NGINX_PORT}/{DASHBOARD_HOST}/yoyang_gigan_analysis'] span:first-child::before " +
    "{content: '요양기간 분석'; position: absolute; left: 0; z-index: 2; color: var(--text-color); background-color: var(--bg-color);} " +
    
    "</style>",
        unsafe_allow_html=True,
    )





'''
def titles():
    return st.markdown(
        """
    <style>
:root {
  --text-color: #808495; /* Light text color */
  --bg-color: transparent; /* Dark background color */
}
a[href="http://211.218.17.10:9090/dashboard/"] span:first-child {
    position: relative;
    z-index: 1;
    color: transparent;
}
a[href="http://211.218.17.10:9090/dashboard/"] span:first-child::before {
    content: "개요";
    position: absolute;
    left: 0;
    z-index: 2;
    color: var(--text-color);
    background-color: var(--bg-color);
}
a[href="http://211.218.17.10:9090/dashboard/yoyang_gigan_analysis"] span:first-child {
    position: relative;
    z-index: 1;
    color: transparent;
}
a[href="http://211.218.17.10:9090/dashboard/yoyang_gigan_analysis"] span:first-child::before {
    content: "요양기간 분석";
    position: absolute;
    left: 0;
    z-index: 2;
    color: var(--text-color);
    background-color: var(--bg-color);
}
</style>
    """,
        unsafe_allow_html=True,
    )

'''