import requests
import numpy as np 
import pandas as pd

def load_profile_main(host, port, location, wonbu_no):
    url = f"http://{host}:{port}/{location}/v1/aim/profile/users/{wonbu_no}/main"
    data = requests.get(url)
    data = pd.DataFrame([data.json()])
    
    return data

def load_profile_yoyang(host, port, location, wonbu_no):
    url = f"http://{host}:{port}/{location}/v1/aim/profile/users/{wonbu_no}/yoyang"
    data = requests.get(url)
    data = pd.DataFrame([data.json()])
    
    return data

def load_similar_users_yoyang_ilsu_first(host, port, location, wonbu_no):
    url = f"http://{host}:{port}/{location}/v1/aim/similar_users/users/{wonbu_no}/yoyang_ilsu_first"
    data = requests.get(url)
    data = data.json()

    SIMILAR_USER_GROUP = data['SIMILAR_USER_GROUP']
    SIMILAR_USER_GROUP_INFO = data['SIMILAR_USER_GROUP_INFO']
    YOYANG_ILSU_DESC = pd.DataFrame(data['YOYANG_ILSU_DESC'])
    YOYANG_ILSU_HOSP_DESC = pd.DataFrame(data['YOYANG_ILSU_HOSP_DESC'])
    SIMILAR_USERS = pd.DataFrame(data['SIMILAR_USERS'])

    return SIMILAR_USER_GROUP, SIMILAR_USER_GROUP_INFO, YOYANG_ILSU_DESC, YOYANG_ILSU_HOSP_DESC, SIMILAR_USERS

def load_similar_users_yoyang_ilsu_re(host, port, location, wonbu_no):
    url = f"http://{host}:{port}/{location}/v1/aim/similar_users/users/{wonbu_no}/yoyang_ilsu_re"
    data = requests.get(url)
    data = data.json()

    SIMILAR_USER_GROUP = data['SIMILAR_USER_GROUP']
    SIMILAR_USER_GROUP_INFO = data['SIMILAR_USER_GROUP_INFO']
    YOYANG_ILSU_DESC = pd.DataFrame(data['YOYANG_ILSU_DESC'])
    YOYANG_ILSU_HOSP_DESC = pd.DataFrame(data['YOYANG_ILSU_HOSP_DESC'])
    SIMILAR_USERS = pd.DataFrame(data['SIMILAR_USERS'])

    return SIMILAR_USER_GROUP, SIMILAR_USER_GROUP_INFO, YOYANG_ILSU_DESC, YOYANG_ILSU_HOSP_DESC, SIMILAR_USERS

def load_similar_users_data_date(host, port, location):
    url = f"http://{host}:{port}/{location}/v1/aim/similar_users/data_date"
    data = requests.get(url)
    data = data.json()

    SIMILAR_USER_DATA_DATE = data

    return SIMILAR_USER_DATA_DATE


def load_yoyang_ilsu_pred(host, port, location, wonbu_no):
    url = f"http://{host}:{port}/{location}/v1/aim/prediction/users/{wonbu_no}"
    data = requests.get(url)
    data = data.json()

    pred = data['YOYANG_ILSU_PRED']

    return pred


def load_model_train_date(host, port, location):
    url = f"http://{host}:{port}/{location}/v1/aim/prediction/model_train_date"
    data = requests.get(url)
    data = data.json()

    TRAIN_DATE = data

    return TRAIN_DATE
