# importing the required libraries
from sqlalchemy import create_engine
import requests
import time
from datetime import date as dt,timedelta
import json
import pandas as pd
from os.path import exists
import sys
import re
import oss2
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# Important Credentials
# -------------------------------------------------------------------------------------------------------------------
engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{database}".format(
                        user=Variable.get("db_user"),
                        password=Variable.get("password"),
                        host=Variable.get("host"),
                        port=Variable.get("port"),
                        database=Variable.get("database")
                        )
    )

date = dt.today() - timedelta(days = 1)
save_path = f"/tmp/files_to_oss/" + str(date) +"_first_reviews.json"
from_bucket_path = f"/tmp/files_from_oss/"+str(date)+"_first_reviews.json"

required_keys = ["id", "auctionNumId", "skuId", "feedback", "reply", "skuMap",
                 "headFrameUrl", "userNick", "headPicUrl", "userStar", "repeatBusiness", "feedbackDate", "userStar",
                 ]
final_keys = ["id", "auctionNumId", "skuId", "feedback","readCount", "commentCount", "reply", "skuMap",
         "headFrameUrl", "userNick", "headPicUrl", "userStar", "repeatBusiness", "feedbackDate"
              ,"create_date","update_date","created_at","userStar","likeCount"]

names = ["readCount", "commentCount", "likeCount","create_date","update_date","created_at","id", "auctionNumId", "skuId", "feedback", "reply", "skuMap",
         "headFrameUrl", "userNick", "headPicUrl", "userStar", "repeatBusiness", "feedbackDate", "userStar",
         ]

auth = oss2.Auth(Variable.get("oss2_key"), Variable.get("oss2_token"))
bucket = oss2.Bucket(auth, 'oss-cn-shanghai.aliyuncs.com', 'tmall-reviews')
endpoint = Variable.get("review_api_endpoint")
# --------------------------------------------------------------------------------------------------------------------


# Querying the data from the Database (to get required product ids)
def query_from_db():

    query = "SELECT product_id,review_crawled FROM raw_ecomm.product where create_date = '" + str(date)+"'  and review_crawled in ('0','fail','in_progress');"
    data=pd.read_sql(query,engine)
    row=list(zip(data.product_id,data.review_crawled))
    return row

# Downloading the data from API's and stroing all json documents in one Json file
def api_to_oss_bucket():

    row = query_from_db()  # getting connection credentials in conn and query output in row variable
    if len(row) >=1:  # This will execute if the result of query from database will not be null
        failed_ids = []  # to save the failed ids
        empty_ids = []   # to save the ids whose response is empty
        p_ids = []       # actual ids which are successfully fetched and wrote to json files
        vt = 1
        for id in row:
            print(vt)
            vt = vt+1
            if id[1]=='in_progress':
                p_ids.append(id[0])
            else:
                count = 1
                ext_count = 1
                # external api call. we will try 5 times with 5 seconds interval is api hit fails
                while ext_count <= 5:
                    time.sleep(5)  # Five seconds sleep between each api call if api hit fails
                    parameters = {"num_iid": id[0], "sort": "2"}
                    result = requests.get(endpoint, params=parameters)
                    api_result = result.json()
                    ext_count = ext_count + 1
                    if result.status_code ==500:
                        if vt == 1:
                            print("Internal server error. Please try again after some time (outer Api)")
                            sys.exit()
                        else:
                            print("outer api server fail, saving file to bucket")
                            bucket.put_object_from_file(f"raw_data/tmall/" + str(date) + "/", save_path)
                            parse_json_to_df(p_ids, empty_ids)
                            sys.exit()

                    if result.status_code == 200:
                        break

                # checking if the API response dont have rateList. This is the main list which has required data
                check_rateList = api_result['showapi_res_body']['ret_body']
                # if no rateList exists means no data and this is empty id
                if 'rateList'not in check_rateList:
                    empty_ids.append(id[0])
                    file_save(api_result)
                    sql_update = """Update raw_ecomm.product set review_crawled = %s where product_id = %s"""
                    engine.execute(sql_update, ('in_progress', id[0]))
                else:
                    # if rateList exists but it is empty so again this will be empty id
                    check_rateList2 = json.loads(api_result['showapi_res_body']['ret_body'].replace("\\", ""))['rateList']
                    if len(check_rateList2) == 0:
                        empty_ids.append(id[0])
                        file_save(api_result)
                        sql_update = """Update raw_ecomm.product set review_crawled = %s                                                                                                            where product_id = %s"""
                        engine.execute(sql_update, ('in_progress', id[0]))
                    # this will execute when rateList exists and also it is not null. Mean API response has the data
                    else:
                        total_page = json.loads(api_result['showapi_res_body']['ret_body'].replace("\\", ""))[
                            'totalPage']
                        # again hitting the api with the same id but now with last page values in total_page
                        parameters2 = {"num_iid": id[0], "sort": "2", "page_num": total_page}
                        result = requests.get(endpoint, params=parameters2)

                        # Running the API and if it does not called then updating crawled to fail
                        if result.status_code != 200:
                            sql_update_fail = """Update raw_ecomm.product set review_crawled = %s
                                                                                                 where product_id = %s"""
                            engine.execute(sql_update_fail, ('fail', id[0]))
                            # Recalling the api with same name. Try to hit 5 times if api call is fail
                            while count <= 5:
                                time.sleep(5)  # Five seconds sleep between each api call if api hit fails
                                parameters2 = {"num_iid": id[0], "sort": "2", "page_num": total_page}
                                result = requests.get(endpoint, params=parameters2)
                                count = count + 1
                                if result.status_code == 500:
                                    if vt == 1:
                                        print("Internal server error. Please try again after some time (inner api)")
                                        sys.exit()
                                    else:
                                        print("inner api server fail, saving file to bucket")
                                        bucket.put_object_from_file(f"raw_data/tmall/" + str(date) + "/",
                                                                    save_path)
                                        parse_json_to_df(p_ids, empty_ids)
                                        sys.exit()
                                if result.status_code == 200:
                                    break
                        if count == 5:
                            sql_update_fail = """Update raw_ecomm.product set review_crawled = %s
                                                                                 where product_id = %s"""
                            engine.execute(sql_update_fail, ('fail', id[0]))
                            failed_ids.append(id[0])  # id which is called five times and faild every time
                            pass
                        else:
                            # this will execute when API response is saved to the json file. Review_crawled will be in_progress
                            result = result.json()
                            sql_update_in_prorgess = """Update raw_ecomm.product set review_crawled = %s
                                                                                           where product_id = %s"""
                            engine.execute(sql_update_in_prorgess, ('in_progress', id[0]))
                            print("Record Updated successfully ")
                            p_ids.append(id[0])

                            exists_file = exists(save_path)
                            # if file exists it will be opened and new data will append to this file
                            file_save(result)
        # uploading the final json file to oss-bucket
        bucket.put_object_from_file(f"raw_data/tmall/"+str(date)+"/", save_path)

        pp = []
        for t in p_ids:
            pp.append(t)
        # this function returns the db connection details, ids which data is stored in file and ids with empty response
        return pp, empty_ids
    else:
        print("No product ID's fetched form database")
        sys.exit()


# loading the data form OSS bucket, parsing all json documents and converting it to a dataframe

def parse_json_to_df():
    p_ids, empty_ids = api_to_oss_bucket()
    # reading the json file from oss-bucket
    bucket.get_object_to_file(f"raw_data/tmall/"+str(date)+"/", from_bucket_path)
    with open(from_bucket_path, 'r') as rd:
        json_data = json.load(rd)

        # on each index of list there is a json document which has multiple records
        # so iterating on each json documents
        for index,elem in enumerate(json_data):
            record = json_data[index]
            rateListe = record["showapi_res_body"]["ret_body"]
            if "rateList" not in rateListe:
                continue
            rateList = json.loads(record["showapi_res_body"]["ret_body"].replace("\\", ""))["rateList"]

            if len(rateList)==0:
                pass

            else:
                track_id = json.loads(record["showapi_res_body"]["ret_body"].replace("\\", ""))["rateList"][0][
                    "auctionNumId"]
                # print("Track id is: ", track_id)

                # for ids which data is stored in file.
                # additionally this will also help to determine only new appended ids in case if the query if run
                # for the same Date again
                if (track_id in p_ids):
                    final_id = track_id
                    dataList = []

                    # parsing json documents processing and storing in pandas dataframe
                    for item in rateList:

                        itemData = []
                        if "interactInfo" not in item.keys():
                            readCount = 0
                            commentCount = 0
                            likeCount = 0
                            itemData.append(readCount)
                            itemData.append(commentCount)
                            itemData.append(likeCount)
                            itemData.append(date.today())
                            itemData.append(date.today())
                            itemData.append(date.today())
                        else:
                            readCount = item["interactInfo"]["readCount"]
                            commentCount = item["interactInfo"]["commentCount"]
                            likeCount = item["interactInfo"]["likeCount"]
                            itemData.append(readCount)
                            itemData.append(commentCount)
                            itemData.append(likeCount)
                            itemData.append(date.today())
                            itemData.append(date.today())
                            itemData.append(date.today())

                        for key in required_keys:
                            if key == "headFrameUrl" or key == "skuId":
                                if key == "headFrameUrl":
                                    if key not in item.keys():
                                        itemData.append("")
                                    else:
                                        # print(item[key])
                                        result = re.search('StrV3=(.*)%3D%3D', item[key])
                                        result = result.group(1)
                                        # itemData.append(str(item[key])[62:84])
                                        itemData.append(str(result))

                                else:
                                    if key not in json.loads(record["showapi_res_body"]["ret_body"].replace("\\", ""))[
                                        "rateList"][0].keys():
                                        itemData.append("")
                                    else:
                                        itemData.append(item[key])
                            else:
                                itemData.append(item[key])

                        dataList.append(itemData)
                    data_frame = pd.DataFrame(dataList)

                    data_frame.columns = names
                    # This is the dataframe with all required values parsed from json file
                    final_data = data_frame[final_keys]

                    # filtering the dataframe for only records with minimum date.
                    min_date = min(final_data['feedbackDate'])
                    final_data = final_data[final_data['feedbackDate'] == min_date]

                    # Data inserting query
                    postgres_insert_query = """ INSERT INTO raw_ecomm.review
                                                                        (review_id, product_id, sku_id, review_body, review_read_count,review_comment_count,
                                                                        review_reply_body,sku_map,user_id,user_name,user_uploaded_image,user_stars,
                                                                        repeat_buyer,review_date,create_date,update_date,created_at,user_star,like_count) VALUES
                                                                        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

                    # parsing values from complete dataframe. These values will be inserted into database review table
                    for i in range(len(final_data)):
                        id = str(final_data["id"].values[i])
                        aunctionId = str(final_data["auctionNumId"].values[i])
                        skuId = str(final_data["skuId"].values[i])
                        feedback = str(final_data["feedback"].values[i])
                        readCount = int(final_data["readCount"].values[i])
                        commentCount = int(final_data["commentCount"].values[i])
                        reply = str(final_data["reply"].values[i])
                        skuMap = str(final_data["skuMap"].values[i])
                        headFrameUrl = str(final_data["headFrameUrl"].values[i])
                        userNick = str(final_data["userNick"].values[i])
                        headPicUrl = str(final_data["headPicUrl"].values[i])
                        userStar = final_data["userStar"].values[i][0]
                        repeatBusiness = final_data["repeatBusiness"].values[i]
                        feedbackDate = pd.to_datetime(final_data["feedbackDate"].values[i])
                        create_date = pd.to_datetime(final_data["create_date"].values[i])
                        update_date = pd.to_datetime(final_data["update_date"].values[i])
                        created_at = pd.to_datetime(final_data["created_at"].values[i])
                        # userStar = int(final_data["userStar"].values[i])
                        likeCount = int(final_data["likeCount"].values[i])

                    # values to be inserted into the database
                    query_values = (
                        id,
                        aunctionId,
                        skuId,
                        feedback,
                        readCount,
                        commentCount,
                        reply,
                        skuMap,
                        headFrameUrl,
                        userNick,
                        headPicUrl,
                        userStar,
                        repeatBusiness,
                        feedbackDate,
                        create_date,
                        update_date,
                        created_at,
                        userStar,
                        likeCount
                    )
                    # execution of insertion query
                    engine.execute(postgres_insert_query, query_values)
                    sql_update_done = """Update raw_ecomm.product set review_crawled = %s
                                                                     where product_id = %s"""
                    engine.execute(sql_update_done, ('done', final_id))

    # for all ids with empty responses the review_crawled status will also done.
    for e_id in empty_ids:
        sql_no_review = """Update raw_ecomm.product set review_crawled = %s
                                                     where product_id = %s"""
        engine.execute(sql_no_review, ('done', e_id))

def file_save(api_result):
    exists_file = exists(save_path)
    if exists_file:

        with open(save_path, 'r') as fp:
            listObj = json.load(fp)
            listObj.append(api_result)
            with open(save_path, 'w') as wt:
                json.dump(listObj, wt)
    else:
        # if no file exist with this name. New file will be created and data will be stored in it.
        listObj = []
        listObj.append(api_result)
        with open(save_path, 'w') as json_file:
            json.dump(listObj, json_file)

# function calling

default_args = {'owner': 'airflow',
'depends_on_past': False,
'start_date': dt.datetime(2022,9,6),
'email_on_failure': False,
'email_on_retry': False,
'schedule_interval': '0 16 * * *', #set this as required
'retries': 0,
'pool': 'default_pool',
#'retry_delay': dt.timedelta(hours=10),
'max_active_runs': 1
}

dag = DAG(
    dag_id = "Review_Crawl_API",
    description = "pull onebound tmall review data",
    default_args = default_args,
    schedule_interval= '* * * * *',
    catchup = False
    )
s0=PythonOperator(
    dag = dag,
    task_id = "review_crawl_1",
    python_callable = parse_json_to_df,
    provide_context = True)
s1 = PythonOperator(
    dag = dag,
    task_id = "review_crawl_2",
    python_callable = parse_json_to_df,
    provide_context = True)
s2 = PythonOperator(
    dag = dag,
    task_id = "review_crawl_3",
    python_callable = parse_json_to_df,
    provide_context = True)
s3 = PythonOperator(
    dag = dag,
    task_id = "review_crawl_4",
    python_callable = parse_json_to_df,
    provide_context = True)

s0>>s1>>s2>>s3