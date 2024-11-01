from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def etl_data(ti):

    #Extract dummy user data and parse into a dataframe
    response_users = requests.get('https://dummyjson.com/users?limit=0')
    if response_users.status_code != 200:
        raise ValueError("Failed to fetch users data")
    users = response_users.json()
    users = pd.json_normalize(users['users'])

    #Extract dummy user posts data and parse into a dataframe
    response_posts = requests.get('https://dummyjson.com/posts?limit=0')
    if response_posts.status_code != 200:
        raise ValueError("Failed to fetch users data")
    posts = response_posts.json()
    posts = pd.json_normalize(posts['posts'])

    usercols = ["id","firstName", "lastName", 'age', 'gender', 'address.city', 'address.stateCode', 'university', 'company.name', 'company.title']
    users = users[usercols]

    #Renaming columns for ease of understanding
    users.rename(columns={"id":"userId", "address.city":"city", "address.stateCode":"state", "company.name":"company", "company.title":"title"}, inplace=True)
    posts.rename(columns={"id":"postId", "reactions.likes":"likes", "reactions.dislikes":"dislikes"}, inplace=True)

    #Data loading into Postgres
    postgres_hook = PostgresHook(postgres_conn_id='dummy_connection')

    #Inserting posts
        # Inserting posts in bulk
    post_values = posts[['postId', 'title', 'body', 'tags', 'views', 'likes', 'dislikes']].values.tolist()
    postgres_hook.insert_rows(table="posts", rows=post_values, target_fields=["postId", "title", "body", "tags", "views", "likes", "dislikes"])

    # Inserting users in bulk
    user_values = users[['userId', 'firstName', 'lastName', 'age', 'gender', 'city', 'state', 'company', 'title']].values.tolist()
    postgres_hook.insert_rows(table="users", rows=user_values, target_fields=["userId", "firstName", "lastName", "age", "gender", "city", "state", "company", "title"])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=0),
}

dag = DAG(
    'etl_dummy_data',
    default_args=default_args,
    description='A simple DAG to extract synthetic user and post data from DummyJSON API and loading it into PostgreSQL',
    schedule_interval=timedelta(days=1),
)

etl_data_task = PythonOperator(
    task_id='etl_data',
    python_callable=etl_data,
    dag=dag,
)

create_posts_table_task = PostgresOperator(
    task_id='create_posts_table',
    postgres_conn_id='dummy_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS posts (
        postId SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        body TEXT,
        tags TEXT,
        views TEXT,
        userId TEXT,
        likes TEXT,
        dislikes TEXT
    );
    """,
    dag=dag,
)

create_users_table_task = PostgresOperator(
    task_id='create_users_table',
    postgres_conn_id='dummy_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS users (
        userId SERIAL PRIMARY KEY,
        firstName TEXT NOT NULL,
        lastName TEXT,
        age TEXT,
        gender TEXT,
        city TEXT,
        state TEXT,
        university TEXT,
        company TEXT,
        title TEXT
    );
    """,
    dag=dag,
)


#dependencies
create_posts_table_task >> create_users_table_task >> etl_data_task