from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import logging
from datetime import datetime


# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2019, 4, 16),
# }


# def read_xcoms(**context):
#     for idx, task_id in enumerate(context['data_to_read']):
#         data = context['task_instance'].xcom_pull(task_ids=task_id, key='data')
#         logging.info(f'[{idx}] I have received data: {data} from task {task_id}')
#
#
# def launch_docker_container(**context):
#     # just a mock for now
#     logging.info(context['ti'])
#     logging.info(context['image_name'])
#     my_id = context['my_id']
#     context['task_instance'].xcom_push('data', f'my name is {my_id}', context['execution_date'])
#
#
# with DAG('pipeline_python_2', default_args=default_args) as dag:
#     t1 = BashOperator(
#         task_id='print_date1',
#         bash_command='date')
#
#     t2_1_id = 'do_task_one'
#     t2_1 = PythonOperator(
#         task_id=t2_1_id,
#         provide_context=True,
#         op_kwargs={
#             'image_name': 'task1',
#             'my_id': t2_1_id
#         },
#         python_callable=launch_docker_container
#     )
#
#     t2_2_id = 'do_task_two'
#     t2_2 = PythonOperator(
#         task_id=t2_2_id,
#         provide_context=True,
#         op_kwargs={
#             'image_name': 'task2',
#             'my_id': t2_2_id
#         },
#         python_callable=launch_docker_container
#     )
#
#     t3 = PythonOperator(
#         task_id='read_xcoms',
#         provide_context=True,
#         python_callable=read_xcoms,
#         op_kwargs={
#             'data_to_read': [t2_1_id, t2_2_id]
#         }
#     )
#
#     t1 >> [t2_1, t2_2] >> t3


def greet():
    print('Writing in file')
    with open('path/to/file/greet.txt', 'a+', encoding='utf8') as f:
        now = datetime.now()
        t = now.strftime("%Y-%m-%d %H:%M")
        f.write(str(t) + '\n')
    return 'Greeted'


def respond():
    return 'Greet Responded Again'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 9, 24, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('my_simple_dag',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         ) as dag:
    opr_hello = BashOperator(task_id='say_Hi',
                             bash_command='echo "Hi!!"')

    opr_greet = PythonOperator(task_id='greet',
                               python_callable=greet)
    opr_sleep = BashOperator(task_id='sleep_me',
                             bash_command='sleep 5')

    opr_respond = PythonOperator(task_id='respond',
                                 python_callable=respond)
opr_hello >> opr_greet >> opr_sleep >> opr_respond
