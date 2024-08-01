from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'etlspeechbash',
    description='A simple DAG that prints Hello Vinod',
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
)

task1= BashOperator(
        task_id='etlspeechbash_kubernetes_executor',
        bash_command='echo Kubernetes',
        queue = 'kubernetes',
        dag=dag,
    )
task2 = BashOperator(
        task_id='etlspeechbash_Celery_Executor',
        bash_command='java --add-exports java.base/sun.nio.ch=ALL-UNNAMED -jar "opt/airflow/etlspeech-assembly-0.1.0-SNAPSHOT.jar" "admin" "password" "http://host.docker.internal:9000" "s3a://warehouse/micrawdata1" "s3a://warehouse/tbl_engagement_speech_silver" "3456000"',
        dag=dag,
    )

# task1
task2
