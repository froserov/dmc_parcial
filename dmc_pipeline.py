
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from joblib import dump, load
import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
import pickle


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 26),
    'email ':['franja_n1994@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Automl_workflow',
    default_args=default_args,
    description='Pipe for Kaggle competition',
##Cambiar para que sea una vez a la semana
    schedule_interval=None,
)


def GetDataKaggle():
    os.environ['KAGGLE_USERNAME'] = 'franciscorosero'
    os.environ['KAGGLE_KEY'] = 'b05b1d20329fe39d24b7cbf83eb95d91'
    api = KaggleApi()
    api.authenticate()
    # Download the competition files
    competition_name = 'playground-series-s4e4'
    download_path = 'data/'
    api.competition_download_files(competition_name, path=download_path)
    # Unzip the downloaded files
    for item in os.listdir(download_path):
        if item.endswith('.zip'):
            zip_ref = zipfile.ZipFile(os.path.join(download_path, item), 'r')
            zip_ref.extractall(download_path)
            zip_ref.close()
    # Ruta donde se descomprimieron los archivos CSV
    csv_folder = 'data/'
    # Listar todos los archivos en la carpeta descomprimida
    csv_files = [file for file in os.listdir(csv_folder) if file.endswith('.csv')]
    # Leer cada archivo CSV en un DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(csv_folder, csv_file)
        df = pd.read_csv(file_path)

        data = df

        return data.data.tolist(), data.target.tolist()

# Llamar a la función load_data
#GetDataKaggle()


def AutoML_Pycaret(test_data,sample_submission, *args, **kwargs):
    # Cargar modelo entrenado desde el archivo .pkl
    with open('final_model_abalone.pkl', 'rb') as f:
        model = pickle.load(f)
    # Aplicar modelo a los datos
    resultados = model.predict(test_data)
    sample_submission['Rings']=resultados
    sample_submission.to_csv('submission_0.csv', index=False)
    return sample_submission


def SubmitKaggle():
    api.competition_submit(file_name="submissions/submission_0.csv",
    message="First submission",
    competition="playground-series-s4e4"
return


GetDataKaggle_task = PythonOperator(
    task_id='GetDataKaggle',
    python_callable=GetDataKaggle,
    dag=dag,
)


AutoML_Pycaret_task = PythonOperator(
    task_id='AutoML_Pycaret',
    python_callable=AutoML_Pycaret,
    op_kwargs={'test_data': datos_de_test},
    dag=dag,
)


SubmitKaggle_task = PythonOperator(
    task_id='SubmitKaggle',
    python_callable=SubmitKaggle,
    dag=dag,
)



GetDataKaggle_task >> AutoML_Pycaret_task >> SubmitKaggle_task >> 
