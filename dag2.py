from datetime import timedelta,datetime
import pandas as pd
from airflow import DAG 
from airflow.operators.python import PythonOperator
import requests
from sqlalchemy import create_engine
import psycopg2



url = 'https://raw.githubusercontent.com/ezenavarro1/DataENG/main/codebar.csv'
df = pd.read_csv(url)

def generar_urlcode():
  lista_codebar= df["code_bar"]
  lista_url_code = []
  lista_url_code = ['https://api.mercadolibre.com/products/search?status=active&site_id=MLA&product_identifier='+ str(i) for i in lista_codebar]
  return lista_url_code

lista_url_code = generar_urlcode()
df["url_code"] = pd.DataFrame(lista_url_code)

def extraer_mlacatalogo():
  mla_catalogo = []
  mla_catalogo = ['Null' if requests.get(i).json()['results'] == [] else requests.get(i).json()['results'][0]['id'] for i in lista_url_code]
  return mla_catalogo

mla_catalogo = extraer_mlacatalogo()
df["mla"] = pd.DataFrame(mla_catalogo)

def generar_mlaurl():
  ureles_mla= []
  ureles_mla = ['https://api.mercadolibre.com/products/'+ str(i)+'/items' for i in mla_catalogo]
  return ureles_mla
  
ureles_mla = generar_mlaurl()
df["url_mla"] = pd.DataFrame(ureles_mla)

def extraer_precios():
  prices1 = []
  prices1 = [requests.get(i).json()['results'][0]['price'] if list(requests.get(i).json().keys())[0] == 'paging' else 'Null' for i in ureles_mla]
  return prices1

prices1 = extraer_precios()
df['price 1'] = pd.DataFrame(prices1)

def extraer_vendedores():
  sellers1 = []
  sellers1 = [requests.get(i).json()['results'][0]['seller_id'] if list(requests.get(i).json().keys())[0] == 'paging' else 'Null' for i in ureles_mla]
  return sellers1

sellers1 = extraer_vendedores()
df['seller 1'] = pd.DataFrame(sellers1)

def share_tables():
  engine = create_engine('postgresql://ezenavarro511_coderhouse:o204GOvJcY@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database')

  df.to_sql('productos_meli', engine, index=False, if_exists='replace')

default_args ={ 
      'owner': 'Eze Navarro',
      'start_date': datetime(2023,7,27),
      'retries':3,
      'retry_delay': timedelta(minutes = 2),
      'catchup': False,
}


with DAG(
    default_args = default_args,
    dag_id = 'dag_con_conexion_postgres2',
    description = 'DAG Entregable',
    start_date = datetime(2023,7,13),
    schedule = '@daily',
    catchup = False
  )as dag:

  generar_urlcode = PythonOperator(
    task_id = 'generar_urlcode',
    python_callable = generar_urlcode
  )
  extraer_mlacatalogo = PythonOperator(
    task_id = 'extraer_mlacatalogo',
    python_callable = extraer_mlacatalogo
  )
  generar_mlaurl = PythonOperator(
    task_id = 'generar_mlaurl',
    python_callable = generar_mlaurl
  )
  extraer_precios = PythonOperator(
    task_id = 'extraer_precios',
    python_callable = extraer_precios
  )
  extraer_vendedores = PythonOperator(
    task_id = 'extraer_vendedores',
    python_callable = extraer_vendedores
  )
  share_tables = PythonOperator(
    task_id = 'share_tables',
    python_callable = share_tables
  )


generar_urlcode >> extraer_mlacatalogo >> generar_mlaurl >> extraer_precios >> extraer_vendedores >> share_tables 