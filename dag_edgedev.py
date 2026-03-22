from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'victor',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'vibration_alarms_smart_sensor',
    default_args=default_args,
    description='Extração inteligente baseada em byte offset (Marca d\'água)',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
)

def fetch_and_ingest(**kwargs):
   
    execution_date = kwargs['logical_date']
    data_str = execution_date.strftime('%Y-%m-%d')
    
    
    remote_folder = "/home/victor/edgelab/leituras"
    file_name = f"anomalias_{data_str}.jsonl"
    remote_path = f"{remote_folder}/{file_name}"
    
    
    offset_key = f"offset_anomalias_{data_str}"
    current_offset = int(Variable.get(offset_key, default_var=0))
    
    print(f"Buscando metadados do arquivo: {remote_path}")
    print(f"Ponteiro atual (Marca d'água): {current_offset} bytes")

    
    ssh_hook = SSHHook(ssh_conn_id='edge_device_ssh')
    
    try:
        sftp_client = ssh_hook.get_conn().open_sftp()
    except Exception as e:
        print(f"Falha ao conectar via SFTP. O dispositivo Edge pode estar offline: {e}")
        return

    
    try:
        file_stat = sftp_client.stat(remote_path)
        file_size = file_stat.st_size
        print(f"Tamanho total do arquivo remoto agora: {file_size} bytes")
    except IOError:
        print(f"O arquivo {file_name} ainda não foi criado hoje. Nenhuma anomalia ocorreu.")
        sftp_client.close()
        return

    
    if file_size == current_offset:
        print("Nenhuma anomalia nova. O arquivo não cresceu desde a última leitura.")
        sftp_client.close()
        return
    elif file_size < current_offset:
        print("Atenção: Arquivo remoto encolheu! O ponteiro será zerado.")
        current_offset = 0

    
    print(f"Baixando payload a partir do byte {current_offset}...")
    remote_file = sftp_client.file(remote_path, 'r')
    remote_file.seek(current_offset)
    
    
    new_data = remote_file.read().decode('utf-8')
    remote_file.close()
    sftp_client.close()

    if not new_data.strip():
        print("Nenhum texto extraído.")
        return

    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    
    linhas = new_data.strip().split('\n')
    registros_inseridos = 0

    for linha in linhas:
        if not linha.strip():
            continue
            
        try:
            
            json_obj = json.loads(linha)
            
            
            sql = "INSERT INTO vibra_alarms (alarm) VALUES (%s)"
            pg_hook.run(sql, parameters=(json.dumps(json_obj),))
            registros_inseridos += 1
            
        except Exception as e:
            print(f"Falha de integridade ao parsear a linha JSON: {linha}. Erro: {e}")

    
    print(f"Sucesso! {registros_inseridos} novos registros de vibração gravados no Data Lake.")
    Variable.set(offset_key, file_size)
    print(f"Novo ponteiro salvo no Airflow: {file_size} bytes.")


processa_anomalias = PythonOperator(
    task_id='fetch_and_ingest_anomalies_by_byte',
    python_callable=fetch_and_ingest,
    dag=dag,
)