"""
Healtcheck Huawei: sumarizado de rutas OSPF
"""
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import re
import pandas as pd
from airflow.hooks.base_hook import BaseHook
import ansible_runner
import textfsm
import mysql.connector
from lib.L_teco_db import *

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
#Argumentos
default_args = {
    'owner': 'transporte',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['tambo_core@teco.com.ar'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'provide_context': True,
    'dag_type': 'custom'
}

#DAG
dag = DAG(
dag_id= DAG_ID, 
    schedule_interval= None, 
    default_args=default_args
)

#Función para habilitar el código del DAG en Sphinx
def doc_sphinx():
    pass

#Variables
path_ne_hc = "/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP"

def pareso_y_registro_disp_ospf(mysql_conn,credenciales_mysql,database,PATH_HC_NE,**kwargs):

    """
    # obtengo variables xcom del contexto
    task_id = "get_id"
    ti = kwargs['ti']
    ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
    nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
    user = ti.xcom_pull(key="user", task_ids=[task_id])[0]
    """
    
    usrsql = credenciales_mysql.login
    pswsql = credenciales_mysql.password
    mysql_host = credenciales_mysql.password
    mysql_host = "10.247.2.42"

    mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
    mycursor = mydb.cursor()

    # hardcodeo las variables para probar
    nodo = "BEL5MU"
    ID = 100
    user = "Eze_559950"

    # archivo con output obtenido de equipo
    output_txt = PATH_HC_NE + "/" + nodo + "/output/HUAWEI_" + nodo + "_ospf.txt"

    # archivo con el template txtfsm para el parseo del output
    template = "/usr/local/tambo/cels/cel_transporte/airflow2/dags/HC_Huawei_EAP/templates/parser_disp_ospf_huawei.fsm"
    #template = "/usr/local/airflow/dags/cel_transporte/HC_NE_cisco/templates/parser_show_ospf_cisco.fsm"

    with open(output_txt) as o_f, open(template) as t_f:

        re_table = textfsm.TextFSM(t_f) # inicializo el parser con el template
        result = re_table.ParseText(o_f.read()) # aplico el parser al texto del output del equipo
        print(result)

        """
        for row in result:
            #Proceso el resultado y el header para convertir los array del resultado en diccionarios
            dict_result = [dict(zip(re_table.header, pr)) for pr in result]

        for x in dict_result:

            if str(x['route_source']) != "":
                origen_rutas = str(x['route_source'])
                cnt_rutas_act = x['routes']
                cnt_rutas_bck = x['backup']
                memory_rutas = x['memory']

                print("Origen de las rutas: " + origen_rutas)
                print("Cantidad de rutas activas: " + str(cnt_rutas_act))
                print("Cantidad de rutas backup: " + str(cnt_rutas_bck))
                print("Memoria ocupada por las rutas: " + str(memory_rutas))

            #Inserto los valores en la base de datos
            
            sql = "INSERT INTO HC_NE_cisco_ospf_routes (nodo,ID,user,origen_rutas,cnt_rutas_act,cnt_rutas_bck,memory_rutas) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE cnt_rutas_act=%s"
            val = (nodo,ID,user,origen_rutas,cnt_rutas_act,cnt_rutas_bck,memory_rutas,cnt_rutas_act)

            mycursor.execute(sql, val)
            mydb.commit()
        """

#Tareas

#Llamado al Ansible desde Python
def _teco_ansible_runner(pbook_dir,playbook,connection,inventory,dict_extravars,**kwargs):
    extravars_to_add = dict(                                                                  
        ansible_user=BaseHook.get_connection(connection).login,           
        ansible_password=BaseHook.get_connection(connection).password)
    dict_extravars.update(extravars_to_add)
    r = ansible_runner.run(private_data_dir=pbook_dir, playbook=playbook, inventory=inventory, extravars=dict_extravars, rotate_artifacts=20, artifact_dir='/usr/local/tambo/files/cels/cel_core/tmp/artifacts/')
    print(str(r))

#Conexion con Ansible
teco_ansible_runner = PythonOperator(
        task_id="teco_ansible_runner",
        python_callable=_teco_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_Huawei_EAP/ansible/',
            'playbook':'display_ip_routing-table_statistics_EA.yaml',
            'connection':'credenciales_equipos',
            'inventory':'/io/cel_core/per/host_ansible/inventario/inventory_grafos',
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )

# parseo e insercion en BD de info
pareso_y_registro_disp_ospf = PythonOperator(
        task_id="pareso_y_registro_disp_ospf",
        python_callable=pareso_y_registro_disp_ospf,
        op_kwargs={"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos'),'PATH_HC_NE': path_ne_hc},
        dag=dag
    )

#Secuencia
teco_ansible_runner >> pareso_y_registro_disp_ospf