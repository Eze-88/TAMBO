"""
    Healt Check de NE Huawei

"""

# import defaults
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from teco_ansible.operators.tecoAnsibleOperator import tecoCallAnsible
from datetime import datetime, timedelta

from time import sleep
from ttp import ttp
import json
import re
import shutil

from lib.teco_events import *
import glob

#import pymongo para formio custom
import pymongo
from bson.objectid import ObjectId

#xcom
from lib.teco_data_management import push_data

#parser txtfsm
import textfsm

#mysql
import mysql.connector
from lib.L_teco_db import * 
from time import sleep

#branch
from airflow.operators.python_operator import BranchPythonOperator

#ansible runner
import ansible_runner
from airflow.hooks.base_hook import BaseHook

#diff lib
import difflib

#Formio
from lib.tambo import *

#paramiko
import paramiko
import telnetlib
import time

####### test
#import os
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from lib.teco_data_management import *
# from teco_mae_operator.operators.tecoMaeOperator import TecoMaeOperator
# from datetime import datetime, timedelta
# from lib.L_teco_db import insert_influxdb

########################


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
#arg
default_args = {
    'owner': 'core',
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
            
#dag
dag = DAG(
dag_id= DAG_ID, 
    schedule_interval= None, 
    default_args=default_args
)


#función para habilitar el código del DAG en Sphinx
def doc_sphinx():
    pass

## funcion generada por Lucas afijo para obtencion de info formio en base a submision_id o nombre formulario (todos los collection)
def read_mongo(id_submit = None, name_form = None ,db= None ,collection = None, host = None, port = 27017):
    mCli = pymongo.MongoClient(host,port)
    #SETEO la DB a USAR : formio
    mDb = mCli[db]
    #SETEO la colección: submissions
    mCollection = mDb[collection]
    data=[]
    if id_submit != None and name_form == None:
        try:
            #Filtro el documento dentro de la coleción que tiene la key "title" con valor "inventario_cmts"
            for doc in mCollection.find({"_id":ObjectId(id_submit)}):
              data.append(doc)  
            result = data
        except:
            result = data
    if id_submit == None and name_form != None:
        try:
            #Filtro el documento dentro de la coleción que tiene la key "title" con valor "inventario_cmts"
            for doc in mCollection.find({"title": name_form}):
              data.append(doc)  
            result = data
        except:
            result = data
    print(result)
    return  result

##########################################
# Funciones
##########################################

def _incrementar_progreso(mysql_conn,credenciales_mysql,database,**kwargs):


    hitos = 2
    incremento = round((100/hitos)-5) # le resto 5% porque ese cinco se asigna apenas arranca (inicio no cuenta como hito)

    task_id = "get_id"
    ti = kwargs['ti']
    ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
    nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
    user = ti.xcom_pull(key="user", task_ids=[task_id])[0]


    usrsql = credenciales_mysql.login
    pswsql = credenciales_mysql.password
    #mysql_host = credenciales_mysql.host
    mysql_host = "10.247.2.42"

    mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
    mycursor = mydb.cursor()

    # rescato progreso
    sql = "SELECT progreso from HC_NE_index where nodo = '" +str(nodo) + "' and user = '"+ str(user) + "' and ID = '"+str(ID)+"'"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()

    progreso_current = round(myresult[0][0])
    progreso_current += incremento

    print("progreso:" + str(progreso_current))

    if progreso_current >= 95:   # si me pase o llegue casi, redondeo a 100
        progreso_current = 100

    #actualizo progreso
    sql = "UPDATE HC_NE_index SET progreso = '"+ str(progreso_current) +"' where nodo = '" +str(nodo) + "' and user = '"+ str(user) + "' and ID = '"+str(ID)+"'"
    mycursor.execute(sql)
    mydb.commit()

    if progreso_current == 100:
        print("Finalizo el health check ::::::::::::::")
        sql = "UPDATE HC_NE_index SET estado = 'Finalizado', cod_estado = '20' where nodo = '" +str(nodo) + "' and user = '"+ str(user) + "' and ID = '"+str(ID)+"'"
        mycursor.execute(sql)
        mydb.commit()
        #Borro outputs de corrida, la carpeta completa !!!!!!!
        try:
            output_path = "/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP/"+nodo+"/output"
            shutil.rmtree(output_path)
        except:
            print("WARNING: No se pudo borrar directorios output temporales, segun opciones elegidas puede eno haber sido creado")
        # try:
        #     output_path = "/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP/"+nodo+"/prepost"
        #     shutil.rmtree(output_path)
        # except:
        #     print("WARNING: No se pudo borrar directorio prepost temporal, segun opciones elegidas puede eno haber sido creado")


# funcion para obtener el id del health-check, solo lo asigna si pasaron x minutos desde ultima ejecucion de health check en ese nodo
def hc_ne_get_id(mysql_conn,credenciales_mysql,database,**kwargs):
    


        usrsql = credenciales_mysql.login
        pswsql = credenciales_mysql.password
        #mysql_host = credenciales_mysql.password
        mysql_host = "10.247.2.42"
        #mysql_db = "transporte"


        #####################################
        #!!!!!!!!  harcodeo input formio (LEE FORMIO ACA)
        # nodo = "BEL5MU"
        # user = "esteban33"
        
        #####################################

        """
        try:
            modo = kwargs['dag_run'].conf['modo']
            submit_id = kwargs['dag_run'].conf['submit_id']
        except:
            modo = "formio" # no se invoco por unificado ni por cron, usa el formulario original por vendor
        """


        ############ FORMIO input  #################3
        modo = "unificado"
        log={}
#<-----------------
        if modo == "formio":
            print("Input via formio legacy")
            log = TAMBO.GetForms(DAG_ID)

            array_chk = []
            if log["interfaces"] :
                array_chk.append("Ansible_juniper_interfaces")
            if log["logs"] :
                array_chk.append("Ansible_juniper_logs")
            if log["nivelesop"] :
                array_chk.append("Ansible_juniper_optica")
            if log["route_reflector"] :
                array_chk.append("Ansible_juniper_rr_totals")
            if log["prepost"] :
                array_chk.append("Ansible_juniper_prepost")

            # if log["temperatura"] :
            #     array_chk.append("Ansible_temperatura")

            if log["protocolos"] :
                array_chk.append("Ansible_juniper_ospf_nei")

        elif modo == "unificado":
            print("Input via formulario unificado y selector")
            #data_submit = read_mongo(id_submit=submit_id,name_form=None,db="formio",collection="submissions",host="tambo_mongodb")[0]
            # log["name_ne"] = data_submit.get('data').get('name_ne')
            # log["label"] = data_submit.get('data').get('label')

            nodo = "ROA2.HOR1"
            user = "test_eze"

            log["interfaces"] = False  ## hardcodeado prueba
            log["logs"] = False
            log["nivelesop"] = True
            log["route_reflector"] = False
            log["prepost"] = False
            log["temperatura"] = False
            log["protocolos"] = False

            # log["interfaces"] = data_submit.get('data').get('interfaces')
            # log["logs"] = data_submit.get('data').get('logs')
            # log["nivelesop"] = data_submit.get('data').get('nivelesop')
            # log["route_reflector"] = data_submit.get('data').get('route_reflector')
            # log["prepost"] = data_submit.get('data').get('prepost')
            # log["temperatura"] = data_submit.get('data').get('temperatura')
            # log["protocolos"] = data_submit.get('data').get('protocolos')


            array_chk = []
            if log["interfaces"] :
                array_chk.append("Ansible_juniper_interfaces")
            if log["logs"] :
                array_chk.append("Ansible_juniper_logs")
            if log["nivelesop"] :
                array_chk.append("Ansible_juniper_optica")
            if log["route_reflector"] :
                array_chk.append("Ansible_juniper_rr_totals")
            if log["prepost"] :
                array_chk.append("Ansible_juniper_prepost")

            # if log["temperatura"] :
            #     array_chk.append("Ansible_temperatura")

            if log["protocolos"] :
                array_chk.append("Ansible_juniper_ospf_nei")

        elif modo == "cron":
            print("Input via ejecucion scheduleada masiva")
            log["name_ne"] = kwargs['dag_run'].conf['nodo_cron']
            log["label"] = "TAMBO" # label a usar para el croneo

            log["interfaces"] = True
            log["logs"] = True
            log["nivelesop"] = True
            log["route_reflector"] = False # esta info no debe registrarse en el croneado para no estresar RR
            log["prepost"] = True
            log["temperatura"] = True
            log["protocolos"] = True

            array_chk = []
            if log["interfaces"] :
                array_chk.append("Ansible_juniper_interfaces")
            if log["logs"] :
                array_chk.append("Ansible_juniper_logs")
            if log["nivelesop"] :
                array_chk.append("Ansible_juniper_optica")
            if log["route_reflector"] :
                array_chk.append("Ansible_juniper_rr_totals")
            if log["prepost"] :
                array_chk.append("Ansible_juniper_prepost")

            # if log["temperatura"] :
            #     array_chk.append("Ansible_temperatura")

            if log["protocolos"] :
                array_chk.append("Ansible_juniper_ospf_nei")

        else:
            print("ERROR::: Modo de ejecucion desconocido")

        #nodo = (log["name_ne"]).upper()
        #user = (log["label"]).upper()

        print("nodo::::"+str(nodo))
        print("label:::"+str(user))
        """
        try:
            print(":::modo:" +modo)
            print(":::submit_id:" + str(submit_id))
        except:
            print(":::no tengo sumbitID")
        """


#<------------------

        # chk_nivelesop = (log["nivelesop"])
        # chk_temperatura = (log["temperatura"])
        # chk_recursosint = (log["recursosint"])

        # chk_conectividad = (log["conectividad"])
        # chk_configuracion = (log["configuracion"])
        # chk_prepost = (log["prepost"])  

        key = "chequeos"
        value = array_chk          
        push_data(kwargs, key, value)

        ###################################

        #print("::::::::::::::::: Instancio la conexion a la BD MAriadb")

        mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
        mycursor = mydb.cursor()

        sql = "SELECT nodo,ID,TIMESTAMP,user,cod_estado from HC_NE_index where nodo = '" +nodo +"'  ORDER BY TIMESTAMP DESC LIMIT 1 ;" 

        print("sql select: "+sql)
        mycursor.execute(sql)
        myresult = mycursor.fetchall()

        print("myresult "+str(myresult))

        #Registro dates actual y registrado 
        timenow_date = datetime.now()
        timenow = '{}'.format(datetime.now().strftime('/%Y%m/%d %H:%M:%S'))

        try:
            timelast = myresult[0][2]
            cod_estado = myresult[0][4]
            resta = timenow_date-timelast
            diff = resta.total_seconds() / 60
        except:    #  seguramente es la prier entrada para ese nodo/user, seteo variables para que proceda
            cod_estado = 0
            diff = 999


        #obtengo la diferencia en minutos 


        print("diferencia entre request al nodo (en minutos) : "+ str(diff))
        print("diffff:"+ str(diff))
        #cod_estado 10 significa en Progereso
        timeout = 15 # o si ya pasaron 15 minutos la doy por muerta
        if cod_estado != 10 or diff > timeout : # hardcodeo diff minima para que no controle y permita siempre!!!!
            print("Entrego ID:")

            if cod_estado != 0: # hay mediciones de este nodo , asique necesito ultimo ID de la rama si es que existe, sino inicio rama
                sql = "SELECT nodo,ID,TIMESTAMP,user from HC_NE_index where nodo = '" +nodo + "' and user = '"+ user + "' and ID = (SELECT MAX(ID) from HC_NE_index where nodo = '" +nodo + "' and user = '"+ user +"') ;" 

                mycursor.execute(sql)
                myresult = mycursor.fetchall()

                # me fijo si ya exist rama para continuar id o si arranco nueva, si no encontro 
                try:
                    last_ID = myresult[0][1]
                    new_ID = myresult[0][1] + 1
                except:
                    last_ID = 1
                    new_ID = 1                    


            else:
                last_ID = 1
                new_ID = 1

            #######  genero entrada nueva con ID nuevo !!!!!!!!!!!!!

            link = "http://10.247.2.42/grafana/d/C8M_Vkh7m/health_check_ne?orgId=1&var-nodo="+str(nodo)+"&var-user="+str(user)+"&var-ID_pre="+str(last_ID)+"&var-ID_post="+str(new_ID)
            estado = "En Progreso"
            cod_estado = 10
            progreso = 5
            TIMESTAMP = timenow_date

            print("INSERT new ID:" + str(new_ID))
            sql = "INSERT INTO HC_NE_index (nodo,ID,user,link,estado,cod_estado,progreso,TIMESTAMP) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE estado=%s"
            val = (nodo,new_ID,user,link,estado,str(cod_estado),str(progreso),TIMESTAMP,"Error")

            mycursor.execute(sql, val)
            mydb.commit()



            #pusheo data al entorno de airflow
            key = "ID"
            value = new_ID          
            push_data(kwargs, key, value)

            key = "nodo"
            value = nodo.upper()          
            push_data(kwargs, key, value)

            key = "user"
            value = user          
            push_data(kwargs, key, value)



        else:
            print ("HEalth check en curso - reintente mas tarde")

            key = "ID"
            value = 0          
            push_data(kwargs, key, value)

            key = "nodo"
            value = nodo.upper()          
            push_data(kwargs, key, value)

            key = "user"
            value = user          
            push_data(kwargs, key, value)

            return 0 # no entrego id


####
def _elegir_camino1(**kwargs):

    # obtengo variables xcom del contexto
    task_id = "get_id"
    ti = kwargs['ti']
    ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
    nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
    user = ti.xcom_pull(key="user", task_ids=[task_id])[0]

    if ID != 0: # Continuo
        return 'Ansible_juniper_display_health'
    else: # Interrumpo el flujo dado que hay otro healthcheck reciente generado sobre mismo nodo
        return 'exit_nodo_ocupado'


def _elegir_camino2(**kwargs):

    try:
        # obtengo variables xcom del contexto
        task_id = "get_id"
        ti = kwargs['ti']
        ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
        nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
        user = ti.xcom_pull(key="user", task_ids=[task_id])[0]
        task_list = ti.xcom_pull(key="chequeos", task_ids=[task_id])[0]

        task_id = "parseo_e_insercion_juniper_en_bd_info_cpu_memoria"
        max_mem = ti.xcom_pull(key="max_mem", task_ids=[task_id])[0]
        max_cpu = ti.xcom_pull(key="max_cpu", task_ids=[task_id])[0]

        max_mem = 50
        max_cpu = 50

        #### hardcodeo:
        ## armo lista de tasks sobre los que continuare ejecucion

        # task_list = []
        # task_list.append("Ansible_display_optical")
        # task_list.append("Ansible_display_logs")
        # task_list.append("Ansible_display_prepost")
        # task_list.append("Ansible_display_interface")
        # task_list.append("Ansible_rr_totals")

    #exit_nodo_sobrecargado

        if float(max_mem) > 90.0 or float(max_cpu) > 90.0:
            #nodo_sobrecargado
            return 'exit_nodo_sobrecargado'
        elif float(max_cpu) < 0.0 or float(max_mem) < 0.0:
            #no pudo leer o parsear mem/cpu
            print("no pudo leer o parsear mem/cpu")
            return 'exit_abort'
        else:
            if len(task_list) > 0 : # Continuo
                return task_list
            else: # No se eligio ninguna opcion, tomo una por default
                return 'Ansible_juniper_prepost'
    except:
        print("fallo al obtener cpu/mem de XCOM")
        return 'exit_abort'


def _exit_nodo_ocupado(mysql_conn,credenciales_mysql,database,**kwargs):

    # obtengo variables xcom del contexto
    task_id = "get_id"
    ti = kwargs['ti']
    ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
    nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
    user = ti.xcom_pull(key="user", task_ids=[task_id])[0]

    usrsql = credenciales_mysql.login
    pswsql = credenciales_mysql.password
    mysql_host = credenciales_mysql.password
    mysql_host = "10.247.2.42"


    mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
    mycursor = mydb.cursor()


    print("#################################################################################")
    print("#### Se ejecuto recientemente un health check sobre el nodo seleccionado       ##")    
    print("#### Para no sobrecargar el equipo aguarde unos minutos e intentelo nuevamente ##")    
    print("#################################################################################")


    timenow_date = '{}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    progreso = 0
    estado = "Ignored"
    cod_estado = 40
    TIMESTAMP = timenow_date
    link = "Ya existe una solicitud en curso"
    new_ID = 0

    sql = "INSERT INTO HC_NE_index (nodo,ID,user,link,estado,cod_estado,progreso,TIMESTAMP) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE estado=%s"
    val = (nodo,new_ID,user,link,estado,str(cod_estado),str(progreso),TIMESTAMP,"Ignorado")

    mycursor.execute(sql, val)
    mydb.commit()

def exit_abort(mysql_conn,credenciales_mysql,database,**kwargs):

    # obtengo variables xcom del contexto
    task_id = "get_id"
    ti = kwargs['ti']
    ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
    nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
    user = ti.xcom_pull(key="user", task_ids=[task_id])[0]

    usrsql = credenciales_mysql.login
    pswsql = credenciales_mysql.password
    mysql_host = credenciales_mysql.password
    mysql_host = "10.247.2.42"


    mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
    mycursor = mydb.cursor()

    print("#################################################################################")
    print("#### El nodo se encuentra con alta carga de recursos (cpu/memoria)             ##")    
    print("#### Se interrumpe secuencias automaticas, hasta normalizaerce el equipo       ##")    
    print("#################################################################################")


    timenow_date = '{}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    progreso = 0
    estado = "Abortada"
    cod_estado = 30
    TIMESTAMP = timenow_date
    link = "No se logro dialogo base hacia el equipo"



    print("Finalizo el health check como Abortado ::::::::::::::")
    sql = "UPDATE HC_NE_index SET estado = 'Finalizado', cod_estado = '30', link = '"+str(link)+"' where nodo = '" +str(nodo) + "' and user = '"+ str(user) + "' and ID = '"+str(ID)+"'"
    mycursor.execute(sql)
    mydb.commit()

def _exit_nodo_sobrecargado(mysql_conn,credenciales_mysql,database,**kwargs):

    # obtengo variables xcom del contexto
    task_id = "get_id"
    ti = kwargs['ti']
    ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
    nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
    user = ti.xcom_pull(key="user", task_ids=[task_id])[0]

    usrsql = credenciales_mysql.login
    pswsql = credenciales_mysql.password
    mysql_host = credenciales_mysql.password
    mysql_host = "10.247.2.42"


    mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
    mycursor = mydb.cursor()


    print("#################################################################################")
    print("#### El nodo se encuentra con alta carga de recursos (cpu/memoria)             ##")    
    print("#### Se interrumpe secuencias automaticas, hasta normalizaerce el equipo       ##")    
    print("#################################################################################")


    timenow_date = '{}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    progreso = 0
    estado = "Abortada"
    cod_estado = 30
    TIMESTAMP = timenow_date
    link = "Se detecta alta carga de cpu/memoria en el nodo, interrumpcion preventiva del proceso"
    
    # new_ID = 0
    # sql = "INSERT INTO HC_NE_index (nodo,ID,user,link,estado,cod_estado,progreso,TIMESTAMP) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE estado=%s"
    # val = (nodo,new_ID,user,link,estado,str(cod_estado),str(progreso),TIMESTAMP,"Ignorado")
    # mycursor.execute(sql, val)
    # mydb.commit()

    print("Finalizo el health check como Abortado por alta carga de cpu ::::::::::::::")
    sql = "UPDATE HC_NE_index SET estado = 'Abortada', cod_estado = '30', link = '"+str(link)+"' where nodo = '" +str(nodo) + "' and user = '"+ str(user) + "' and ID = '"+str(ID)+"'"
    mycursor.execute(sql)
    mydb.commit()

##########################
# Funciones parseo e insercion custom
###########################



def parseo_e_insercion_juniper_en_bd_info_cpu_memoria(PATH_HC_NE,mysql_conn,credenciales_mysql,database,**kwargs):

    try:
        # obtengo variables xcom del contexto
        task_id = "get_id"
        ti = kwargs['ti']
        ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
        nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
        user = ti.xcom_pull(key="user", task_ids=[task_id])[0]


        usrsql = credenciales_mysql.login
        pswsql = credenciales_mysql.password
        mysql_host = credenciales_mysql.password
        mysql_host = "10.247.2.42" # hardcodeado durante transicion


        mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
        mycursor = mydb.cursor()

        ##### PARSEO INFO mem cpu de linecards ###
        # archivo con output obtenido de equipo
        output_txt = PATH_HC_NE + "/" + nodo + "/output/JUNIPER_" + nodo + "_show_chassis_fsm.txt"
        
        # archivo con el template txtfsm para el parseo del output
        template = "/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/templates/parser_show_chassis_fpc.fsm"

        with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
            re_table = textfsm.TextFSM(f) # inicializo el parser con el template
            result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

            print("Resultado crudo")
            print(result)

            print("Resultado")
            encabezado = re_table.header
            print(encabezado)


            #Proceso el resultado y el header para convertir los array del resultado en diccionarios
            dict_result = [dict(zip(re_table.header, pr)) for pr in result]

            max_cpu = 0.0
            max_mem = 0.0


            #Si no pudo obtener ningun registro del parseo reporto el warning
            if len(dict_result) == 0:
                print("parseo cpu y memoria dio vacio")
                warning = "No se pudo obtener informacion de CPU ni Memoria del equipo"
                tipo = "parseo"
                sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                val = (nodo,ID,user,warning,tipo)
                mycursor.execute(sql, val)
                mydb.commit()
                max_cpu = -1.0 ## seteo CPU negativo para  que aborte
                max_mem = -1.0 ## seteo mem  negativo para  que aborte 

            for x in dict_result:

                #registro maximo
                if float(x["memoria_buffer"]) > max_mem:
                    max_mem = float(x["memoria_buffer"])

                if float(x["memoria_heap"]) > max_mem:
                    max_mem = float(x["memoria_heap"])

                if float(x["cpu_total"]) > max_cpu:
                    max_cpu = float(x["cpu_total"])

                if float(x["cpu_interrupt"]) > max_cpu:
                    max_cpu = float(x["cpu_interrupt"])            

                ### INSERTO EN INFLUX DB ####

                ##### genero el registro a insertar en la base de datos influx

                nodo=nodo
                ID = ID
                user = user

                state = str(x["state"])
                slot =  int(x["slot"])
                temperatura =  int(x["temperatura"])
                ram =  int(x["ram"])                
                cpu_total =  int(x["cpu_total"])
                cpu_interrupt =  int(x["cpu_interrupt"])
                memoria_buffer =  int(x["memoria_buffer"])
                memoria_heap =  int(x["memoria_heap"])



                print("inserto en sql-------------------------------------")
                TIMESTAMP = datetime.now()
                sql = "INSERT INTO HC_NE_juniper_mem_cpu (TIMESTAMP,nodo,ID,user,slot,temperatura,ram,cpu_total,cpu_interrupt,memoria_buffer,memoria_heap,state) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                val = (TIMESTAMP,nodo,ID,user,slot,temperatura,ram,cpu_total,cpu_interrupt,memoria_buffer,memoria_heap,state)
                try:
                        mycursor.execute(sql, val)
                        mydb.commit()
                except:
                        #Si no pudo subir info a bd sql reporto el warning
                        print("No pudo cargarse la info de cpu y memoria en la BD")
                        warning = "Problemas con la BD - No pudo cargarse la info de CPU y memoria del equipo"
                        tipo = "bd"
                        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                        val = (nodo,ID,user,warning,tipo)
                        mycursor.execute(sql, val)
                        mydb.commit()


        ##### PARSEO INFO mem cpu de routing_engine ###
        # archivo con output obtenido de equipo
        output_txt = PATH_HC_NE + "/" + nodo + "/output/JUNIPER_" + nodo + "_show_chassis_re.txt"
        
        # archivo con el template txtfsm para el parseo del output
        template = "/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/templates/parser_show_chassis_re.fsm"

        with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
            re_table = textfsm.TextFSM(f) # inicializo el parser con el template
            result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

            print("Resultado crudo")
            print(result)

            print("Resultado")
            encabezado = re_table.header
            print(encabezado)


            #Proceso el resultado y el header para convertir los array del resultado en diccionarios
            dict_result = [dict(zip(re_table.header, pr)) for pr in result]

            max_cpu_re = 0.0
            max_mem_re = 0.0


            #Si no pudo obtener ningun registro del parseo reporto el warning
            if len(dict_result) == 0:
                print("parseo cpu y memoria de routing engine dio vacio")
                warning = "No se pudo obtener informacion de CPU ni Memoria de la routing engine"
                tipo = "parseo"
                sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                val = (nodo,ID,user,warning,tipo)
                mycursor.execute(sql, val)
                mydb.commit()
                max_cpu = -1.0 ## seteo CPU negativo para  que aborte


            for x in dict_result:

                #registro maximo
                if float(x["mem_percent"]) > max_mem_re:
                    max_mem_re =float(x["mem_percent"])

                if  (100.0 - float(x["cpu_percent"])) > max_cpu_re:
                    max_cpu_re = 100.0 - float(x["cpu_percent"])         

                re_rol = str(x["re_rol"])
                slot =  int(x["slot"])
                temp =  int(x["temp"])
                cpu_temp =  int(x["temp"])              
                mem_percent =  int(x["mem_percent"])
                cpu_percent =  100.0 - float(x["cpu_percent"])


                print("inserto en sql-------------------------------------")

                sql = "INSERT INTO HC_NE_juniper_mem_cpu_re (nodo,ID,user,slot,re_rol,temp,cpu_temp,mem_percent,cpu_percent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                val = (nodo,ID,user,slot,re_rol,temp,cpu_temp,mem_percent,cpu_percent)
                try:
                        mycursor.execute(sql, val)
                        mydb.commit()
                except:
                        #Si no pudo subir info a bd sql reporto el warning
                        print("No pudo cargarse la info de cpu y memoria de roting engine en la BD")
                        warning = "Problemas con la BD - No pudo cargarse la info de CPU y memoria del equipo"
                        tipo = "bd"
                        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                        val = (nodo,ID,user,warning,tipo)
                        mycursor.execute(sql, val)
                        mydb.commit()

        if max_mem >= 0:
            if max_mem_re > max_mem:
                max_mem = max_mem_re
        if max_cpu >= 0:
            if max_cpu_re > max_cpu:
                max_cpu = max_cpu_re

        # pusheo valores mem/cpu
        #### info a pushear:
        print("maxmem::::"+ str(max_mem))
        print("maxcpu::::"+str(max_cpu))
        key = "max_mem"
        value = max_mem
        push_data(kwargs, key, value)

        key = "max_cpu"
        value = max_cpu
        push_data(kwargs, key, value)

        ##### PARSEO INFO CON TXTFSM (para obtencion info de BGP)###
        # archivo con output obtenido de equipo
        output_txt = PATH_HC_NE + "/" + nodo + "/output/JUNIPER_" + nodo + "_bgp_info.txt"
        
        # archivo con el template txtfsm para el parseo del output
        template = "/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/templates/parser_show_bgp_info.fsm"

        with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
            re_table = textfsm.TextFSM(f) # inicializo el parser con el template
            result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

            print("Resultado crudo")
            print(result)

            print("Resultado")
            encabezado = re_table.header
            print(encabezado)


            #Proceso el resultado y el header para convertir los array del resultado en diccionarios
            dict_result = [dict(zip(re_table.header, pr)) for pr in result]

            # esta info no la subo a BD sino a XCOM:


            ## puede que el equipo no tenga BGP, entonces pongo asn y loopbacksource en "none"
            #### info a pushear:
            key = "asn"
            try:
                value = dict_result[0]["asn"]
            except:
                value = "none"
            #pusheo
            push_data(kwargs, key, value)

            #### info a pushear:
            key = "loopback"
            try:
                value = dict_result[0]["loopback"]
            except:
                value = "none"
                print("NO PUDO OBTENER INFORMACION BGP DEL NODO, es posible que dicho protocolo no este habilitado")
            #pusheo
            push_data(kwargs, key, value)

    except:
        print("fallo acceso/parseo del archivo de cpu/mem/loopack")
        warning = "No se pudo obtener informacion de cpu/memoria del equipo"
        tipo = "parseo"
        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
        val = (nodo,ID,user,warning,tipo)
        mycursor.execute(sql, val)
        mydb.commit()


def parseo_y_registro_juniper_de_info_logs(mysql_conn,credenciales_mysql,database,PATH_HC_NE,**kwargs):

    try:
        # obtengo variables xcom del contexto
        task_id = "get_id"
        ti = kwargs['ti']
        ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
        nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
        user = ti.xcom_pull(key="user", task_ids=[task_id])[0]

        usrsql = credenciales_mysql.login
        pswsql = credenciales_mysql.password
        mysql_host = credenciales_mysql.password
        mysql_host = "10.247.2.42"
        #mysql_db = "transporte"  <---  hardodeo productivo porqye testing y desarrollo no funcionan

        mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
        mycursor = mydb.cursor()

        ##### PARSEO INFO CON TXTFSM ###

        
        # archivo con el template txtfsm para el parseo del output
        template = "/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/templates/parser_juniper_logs.fsm"

        # archivo con output obtenido de equipo
        output_txt = PATH_HC_NE + "/" + nodo + "/output/JUNIPER_" + nodo + "_logs.txt"
        #print("logs output:::::" + output_txt)
        with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
                    re_table = textfsm.TextFSM(f) # inicializo el parser con el template
                    result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

                    # print("Resultado crudo")
                    # print(result)

                    print("Resultado")
                    encabezado = re_table.header
                    print(encabezado)


                    #Proceso el resultado y el header para convertir los array del resultado en diccionarios
                    dict_result = [dict(zip(re_table.header, pr)) for pr in result]

                    #print(str(dict_result))

                    #Si no pudo obtener ningun registro del parseo reporto el warning
                    if len(dict_result) == 0:
                        print("parseo logs dio vacio")
                        warning = "No obtuvo ningun registro de log del equipo"
                        tipo = "parseo"
                        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                        val = (nodo,ID,user,warning,tipo)
                        mycursor.execute(sql, val)
                        mydb.commit()

                    for x in dict_result:

                        print("fila:::: "+str(x))

                        mes= str(x["mes"])
                        dia= str(x["dia"])
                        hora= str(x["hora"])
                        texto= str(x["texto"])
                        trap = str(x["trap"])
                        elemento = str(x["elemento"])
                        proceso = str(x["proceso"])
                        full_trap = trap + "-" + texto

                        #Leo bd sql para barrer todos los patrones de logs criticos registrados en la base de conocimiento

                        sql = "SELECT patron,descripcion from HC_NE_logs_patrones"
                        mycursor.execute(sql)
                        myresult = mycursor.fetchall()

                        for pattern in myresult:

                            print("busco patron: "+ pattern[0])
                            r1 = re.search(pattern[0],full_trap)
                            descripcion = pattern[1]

                            # si hubo match registro log como critico y salgo del for
                            if r1:
                                # print("log: " + full_trap)
                                # print("-------hubo match -->"+str(r1) )


                                print("inserto en mysql---------------")
                                ##### Realizo la insercion del registro

                                sql = "INSERT INTO HC_NE_juniper_logs (nodo,ID,user,texto,trap,elemento,proceso,mes,descripcion,dia,hora) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE texto=%s,trap=%s"
                                val = (nodo,ID,user,texto,trap,elemento,proceso,mes,descripcion,dia,hora,texto,trap)

                                try:
                                    mycursor.execute(sql, val)
                                    mydb.commit()
                                except:
                                    #Si no pudo subir info a bd sql reporto el warning
                                    warning = "Problemas con la BD - No pudo cargarse la info de los logs del equipo"
                                    tipo = "bd"
                                    sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                                    val = (nodo,ID,user,warning,tipo)
                                    mycursor.execute(sql, val)
                                    mydb.commit()

                                break

    except:
        print("fallo acceso/parseo del archivo de logs")
        warning = "No se pudo obtener informacion de los logs del equipo"
        tipo = "parseo"
        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
        val = (nodo,ID,user,warning,tipo)
        mycursor.execute(sql, val)
        mydb.commit()

## pareo optica
def parseo_y_registro_juniper_optica(mysql_conn,credenciales_mysql,database,PATH_HC_NE,**kwargs):

    try:
        # obtengo variables xcom del contexto

        task_id = "get_id"
        ti = kwargs['ti']
        ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
        nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
        user = ti.xcom_pull(key="user", task_ids=[task_id])[0]

        usrsql = credenciales_mysql.login
        pswsql = credenciales_mysql.password
        mysql_host = credenciales_mysql.password
        mysql_host = "10.247.2.42"
        #mysql_db = "transporte"  <---  hardodeo productivo porqye testing y desarrollo no funcionan

        mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
        mycursor = mydb.cursor()


        encontro_info = False

        ##### PARSEO INFO CON TXTFSM ###

        #### REALIZO parseo y carga en BD para interfaces de 100G (et-)
        # archivo con el template txtfsm para el parseo del output
        template = "/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/templates/parser_juniper_optics.fsm"

        # archivo con output obtenido de equipo
        output_txt = PATH_HC_NE + "/" + nodo + "/output/JUNIPER_" + nodo + "_optics.txt"

        destino = "/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/templates/prueba_optics.txt"
        shutil.copy(output_txt,destino)

        #print("logs output:::::" + output_txt)
        with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
                    re_table = textfsm.TextFSM(f) # inicializo el parser con el template
                    result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

                    # print("Resultado crudo")
                    # print(result)

                    print("Resultado")
                    encabezado = re_table.header
                    print(encabezado)


                    #Proceso el resultado y el header para convertir los array del resultado en diccionarios
                    dict_result = [dict(zip(re_table.header, pr)) for pr in result]

                    print("Resultados 100G"+str(dict_result))
                    print(str(dict_result))

                    #Si no pudo obtener ningun registro del parseo reporto el warning
                    if len(dict_result) > 0:
                        encontro_info = True

                    for x in dict_result:

                        #print("fila:::: "+str(x))

                        interface = str(x["interface"])
                        OutputHighWarning = str(x["OutputHighWarning"])
                        OutputLowWarning = str(x["OutputLowWarning"])
                        RxHighWarning = str(x["RxHighWarning"])
                        RxLowWarning = str(x["RxLowWarning"])
                        lane = str(x["lane"])
                        tx_power_lane = str(x["tx_power_lane"])
                        rx_power_lane = str(x["rx_power_lane"])
                        amp = str(x["amp"])
                        grados = str(x["grados"])

                        #10G
                        OutputPower = ""
                        RxPower = ""


                        print("inserto en mysql---------------")
                        ##### Realizo la insercion del registro

                        sql = "INSERT INTO HC_NE_juniper_optics (nodo,ID,user,interface,OutputHighWarning,OutputLowWarning,RxHighWarning,lane,tx_power_lane,rx_power_lane,amp,grados,OutputPower,RxPower,RxLowWarning) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        val = (nodo,ID,user,interface,OutputHighWarning,OutputLowWarning,RxHighWarning,lane,tx_power_lane,rx_power_lane,amp,grados,OutputPower,RxPower,RxLowWarning)


                        try:
                            mycursor.execute(sql, val)
                            mydb.commit()
                        except:
                            #Si no pudo subir info a bd sql reporto el warning
                            print("Fallo insercion en BD de niveles optico 10g")
                            warning = "Problemas con la BD - No pudo cargarse la info de los niveles opticos de los puertos del equipo"
                            tipo = "bd"
                            sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                            val = (nodo,ID,user,warning,tipo)
                            mycursor.execute(sql, val)
                            mydb.commit()


        #### REALIZO parseo y carga en BD para interfaces de 10G (xe-)
        # archivo con el template txtfsm para el parseo del output
        template = "/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/templates/parser_juniper_optics10.fsm"

        # archivo con output obtenido de equipo
        output_txt = PATH_HC_NE + "/" + nodo + "/output/JUNIPER_" + nodo + "_optics.txt"
        #print("logs output:::::" + output_txt)
        with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
                    re_table = textfsm.TextFSM(f) # inicializo el parser con el template
                    result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

                    # print("Resultado crudo")
                    # print(result)

                    print("Resultado")
                    encabezado = re_table.header
                    print(encabezado)


                    #Proceso el resultado y el header para convertir los array del resultado en diccionarios
                    dict_result = [dict(zip(re_table.header, pr)) for pr in result]

                    print("opticas 10G")
                    print("Resultados 10G"+str(dict_result))

                    #Si no pudo obtener ningun registro del parseo reporto el warning
                    if len(dict_result) > 0:
                        encontro_info = True

                    for x in dict_result:

                        #print("fila:::: "+str(x))

                        #"100G"
                        interface = str(x["interface"])
                        OutputHighWarning = str(x["OutputHighWarning"])
                        OutputLowWarning = str(x["OutputLowWarning"])
                        RxHighWarning = str(x["RxHighWarning"])
                        RxLowWarning = str(x["RxLowWarning"])
                        lane = ""
                        tx_power_lane = ""
                        rx_power_lane = ""
                        amp = ""
                        grados = ""

                        #10G
                        OutputPower = str(x["OutputPower"])
                        RxPower = str(x["RxPower"])



                        print("inserto en mysql---------------")
                        ##### Realizo la insercion del registro

                        sql = "INSERT INTO HC_NE_juniper_optics (nodo,ID,user,interface,OutputHighWarning,OutputLowWarning,RxHighWarning,lane,tx_power_lane,rx_power_lane,amp,grados,OutputPower,RxPower,RxLowWarning) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        val = (nodo,ID,user,interface,OutputHighWarning,OutputLowWarning,RxHighWarning,lane,tx_power_lane,rx_power_lane,amp,grados,OutputPower,RxPower,RxLowWarning)

                        try:
                            mycursor.execute(sql, val)
                            mydb.commit()
                        except:
                            #Si no pudo subir info a bd sql reporto el warning
                            warning = "Problemas con la BD - No pudo cargarse la info de los niveles opticos de los puertos del equipo"
                            tipo = "bd"
                            sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                            val = (nodo,ID,user,warning,tipo)
                            mycursor.execute(sql, val)
                            mydb.commit()



        #Si no pudo obtener ningun registro del parseo reporto el warning
        if not encontro_info:
            print("parseo opticas dieron vacios")
            warning = "No obtuvo ningun registro de potencias opticas"
            tipo = "parseo"
            sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
            val = (nodo,ID,user,warning,tipo)
            mycursor.execute(sql, val)
            mydb.commit()



    except:
        print("fallo acceso/parseo del archivo de logs")
        warning = "No se pudo obtener informacion de los logs del equipo"
        tipo = "parseo"
        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
        val = (nodo,ID,user,warning,tipo)
        mycursor.execute(sql, val)
        mydb.commit()


####  PArseo comando prefix totales en RR
def parseo_e_insercion_en_bd_rr_totales(mysql_conn,credenciales_mysql,database,PATH_HC_NE,**kwargs):

    try:
        # obtengo variables xcom del contexto
        task_id = "get_id"
        ti = kwargs['ti']
        ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
        nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
        user = ti.xcom_pull(key="user", task_ids=[task_id])[0]

        try:
            task_id = "parseo_e_insercion_juniper_en_bd_info_cpu_memoria"
            asn = ti.xcom_pull(key="asn", task_ids=[task_id])[0]
        except:
            print("::: Warning, no pudo obtenerse ASN")

        usrsql = credenciales_mysql.login
        pswsql = credenciales_mysql.password
        mysql_host = credenciales_mysql.password
        mysql_host = "10.247.2.42"
        #mysql_db = "transporte"  <---  hardodeo productivo porqye testing y desarrollo no funcionan

        mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
        mycursor = mydb.cursor()

        ##### PARSEO INFO CON TXTFSM ###


        # archivo con output obtenido de equipo
        output_txt = PATH_HC_NE + "/" + nodo + "/output/JUNIPER_" + nodo + "_rr.txt"
        
        # archivo con el template txtfsm para el parseo del output
        template = "/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/templates/parser_display_rr_totals.fsm"

        with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
            re_table = textfsm.TextFSM(f) # inicializo el parser con el template
            result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

            print("Resultado crudo")
            print(result)

            print("Resultado")
            encabezado = re_table.header
            print(encabezado)


            #Proceso el resultado y el header para convertir los array del resultado en diccionarios
            dict_result = [dict(zip(re_table.header, pr)) for pr in result]

            #Si no pudo obtener ningun registro del parseo reporto el warning
            if len(dict_result) == 0 and asn == "7303":
                print("parse bgp de los RR dio vacio")
                warning = "No se pudo obtener informacion de los Route Reflectors del equipo"
                tipo = "parseo"
                sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                val = (nodo,ID,user,warning,tipo)
                mycursor.execute(sql, val)
                mydb.commit()

            for x in dict_result:
                ### INSERTO EN mysql ####

                
                        rr = str(x["rr"])
                        extremo = "rr"
                        address_family = str(x["address_family"])
                        pfx_recibidos = int(x["total"])


                        sql = "INSERT INTO HC_NE_route_reflector (nodo,ID,user,address_family,pfx_recibidos,extremo,rr) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE pfx_recibidos=%s"
                        val = (nodo,ID,user,address_family,pfx_recibidos,extremo,rr,pfx_recibidos)
                        try:
                            mycursor.execute(sql, val)
                            mydb.commit()
                        except:
                            #Si no pudo subir info a bd sql reporto el warning
                            warning = "Problemas con la BD - No pudo cargarse la info sobre los Route Reflectors del equipo"
                            tipo = "bd"
                            sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                            val = (nodo,ID,user,warning,tipo)
                            mycursor.execute(sql, val)
                            mydb.commit()

        ## Borro archivo output  (ya lo procese ademas en este caso hago add en el ansible asique es mandatorio borrarlo)
        with open(output_txt, "w") as f_o:  # abro los dos archivos, el del template y el del output del equipo
            f_o.write("")
    except:
        print("fallo acceso/parseo del archivo de rr")
        warning = "No se pudo obtener informacion de los Route Reflectors"
        tipo = "parseo"
        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
        val = (nodo,ID,user,warning,tipo)
        mycursor.execute(sql, val)
        mydb.commit()


## diff y generacion html prepost (shows y config)

def _diff_y_renderizado_prepost(mysql_conn,credenciales_mysql,database,PATH_HC_NE,**kwargs):

    try:
        # obtengo variables xcom del contexto
        task_id = "get_id"
        ti = kwargs['ti']
        ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
        nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
        user = ti.xcom_pull(key="user", task_ids=[task_id])[0]

        usrsql = credenciales_mysql.login
        pswsql = credenciales_mysql.password
        mysql_host = credenciales_mysql.password
        mysql_host = "10.247.2.42"
        #mysql_db = "transporte"  <---  hardodeo productivo porqye testing y desarrollo no funcionan

        mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
        mycursor = mydb.cursor()

        ######################################################################################
        #### proceso  diferencia entre shows pre/post#########################################
        ######################################################################################

        #nodo+"_prepost_"+user+"_"+ID+".log"

        PATH_WWW = "/usr/local/grafos/esteban/www/prepost"

        # archivo con output obtenido de equipo
        ID_pre = int(ID) - 1
        if ID == 1: # si es el primero comparalo consigomismo
            ID_pre = 1
        input_log_post = PATH_HC_NE + "/" + nodo+ "/prepost/"+ nodo +"_prepost_"+str(user)+"_"+str(ID)+".log"
        input_log_pre = PATH_HC_NE + "/" + nodo+ "/prepost/"+ nodo +"_prepost_"+str(user)+"_"+str(ID_pre)+".log"
        output_html = PATH_WWW + "/" + nodo + "_prepost_" +str(user)+"_"+str(ID)+".html"

        # genero html diff:

        #leo archivo target
        try:
            with open(input_log_post) as t:
                target_lines=t.readlines()
        except FileNotFoundError:
                raise FileNotFoundError("Post file path is not correct")
        #leo cada uno de los archivos source

        #leo archivo source
        try:
            with open(input_log_pre) as s:
                source_lines=s.readlines()
        except FileNotFoundError:
                raise FileNotFoundError("Pre file path is not correct")


        # with open(arch_conf) as file_input: 
        # 	conf_data = file_input.readlines()
        # 	for linea_confg in conf_data:
        # 		linea_confg=linea_confg.strip()
                
        #genero html que muestre diferencias entre el source y target
        #/wrap column original 60 o 70 mejor
        diff=difflib.HtmlDiff(wrapcolumn=60).make_file(source_lines, target_lines, "PRE_POST ", output_html)
        #grabo archivo de diferencias html

        try:
            with open(output_html, 'w') as html:
                html.write(diff)
            # ya genere diff asique puedo borrar archivo pre, en la proxima ejecucion usara el nuevo post como pre
            os.remove(input_log_pre)
        except FileNotFoundError:
            raise FileNotFoundError("Output pre_post dir path is not correct: {0}".format(output_dir))


        ######################################################################################
        ####        proceso  diferencia entre configs       ##################################
        ######################################################################################
        
        #nodo+"_prepost_"+user+"_"+ID+".log"

        PATH_WWW = "/usr/local/grafos/esteban/www/prepost"

        # archivo con output obtenido de equipo

        input_log_post = PATH_HC_NE + "/" + nodo+ "/config/"+ nodo +"_prepost_"+str(user)+"_"+str(ID)+".log"
        input_log_pre = PATH_HC_NE + "/" + nodo+ "/config/"+ nodo +"_prepost_"+str(user)+"_"+str(ID_pre)+".log"
        output_html = PATH_WWW + "/" + nodo + "_diffconfig_" +str(user)+"_"+str(ID)+".html"

        # genero html diff:

        #leo archivo target
        try:
            with open(input_log_post,encoding='utf-8', errors='ignore') as t:
                target_lines=t.readlines()
        except FileNotFoundError:
                raise FileNotFoundError("Post file path is not correct")
        #leo cada uno de los archivos source

        #leo archivo source
        try:
            with open(input_log_pre,encoding='utf-8', errors='ignore') as s:
                source_lines=s.readlines()
        except FileNotFoundError:
                raise FileNotFoundError("Pre file path is not correct")


        # with open(arch_conf) as file_input: 
        # 	conf_data = file_input.readlines()
        # 	for linea_confg in conf_data:
        # 		linea_confg=linea_confg.strip()
                
        #genero html que muestre diferencias entre el source y target
        #/wrap column original 60 o 70 mejor
        diff=difflib.HtmlDiff(wrapcolumn=60).make_file(source_lines, target_lines, "PRE_POST ", output_html)
        #grabo archivo de diferencias html

        try:
            with open(output_html, 'w') as html:
                html.write(diff)
            # ya genere diff asique puedo borrar archivo pre, en la proxima ejecucion usara el nuevo post como pre
            os.remove(input_log_pre)
        except FileNotFoundError:
            raise FileNotFoundError("Output pre_post dir path is not correct: {0}".format(output_dir))

    except:
        print("fallo acceso/parseo del archivo de pre_post")
        warning = "No se pudo registrar archivos de PRE/POST"
        tipo = "parseo"
        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
        val = (nodo,ID,user,warning,tipo)
        mycursor.execute(sql, val)
        mydb.commit()


def hc_ne_ansible_runner(pbook_dir,playbook,inventory,connection,dict_extravars,**kwargs):

    # obtengo variables xcom del contexto
    task_id = "get_id"
    ti = kwargs['ti']
    ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
    nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
    user = ti.xcom_pull(key="user", task_ids=[task_id])[0]

    loopback_peer = "" # puede que en esta instancia todavia no subieras a xcom este atributo, asique lo inicializo y luego pruebo con try
    try:
        task_id = "parseo_e_insercion_juniper_en_bd_info_cpu_memoria"
        loopback = ti.xcom_pull(key="loopback", task_ids=[task_id])[0]
    except:
        print("no pudo tomar Loopback")
        pass # puede que en esta instancia todavia no subieras a xcom este atributo


    #inventory = "/usr/local/airflow/dags/cel_transporte/HC_NE_huawei_v2/ansible/inventory/inventory_custom_"+str(nodo)+str(user)+str(ID)
    #inventory = "/io/cel_core/per/host_ansible/inventario/inventory_grafos"


    extravars_to_add = dict(                                                                  
        ansible_user=BaseHook.get_connection(connection).login,           
        ansible_password=BaseHook.get_connection(connection).password,
        ID = str(ID),
        nodo = str(nodo),
        user = str(user),
        loopback = str(loopback),
        inventory = inventory)

    dict_extravars.update(extravars_to_add)

    ##
    print(str(dict_extravars))


    #r = ansible_runner.run(private_data_dir=pbook_dir, playbook=playbook, inventory=inventory, extravars=dict_extravars, rotate_artifacts=20, artifact_dir='/usr/local/tambo/files/cels/cel_core/tmp/artifacts/')    
    r = ansible_runner.run(private_data_dir=pbook_dir, playbook=playbook, inventory=inventory, extravars=dict_extravars, rotate_artifacts=50, artifact_dir='/usr/local/tambo/files/cels/cel_core/tmp/')
    print("-----> r: "+str(r))




def parseo_e_insercion_sql(info_parseo,mysql_conn,credenciales_mysql,database,PATH_HC_NE,**kwargs):

    try:


        # obtengo variables xcom del contexto
        task_id = "get_id"
        ti = kwargs['ti']
        ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
        nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
        user = ti.xcom_pull(key="user", task_ids=[task_id])[0]
        
        usrsql = credenciales_mysql.login
        pswsql = credenciales_mysql.password
        mysql_host = credenciales_mysql.password
        mysql_host = "10.247.2.42"
        #mysql_db = "transporte"  <---  hardodeo productivo porqye testing y desarrollo no funcionan

        mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
        mycursor = mydb.cursor()

        tabla = str(info_parseo["tabla"])

        ##### PARSEO INFO CON TXTFSM ###

        
        # archivo con el template txtfsm para el parseo del output
        template = "/usr/local/airflow/dags/cel_transporte/HC_NE_"+str(info_parseo["marca"]).lower()+"/templates/"+ str(info_parseo["template_fsm"])

        # archivo con output obtenido de equipo
        output_txt = PATH_HC_NE + "/" + nodo + "/output/"+ str(info_parseo["marca"]) +"_" + nodo + "_" + str(info_parseo["output_ansible"])
        with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
                    re_table = textfsm.TextFSM(f) # inicializo el parser con el template
                    result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

                    print("Resultado crudo")
                    print(result)

                    print("Resultado")
                    encabezado = re_table.header
                    print(encabezado)


                    #Proceso el resultado y el header para convertir los array del resultado en diccionarios
                    dict_result = [dict(zip(re_table.header, pr)) for pr in result]

                    #Si no pudo obtener ningun registro del parseo reporto el warning
                    if len(dict_result) == 0:
                        print("parseo dio vacio")
                        warning = "No se pudo obtener" + str(info_parseo["descripcion"])
                        tipo = "parseo"
                        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                        val = (nodo,ID,user,warning,tipo)
                        mycursor.execute(sql, val)
                        mydb.commit()

                    print(str(dict_result))

                    campos = ("nodo","user","ID") + info_parseo["campos"]
                    campos_str="("
                    for x in campos:
                        if campos_str == "(":
                            campos_str+=str(x)
                        else:
                            campos_str+=","+str(x)
                    campos_str += ")"
                    
                    for x in dict_result:
                        #genero lista tring de values en mismo orden que se definieron campos
                        values_array=[nodo,user,ID]
                        print(".................."+str(values_array))
                        values_ref_array=["%s","%s","%s"]
                        val = (nodo,user,ID)

                        for key in info_parseo["campos"]:
                            val = val + (str(x[key]),)
                            values_ref_array.append("%s")
                            values_ref = ','.join(map(str, values_ref_array))
                       
                        sql = "INSERT INTO "+ str(tabla) +" "+ campos_str + " VALUES (" +str(values_ref) + ");"

                        print("val: "+str(val))
                        #print("SQL:::::::::::   "+str(sql))

                        try:
                            mycursor.execute(sql,val)
                            #mycursor.execute(sql)
                            mydb.commit()
                        except:
                            print(":::::: No pudo realizarce insercion en BD - debido a probemas con la carga o registro duplicado")
                                #Si no pudo subir info a bd sql reporto el warning
                            warning = "Problemas con la BD - No pudo cargarse "+str(info_parseo["descripcion"])
                            tipo = "bd"
                            sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
                            val = (nodo,ID,user,warning,tipo)
                            mycursor.execute(sql, val)
                            mydb.commit()

    except:
        print("fallo acceso/parseo del archivo de " + str(info_parseo["descripcion"]))
        warning = "No se pudo obtener: " + str(info_parseo["descripcion"])
        tipo = "parseo"
        sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
        val = (nodo,ID,user,warning,tipo)
        mycursor.execute(sql, val)
        mydb.commit()


def _on_task_run_fail_parseo(context):
        print("::::: Prueba manejo errores - Fallo task de parseo")
        #Se dificulta customizar la respuesta al task que la origina, no tengo sus variables 
        # para por ejemplo detallar que fallo, tendria qe armar un task de falla por task padre..

        task_instance: TaskInstance = context.get("task_instance")
        print("Fallo task: "+str(task_instance))

        # obtengo variables xcom del contexto
        task_id = "get_id"
        ti = context['ti']
        ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
        nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
        user = ti.xcom_pull(key="user", task_ids=[task_id])[0]
        
        # usrsql = credenciales_mysql.login
        # pswsql = credenciales_mysql.password
        # mysql_host = credenciales_mysql.password
        # mysql_host = "10.247.2.42"
        # #mysql_db = "transporte"  <---  hardodeo productivo porqye testing y desarrollo no funcionan

        print("fin falure callback")

        # mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
        # mycursor = mydb.cursor()

        # print("fallo acceso/parseo del archivo de " + str(context["info_parseo"]["descripcion"]))
        # warning = "No se pudo obtener: " + str(context["info_parseo"]["descripcion"])
        # tipo = "parseo"
        # sql = "INSERT INTO HC_NE_integridad (nodo,ID,user,warning,tipo) VALUES (%s,%s,%s,%s,%s)"
        # val = (nodo,ID,user,warning,tipo)
        # mycursor.execute(sql, val)
        # mydb.commit()

#############  MAIN ###############################

            
#tasks

path_ne_hc = "/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP"

###################################################################
#  Operador de inicio
###############################################################3###

get_id = PythonOperator(
        task_id="get_id",
        python_callable=hc_ne_get_id,
        op_kwargs={"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos')},
        dag=dag
    )



###################################################################
#  Operador pre_post
###############################################################3###


diff_y_renderizado_juniper_prepost = PythonOperator(
        task_id="diff_y_renderizado_juniper_prepost",
        python_callable=_diff_y_renderizado_prepost,
        op_kwargs={"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos'),'PATH_HC_NE': path_ne_hc },
        dag=dag
    )



###################################################################
#  Operadores de Flujo
###############################################################3###

elegir_camino1 = BranchPythonOperator(
        task_id="elegir_camino1",
        python_callable=_elegir_camino1,
        dag=dag
    )

elegir_camino2 = BranchPythonOperator(
        task_id="elegir_camino2",
        python_callable=_elegir_camino2,
        dag=dag
    )


# Interrumpo proceso por nodo ocupado
exit_nodo_ocupado = PythonOperator(
        task_id="exit_nodo_ocupado",
        python_callable=_exit_nodo_ocupado,
        op_kwargs={"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos')},
        dag=dag
    )

exit_nodo_sobrecargado = PythonOperator(
        task_id="exit_nodo_sobrecargado",
        python_callable=_exit_nodo_sobrecargado,
        op_kwargs={"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos')},
        dag=dag
    )

exit_abort = PythonOperator(
        task_id="exit_abort",
        python_callable=exit_abort,
        op_kwargs={"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos')},
        dag=dag
    )


incremento_progreso_1 = PythonOperator(
        task_id="incremento_progreso_1",
        python_callable=_incrementar_progreso,
        op_kwargs={"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos') },
        dag=dag
    )

incremento_progreso_fin = PythonOperator(
        task_id="incremento_progreso_fin",
        trigger_rule='none_failed_or_skipped',
        #trigger_rule='all_done',
        python_callable=_incrementar_progreso,
        op_kwargs={"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos') },
        dag=dag
    )


def trafico_grafana_juniper(mysql_conn,credenciales_mysql,database,PATH_HC_NE,**kwargs):
        
        task_id = "get_id"
        ti = kwargs['ti']
        ID = ti.xcom_pull(key="ID", task_ids=[task_id])[0]
        nodo = ti.xcom_pull(key="nodo", task_ids=[task_id])[0]
        user = ti.xcom_pull(key="user", task_ids=[task_id])[0]
        
        usrsql = credenciales_mysql.login
        pswsql = credenciales_mysql.password
        mysql_host = credenciales_mysql.password
        mysql_host = "10.247.2.42"
        #mysql_db = "transporte"  <---  hardodeo productivo porqye testing y desarrollo no funcionan

        mydb = mysql_conn.connect(host = mysql_host,user = usrsql,passwd = pswsql,database = database)
        mycursor = mydb.cursor()

        if ID == 1:
            ID_pre = 1
        else:
            ID_pre = ID - 1

        sql = "select interface from HC_NE_juniper_ospf_nei_det where nodo ='"+ str(nodo) +"' and user ='"+ str(user)+ "' and (ID = '" + str(ID) + "' OR ID = '"+ str(ID_pre)+"')"
        print("sql: "+sql)
        mycursor.execute(sql)
        myresult = mycursor.fetchall()

        ########## genero url trafico troncales
        inicio_url_grafana = "http://10.200.100.53:3000/d/LhOPvGKnk/health_check_trafico_troncales?orgId=31"
        rango_tiempo= "&from=now-2d&to=now"
        var_nodo = "&var-nodo="+str(nodo)
        var_interfaces = ""
        var_interfaces_ten = ""


        for interf in myresult:
            interface_ajustada=interf[0].replace("/","_")
            interface_ajustada=interface_ajustada.replace("TenGigE","te-")
            interface_ajustada=interface_ajustada.replace("GigabitEthernet","ge-")

            interface_ajustada=re.sub("(ae\d+)\.0",r'\1',str(interface_ajustada))

            #print("....................."+interface_ajustada)
            m = re.search("te-",str(interface_ajustada))
            if m:
                print("hubo_match")
                #es de TenG
                var_interfaces_ten += "&var-interface_TenG="+str(interface_ajustada)
            else:
                var_interfaces += "&var-interface="+str(interface_ajustada)

        print("--->"+var_interfaces)
        print("--->"+var_interfaces_ten)
        url_trafico_grafana = inicio_url_grafana + rango_tiempo + var_nodo + var_interfaces + var_interfaces_ten

        print(">>>>>>>>>>>>>> trafico >>>>>>>>>>>>>>>>:::  "+url_trafico_grafana)

        tipo = "Evolucion Trafico Troncales"
        sql = "INSERT INTO HC_NE_grafana_trafico (nodo,ID,user,tipo,url_grafana) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE url_grafana=%s"
        val = (nodo,ID,user,tipo,url_trafico_grafana,url_trafico_grafana)
        mycursor.execute(sql, val)
        mydb.commit()
        ########## FIN: genero url trafico troncales

        ########## genero url Performance
        inicio_url_grafana = "http://10.200.100.53:3000/d/tTkEcQJnk/healt_check_ne_performance_transporte?orgId=31"
        rango_tiempo= "&from=now-2d&to=now"
        var_nodo = "&var-nodo="+str(nodo)


        url_echovault = inicio_url_grafana + rango_tiempo + var_nodo

        print(">>>>>>>>>>>>>>>> echovault >>>>>>>>>>>>>>:::  "+url_echovault)

        tipo = "Monitoreo de Performance del transporte"
        sql = "INSERT INTO HC_NE_grafana_trafico (nodo,ID,user,tipo,url_grafana) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE url_grafana=%s"
        val = (nodo,ID,user,tipo,url_echovault,url_echovault)
        mycursor.execute(sql, val)
        mydb.commit()
        ########## FIN: genero url trafico troncales

#http://10.200.100.53:3000/d/ICJ5pMSnk/degradacion_por_nodo?orgId=36&var-prov=All&var-host=CAS1MB&from=now-2d&to=now


###############################################
#Operadores de PArseo e insercion en BD
###############################################

### operador parseo e insercion bd unificado

info_parseo_descripciones = {
                "template_fsm":"parser_juniper_interfaces.fsm",
                "output_ansible":"interfaces.txt", #parcial
                "marca":"JUNIPER",
                "tabla": "HC_NE_juniper_interfaces", # nombre completo de la tabla
                "campos": ("interface","status","status_admin","descripcion"), #campos del SQL y values de FSM (nodo,user,ID no van aca)
                "descripcion": "informacion de interfaces del equipo"
            }

parseo_juniper_interfaces = PythonOperator(
        task_id="parseo_juniper_interfaces",
        python_callable=parseo_e_insercion_sql,
        #on_failure_callback=_on_task_run_fail_parseo,
        op_kwargs={"info_parseo":info_parseo_descripciones, "mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos'),'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP' },
        dag=dag
    )

### unificado bgp

info_parseo_bgp = {
                "template_fsm":"parser_juniper_show_bgp_all_summary.fsm",
                "output_ansible":"bgp_all_summary.txt", #parcial
                "marca":"JUNIPER",
                "tabla": "HC_NE_juniper_bgp_summary", # nombre completo de la tabla
                "campos": ("peer_ip","state","family","peer_as","time","hour","prefrcv","prefact","prefacp","prefdmp"), #campos del SQL y values de FSM (nodo,user,ID no van aca)
                "descripcion": "informacion de peering bgp del equipo"
            }

parseo_juniper_bgp = PythonOperator(
        task_id="parseo_juniper_bgp",
        python_callable=parseo_e_insercion_sql,
        #on_failure_callback=_on_task_run_fail_parseo,
        op_kwargs={"info_parseo":info_parseo_bgp, "mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos'),'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP' },
        dag=dag
    )


### unificado bgp temperature_threshold

info_parseo_temperature_threshold = {
                "template_fsm":"parser_show_juniper_temperature_threshold.fsm",
                "output_ansible":"show_temperature_thresholds.txt", #parcial
                "marca":"JUNIPER",
                "tabla": "HC_NE_juniper_temperatura_umbrales", # nombre completo de la tabla
                "campos": ("tipo","slot","threshold"), #campos del SQL y values de FSM (nodo,user,ID no van aca)
                "descripcion": "informacion de threshold de temperatura por slot"
            }

parseo_juniper_temperature_threshold = PythonOperator(
        task_id="parseo_juniper_temperature_threshold",
        python_callable=parseo_e_insercion_sql,
        #on_failure_callback=_on_task_run_fail_parseo,
        op_kwargs={"info_parseo":info_parseo_temperature_threshold, "mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos'),'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP' },
        dag=dag
    )



### unificado mpls

info_parseo_mpls = {
                "template_fsm":"parser_juniper_mpls_interface.fsm",
                "output_ansible":"mpls_interface.txt", #parcial
                "marca":"JUNIPER",
                "tabla": "HC_NE_juniper_mpls_interface", # nombre completo de la tabla
                "campos": ("interface","state"), #campos del SQL y values de FSM (nodo,user,ID no van aca)
                "descripcion": "informacion de interfaces mpls del equipo"
            }

parseo_juniper_mpls = PythonOperator(
        task_id="parseo_juniper_mpls",
        python_callable=parseo_e_insercion_sql,
        #on_failure_callback=_on_task_run_fail_parseo,
        op_kwargs={"info_parseo":info_parseo_mpls, "mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos'),'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP' },
        dag=dag
    )

### unificado ospf

info_parseo_ospf_nei = {
                "template_fsm":"parser_juniper_ospf_nei_det.fsm",
                "output_ansible":"ospf_nei_det.txt", #parcial
                "marca":"JUNIPER",
                "tabla": "HC_NE_juniper_ospf_nei_det", # nombre completo de la tabla
                "campos": ("interface","state","area","address"), #campos del SQL y values de FSM (nodo,user,ID no van aca)
                "descripcion": "informacion de interfaces vecindades OSPF del equipo"
            }

parseo_juniper_ospf_nei = PythonOperator(
        task_id="parseo_juniper_ospf_nei",
        python_callable=parseo_e_insercion_sql,
        #on_failure_callback=_on_task_run_fail_parseo,
        op_kwargs={"info_parseo":info_parseo_ospf_nei, "mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos'),'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP' },
        dag=dag
    )

### unificado opticos

info_parseo_juniper_optics = {
                "template_fsm":"parser_juniper_optics_TEST.fsm",
                "output_ansible":"optics.txt",
                "marca":"JUNIPER",
                "tabla": "HC_NE_juniper_optics",
                "campos": ("interface","OutputHighWarning","OutputLowWarning","RxHighWarning","lane","amp","grados","OutputPower","RxPower","RxLowWarning"),
                "descripcion": "informacion de los valores opticos"
            }

parseo_juniper_optics = PythonOperator(
        task_id="parseo_juniper_optics",
        python_callable=parseo_e_insercion_sql,
        op_kwargs={"info_parseo":info_parseo_juniper_optics, "mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos'),'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'},
        dag=dag
    )


### operadores de parseo custom

parseo_e_insercion_juniper_en_bd_info_cpu_memoria = PythonOperator(
        task_id="parseo_e_insercion_juniper_en_bd_info_cpu_memoria",
        python_callable=parseo_e_insercion_juniper_en_bd_info_cpu_memoria,
        op_kwargs={'PATH_HC_NE': path_ne_hc,"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos') },
        dag=dag
    )


parseo_e_insercion_juniper_en_bd_rr_totales = PythonOperator(
        task_id = "parseo_e_insercion_juniper_en_bd_rr_totales",
        python_callable = parseo_e_insercion_en_bd_rr_totales,
        op_kwargs={'PATH_HC_NE': path_ne_hc,"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos') },
        dag=dag
    )

parseo_y_registro_juniper_de_info_logs = PythonOperator(
        task_id = "parseo_y_registro_juniper_de_info_logs",
        python_callable = parseo_y_registro_juniper_de_info_logs,
        op_kwargs={'PATH_HC_NE': path_ne_hc,"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos') },
        dag=dag
    )

# parseo_y_registro_juniper_optica = PythonOperator(
#         task_id = "parseo_y_registro_juniper_optica",
#         python_callable = parseo_y_registro_juniper_optica,
#         op_kwargs={'PATH_HC_NE': path_ne_hc,"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos') },
#         dag=dag
#     )
#############################



###################
# operadores ansible runner
######################

Ansible_juniper_display_health = PythonOperator(
        task_id="Ansible_juniper_display_health",
        python_callable=hc_ne_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/',
            'playbook':'transporte_hc_ne_juniper_show_chassis_fpc.yaml',
            'connection':'credenciales_equipos',
            'inventory': "/io/cel_core/per/host_ansible/inventario/inventory_grafos",
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )

Ansible_juniper_interfaces = PythonOperator(
        task_id="Ansible_juniper_interfaces",
        python_callable=hc_ne_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/',
            'playbook':'transporte_hc_ne_juniper_interfaces.yaml',
            'connection':'credenciales_equipos',
            'inventory': "/io/cel_core/per/host_ansible/inventario/inventory_grafos",
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )

Ansible_juniper_rr_totals = PythonOperator(
        task_id="Ansible_juniper_rr_totals",
        python_callable=hc_ne_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/',
            'playbook':'transporte_hc_ne_juniper_rr.yaml',
            'connection':'credenciales_equipos',
            'inventory': "/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/inventory/inventory_rr",
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )

Ansible_juniper_logs = PythonOperator(
        task_id="Ansible_juniper_logs",
        python_callable=hc_ne_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/',
            'playbook':'transporte_hc_ne_juniper_logs.yaml',
            'connection':'credenciales_equipos',
            'inventory': "/io/cel_core/per/host_ansible/inventario/inventory_grafos",
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )

Ansible_juniper_prepost = PythonOperator(
        task_id="Ansible_juniper_prepost",
        python_callable=hc_ne_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/',
            'playbook':'transporte_hc_ne_juniper_pre_post.yaml',
            'connection':'credenciales_equipos',
            'inventory': "/io/cel_core/per/host_ansible/inventario/inventory_grafos",
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )


Ansible_juniper_bgp = PythonOperator(
        task_id="Ansible_juniper_bgp",
        python_callable=hc_ne_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/',
            'playbook':'transporte_hc_ne_juniper_bgp.yaml',
            'connection':'credenciales_equipos',
            'inventory': "/io/cel_core/per/host_ansible/inventario/inventory_grafos",
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )


Ansible_juniper_ospf_nei = PythonOperator(
        task_id="Ansible_juniper_ospf_nei",
        python_callable=hc_ne_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/',
            'playbook':'transporte_hc_ne_juniper_ospf_nei_det.yaml',
            'connection':'credenciales_equipos',
            'inventory': "/io/cel_core/per/host_ansible/inventario/inventory_grafos",
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )

Ansible_juniper_mpls = PythonOperator(
        task_id="Ansible_juniper_mpls",
        python_callable=hc_ne_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/',
            'playbook':'transporte_hc_ne_juniper_mpls_interface.yaml',
            'connection':'credenciales_equipos',
            'inventory': "/io/cel_core/per/host_ansible/inventario/inventory_grafos",
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )

Ansible_juniper_optica = PythonOperator(
        task_id="Ansible_juniper_optica",
        python_callable=hc_ne_ansible_runner,
        op_kwargs={
            'pbook_dir':'/usr/local/airflow/dags/cel_transporte/HC_NE_juniper/ansible/',
            'playbook':'transporte_hc_ne_juniper_optics.yaml',
            'connection':'credenciales_equipos',
            'inventory': "/io/cel_core/per/host_ansible/inventario/inventory_grafos",
            'dict_extravars':{'PATH_HC_NE':'/io/cel_transporte/per/HealthCheck/NE/TRANSPORTE_IP'}
            },
        dag=dag
    )

trafico_grafana_juniper = PythonOperator(
        task_id="trafico_grafana_juniper",
        python_callable=trafico_grafana_juniper,
        op_kwargs={'PATH_HC_NE': path_ne_hc,"mysql_conn": mysql.connector,"database": "transporte","credenciales_mysql":BaseHook.get_connection('SQL_Grafos') },
        dag=dag
    )


get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> Ansible_juniper_ospf_nei >> parseo_juniper_ospf_nei >> trafico_grafana_juniper
get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> Ansible_juniper_ospf_nei >> parseo_juniper_ospf_nei >> Ansible_juniper_bgp >> parseo_juniper_bgp >> Ansible_juniper_mpls >> parseo_juniper_mpls >> incremento_progreso_fin
get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> Ansible_juniper_prepost >> diff_y_renderizado_juniper_prepost >> incremento_progreso_fin
get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> Ansible_juniper_logs >> parseo_y_registro_juniper_de_info_logs >> incremento_progreso_fin
get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> Ansible_juniper_interfaces >> parseo_juniper_interfaces >> incremento_progreso_fin
get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> Ansible_juniper_rr_totals >> parseo_e_insercion_juniper_en_bd_rr_totales >> incremento_progreso_fin
get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> exit_nodo_sobrecargado
get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> exit_abort
#get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> Ansible_juniper_optica >> parseo_y_registro_juniper_optica >> incremento_progreso_fin
get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_e_insercion_juniper_en_bd_info_cpu_memoria >> incremento_progreso_1 >> elegir_camino2 >> Ansible_juniper_optica >> parseo_juniper_optics >> incremento_progreso_fin
get_id >> elegir_camino1 >> Ansible_juniper_display_health >> parseo_juniper_temperature_threshold
get_id >> elegir_camino1 >> exit_nodo_ocupado
