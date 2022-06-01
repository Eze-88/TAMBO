from itertools import count
import textfsm

#template = "c:/Users/ezequ/Repositorios_GIT/TAMBO/Pruebas/Parseo_VPLS/parseo_vpls_huawei.fsm"
template = "c:/Users/ezequ/Repositorios_GIT/TAMBO/Pruebas/Parseo_VPLS/parseo_l2vc_huawei.fsm"
output_txt = "c:/Users/ezequ/Repositorios_GIT/TAMBO/Pruebas/Parseo_VPLS/crudo_huawei.txt"

with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
    re_table = textfsm.TextFSM(f) # inicializo el parser con el template
    result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

    #Proceso el resultado y el header para convertir los array del resultado en diccionarios
    dict_result = [dict(zip(re_table.header, pr)) for pr in result]

    #Para adaptar el nombre ficticio de la interfaz al util
    # for item in dict_result:
    #     if item['tipo_interfaz'] == 'GE':
    #         item['interfaz'] = 'GigabitEthernet' + item['interfaz']

    for item in dict_result:
        print(item)
    print(len(dict_result))