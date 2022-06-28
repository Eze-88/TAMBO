import textfsm
from tabulate import tabulate

template = "C:/Users/u559950/Documents/Desarrollo y programacion/TAMBO/Pruebas/Parseo_modulos_opticos/parseo_huawei_opticos.fsm"
output_txt = "C:/Users/u559950/Documents/Desarrollo y programacion/TAMBO/Pruebas/Parseo_modulos_opticos/crudo_huawei_v5_ATN.txt"

with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
    re_table = textfsm.TextFSM(f) # inicializo el parser con el template
    result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

    print(tabulate(result, headers=re_table.header))

    #Proceso el resultado y el header para convertir los array del resultado en diccionarios
    dict_result = [dict(zip(re_table.header, pr)) for pr in result]
    print(len(dict_result))

    #Para adaptar el nombre ficticio de la interfaz al util
    # for item in dict_result:
    #     if item['tipo_interfaz'] == 'GE':
    #         item['interfaz'] = 'GigabitEthernet' + item['interfaz']

    # for item in dict_result:
    #     print(item)
    # print(len(dict_result))