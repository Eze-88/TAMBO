from itertools import count
from re import template
import textfsm

template = "c:/Users/u559950/Documents/Desarrollo y programacion/TAMBO/Pruebas/Parseo_val_opt_Jn_Beta/parseo.fsm"
output_txt = "c:/Users/u559950/Documents/Desarrollo y programacion/TAMBO/Pruebas/Parseo_val_opt_Jn_Beta/crudo.txt"

with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
    re_table = textfsm.TextFSM(f) # inicializo el parser con el template
    result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

    #Proceso el resultado y el header para convertir los array del resultado en diccionarios
    dict_result = [dict(zip(re_table.header, pr)) for pr in result]

    i = 0   # indice para contar los resultados obtenidos
    for x in dict_result:
        print(x["interface"] + " " + x["lane"]+ " " + x["amp"]+ " " + x["OutputPower"]+ " " + x["RxPower"]+ " " + x["grados"]+ " " + x["OutputHighWarning"]+ " " + x["OutputLowWarning"]+ " " + x["RxHighWarning"]+ " " + x["RxLowWarning"])
        i+=1
    print(i)