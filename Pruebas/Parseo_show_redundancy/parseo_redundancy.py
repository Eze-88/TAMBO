import textfsm

template = "c:/Users/u559950/Documents/Desarrollo y programacion/TAMBO/Pruebas/Parseo_show_redundancy/parseo.fsm"
output_txt = "c:/Users/u559950/Documents/Desarrollo y programacion/TAMBO/Pruebas/Parseo_show_redundancy/crudo.txt"

with open(template) as f, open(output_txt) as f_2:  # abro los dos archivos, el del template y el del output del equipo
    re_table = textfsm.TextFSM(f) # inicializo el parser con el template
    result = re_table.ParseText(f_2.read()) # aplico el parser al texto del output del equipo

    #Proceso el resultado y el header para convertir los array del resultado en diccionarios
    dict_result = [dict(zip(re_table.header, pr)) for pr in result]

    print(dict_result)