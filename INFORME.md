# Informe

El scneario 1 ya venia solucionado, se fueron resolviendo uno a uno los escenarios, a continuación se explica brevemente cómo se solucionó cada uno.

## Scenario 2

- En Message_handler del gateway:
    - Por cada conexion a un cliente se crea una instancia de MessageHandler para atenderlo, por lo que se puede obtener el id único por cliente y mandarlo en cada mensaje, cambiando la estructura enviada al SUM a:
    message[1]= [client_id, fruta, cantidad]
    - Por la limitación en aggreagation de sólo poder cambiar el message_hanlder, el join responde a todos los clientes los resultados de top. Pero ahora cada cliente en el message_handler cuando recibe la respuesta del sistema revisa si el mensaje es para él ya que el mensaje lleva el id del cliente.
- En sum:
    - El diccionario pasa de ser un mapa simple { fruit: FruitItem } a un diccionario anidado por cliente: { id_client: { fruit: FruitItem } }.
    - Se hacen las sumatorias de fruta agrupadas individualmente dentro del sub-diccionario de cada id_client.
    - Cuando le llega el EOF de un id_client, se remueven los datos de ese cliente, envía uno a uno los mensajes agregados con la estructura [id_client, fruta, cantidad] y finalmente envía el mensaje de cierre para ese cliente [id_client].
- En Aggregation:
    - fruit_top ahora pasa a ser un diccionario y no una lista.  Recibe las sumatorias de cada fruta por cliente, y toma el top de amount de frutas por cliente y las manda al join.

## Scenario 3

Para que los nodos de sum sepan cuándo el cliente manda su EOF, a la instancia que le llega el eof, la manda por un exhange, en un segundo hilo están recibiendo los mensajes de ese exhange, logrando así que todas se enteren cuando se recibe el eof, y pasar a enviar las sumas que cada uno hizo al aggregation. Cada sum envía su propio EOF. El único nodo Aggregation cuenta los EOF recibidos por cliente y al recibir SUM_AMOUNT EOF, calcula y manda el top.

## Scenario 4

Ahora que hay varios Aggregators, si cada Sum enviara todas sus frutas a todos los Aggregators, se produciría redundancia, duplicación y un aumento innecesario de la carga. La solución debe evitar el broadcast masivo.

Para ello se hace una distribución por fruta. Cada fruta se asigna a una de las instancias de Aggregation. La elección se hace mediante una función hash y luego se toma el módulo de la cantidad de Aggregators, aprovechando que conocemos la cantidad de aggregators: aggregation_id = hash(fruta) mod AGGREGATION_AMOUNT. En particular se usa zlib.crc32(), ya que la función hash() nativa de Python no es determinística entre distintos procesos o ejecuciones.

Con esto se logra que las cantidades de una misma fruta van a ir al mismo Aggregator,cada Aggregator recibe una porción del conjunto total de frutas, se reduce la duplicación de trabajo y el sistema escala mejor con más nodos.

Aunque los datos de las frutas se dirigen a un Aggregator específico según sea la fruta, los mensajes de EOF se envían a todos los Aggregators desde cada nodo sum.

El join lleva un conteo por cliente de los aggregators, cuando todos los aggregators le hayan mandado eof del cliente i, arma el top final para enviarlo al cliente i. 

## Scenario 5

Para evitar nombres de exchanges estáticos (hardcoded) y soportar identificadores dinámicos según el entorno de despliegue, el exchange de control en SUM se parametriza dinámicamente utilizando el prefijo configurado: SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control".

También agrego un lock en sum para garantizar exclusión mutua entre los dos hilos (el que está recibiendo mensajes y el que está esperando el mensaje eof en el exhange).  Asegurando que no se lean o borren datos del cliente antes de terminar de procesar todas sus frutas recibidas.

Se maneja la señal SIGTERM en sum, aggregator y join.

## Resumen de Diseño y Escalabilidad

- Coordinación entre Sum y Aggregation:
Para sincronizar los cierres se usa un exchange de difusión (f"{SUM_PREFIX}_control"). Cuando a un Sum le llega el EOF de un cliente por su cola principal, avisa por ese exchange a los demás Sums para que todos envíen sus sumas parciales al siguiente nodo. Los datos de las frutas no se mandan a todos los nodos siguientes, sino que se reparten usando crc32(fruta) % AGGREGATION_AMOUNT para que cada fruta caiga siempre en el mismo Aggregator. Por su parte, los EOFs sí se envían a todos los Aggregators. Cada Aggregator junta SUM_AMOUNT EOFs antes de mandar su top parcial al Join, y el Join espera que le lleguen AGGREGATION_AMOUNT respuestas para armar el top final.
- Escalabilidad ante grandes volúmenes de datos y clientes:
El sistema escala a múltiples clientes de forma concurrente porque todos los nodos (Sum, Aggregation, Join) guardan sus datos en estructuras anidadas indexadas por id_client. Esto aísla el estado de cada cliente y permite liberar la memoria borrrando las partes correspondientes a cada cliente apenas se termina de procesar. También el volumen de datos se controla haciendo que los nodos Sum van sumando todo en su memoria local y mandan únicamente el resultado final, además de que las frutas se reparten entre varios Sums y luego en varios Aggregators para procesar grandes volúmenes de datos en paralelo.
- Controles y Robustez:
Se metió un threading.Lock() en Sum para solucionar las condiciones de carrera entre el hilo que lee la cola de RabbitMQ y el hilo secundario que escucha el exchange de control, evitando que un hilo borre el diccionario de un cliente mientras el otro sigue sumando registros. También se implementó el manejo de la señal SIGTERM en todos los servicios para garantizar graceful shutdown y el parametrizado dinámico de colas/exchanges por variables de entorno.