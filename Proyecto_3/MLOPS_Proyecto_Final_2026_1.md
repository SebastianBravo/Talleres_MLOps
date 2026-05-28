Operaciones de Machine Learning Proyecto Final 

Nivel 4: Automatizaci´on, decisi´on de reentrenamiento y despliegue GitOps 

Pontificia Universidad Javeriana Cristian Javier Diaz Alvarez 

20 de mayo de 2026 

## **1. Descripci´on** 

Este proyecto final eval´ua la capacidad del estudiante para dise˜nar, implementar, desplegar y explicar un sistema de _Machine Learning Operations_ (MLOps) con un flujo automatizado de recolecci´on de datos, validaci´on, procesamiento, entrenamiento, versionamiento, despliegue, inferencia y observabilidad. El objetivo no es ´unicamente entrenar un modelo, sino construir un sistema que tome decisiones t´ecnicas justificadas durante el ciclo de vida del modelo. 

El sistema debe consumir datos desde una API externa, almacenar la informaci´on cruda, procesarla, evaluar si existe evidencia suficiente para reentrenar un modelo, ejecutar el entrenamiento cuando corresponda, registrar los experimentos en MLflow, comparar el nuevo modelo contra el modelo actualmente productivo y actualizar la fuente de verdad del modelo solamente cuando el nuevo modelo demuestre mejor desempe˜no seg´un las m´etricas definidas. La API de inferencia debe consumir el modelo productivo desde MLflow sin requerir cambios de c´odigo ni redespliegue de la aplicaci´on. 

El proyecto debe ser desarrollado bajo una visi´on de producci´on. Por lo tanto, se espera que los estudiantes consideren escenarios de fallo, trazabilidad, reproducibilidad, versionamiento, pruebas de carga, observabilidad, l´ımites de recursos, despliegue en Kubernetes y gesti´on declarativa mediante GitOps. 

## **1.1. Objetivos de aprendizaje** 

Al finalizar el proyecto, el estudiante deber´a ser capaz de: 

- Dise˜nar un flujo MLOps automatizado que integre recolecci´on de datos, validaci´on, procesamiento, entrenamiento, registro de modelos e inferencia. 

- Implementar DAGs en Airflow con m´ultiples tareas y bifurcaciones de decisi´on. 

- Separar correctamente datos crudos y datos procesados en bases de datos distintas o esquemas claramente diferenciados. 

Registrar experimentos, m´etricas, par´ametros, artefactos y modelos en MLflow. 

- Definir criterios t´ecnicos para decidir si se debe entrenar un nuevo modelo o no. 

- Comparar un modelo candidato contra el modelo actualmente productivo y justificar si debe promoverse o rechazarse. 

Exponer el modelo productivo mediante FastAPI, tomando MLflow como fuente de verdad. 

- Construir una interfaz en Streamlit para inferencia y visualizaci´on del historial del proceso de entrenamiento. 

Automatizar la construcci´on y publicaci´on de im´agenes de contenedor mediante GitHub Actions. 

- Desplegar todos los componentes en Kubernetes usando manifiestos versionados y sincronizados mediante Argo CD. 

1 

Instrumentar al menos la API de inferencia con m´etricas observables en Prometheus y Grafana. 

- Ejecutar pruebas de carga con Locust y analizar sus efectos en la API mediante dashboards de observabilidad. 

## **1.2. Condiciones generales del proyecto** 

Todos los componentes del sistema deben ejecutarse en contenedores. Las im´agenes propias del proyecto deben construirse mediante workflows de GitHub Actions y publicarse en DockerHub. No se permite construir im´agenes manualmente en la m´aquina de despliegue. Kubernetes debe consumir las im´agenes desde DockerHub u otro registro de contenedores equivalente autorizado por el docente. 

Todos los componentes deben desplegarse en un cl´uster de Kubernetes local o proporcionado para el curso. Los manifiestos, archivos Helm o configuraciones equivalentes deben estar versionados en el repositorio Git. Argo CD debe ser el mecanismo encargado de sincronizar los manifiestos con el cl´uster, adoptando una visi´on GitOps. 

Cada contenedor debe declarar expl´ıcitamente `resources.requests` y `resources.limits` . La ausencia de l´ımites y m´ınimos de recursos ser´a considerada una deficiencia t´ecnica, ya que impide evaluar el comportamiento del sistema bajo condiciones controladas. 

## **2. Descripci´on del dataset** 

Los datos corresponden a registros de propiedades inmobiliarias provenientes de un escenario inspirado en listados de bienes ra´ıces. El problema de aprendizaje consiste en estimar el precio de una propiedad a partir de sus caracter´ısticas estructurales, geogr´aficas y comerciales. 

||**Variable**|**Descripci´on**|
|---|---|---|
|1|brokered<br>~~b~~y|Agencia o corredor codifcado categ´oricamente.|
|2|status|Estado de la vivienda, por ejemplo, lista para la venta o<br>lista para construir.|
|3|price|Precio objetivo de la propiedad. Corresponde al precio<br>de cotizaci´on actual o al precio de venta reciente cuando<br>aplique.|
|4|bed|N´umero de habitaciones.|
|5|bath|N´umero de ba˜nos.|
|6|acre<br>~~l~~ot|Tama˜no del terreno en acres.|
|7|street|Direcci´on codifcada categ´oricamente.|
|8|city|Ciudad donde se encuentra la propiedad.|
|9|state|Estado o regi´on.|
|10|zip<br>code|C´odigo postal.|
|11|house<br>~~s~~ize|´Area habitable de la vivienda, expresada en pies cuadra-<br>dos.|
|12|prev<br>~~s~~old<br>~~d~~ate|Fecha de venta anterior, cuando exista.|



Tabla 1: Descripci´on de variables del dataset. 

El dataset no ser´a entregado completo desde el inicio. La informaci´on ser´a obtenida por partes mediante una API externa. Cada petici´on representa un nuevo lote de datos disponible en el sistema. Por lo tanto, los estudiantes deben construir un proceso incremental que eval´ue cada lote, actualice las tablas correspondientes y determine si el nuevo lote justifica un nuevo entrenamiento. 

## **2.1. Carga del dataset desde API** 

Los datos ser´an obtenidos a trav´es de una API externa expuesta como imagen de DockerHub. La URL definitiva de la imagen ser´a informada por el docente: 

2 

## **DockerHub image: cristiandiaz13/mlops-puj:data-api-pf-v1** 

La API entregar´a subconjuntos de datos distintos de acuerdo con el grupo asignado (pueden usar 1 para el grupo sin problema) y el avance de las peticiones. Cada ejecuci´on del DAG debe consumir un lote, almacenarlo en la base de datos cruda y continuar con el proceso completo de validaci´on, transformaci´on y decisi´on de entrenamiento. No se permite descargar todos los lotes de una vez para luego entrenar un ´unico modelo final. 

Cada petici´on debe tratarse como un nuevo evento de datos en el sistema. Por esta raz´on, el DAG debe registrar el identificador del lote, fecha de ejecuci´on, cantidad de registros, esquema recibido, clases o categor´ıas nuevas detectadas, validaciones realizadas, decisi´on tomada y resultado final. 

No se recomienda usar la interfaz gr´afica de Swagger para inspeccionar grandes vol´umenes de datos. La API debe consumirse desde c´odigo, preferiblemente desde una tarea de Airflow dise˜nada para manejar errores, reintentos y validaciones de respuesta. Para instanciar la imagen pueden usar esta referencia: **docker run –rm -p 8000:80 cristiandiaz13/mlops-puj:data-api-pf-v1** 

## **3. Arquitectura** 

La arquitectura de referencia integra herramientas de orquestaci´on, almacenamiento, seguimiento de experimentos, registro de modelos, inferencia, visualizaci´on, CI/CD, observabilidad y despliegue declarativo. El sistema debe reflejar un flujo completo de MLOps y no una colecci´on aislada de servicios. 

Figura 1: Arquitectura de referencia del proyecto final. 

El flujo m´ınimo esperado es el siguiente: 

3 

1. GitHub almacena el c´odigo fuente, DAGs, Dockerfiles, manifiestos de Kubernetes, configuraciones de observabilidad y documentaci´on. 

2. GitHub Actions construye y publica im´agenes de contenedor en DockerHub. 

3. Argo CD sincroniza los manifiestos versionados en Git con el cl´uster de Kubernetes. 

4. Airflow consume datos desde la API externa, valida el lote, almacena datos crudos y genera datos procesados. 

5. El DAG decide si se debe entrenar un nuevo modelo con base en criterios t´ecnicos definidos por el equipo. 

6. Si se entrena, el modelo candidato se registra en MLflow junto con m´etricas, par´ametros, artefactos, validaciones y explicaci´on de la decisi´on. 

7. El modelo candidato se compara contra el modelo productivo actual. 

8. Si el candidato mejora el desempe˜no seg´un las reglas definidas, se actualiza su alias, tag o etiqueta productiva en MLflow. 

9. FastAPI consulta MLflow como fuente de verdad y debe poder actualizar el modelo cargado sin reconstruir ni redesplegar la imagen. 

10. Streamlit permite probar inferencias y consultar el historial de decisiones de entrenamiento y promoci´on de modelos. 

11. Prometheus recolecta m´etricas de la API de inferencia y Grafana permite visualizar el comportamiento del sistema, especialmente durante pruebas de carga con Locust. 

## **4. Requerimientos funcionales** 

## **4.1. RF1. Recolecci´on incremental de datos** 

El sistema debe consumir la API de datos desde Airflow. Cada ejecuci´on del DAG debe solicitar un nuevo lote de datos, almacenarlo en RAW DATA y registrar metadatos de la ejecuci´on. El sistema debe evitar reprocesar accidentalmente un mismo lote, salvo que el estudiante implemente una estrategia expl´ıcita de reprocesamiento controlado. 

## **4.2. RF2. Persistencia separada de datos crudos y procesados** 

La informaci´on recibida desde la API debe almacenarse sin modificaciones en una base o esquema RAW DATA. Posteriormente, el pipeline debe generar una versi´on procesada en CLEAN DATA. La separaci´on entre ambos niveles es obligatoria, ya que permite trazabilidad, auditor´ıa y reproducibilidad. 

## **4.3. RF3. Validaci´on de datos y control de cambios** 

Antes de entrenar, el DAG debe ejecutar validaciones sobre el lote recibido y sobre el conjunto acumulado disponible. Como m´ınimo, se espera evaluar: 

Cambios de esquema: columnas nuevas, columnas faltantes o cambios de tipo de dato. 

Calidad de datos: nulos, duplicados, rangos inv´alidos, valores extremos y consistencia del target. 

- Volumen de datos: cantidad de registros nuevos y proporci´on frente al hist´orico. 

- Cambios en distribuci´on: comparaci´on entre el lote nuevo y la distribuci´on hist´orica o de referencia. 

- Aparici´on de nuevas categor´ıas o clases en variables categ´oricas. 

Suficiencia de datos para entrenamiento y validaci´on. 

4 

El sistema no debe fallar autom´aticamente ante una nueva categor´ıa. Debe aplicar una estrategia robusta, por ejemplo, codificadores capaces de manejar categor´ıas desconocidas, actualizaci´on controlada del espacio categ´orico, uso de `handle unknown` , agrupaci´on en categor´ıa _other_ o l´ogica equivalente t´ecnicamente justificada. 

## **4.4. RF4. Decisi´on autom´atica de entrenamiento** 

El DAG debe incluir una bifurcaci´on expl´ıcita que decida si se entrena o no se entrena un nuevo modelo. Esta decisi´on no puede depender ´unicamente de periodicidad ni de la cantidad bruta de lotes recolectados. Debe sustentarse en reglas t´ecnicas. 

Ejemplos de criterios v´alidos: 

Se detect´o cambio significativo en la distribuci´on de una o varias variables relevantes. 

- Se detectaron nuevas categor´ıas con frecuencia suficiente para afectar el modelo. 

- El lote nuevo aumenta el volumen acumulado en un porcentaje m´ınimo definido. 

- Se observa degradaci´on del desempe˜no del modelo actual sobre una muestra reciente etiquetada. 

- El esquema cambi´o, pero el pipeline pudo adaptarse y validar correctamente los datos. 

- No se entrena porque el lote es demasiado peque˜no, incompleto, inv´alido o no aporta variaci´on relevante. 

Toda decisi´on debe quedar registrada en una tabla de auditor´ıa del proceso y debe ser visible desde Streamlit. 

## **4.5. RF5. Entrenamiento y registro en MLflow** 

Cuando el DAG determine que s´ı debe entrenar, debe ejecutar el pipeline de entrenamiento, registrar el experimento en MLflow y guardar como m´ınimo: 

Identificador del lote o conjunto de lotes usados. 

- Versi´on del c´odigo o commit asociado. 

- Par´ametros del modelo y del preprocesamiento. 

- M´etricas de entrenamiento, validaci´on y prueba. 

- Artefactos relevantes, como gr´aficos de desempe˜no, matriz de errores o reportes de interpretabilidad. 

Modelo serializado usando la estructura de MLflow Models. 

Raz´on por la cual se decidi´o entrenar. 

## **4.6. RF6. Comparaci´on contra el modelo productivo** 

Entrenar un modelo no implica promoverlo autom´aticamente. Despu´es del entrenamiento, el DAG debe comparar el modelo candidato contra el modelo productivo actual registrado en MLflow. La comparaci´on debe usar las m´etricas definidas por el equipo. Para un problema de regresi´on de precios, se recomienda usar m´etricas como MAE, RMSE, MAPE o _R_[2] , justificando cu´al de ellas ser´a prioritaria. 

El modelo candidato solo debe promoverse si mejora al modelo productivo bajo una regla expl´ıcita, por ejemplo: 

_Promover si el MAE disminuye al menos 3 % y el RMSE no empeora m´as de 1 %._ 

Si el modelo candidato no supera al productivo, debe quedar registrado como experimento, pero no debe reemplazar el modelo usado por la API. 

5 

## **4.7. RF7. Actualizaci´on del modelo en la API sin redespliegue** 

FastAPI debe usar MLflow como fuente de verdad del modelo productivo. No se permite quemar rutas locales, nombres de archivos fijos o versiones espec´ıficas dentro del c´odigo de inferencia. 

La API debe implementar un mecanismo de actualizaci´on del modelo sin redesplegar la aplicaci´on. Se aceptan estrategias como: 

Endpoint administrativo protegido para recargar el modelo desde MLflow. 

Verificaci´on peri´odica del alias, tag o stage productivo en MLflow. 

Recarga bajo demanda cuando la versi´on productiva registrada cambie. 

La estrategia debe evitar que una petici´on de inferencia quede en estado inconsistente durante la recarga. El estudiante debe explicar c´omo maneja concurrencia, errores de descarga y fallback al modelo previamente cargado. 

## **4.8. RF8. Registro de inferencias** 

Cada petici´on realizada a la API de inferencia debe dejar registro en RAW DATA o en una tabla de eventos de inferencia conectada al dominio RAW. El registro debe incluir, como m´ınimo, fecha y hora, datos de entrada, predicci´on, versi´on del modelo usada, estado de la petici´on y errores si existen. Estos datos deben poder usarse posteriormente como insumo para monitoreo o futuros entrenamientos. 

## **4.9. RF9. Interfaz de usuario en Streamlit** 

La interfaz de Streamlit debe tener dos secciones obligatorias: 

1. **Inferencia:** formulario para ingresar datos de una propiedad, consumir la API FastAPI, visualizar la predicci´on y mostrar la versi´on del modelo utilizada. 

2. **Historial de entrenamiento y despliegue:** vista que muestre el resultado de cada lote procesado. Debe indicar si se entren´o o no, la raz´on de la decisi´on, si el modelo fue promovido o rechazado, el cambio de desempe˜no frente al modelo productivo y los identificadores de MLflow asociados. 

Ejemplo de historial esperado: 

- **Batch 1:** entren´o porque fue la l´ınea base inicial. Se promovi´o porque no exist´ıa modelo productivo previo. 

- **Batch 2:** no entren´o porque el lote fue peque˜no y no present´o cambio de distribuci´on relevante. 

- **Batch 3:** entren´o por cambio en la distribuci´on de `house` ~~`s`~~ `ize` y aparici´on de nuevas ciudades. No se promovi´o porque el MAE fue 4 % peor que el modelo productivo. 

- **Batch 4:** entren´o por acumulaci´on suficiente de datos recientes y mejora de cobertura categ´orica. Se promovi´o porque redujo el MAE en 6 % y mantuvo estable el RMSE. 

La cantidad m´axima de lotes disponibles ser´a determinada por la API de datos. La interfaz debe mostrar el historial seg´un los lotes efectivamente recolectados por el equipo. 

## **4.10. RF10. Observabilidad y pruebas de carga** 

La API de inferencia debe exponer m´etricas para Prometheus. Como m´ınimo se espera observar: 

Cantidad de peticiones recibidas. 

Latencia de inferencia. 

Tasa de errores. 

6 

Estado o versi´on del modelo cargado. 

Consumo de recursos del contenedor, cuando sea posible desde Kubernetes. 

Se debe desplegar Locust y ejecutar pruebas de carga contra la API de inferencia. El estudiante debe mostrar en Grafana el efecto de la prueba de carga sobre la API, por ejemplo aumento de peticiones, variaci´on de latencia, errores y comportamiento del contenedor. 

## **5. Dise˜no esperado de DAGs en Airflow** 

El DAG principal debe tener m´ultiples tareas y bifurcaciones. La siguiente estructura es una referencia m´ınima; los estudiantes pueden ampliarla si justifican t´ecnicamente su dise˜no. 

1. **start:** inicio de la ejecuci´on. 

2. **fetch** ~~**b**~~ **atch** ~~**f**~~ **rom** ~~**a**~~ **pi:** consume un lote desde la API externa. 

3. **store** ~~**r**~~ **aw batch:** almacena la respuesta original en RAW DATA. 

4. **validate** ~~**s**~~ **chema:** valida columnas, tipos y estructura del lote. 

5. **validate** ~~**d**~~ **ata quality:** eval´ua calidad, nulos, duplicados, rangos y consistencia. 

6. **detect** ~~**n**~~ **ew** ~~**c**~~ **ategories:** identifica categor´ıas nuevas y define c´omo incorporarlas sin romper el pipeline. 

7. **detect** ~~**d**~~ **ata** ~~**d**~~ **rift:** compara la distribuci´on del lote con una referencia hist´orica. 

8. **preprocess** ~~**d**~~ **ata:** transforma el lote y actualiza CLEAN DATA. 

9. **decide** ~~**t**~~ **raining:** bifurca el flujo entre entrenar o no entrenar. 

10. **skip training:** registra la raz´on por la cual no se entren´o. 

11. **train candidate** ~~**m**~~ **odel:** entrena un modelo candidato. 

12. **evaluate candidate model:** calcula m´etricas y genera artefactos. 

13. **register** ~~**c**~~ **andidate in** ~~**m**~~ **lflow:** registra el modelo candidato y sus artefactos. 

14. **compare** ~~**w**~~ **ith** ~~**p**~~ **roduction:** compara el candidato contra el modelo productivo actual. 

15. **decide** ~~**p**~~ **romotion:** bifurca entre promover o rechazar el candidato. 

16. **promote** ~~**m**~~ **odel:** actualiza alias, stage o tag productivo en MLflow. 

17. **reject** ~~**m**~~ **odel:** registra la raz´on del rechazo. 

18. **notify** ~~**o**~~ **r log** ~~**r**~~ **esult:** actualiza la tabla de historial visible por Streamlit. 

19. **end:** fin de la ejecuci´on. 

El DAG debe ser resistente a fallos. Como m´ınimo, se espera el uso de reintentos, manejo de errores de conexi´on, validaciones antes de escritura, logs claros y estados finales interpretables. No se debe ocultar un error cr´ıtico como si fuera una ejecuci´on exitosa. 

## **6. Componentes** 

## **6.1. API Data Source** 

Este componente entrega los lotes de datos que alimentan el sistema. No es desarrollado por los estudiantes, pero debe ser desplegado o consumido seg´un las instrucciones del docente. La API simula una fuente externa de datos que cambia en el tiempo. 

Los estudiantes deben implementar un cliente robusto para consumirla desde Airflow. Este cliente debe manejar timeouts, errores HTTP, respuestas vac´ıas, fin de datos disponibles y reintentos controlados. 

7 

## **6.2. Airflow** 

Airflow es el orquestador principal del flujo de datos y entrenamiento. Debe ejecutarse en Kubernetes y sus DAGs deben estar versionados en Git. El despliegue puede apoyarse en Helm si el equipo lo considera conveniente. 

Airflow debe encargarse de recolectar datos, validar, procesar, decidir si se entrena, ejecutar el entrenamiento, registrar en MLflow, comparar modelos y registrar el historial del proceso. Cada tarea debe tener una responsabilidad clara. No es aceptable implementar todo el flujo en una ´unica tarea monol´ıtica. 

## **6.3. MLflow** 

MLflow debe funcionar como servidor central de experimentos, artefactos y registro de modelos. Debe usar una base de datos externa para metadatos y un bucket para artefactos. No se permite SQLite como base de metadatos para este proyecto. 

MLflow debe conservar todas las versiones relevantes de modelos. El modelo productivo debe poder identificarse mediante un mecanismo consistente: alias, tag, stage o convenci´on documentada. La API de inferencia debe consultar este mecanismo para cargar el modelo correcto. 

## **6.4. Sistema de archivos o bucket** 

El almacenamiento de artefactos debe ser compatible con MLflow. Se recomienda MinIO por su compatibilidad con S3 y porque puede desplegarse dentro del cl´uster. El bucket debe conservar modelos, reportes, gr´aficos, explicaciones, objetos serializados y dem´as artefactos generados durante la experimentaci´on. 

El equipo debe documentar c´omo se crean el bucket, credenciales, secretos y pol´ıticas de acceso necesarias. 

## **6.5. Base de datos de metadatos de MLflow** 

Debe usarse una base de datos relacional para almacenar metadatos de MLflow. Se recomienda PostgreSQL. Esta base no debe mezclarse conceptualmente con RAW DATA ni CLEAN DATA, aunque el equipo puede usar el mismo motor de base de datos con bases o esquemas separados si lo justifica y configura correctamente. 

## **6.6. Base de datos RAW DATA** 

RAW DATA almacena los lotes tal como llegan desde la API y los eventos de inferencia generados por FastAPI. Debe permitir reconstruir qu´e datos estaban disponibles en cada ejecuci´on del DAG. Tambi´en debe guardar metadatos m´ınimos de ingesti´on, como identificador de lote, fecha, estado, hash o identificador de respuesta cuando aplique. 

## **6.7. Base de datos CLEAN DATA** 

CLEAN DATA almacena los datos transformados y listos para entrenamiento. Debe existir una relaci´on trazable entre los registros procesados y los lotes crudos que los originaron. El pipeline de transformaci´on debe ser reproducible y versionado. 

## **6.8. FastAPI** 

FastAPI expone el servicio de inferencia. Debe cargar el modelo productivo desde MLflow y responder predicciones a partir de los datos ingresados por el usuario o por pruebas de carga. Debe registrar cada inferencia y exponer m´etricas para Prometheus. 

- La API debe incluir, como m´ınimo: 

Endpoint de salud, por ejemplo `/health` . 

Endpoint de inferencia, por ejemplo `/predict` . 

8 

- Endpoint de m´etricas, por ejemplo `/metrics` . 

- Mecanismo documentado de recarga del modelo desde MLflow. 

- Validaci´on de entrada mediante esquemas, por ejemplo con Pydantic. 

## **6.9. Streamlit** 

Streamlit es la interfaz de usuario del proyecto. Debe consumir FastAPI para inferencia y consultar la base de datos o servicio correspondiente para mostrar el historial de entrenamiento. No debe reemplazar a MLflow; su funci´on es presentar al usuario final y al evaluador una vista clara del comportamiento del sistema. 

La interfaz debe mostrar expl´ıcitamente la versi´on del modelo usada, la predicci´on obtenida y el historial de decisiones por lote. 

## **6.10. Locust** 

Locust debe utilizarse para ejecutar pruebas de carga sobre FastAPI. El repositorio debe incluir el archivo de prueba, instrucciones de ejecuci´on y evidencia del comportamiento observado. La prueba debe ser suficientemente clara para mostrar cambios en m´etricas de Prometheus y Grafana. 

## **6.11. Prometheus y Grafana** 

Prometheus debe recolectar m´etricas de la API de inferencia. Grafana debe visualizar dichas m´etricas en al menos un dashboard. Se espera que el dashboard permita evidenciar el impacto de las pruebas de carga. Es recomendable versionar el dashboard mediante ConfigMap u otro mecanismo declarativo. 

## **6.12. GitHub Actions** 

GitHub Actions debe automatizar la integraci´on continua. Como m´ınimo, debe construir las im´agenes propias, etiquetarlas y publicarlas en DockerHub. Se espera que existan workflows para los componentes desarrollados por el equipo, por ejemplo FastAPI, Streamlit y cualquier imagen personalizada de Airflow o tareas de entrenamiento. 

El versionamiento de im´agenes debe ser claro. Se recomienda publicar etiquetas asociadas al commit SHA y, cuando aplique, una etiqueta estable para ambientes de desarrollo. 

## **6.13. DockerHub** 

DockerHub funciona como registro de im´agenes. Kubernetes no debe depender de im´agenes construidas localmente. Los manifiestos deben referenciar im´agenes publicadas y versionadas. 

## **6.14. Kubernetes** 

Todos los componentes deben estar desplegados en Kubernetes. Los estudiantes deben definir manifiestos o charts que incluyan, seg´un corresponda, `Deployment` , `StatefulSet` , `Service` , `Ingress` , `ConfigMap` , `Secret` , `PersistentVolumeClaim` , probes y recursos. 

Cada componente debe declarar: 

- `resources.requests.cpu` 

- `resources.requests.memory` 

- `resources.limits.cpu` 

- `resources.limits.memory` 

- `readinessProbe` , cuando aplique. 

- `livenessProbe` , cuando aplique. 

9 

## **6.15. Argo CD** 

Argo CD debe sincronizar el estado deseado del repositorio Git con el cl´uster de Kubernetes. El equipo debe demostrar que los cambios en manifiestos o versiones de imagen se reflejan mediante sincronizaci´on de Argo CD. No se considera suficiente aplicar manifiestos manualmente con `kubectl apply` como mecanismo principal de despliegue. 

## **7. Requerimientos no funcionales** 

- **Reproducibilidad:** debe ser posible reconstruir qu´e datos, c´odigo, par´ametros y artefactos generaron cada modelo. 

- **Trazabilidad:** cada lote debe tener un historial claro desde su ingesti´on hasta la decisi´on de entrenamiento y posible promoci´on. 

- **Resiliencia:** el sistema debe manejar errores esperables sin perder el estado del proceso. 

- **Escalabilidad m´ınima:** los servicios deben poder ejecutarse con l´ımites de recursos definidos y comportarse de forma observable bajo carga. 

- **Mantenibilidad:** el repositorio debe estar organizado por componentes y contener instrucciones claras. 

- **Seguridad b´asica:** credenciales, tokens y contrase˜nas no deben quedar quemados en el c´odigo fuente. Deben usarse Secrets o mecanismos equivalentes. 

- **Observabilidad:** la API debe exponer m´etricas ´utiles y el equipo debe demostrar su visualizaci´on en Grafana. 

## **8. Entregables** 

1. Repositorio p´ublico o accesible para el docente con todo el c´odigo fuente. 

2. DAGs de Airflow funcionales y documentados. 

3. Workflows de GitHub Actions para construir y publicar im´agenes en DockerHub. 

4. Manifiestos o charts de Kubernetes versionados en Git. 

5. Aplicaci´on de Argo CD configurada para desplegar el sistema. 

6. MLflow funcional con backend de metadatos y almacenamiento de artefactos. 

7. Bases o esquemas RAW DATA y CLEAN DATA con evidencia de uso. 

8. FastAPI funcional consumiendo el modelo productivo desde MLflow. 

9. Streamlit con inferencia e historial de entrenamiento/despliegue. 

10. Prometheus y Grafana recolectando y visualizando m´etricas de la API. 

11. Locust configurado y evidencia de prueba de carga. 

12. Documentaci´on t´ecnica del proyecto, decisiones de dise˜no, criterios de entrenamiento y reglas de promoci´on. 

13. Video de sustentaci´on publicado en YouTube con duraci´on m´axima de 10 minutos. 

## **9. Criterios m´ınimos de evaluaci´on t´ecnica** 

10 

|**Criterio**|**Evidencia esperada**|
|---|---|
|Orquestaci´on|DAGs con m´ultiples tareas, bifurcaciones y manejo de errores.|
|Datos|Separaci´on entre RAW DATA y CLEAN DATA, trazabilidad por<br>lote y registro de inferencias.|
|Decisi´on de entrenamien-<br>to|Reglas expl´ıcitas basadas en datos, cambios de distribuci´on, cali-<br>dad, volumen, nuevas categor´ıas o desempe˜no.|
|MLfow|Registro completo de experimentos, m´etricas, artefactos y modelos<br>versionados.|
|Promoci´on de modelos|Comparaci´on contra modelo productivo y promoci´on condicionada<br>por m´etricas.|
|Inferencia|API que carga el modelo desde MLfow y puede actualizarlo sin<br>redespliegue.|
|Streamlit|Interfaz de inferencia e historial claro de lotes, decisiones y resul-<br>tados.|
|CI/CD|Im´agenes construidas por GitHub Actions y publicadas en Doc-<br>kerHub.|
|GitOps|Desplieguegestionadopor Argo CD desde manifestos versionados.|
|Kubernetes|Recursos, probes, servicios, secretos, persistencia y despliegues co-<br>rrectamente defnidos.|
|Observabilidad|M´etricas de API visibles en Grafana y evidencia de prueba de carga<br>con Locust.|
|Documentaci´on|Explicaci´on t´ecnica clara, decisiones justifcadas y evidencia repro-<br>ducible.|



## **10. Video de sustentaci´on** 

Como sustentaci´on, cada equipo debe entregar un video publicado en YouTube con duraci´on m´axima de 10 minutos. El video debe mostrar, como m´ınimo: 

- Organizaci´on del repositorio. 

- Arquitectura implementada y comunicaci´on entre componentes. 

- Ejecuci´on o evidencia de los workflows de GitHub Actions. 

- Despliegue del sistema mediante Argo CD. 

- Ejecuci´on del DAG de Airflow y explicaci´on de sus bifurcaciones. 

- Registro de experimentos y modelos en MLflow. 

- Caso donde se entrena y se promueve un modelo. 

- Caso donde se entrena pero no se promueve, o donde no se entrena por decisi´on t´ecnica. 

- Uso de FastAPI y Streamlit para inferencia. 

- Historial de entrenamiento visible en Streamlit. 

- Prueba de carga con Locust y efecto observado en Grafana. 

## **11. Consideraciones cr´ıticas** 

Este proyecto no se eval´ua ´unicamente por lograr una predicci´on. Se eval´ua la capacidad de construir un sistema MLOps coherente, automatizado, observable y t´ecnicamente defendible. Un modelo con buen desempe˜no pero sin trazabilidad, sin control de versiones, sin justificaci´on de reentrenamiento o sin despliegue reproducible no cumple el objetivo del nivel. 

11 

Los estudiantes deben evitar soluciones que dependan de acciones manuales no documentadas. En particular, no se debe actualizar el modelo de la API editando c´odigo, copiando archivos manualmente al contenedor o reconstruyendo la imagen de inferencia por cada nuevo modelo. La fuente de verdad del modelo productivo debe ser MLflow. 

12 

