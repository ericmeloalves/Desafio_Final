from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_CLIENTES")

# Espaço para tratar e juntar os campos e a criação do modelo dimensional
+--------------------+------------+----------+---------------+------------+--------------+----------+-----------+--------------------+-----------+----------+------------+----------------------+------------+--------------------------------+-----------------+-------------------+-----------+--------------+---------+---+--------------+---------------+-------------+-------------------+-------------+--------+----------------+------------+-----------+------------------+-----------+--------------------+-------+--------------------+--------------------+--------------------+--------------------+-----+------------+-------------+-------------+---+-----+----+
|actual_delivery_date|customer_key|  date_key|discount_amount|invoice_date|invoice_number|item_class|item_number|                item|line_number|list_price|order_number|promised_delivery_date|sales_amount|sales_amount_based_on_list_price|sales_cost_amount|sales_margin_amount|sales_price|sales_quantity|sales_rep|u_m|address_number|business_family|business_unit|           customer|customer_type|division|line_of_business|       phone|region_code|regional_sales_mgr|search_type|                city|country|  customer_address_1|  customer_address_2|  customer_address_3|  customer_address_4|state|    zip_code|division_name|  region_name|Day|Month|Year|
+--------------------+------------+----------+---------------+------------+--------------+----------+-----------+--------------------+-----------+----------+------------+----------------------+------------+--------------------------------+-----------------+-------------------+-----------+--------------+---------+---+--------------+---------------+-------------+-------------------+-------------+--------+----------------+------------+-----------+------------------+-----------+--------------------+-------+--------------------+--------------------+--------------------+--------------------+-----+------------+-------------+-------------+---+-----+----+
|          27/10/2019|    10014495|26/10/2018|         915,99|  28/10/2018|        117050|       P01|      17801|Better Fancy Cann...|       1000|   1431,23|      213985|            27/10/2019|      515,24|                         1431,23|                0|             515,24|     515,24|             1|      161| EA|      10014495|             R3|            1|Harvard Supermarket|           G2|       2|                |816-455-8733|          1|               S16|          C|           Clackamas|     US| 12900 SE Capps Road|                 ...|                 ...|                 ...|   OR|       97015|     Domestic|      Western| 28|   10|2018|
|          18/10/2019|    10016858|18/10/2018|        6112,56|  20/10/2018|        100604|       P01|      33446|Best Choice Low F...|       2000|    974,69|      200644|            18/10/2019|     5583,72|                        11696,28|                0|            5583,72|     465,31|            12|      159| EA|      10016858|             R2|            1|          IBVA Shop|           G2|       2|                |816-455-8733|          1|                S5|          C|           Beaverton|     US|6590 SW Fallbrook...|(Commercial Indus...|                 ...|                 ...|   OR|       97008|     Domestic|      Western| 20|   10|2018|
|          29/12/2018|    10000486|01/01/2018|         282,33|  03/01/2018|        329700|       P01|      38842|        Ebony Lemons|      11000|    192,91|      124076|            29/12/2018|       296,4|                          578,73|           160,72|             135,68|       98,8|             3|      162| EA|      10000486|             R3|            1|      Aberdeen Shop|           G2|       1|                |816-455-8733|          5|               S14|          C|                null|   null|                null|                null|                null|                null| null|        null|International|International| 03|   01|2018|
|          02/01/2019|    10019194|02/01/2018|         319,43|  04/01/2018|        329856|       P01|      28771|       Ebony Oranges|      35000|    157,76|      124056|            02/01/2019|      311,61|                          631,04|           213,35|              98,26|    77,9025|             4|      108| SE|      10019194|             R3|            1|     Kerite Company|           G2|       1|                |816-455-8733|          5|               S14|          C|                null|   null|                null|                null|                null|                null| null|        null|International|International| 04|   01|2018|
|          02/01/2019|    10015253|03/01/2018|         265,45|  05/01/2018|        329966|       P01|      37170|Fast BBQ Potato C...|       8000|    190,85|      124291|            02/01/2019|       307,1|                          572,55|            127,9|              179,2|102,3666667|             3|      158| EA|      10015253|             R3|            1|     Hekimian Store|           G3|       1|                |816-455-8733|          5|                S8|          C|                null|   null|                null|                null|                null|                null| null|        null|International|International| 05|   01|2018|
|          08/01/2019|    10011345|08/01/2018|         423,69|  10/01/2018|        330315|       P01|     465052|  Club String Cheese|       5000|    960,15|      124088|            08/01/2019|      536,46|                          960,15|           300,27|             236,19|     536,46|             1|      131| EA|      10011345|             R3|            1|      Emergent Shop|           G2|       1|                |816-455-8733|          5|               S16|          C|                null|   null|                null|                null|                null|                null| null|        null|International|International| 10|   01|2018|
|          08/01/2019|    10022962|08/01/2018|         823,28|  10/01/2018|        119676|       P01|      26651|Gorilla Mild Ched...|      10000|    352,96|      216645|            08/01/2019|      941,52|                          1764,8|           418,41|             523,11|    188,304|             5|      147| EA|      10022962|             R3|            1|       Reflex Store|           G1|       1|              M1|816-455-8733|          5|               S19|          C|                null|   null|                null|                null|                null|                null| null|        null|International|International| 10|   01|2018|
|          08/01/2019|    10027119|08/01/2018|         227,55|  10/01/2018|        330305|          |      64798|American Roasted ...|       7000|    456,57|      124350|            08/01/2019|      229,02|                          456,57|           130,72|               98,3|     229,02|             1|      155| EA|      10027119|             R3|            1|       Yurie Market|           G2|       1|                |816-455-8733|          5|               S16|          C|                 ...|     UK|                 ...|                 ...|                 ...|                 ...|     |            |International|International| 10|   01|2018|
|          09/01/2019|    10016548|09/01/2018|     21210,7536|  11/01/2018|        330442|       P01|      26502|Bravo Large Canne...|       4000|  905,8107|      122187|            09/01/2019|    22268,16|                      43478,9136|         12813,33|            9454,83|     463,92|            48|      118| EA|      10016548|             R3|            1|    Home Superstore|           G2|       2|                |816-455-8733|          2|               S16|          C|              Mobile|     US| 4700 Rangeline Road|                 ...|                 ...|                 ...|   AL|       36619|     Domestic|     Southern| 11|   01|2018|
|          13/01/2019|    10012422|13/01/2018|         651,05|  15/01/2018|        120072|       P01|      45880|Red Spade Low Fat...|      27000|    767,75|      216976|            13/01/2019|      884,45|                          1535,5|           576,32|             308,13|    442,225|             2|      176| EA|      10012422|             R3|            1|     Finishing Shop|           G2|       2|                |816-455-8733|          2|               S16|          C|             Saginaw|     US|          PO Box 510|                 ...|                 ...|                 ...|   AL|       35137|     Domestic|     Southern| 15|   01|2018|




# criando o fato
ft_vendas = []

#criando as dimensões
dim_clientes = []

# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_hive/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_hive/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(dim_clientes, 'dimclientes')