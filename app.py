from pyspark.sql.functions import max
from pyspark.sql.functions import desc, asc, expr

arquivo = "world_population.csv"

df = spark \
.read.format("CSV")\
.option("inferSchema", "True")\
.option("header", "True")\
.csv(arquivo)

display(df.take(5))
df.count()

#busca o maior valor da coluna apontada
display(df.select(max("2022 Population")).take(1))

#busca valores menores que 6 na coluna Rank ordenado de maneira crescente pela coluna Rank
display(df.filter("Rank < 6").sort("Rank"))

display(df.where("Rank < 4").sort("Rank"))

#ordena de modo decrescente
display(df.orderBy(expr("Rank desc")).take(5))

#exibir estatisticas descritivas
display(df.describe().take(10))

#iterando todas as linhas do dataframee
for i in df.collect():
    print(i)
    print(i[0], i[1] * 2)
    
#adicionando coluna no dataframe
df = df.withColumn('Nova coluna', df['Rank']+1)
display(df.take(20))

#remover coluna do dataframe
df = df.drop('Nova coluna')
df.show(10)

#renomear coluna no dataframe
display(df.withColumnRenamed('Rank','Posição').take(10))