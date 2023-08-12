from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark example').getOrCreate()

data1 = [('James','','Smith','1991-04-01','M',3000),
        ('Michael','Rose','','2000-05-19','M',4000),
        ('Robert','','Williams','1978-09-05','M',4000),
        ('Maria','Anne','Jones','1967-12-01','F',4000),
        ('Jen','Mary','Brown','1980-02-17','F',-1)
]
column1 = ["firstname","middlename","lastname","dob","gender","salary"]

data2 = [
        ("James",None,"Smith","OH","M"),
        ("Anna","Rose","","NY","F"),
        ("Julia","","Williams","OH","F"),
        ("Maria","Anne","Jones","NY","M"),
        ("Jen","Mary","Brown","NY","M"),
        ("Mike","Mary","Williams","OH","M")
]
column2 = ["fname","mname","lname","st","gd"]

df1 = spark.createDataFrame(data=data1, schema = column1)
df1.show()

df2 = spark.createDataFrame(data=data2, schema = column2)
df2.show()
df2.printSchema()









