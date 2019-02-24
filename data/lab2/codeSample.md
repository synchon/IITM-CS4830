
# Code present in screenshot:

## Scala Example

```scala
val hiveContext = new org.apache.spark.sql.SparkSession.Builder().getOrCreate();
val riskFactorDataFrame = spark.read.format("csv").option("header", "true").load("hdfs:///tmp/data/trucks.csv");
riskFactorDataFrame.createOrReplaceTempView("trucks");
```


```scala
val ans = hiveContext.sql("SELECT driverid, jun13_miles FROM trucks LIMIT 15");
ans.show();
```

## Python Example

```python
%pyspark
from pyspark.sql import SQLContext
from pyspark.sql.types import DoubleType

sqlContext = SQLContext(sc)
dfObj = spark.read.csv("hdfs:///tmp/data/trucks.csv", header=True, mode="DROPMALFORMED")
dfObj.columns
```

```python
%pyspark
from pyspark.sql.types import DoubleType
changedTypedfTmp = dfObj.withColumn("jun13_miles", dfObj["jun13_miles"].cast(DoubleType()))
changedTypedf = changedTypedfTmp.withColumn("jun13_gas", dfObj["jun13_gas"].cast(DoubleType()))
changedTypedf.registerTempTable("trucks")
```

```sql
%sql
SELECT jun13_miles, jun13_gas FROM trucks
```
