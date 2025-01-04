from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# SparkSession 생성
spark = SparkSession.builder \
    .appName("ETL Pipeline with Test Data") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.extraClassPath", "./postgresql-42.5.4.jar") \
    .config("spark.driver.extraClassPath", "/app/postgresql-42.5.4.jar") \
    .getOrCreate()

# PostgreSQL 설정
jdbc_url = "jdbc:postgresql://postgres:5432/mydb"
db_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}

# 데이터 읽기 시도
try:
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "input_table") \
        .options(**db_properties) \
        .load()
    print("데이터 읽기 성공")
except Exception as e:
    print("데이터 읽기 실패:", e)
    print("테스트 데이터를 생성합니다...")

    # 테스트 데이터 생성
    schema = StructType([
        StructField("region", StringType(), True),
        StructField("sales", IntegerType(), True)
    ])
    test_data = [
        ("North", 100),
        ("South", 200),
        ("East", 150),
        ("West", 300)
    ]
    df = spark.createDataFrame(test_data, schema)
    print("테스트 데이터 생성 완료")

# 데이터 변환 (예: 지역별 매출 집계)
result_df = df.groupBy("region").sum("sales")

# 결과 데이터 PostgreSQL에 쓰기
result_df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "output_table") \
    .options(**db_properties) \
    .mode("overwrite") \
    .save()

print("ETL 작업 완료")
