### 테스트 데이터 생성

```bash
CREATE TABLE output_table (
    region VARCHAR(50),
    sales INTEGER
);

CREATE TABLE output_table (
    region VARCHAR(50),
    sum_sales INTEGER
);

INSERT INTO input_table (region, sales)
VALUES ('North', 100), ('South', 200), ('East', 150), ('West', 300);
```

### 테스트

```
$ docker compose up
$ docker exec -it spark-master bash
$ spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --jars /app/postgresql-42.5.4.jar \
    --files /app/postgresql-42.5.4.jar \
    /app/test.py
```
