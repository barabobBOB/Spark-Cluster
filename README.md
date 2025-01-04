# Spark-Cluster

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
