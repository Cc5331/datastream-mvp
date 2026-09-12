# 在官方 Flink 1.18.1 基础上加入 UDF 与连接器 jar
FROM flink:1.18.1
COPY lib/*.jar /opt/flink/lib/
# 仅保留当前 MySQL Connector/J，避免重复驱动版本争抢注册。
RUN rm -f /opt/flink/lib/mysql-connector-java-8.0.28.jar
