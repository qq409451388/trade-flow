# 交易流水处理服务 Dockerfile
FROM eclipse-temurin:17-jre-alpine
LABEL maintainer="trade-flow"

WORKDIR /app

COPY target/trade-pipeline-*.jar app.jar

EXPOSE 8083

ENV JAVA_OPTS="-Xms256m -Xmx512m -Duser.timezone=Asia/Shanghai"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
