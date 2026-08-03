# 多阶段构建：Maven 编译 -> 精简 JRE 运行镜像
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline || true
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/mvp-backend-1.0.0.jar app.jar
COPY plugins ./plugins
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
