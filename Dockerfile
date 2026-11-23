FROM eclipse-temurin:17-jre

# Running applications with user privileges helps to mitigate some risks
RUN addgroup --system spring && adduser --system --ingroup spring spring
USER spring:spring

WORKDIR /app
COPY build/libs/*-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]