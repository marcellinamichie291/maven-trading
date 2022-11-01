FROM openjdk:11-jre-slim
COPY --from=build /home/app/target/*.jar /usr/local/lib/binance-client-1.0-SNAPSHOT.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/usr/local/lib/binance-client-1.0-SNAPSHOT.jar"]
