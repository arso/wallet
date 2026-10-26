FROM azul/zulu-openjdk-alpine:25-jre-headless
EXPOSE 8082:8082

RUN mkdir /wallet
RUN adduser -s /bin/true -u 1000 -D -h /wallet wallet \
    && chown -R "wallet" "/wallet" \
    && chmod 700 "/wallet"

WORKDIR /wallet
COPY build/libs/*.jar /wallet/application.jar

RUN apk add curl

USER wallet
ENTRYPOINT ["java", "-jar", "/wallet/application.jar"]
