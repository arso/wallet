package org.arso.wallet.e2e;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

final class Stack {

    private static final Path ROOT = Path.of(System.getProperty("e2e.root"));
    private static final boolean PREBUILT = Boolean.getBoolean("e2e.prebuilt");
    private static final String JRE = "azul/zulu-openjdk-alpine:25-jre-headless";
    private static final String DYNAMODB_URL = "http://dynamodb:8000";
    private static final String KAFKA_URL = "kafka:19092";
    private static final Network NETWORK = Network.newNetwork();

    private static final GenericContainer<?> DYNAMODB = new GenericContainer<>("amazon/dynamodb-local:3.3.2")
            .withNetwork(NETWORK)
            .withNetworkAliases("dynamodb")
            .withExposedPorts(8000)
            .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0")
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withListener(KAFKA_URL)
            .withEnv("KAFKA_NUM_PARTITIONS", "8");
    private static final GenericContainer<?> WALLET_API = app("wallet-api", 8081, 8080)
            .withNetworkAliases("wallet-api")
            .withEnv("WALLET_DYNAMODB_ENDPOINT", DYNAMODB_URL)
            .withEnv("WALLET_BENEFICIARY_URL", "http://mocks:8090")
            .waitingFor(Wait.forHttp("/actuator/health/readiness").forPort(8081).withStartupTimeout(Duration.ofMinutes(2)));
    private static final GenericContainer<?> MOCKS = app("mocks", 8091)
            .withNetworkAliases("mocks")
            .withEnv("MOCKS_KAFKABOOTSTRAPSERVERS", KAFKA_URL)
            .withEnv("MOCKS_AML_WALLETURL", "http://wallet-api:8080")
            .withEnv("MOCKS_VERIFIERS_MINLATENCY", "50ms")
            .withEnv("MOCKS_VERIFIERS_MAXLATENCY", "200ms")
            .withEnv("MOCKS_VERIFIERS_SLOWLATENCY", "4s");
    private static final GenericContainer<?> BRIDGE = new GenericContainer<>(new ImageFromDockerfile("wallet-e2e-bridge", false)
            .withFileFromPath("bridge", ROOT.resolve("bridge/build/install/bridge"))
            .withDockerfileFromBuilder(b -> b.from(JRE).copy("bridge", "/bridge").entryPoint("/bridge/bin/bridge").build()))
            .withNetwork(NETWORK)
            .withEnv("WALLET_DYNAMODB_ENDPOINT", DYNAMODB_URL)
            .withEnv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_URL)
            .withEnv("JAVA_OPTS", "--enable-native-access=ALL-UNNAMED -Xmx128m");
    private static final GenericContainer<?> COMPLETION = app("completion-service", 8083)
            .withEnv("COMPLETION_DYNAMODB_ENDPOINT", DYNAMODB_URL)
            .withEnv("COMPLETION_KAFKA_BOOTSTRAPSERVERS", KAFKA_URL);
    private static final GenericContainer<?> EXECUTOR = app("transfer-executor", 8085)
            .withEnv("EXECUTOR_DYNAMODB_ENDPOINT", DYNAMODB_URL)
            .withEnv("EXECUTOR_KAFKA_BOOTSTRAPSERVERS", KAFKA_URL)
            .withEnv("EXECUTOR_TRANSFER_URL", "http://mocks:8090");
    private static final GenericContainer<?> CHECKER = app("timeout-checker", 8087)
            .withEnv("CHECKER_DYNAMODB_ENDPOINT", DYNAMODB_URL)
            .withEnv("CHECKER_KAFKA_BOOTSTRAPSERVERS", KAFKA_URL);

    static {
        Startables.deepStart(DYNAMODB, KAFKA).join();
        Startables.deepStart(WALLET_API, MOCKS).join();
        Startables.deepStart(BRIDGE, COMPLETION, EXECUTOR, CHECKER).join();
    }

    private Stack() {
    }

    static URI wallet() {
        return URI.create("http://" + WALLET_API.getHost() + ":" + WALLET_API.getMappedPort(8080));
    }

    private static GenericContainer<?> app(String module, int managementPort, Integer... ports) {
        var container = PREBUILT
                ? new GenericContainer<>(DockerImageName.parse("wallet-e2e-" + module))
                : new GenericContainer<>(new ImageFromDockerfile("wallet-e2e-" + module, false)
                .withDockerfile(ROOT.resolve(module).resolve("Dockerfile")));
        container
                .withNetwork(NETWORK)
                .withEnv("JAVA_TOOL_OPTIONS", "-Xmx256m -XX:+UseSerialGC -XX:TieredStopAtLevel=1")
                .withEnv("MANAGEMENT_TRACING_EXPORT_OTLP_ENABLED", "false")
                .waitingFor(Wait.forHttp("/actuator/health").forPort(managementPort).withStartupTimeout(Duration.ofMinutes(2)));
        container.addExposedPort(managementPort);
        for (var port : ports) {
            container.addExposedPort(port);
        }
        return container;
    }
}
