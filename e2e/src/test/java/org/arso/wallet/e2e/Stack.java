package org.arso.wallet.e2e;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

final class Stack {

    static final String STEPS = "aml,account_verified,beneficiary_credibility";

    private static final Path ROOT = Path.of(System.getProperty("e2e.root"));
    private static final boolean PREBUILT = Boolean.getBoolean("e2e.prebuilt");
    private static final String JRE = "azul/zulu-openjdk-alpine:25-jre-headless";
    private static final String DYNAMODB_URL = "http://dynamodb:8000";
    private static final String KAFKA_URL = "kafka:19092";
    private static final Network NETWORK = Network.newNetwork();
    private static final Map<String, ImageFromDockerfile> IMAGES = new HashMap<>();

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

    private static final GenericContainer<?> MOCKS = mocksContainer();

    private static GenericContainer<?> walletApi;
    private static GenericContainer<?> bridge;
    private static GenericContainer<?> executor;

    static {
        Startables.deepStart(DYNAMODB, KAFKA).join();
        walletApi = walletApi(STEPS);
        Startables.deepStart(walletApi, MOCKS).join();
        bridge = bridge();
        executor = executor();
        Startables.deepStart(bridge, executor, completion(), checker()).join();
    }

    private Stack() {}

    static URI wallet() {
        return url(walletApi, 8080);
    }

    static URI mocks() {
        return url(MOCKS, 8090);
    }

    static void restartWalletApi(String steps) {
        walletApi.stop();
        walletApi = walletApi(steps);
        walletApi.start();
    }

    /** The local bridge reads the stream from the start again, so every ledger entry is published a second time. */
    static void restartBridge() {
        bridge.stop();
        bridge = bridge();
        bridge.start();
    }

    static void crashExecutor() {
        executor.stop();
        executor = executor();
        executor.start();
    }

    static GenericContainer<?> startCompletion() {
        var container = completion();
        container.start();
        return container;
    }

    static GenericContainer<?> startExecutor() {
        var container = executor();
        container.start();
        return container;
    }

    /** SIGTERM, so the consumer leaves its group at once. */
    static void stopGracefully(GenericContainer<?> container) {
        container.getDockerClient().stopContainerCmd(container.getContainerId()).withTimeout(30).exec();
        container.stop();
    }

    private static GenericContainer<?> walletApi(String steps) {
        return app("wallet-api", 8081, 8080)
                .withNetworkAliases("wallet-api")
                .withEnv("WALLET_DYNAMODB_ENDPOINT", DYNAMODB_URL)
                .withEnv("WALLET_BENEFICIARY_URL", "http://mocks:8090")
                .withEnv("WALLET_VERIFICATION_STEPS", steps)
                .waitingFor(Wait.forHttp("/actuator/health/readiness").forPort(8081).withStartupTimeout(Duration.ofMinutes(2)));
    }

    private static GenericContainer<?> mocksContainer() {
        return app("mocks", 8091, 8090)
                .withNetworkAliases("mocks")
                .withEnv("MOCKS_KAFKABOOTSTRAPSERVERS", KAFKA_URL)
                .withEnv("MOCKS_AML_WALLETURL", "http://wallet-api:8080")
                .withEnv("MOCKS_VERIFIERS_STEPS", STEPS)
                .withEnv("MOCKS_VERIFIERS_MINLATENCY", "50ms")
                .withEnv("MOCKS_VERIFIERS_MAXLATENCY", "200ms")
                .withEnv("MOCKS_VERIFIERS_SLOWLATENCY", "4s")
                .withEnv("MOCKS_TRANSFER_DELAY", "3s");
    }

    private static GenericContainer<?> bridge() {
        var image = IMAGES.computeIfAbsent("bridge", _ -> new ImageFromDockerfile("wallet-e2e-bridge", false)
                .withFileFromPath("bridge", ROOT.resolve("bridge/build/install/bridge"))
                .withDockerfileFromBuilder(b -> b.from(JRE).copy("bridge", "/bridge").entryPoint("/bridge/bin/bridge").build()));
        return new GenericContainer<>(image)
                .withNetwork(NETWORK)
                .withEnv("WALLET_DYNAMODB_ENDPOINT", DYNAMODB_URL)
                .withEnv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_URL)
                .withEnv("JAVA_OPTS", "--enable-native-access=ALL-UNNAMED -Xmx128m");
    }

    private static GenericContainer<?> completion() {
        return app("completion-service", 8083)
                .withEnv("COMPLETION_DYNAMODB_ENDPOINT", DYNAMODB_URL)
                .withEnv("COMPLETION_KAFKA_BOOTSTRAPSERVERS", KAFKA_URL);
    }

    private static GenericContainer<?> executor() {
        return app("transfer-executor", 8085)
                .withEnv("EXECUTOR_DYNAMODB_ENDPOINT", DYNAMODB_URL)
                .withEnv("EXECUTOR_KAFKA_BOOTSTRAPSERVERS", KAFKA_URL)
                .withEnv("EXECUTOR_TRANSFER_URL", "http://mocks:8090");
    }

    private static GenericContainer<?> checker() {
        return app("timeout-checker", 8087)
                .withEnv("CHECKER_DYNAMODB_ENDPOINT", DYNAMODB_URL)
                .withEnv("CHECKER_KAFKA_BOOTSTRAPSERVERS", KAFKA_URL)
                .withEnv("CHECKER_VERIFICATIONTIMEOUT", "15s");
    }

    private static GenericContainer<?> app(String module, int managementPort, Integer... ports) {
        var container = PREBUILT
                ? new GenericContainer<>(DockerImageName.parse("wallet-e2e-" + module))
                : new GenericContainer<>(IMAGES.computeIfAbsent(module, _ -> new ImageFromDockerfile("wallet-e2e-" + module, false)
                        .withDockerfile(ROOT.resolve(module).resolve("Dockerfile"))));
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

    private static URI url(GenericContainer<?> container, int port) {
        return URI.create("http://" + container.getHost() + ":" + container.getMappedPort(port));
    }
}
