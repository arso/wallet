# Implementation preferences

## Libraries and frameworks

Use freely:

- Java 25, Gradle (Kotlin DSL) with the version catalog
- Spring Boot 4: Web MVC on virtual threads, Actuator
- AWS SDK v2: DynamoDB (sync, Apache 5 HTTP client), DynamoDB Streams; AWS Lambda Java core / events
- Kafka clients, Spring Kafka
- Jackson 3
- Resilience4j
- Micrometer + Prometheus, Micrometer Tracing + OpenTelemetry
- JUnit 5, AssertJ, ArchUnit, Testcontainers (DynamoDB Local, Kafka), WireMock

Do not use:

- WebFlux or any reactive stack
- LocalStack (no license); use DynamoDB Local
- Kafka Connect
- Spring Kafka listener containers / `DefaultErrorHandler` for the worker pool

Ask for approval before adding anything else.

## Rules

- Avoid code comments. Keep only critical ones and TODOs derived from the implementation plan (e.g. `TODO(phase 10)`).
- Keep code clean, short and simple: the less, the better.
- No logging on the critical path (latency). Log errors only when needed, at boundaries.
- Use JDK 25 features.
- Spring Boot: no stereotype annotations for wiring (`@Service`, `@Component`, ...). Use scoped `@Configuration` classes with `@Bean` methods, so integration tests can compose them.
- ArchUnit tests in every module enforce hexagonal architecture with the onion approach (`HexagonalRules`).
- When in doubt, ask.
- After implementing a feature, run the tests. Prefer full integration tests (Spring Boot tests with Testcontainers) over detailed unit tests. E2E tests (REST calls only, asserting on results) come later.
