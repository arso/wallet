plugins {
    application
}

application {
    mainClass = "org.arso.wallet.worker.Replay"
}

dependencies {
    api(project(":libs:ledger-core"))
    api(libs.kafka.clients)
    api(libs.micrometer.core)
    implementation(libs.jackson.databind)
    runtimeOnly(libs.logback.classic)
    runtimeOnly(libs.slf4j.jdk.platform.logging)

    testImplementation(testFixtures(project(":libs:ledger-core")))
    testImplementation(project(":libs:events"))
    testImplementation(libs.testcontainers.kafka)
}
