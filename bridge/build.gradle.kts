plugins {
    application
}

application {
    mainClass = "org.arso.wallet.bridge.config.LocalRunnerMain"
}

dependencies {
    implementation(project(":libs:ledger-core"))
    implementation(project(":libs:events"))
    implementation(libs.kafka.clients)
    implementation(libs.awssdk.apache5)
    implementation(libs.aws.lambda.core)
    implementation(libs.aws.lambda.events)
    implementation(libs.aws.lambda.events.transformer)
    runtimeOnly(libs.logback.classic)
    runtimeOnly(libs.slf4j.jdk.platform.logging)

    testImplementation(testFixtures(project(":libs:ledger-core")))
    testImplementation(libs.testcontainers.kafka)
}
