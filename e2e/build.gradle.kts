dependencies {
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.jackson.databind)
    testImplementation(libs.awaitility)
    testRuntimeOnly(libs.logback.classic)
}

tasks.test { enabled = false }

tasks.register<Test>("e2eTest") {
    description = "Runs the end-to-end tests on the full stack in Docker."
    group = "verification"
    outputs.upToDateWhen { false }
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    val prebuilt = providers.environmentVariable("E2E_PREBUILT_IMAGES").isPresent
    if (!prebuilt) {
        dependsOn(
            ":wallet-api:bootJar",
            ":completion-service:bootJar",
            ":transfer-executor:bootJar",
            ":timeout-checker:bootJar",
            ":mocks:bootJar",
        )
    }
    dependsOn(":bridge:installDist")
    systemProperty("e2e.root", rootDir.absolutePath)
    systemProperty("e2e.prebuilt", prebuilt)
}
