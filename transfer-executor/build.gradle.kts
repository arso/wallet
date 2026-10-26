plugins {
    alias(libs.plugins.spring.boot)
}

dependencies {
    implementation(project(":libs:events"))
    implementation(project(":libs:worker-pool"))
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.resilience4j.circuitbreaker)
    runtimeOnly(libs.micrometer.prometheus)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.wiremock)
    testImplementation(testFixtures(project(":libs:ledger-core")))
    testImplementation(testFixtures(project(":libs:worker-pool")))
}

tasks.jar { enabled = false }
