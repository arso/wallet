plugins {
    alias(libs.plugins.spring.boot)
}

dependencies {
    implementation(project(":libs:ledger-core"))
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.awssdk.apache5)
    runtimeOnly(libs.micrometer.prometheus)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(testFixtures(project(":libs:ledger-core")))
}

tasks.jar { enabled = false }
