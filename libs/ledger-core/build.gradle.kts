plugins {
    `java-test-fixtures`
}

dependencies {
    api(libs.awssdk.dynamodb)
    implementation(libs.awssdk.apache5)
    implementation(libs.micrometer.core)

    testFixturesApi(libs.awssdk.dynamodb)
    testFixturesApi(libs.archunit)
    testFixturesApi(libs.testcontainers)
    testFixturesApi(libs.micrometer.core)
    testFixturesImplementation(libs.assertj)
}
