plugins {
    `java-test-fixtures`
}

dependencies {
    api(libs.awssdk.dynamodb)
    implementation(libs.awssdk.apache5)

    testFixturesApi(libs.awssdk.dynamodb)
    testFixturesApi(libs.archunit)
    testFixturesApi(libs.testcontainers)
}
