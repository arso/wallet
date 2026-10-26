plugins {
    `java-test-fixtures`
}

dependencies {
    api(libs.awssdk.dynamodb)

    testFixturesApi(libs.awssdk.dynamodb)
    testFixturesApi(libs.archunit)
    testFixturesApi(libs.testcontainers)
}
