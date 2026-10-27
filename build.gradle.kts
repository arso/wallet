plugins {
    alias(libs.plugins.spring.boot) apply false
}

subprojects {
    apply(plugin = "java-library")

    group = "org.arso"
    version = "0.0.1-SNAPSHOT"

    repositories { mavenCentral() }

    extensions.configure<JavaPluginExtension> { toolchain { languageVersion = JavaLanguageVersion.of(25) } }

    configurations.matching { it.name in setOf("implementation", "testFixturesApi") }.configureEach {
        project.dependencies.add(name, project.dependencies.platform(rootProject.libs.spring.boot.dependencies))
        project.dependencies.add(name, project.dependencies.platform(rootProject.libs.awssdk.bom))
    }

    dependencies {
        "testImplementation"(rootProject.libs.junit.jupiter)
        "testImplementation"(rootProject.libs.assertj)
        "testRuntimeOnly"(rootProject.libs.junit.launcher)
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        jvmArgs("-Xshare:off", "--enable-native-access=ALL-UNNAMED")
        systemProperty("management.tracing.export.otlp.enabled", "false")
    }

    tasks.withType<JavaExec> { jvmArgs("--enable-native-access=ALL-UNNAMED") }

    plugins.withId("org.springframework.boot") {
        tasks.named<Jar>("bootJar") { manifest { attributes("Enable-Native-Access" to "ALL-UNNAMED") } }
    }
}
