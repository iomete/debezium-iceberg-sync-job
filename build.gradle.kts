val quarkusPluginVersion: String by project

plugins {
    java

    id("io.quarkus") version "2.16.3.Final"
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation(enforcedPlatform("io.quarkus.platform:quarkus-bom:2.16.3.Final"))

    implementation("io.debezium:debezium-server-core:2.1.4.Final")
    implementation("io.debezium:debezium-scripting:2.1.4.Final")
    implementation("io.debezium:debezium-connector-postgres:2.1.4.Final")
    implementation("io.debezium:debezium-connector-mysql:2.1.4.Final")

    implementation("io.debezium:debezium-connector-sqlserver:2.1.4.Final")


    //Apache Spark
    compileOnly("org.apache.spark:spark-sql_2.12:3.2.1") {
        exclude(group = "org.slf4j")
    }
    compileOnly("org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1")
}

configurations.all {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.scala-lang") {
            useVersion("2.12.0")
            because("We need Scala 2.12 for compatibility.")
        }


        if (requested.group == "org.antlr") {
            useVersion("4.8")
            //because("We need Antlr 4.8 for compatibility with Spark 3.3.")
        }
    }
}

group = "io.debezium.server.iomete"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

