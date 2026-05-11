#!/bin/bash
# Downloads all dependency JARs sequentially to avoid Maven Central rate-limiting.
# Each JAR is downloaded one at a time with retry logic.

set -euo pipefail

LIB_DIR="$(cd "$(dirname "$0")" && pwd)/lib"
MAVEN_BASE="https://repo1.maven.org/maven2"
MAX_RETRIES=3
RETRY_DELAY=5

mkdir -p "$LIB_DIR"

download_jar() {
    local group_path="$1"
    local artifact="$2"
    local version="$3"
    local filename="${4:-${artifact}-${version}.jar}"
    local url="${MAVEN_BASE}/${group_path}/${artifact}/${version}/${filename}"
    local dest="${LIB_DIR}/${filename}"

    if [ -f "$dest" ]; then
        return 0
    fi

    for attempt in $(seq 1 $MAX_RETRIES); do
        if curl -sSfL -o "$dest" "$url" 2>/dev/null; then
            echo "  OK: $filename"
            return 0
        fi
        if [ $attempt -lt $MAX_RETRIES ]; then
            sleep $RETRY_DELAY
        fi
    done

    echo "  FAILED: $filename ($url)" >&2
    return 1
}

echo "Downloading dependencies to $LIB_DIR ..."

# JDBC Drivers
download_jar "com/yugabyte" "jdbc-yugabytedb" "42.3.5-yb-4"
download_jar "org/hsqldb" "hsqldb" "2.4.1"
download_jar "org/postgresql" "postgresql" "42.4.5"
download_jar "com/zaxxer" "HikariCP" "3.4.5"

# Transitive deps (explicitly managed versions)
download_jar "javassist" "javassist" "3.12.0.GA"
download_jar "asm" "asm" "3.1"
download_jar "org/hibernate/javax/persistence" "hibernate-jpa-2.0-api" "1.0.1.Final"
download_jar "org/hibernate" "hibernate-commons-annotations" "3.2.0.Final"
download_jar "javax/transaction" "transaction-api" "1.1"
download_jar "org/slf4j" "jcl-over-slf4j" "1.7.36"
download_jar "org/slf4j" "slf4j-api" "1.7.36"
download_jar "com/github/ben-manes/caffeine" "caffeine" "2.9.3"
download_jar "commons-logging" "commons-logging" "1.2"
download_jar "commons-codec" "commons-codec" "1.15"
download_jar "commons-collections" "commons-collections" "3.2.2"
download_jar "commons-lang" "commons-lang" "2.6"
download_jar "commons-digester" "commons-digester" "2.1"
download_jar "commons-beanutils" "commons-beanutils" "1.9.4"
download_jar "antlr" "antlr" "2.7.7"
download_jar "dom4j" "dom4j" "1.6.1"
download_jar "xml-apis" "xml-apis" "1.4.01"
download_jar "javax/transaction" "jta" "1.1"
download_jar "cglib" "cglib" "3.3.0"
download_jar "org/ow2/asm" "asm" "7.1" "asm-7.1.jar"

# Core Libraries
download_jar "org/apache/httpcomponents" "httpclient" "4.3"
download_jar "org/apache/httpcomponents" "httpcore" "4.3"
download_jar "org/apache/httpcomponents" "httpmime" "4.3"
download_jar "log4j" "log4j" "1.2.17"
download_jar "net/sourceforge/collections" "collections-generic" "4.01"
download_jar "commons-configuration" "commons-configuration" "1.6"
download_jar "net/sf/opencsv" "opencsv" "2.3"
download_jar "ch/ethz/ganymed" "ganymed-ssh2" "261"
download_jar "commons-cli" "commons-cli" "1.2"
download_jar "org/hibernate" "hibernate-core" "3.3.0.GA"
download_jar "org/hibernate" "hibernate-entitymanager" "3.6.10.Final"
download_jar "org/hibernate" "hibernate-annotations" "3.5.6-Final"
download_jar "commons-jxpath" "commons-jxpath" "1.3"
download_jar "commons-io" "commons-io" "2.2"
download_jar "org/hdrhistogram" "HdrHistogram" "2.1.4"
download_jar "com/google/code/gson" "gson" "2.8.7"

# Javax
download_jar "javax/persistence" "persistence-api" "1.0"
download_jar "javax/activation" "activation" "1.1.1"
download_jar "javax/jdo" "jdo2-api" "2.3-eb"
download_jar "javax/xml/bind" "jaxb-api" "2.3.0"
download_jar "com/sun/xml/bind" "jaxb-core" "2.3.0.1"
download_jar "com/sun/xml/bind" "jaxb-impl" "2.3.0.1"

# Transitive deps of caffeine 2.9.3
download_jar "org/checkerframework" "checker-qual" "3.19.0"
download_jar "com/google/errorprone" "error_prone_annotations" "2.10.0"

echo "All dependencies downloaded successfully."
