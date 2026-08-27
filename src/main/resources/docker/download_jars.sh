set -e

JARS_DIR="$(dirname "$0")/jars"
mkdir -p "$JARS_DIR"

echo "Downloading JARs into $JARS_DIR ..."

# S3A connector — must match Hadoop 3.3.4 (bundled in spark:3.5.6)
curl -L -o "$JARS_DIR/hadoop-aws-3.3.4.jar" \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

# AWS SDK bundle — compatible version for hadoop-aws 3.3.4
curl -L -o "$JARS_DIR/aws-java-sdk-bundle-1.12.262.jar" \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# PostgreSQL JDBC driver — for Hive metastore
curl -L -o "$JARS_DIR/postgresql-42.7.3.jar" \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"

echo ""
echo "Verifying downloads..."
ls -lh "$JARS_DIR"

echo ""
echo "All JARs downloaded. You can now run: docker compose up -d"