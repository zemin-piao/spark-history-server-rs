#!/bin/bash

# Test script to demonstrate argument-based HDFS reader selection

echo "========================================="
echo "Spark History Server - HDFS Integration Test"
echo "========================================="

echo
echo "1. Testing LOCAL filesystem mode (default):"
echo "Command: ./target/release/spark-history-server --log-directory ./test-data --host localhost --port 18080"
echo "Expected: Uses local file reader"
echo

echo "2. Testing HDFS mode without Kerberos:"
echo "Command: ./target/release/spark-history-server --hdfs --hdfs-namenode hdfs://localhost:9000 --log-directory /spark-events"
echo "Expected: Uses HDFS file reader without authentication"
echo

echo "3. Testing HDFS mode with Kerberos keytab:"
echo "Command: ./target/release/spark-history-server --hdfs \\"
echo "  --hdfs-namenode hdfs://secure-namenode:9000 \\"
echo "  --kerberos-principal spark@EXAMPLE.COM \\"
echo "  --keytab-path /etc/security/keytabs/spark.keytab \\"
echo "  --krb5-config /etc/krb5.conf \\"
echo "  --kerberos-realm EXAMPLE.COM \\"
echo "  --log-directory /hdfs/spark-events"
echo "Expected: Uses HDFS file reader with Kerberos keytab authentication"
echo

echo "4. Testing HDFS mode with custom timeouts:"
echo "Command: ./target/release/spark-history-server --hdfs \\"
echo "  --hdfs-namenode hdfs://namenode:9000 \\"
echo "  --hdfs-connection-timeout 60000 \\"
echo "  --hdfs-read-timeout 120000 \\"
echo "  --log-directory /hdfs/spark-events"
echo "Expected: Uses HDFS file reader with custom timeout settings"
echo

echo "5. Using environment variables for Kerberos (alternative):"
echo "export HDFS_NAMENODE_URL=hdfs://secure-namenode:9000"
echo "export KERBEROS_PRINCIPAL=spark@EXAMPLE.COM"
echo "export KERBEROS_KEYTAB=/etc/security/keytabs/spark.keytab"
echo "export KRB5_CONFIG=/etc/krb5.conf"
echo "export KERBEROS_REALM=EXAMPLE.COM"
echo "./target/release/spark-history-server --hdfs --log-directory /hdfs/spark-events"
echo "Expected: Uses environment variables for configuration"
echo

echo "========================================="
echo "Configuration Notes:"
echo "- Without --hdfs flag: Uses local filesystem reader"
echo "- With --hdfs flag: Uses HDFS reader"  
echo "- Kerberos is enabled if any kerberos-* arguments are provided"
echo "- Environment variables are used as fallbacks"
echo "- All timeouts have sensible defaults but can be customized"
echo "========================================="