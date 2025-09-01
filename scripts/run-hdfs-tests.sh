#!/bin/bash

# HDFS Integration Test Runner
# This script runs all HDFS-related tests for the Spark History Server

echo "========================================="
echo "HDFS Integration Test Runner"
echo "========================================="
echo

echo "🧪 Running HDFS Integration Tests..."
echo

echo "1. Basic HDFS Integration Tests:"
echo "   Testing mock HDFS functionality and basic integration"
cargo test hdfs_integration_test --release --quiet
echo

echo "2. Comprehensive HDFS Tests:"
echo "   Testing file operations, error handling, and performance"
cargo test test_hdfs_comprehensive_file_operations --release --quiet
cargo test test_hdfs_error_handling_scenarios --release --quiet
cargo test test_hdfs_cluster_availability --release --quiet
cargo test test_hdfs_access_patterns --release --quiet
cargo test test_hdfs_concurrent_operations --release --quiet
cargo test test_history_provider_hdfs_integration --release --quiet
echo

echo "3. Kerberos Authentication Tests:"
echo "   Testing various Kerberos authentication scenarios"
cargo test test_kerberos_configuration --release --quiet
cargo test test_hdfs_with_kerberos_authentication --release --quiet
cargo test test_kerberos_valid_authentication --release --quiet
cargo test test_kerberos_expired_tickets --release --quiet
cargo test test_kerberos_invalid_credentials --release --quiet
cargo test test_kerberos_keytab_vs_ticket_cache --release --quiet
cargo test test_kerberos_invalid_keytab --release --quiet
cargo test test_kerberos_invalid_ticket_cache --release --quiet
cargo test test_kerberos_network_errors --release --quiet
cargo test test_kerberos_concurrent_operations --release --quiet
echo

echo "4. Argument-Based Reader Selection Tests:"
echo "   Testing runtime switching between local and HDFS readers"
cargo test test_argument_based_local_reader_selection --release --quiet
cargo test test_argument_based_hdfs_reader_selection --release --quiet
cargo test test_argument_based_hdfs_with_kerberos --release --quiet
cargo test test_runtime_reader_switching --release --quiet
cargo test test_configuration_precedence --release --quiet
echo

echo "========================================="
echo "Real HDFS Tests (Ignored - Run Manually)"
echo "========================================="
echo
echo "The following tests require a real HDFS cluster and are ignored by default:"
echo
echo "🔧 Environment Variables to Set (Optional):"
echo "   export HDFS_NAMENODE_URL=hdfs://your-namenode:9000"
echo "   export SPARK_EVENTS_DIR=/your/spark/events/directory"
echo "   export KERBEROS_PRINCIPAL=your-principal@YOUR.REALM"
echo "   export KERBEROS_KEYTAB=/path/to/your.keytab"
echo "   export KRB5_CONFIG=/path/to/krb5.conf"
echo "   export KERBEROS_REALM=YOUR.REALM"
echo
echo "🧪 Real HDFS Test Commands:"
echo "   # Basic HDFS connection test"
echo "   cargo test test_real_hdfs_connection_health --ignored --release"
echo
echo "   # HDFS directory operations test" 
echo "   cargo test test_real_hdfs_directory_operations --ignored --release"
echo
echo "   # Kerberos authentication test"
echo "   cargo test test_real_hdfs_kerberos_authentication --ignored --release"
echo
echo "   # Spark event logs processing test"
echo "   cargo test test_real_hdfs_spark_event_logs --ignored --release"
echo
echo "   # Full integration test"
echo "   cargo test test_real_hdfs_history_provider_integration --ignored --release"
echo
echo "   # Performance benchmarks"
echo "   cargo test test_real_hdfs_performance_benchmarks --ignored --release"
echo
echo "   # Manual validation guide"
echo "   cargo test test_manual_hdfs_validation --ignored --release"
echo

echo "========================================="
echo "CLI Testing Examples"
echo "========================================="
echo
echo "🚀 Test CLI with Local Filesystem:"
echo "   ./target/release/spark-history-server --log-directory ./test-data"
echo
echo "🚀 Test CLI with HDFS (no Kerberos):"
echo "   ./target/release/spark-history-server \\"
echo "     --hdfs \\"
echo "     --hdfs-namenode hdfs://localhost:9000 \\"
echo "     --log-directory /spark-events"
echo
echo "🚀 Test CLI with HDFS + Kerberos:"
echo "   ./target/release/spark-history-server \\"
echo "     --hdfs \\"
echo "     --hdfs-namenode hdfs://secure-namenode:9000 \\"
echo "     --kerberos-principal spark@EXAMPLE.COM \\"
echo "     --keytab-path /etc/security/keytabs/spark.keytab \\"
echo "     --log-directory /hdfs/spark-events"
echo
echo "========================================="
echo "HDFS Test Summary Complete"
echo "========================================="
echo
echo "✅ All mock HDFS tests should pass without requiring real infrastructure"
echo "🔧 Real HDFS tests require actual HDFS cluster - run manually with --ignored flag"
echo "📚 See README.md for comprehensive HDFS integration documentation"
echo