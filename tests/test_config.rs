use spark_history_server::config::HistoryConfig;

pub fn create_test_config() -> (HistoryConfig, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = HistoryConfig {
        log_directory: "./test-data/spark-events".to_string(),
        max_applications: 100,
        update_interval_seconds: 60,
        max_apps_per_request: 50,
        compression_enabled: true,
        database_directory: Some(temp_dir.path().to_str().unwrap().to_string()),
        hdfs: None,
    };
    (config, temp_dir)
}
