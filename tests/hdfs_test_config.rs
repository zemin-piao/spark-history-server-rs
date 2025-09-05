use spark_history_server::config::HistoryConfig;

pub fn mock_hdfs_config() -> HistoryConfig {
    HistoryConfig {
        log_directory: "/hdfs/spark-events".to_string(),
        max_applications: 100,
        update_interval_seconds: 60,
        max_apps_per_request: 50,
        compression_enabled: true,
        database_directory: None,
        hdfs: None,
        s3: None,
    }
}

pub fn test_hdfs_config() -> HistoryConfig {
    HistoryConfig {
        log_directory: "./examples".to_string(),
        max_applications: 100,
        update_interval_seconds: 60,
        max_apps_per_request: 50,
        compression_enabled: true,
        database_directory: None,
        hdfs: None,
        s3: None,
    }
}

pub fn large_scale_hdfs_config() -> HistoryConfig {
    HistoryConfig {
        log_directory: "/hdfs/large-spark-events".to_string(),
        max_applications: 10000,
        update_interval_seconds: 300,
        max_apps_per_request: 1000,
        compression_enabled: true,
        database_directory: None,
        hdfs: None,
        s3: None,
    }
}

pub const SAMPLE_SPARK_EVENT_LOG: &str = r#"{"Event":"SparkListenerLogStart","Spark Version":"3.2.0"}
{"Event":"SparkListenerApplicationStart","App Name":"SparkPi","App ID":"app-20231120120000-0001","Timestamp":1700481600000,"User":"test-user"}
{"Event":"SparkListenerExecutorAdded","Timestamp":1700481600100,"Executor ID":"driver","Executor Info":{"Host":"localhost","Total Cores":4}}
{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":1700481601000,"Stage Infos":[]}
{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":0,"Stage Attempt ID":0,"Stage Name":"collect at SparkPi.scala:46","Number of Tasks":2}}
{"Event":"SparkListenerTaskStart","Stage ID":0,"Stage Attempt ID":0,"Task Info":{"Task ID":0,"Index":0,"Attempt":0,"Launch Time":1700481601100}}
{"Event":"SparkListenerTaskEnd","Stage ID":0,"Stage Attempt ID":0,"Task Type":"ResultTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":0,"Index":0,"Attempt":0,"Launch Time":1700481601100,"Finish Time":1700481601200}}
{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":0,"Stage Attempt ID":0,"Stage Name":"collect at SparkPi.scala:46","Number of Tasks":2,"Status":"COMPLETE"}}
{"Event":"SparkListenerJobEnd","Job ID":0,"Completion Time":1700481602000,"Job Result":{"Result":"JobSucceeded"}}
{"Event":"SparkListenerApplicationEnd","App ID":"app-20231120120000-0001","Timestamp":1700481603000}
"#;

pub const SAMPLE_COMPRESSED_EVENT_LOG: &str = r#"{"Event":"SparkListenerLogStart","Spark Version":"3.2.0"}
{"Event":"SparkListenerApplicationStart","App Name":"CompressedSparkPi","App ID":"app-compressed-20231120120000-0001","Timestamp":1700481600000,"User":"test-user"}
{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":1700481601000,"Stage Infos":[]}
{"Event":"SparkListenerJobEnd","Job ID":0,"Completion Time":1700481602000,"Job Result":{"Result":"JobSucceeded"}}
{"Event":"SparkListenerApplicationEnd","App ID":"app-compressed-20231120120000-0001","Timestamp":1700481603000}
"#;
