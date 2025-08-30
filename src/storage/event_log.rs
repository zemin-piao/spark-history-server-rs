use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use std::path::Path;

use crate::models::{ApplicationAttemptInfo, ApplicationInfo};

/// Parser for Spark event logs in JSON format
#[derive(Clone)]
pub struct EventLogParser;

impl EventLogParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_application_from_events(
        &self,
        events: Vec<Value>,
        file_path: &Path,
    ) -> Result<ApplicationInfo> {
        if events.is_empty() {
            return Err(anyhow!("No events found in log file: {:?}", file_path));
        }

        let mut app_info = None;
        let mut start_time = None;
        let mut end_time = None;
        let mut last_updated = Utc::now();
        let mut completed = false;

        for event in events {
            match event.get("Event").and_then(|e| e.as_str()) {
                Some("SparkListenerApplicationStart") => {
                    app_info = self.parse_application_start(&event)?;
                    start_time = self.extract_timestamp(&event, "Timestamp");
                }
                Some("SparkListenerApplicationEnd") => {
                    end_time = self.extract_timestamp(&event, "Timestamp");
                    completed = true;
                }
                Some("SparkListenerEnvironmentUpdate") => {
                    if let Some(_app) = &mut app_info {
                        if let Ok(_version) = self.extract_spark_version(&event) {
                            // We'll set this in the attempt info
                        }
                    }
                }
                _ => {
                    // Update last_updated with the latest event timestamp
                    if let Some(ts) = self.extract_timestamp(&event, "Timestamp") {
                        last_updated = ts;
                    }
                }
            }
        }

        let app_info = app_info.ok_or_else(|| anyhow!("No SparkListenerApplicationStart event found"))?;
        
        let start_time = start_time.unwrap_or_else(Utc::now);
        let end_time = end_time.unwrap_or_else(|| {
            if completed { last_updated } else { Utc::now() }
        });

        let attempt = ApplicationAttemptInfo::new(
            app_info.3.clone(), // attempt_id
            start_time,
            end_time,
            last_updated,
            app_info.2.clone(), // spark_user
            completed,
            app_info.4.clone(), // spark_version
        );

        Ok(ApplicationInfo {
            id: app_info.0,
            name: app_info.1,
            cores_granted: None,
            max_cores: None,
            cores_per_executor: None,
            memory_per_executor_mb: None,
            attempts: vec![attempt],
        })
    }

    fn parse_application_start(&self, event: &Value) -> Result<Option<(String, String, String, Option<String>, String)>> {
        let app_id = event
            .get("App ID")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing App ID in SparkListenerApplicationStart"))?;

        let app_name = event
            .get("App Name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing App Name in SparkListenerApplicationStart"))?;

        let user = event
            .get("User")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let attempt_id = event
            .get("App Attempt ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Try to extract Spark version, default to unknown
        let spark_version = event
            .get("Spark Version")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        Ok(Some((
            app_id.to_string(),
            app_name.to_string(),
            user,
            attempt_id,
            spark_version,
        )))
    }

    fn extract_timestamp(&self, event: &Value, field: &str) -> Option<DateTime<Utc>> {
        event.get(field)
            .and_then(|v| v.as_i64())
            .and_then(|ts| Utc.timestamp_millis_opt(ts).single())
    }

    fn extract_spark_version(&self, event: &Value) -> Result<String> {
        event
            .get("Spark Properties")
            .and_then(|props| props.as_array())
            .and_then(|arr| {
                arr.iter().find(|item| {
                    item.as_array()
                        .and_then(|pair| pair.get(0))
                        .and_then(|key| key.as_str())
                        .map(|k| k == "spark.app.version" || k == "spark.version")
                        .unwrap_or(false)
                })
            })
            .and_then(|item| item.as_array())
            .and_then(|pair| pair.get(1))
            .and_then(|val| val.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Spark version not found in environment update"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_application_start() {
        let parser = EventLogParser::new();
        let event = json!({
            "Event": "SparkListenerApplicationStart",
            "App ID": "app-20231120120000-0001",
            "App Name": "Test Application",
            "User": "testuser",
            "App Attempt ID": "1",
            "Timestamp": 1700486400000i64,
            "Spark Version": "3.5.0"
        });

        let result = parser.parse_application_start(&event).unwrap();
        assert!(result.is_some());

        let (app_id, app_name, user, attempt_id, spark_version) = result.unwrap();
        assert_eq!(app_id, "app-20231120120000-0001");
        assert_eq!(app_name, "Test Application");
        assert_eq!(user, "testuser");
        assert_eq!(attempt_id, Some("1".to_string()));
        assert_eq!(spark_version, "3.5.0");
    }

    #[test]
    fn test_extract_timestamp() {
        let parser = EventLogParser::new();
        let event = json!({
            "Timestamp": 1700486400000i64
        });

        let timestamp = parser.extract_timestamp(&event, "Timestamp").unwrap();
        assert_eq!(timestamp.timestamp_millis(), 1700486400000);
    }
}