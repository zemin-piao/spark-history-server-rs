use rand::Rng;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SyntheticDataGenerator {
    rng: rand::rngs::ThreadRng,
    base_timestamp: u64,
    app_ids: Vec<String>,
    event_types: Vec<&'static str>,
    current_event_id: i64,
}

impl Default for SyntheticDataGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl SyntheticDataGenerator {
    pub fn new() -> Self {
        let rng = rand::thread_rng();
        let base_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Generate realistic app IDs
        let app_ids = (0..100).map(|i| format!("app-{:04}-spark", i)).collect();

        let event_types = vec![
            "SparkListenerApplicationStart",
            "SparkListenerApplicationEnd",
            "SparkListenerJobStart",
            "SparkListenerJobEnd",
            "SparkListenerStageSubmitted",
            "SparkListenerStageCompleted",
            "SparkListenerTaskStart",
            "SparkListenerTaskEnd",
            "SparkListenerExecutorAdded",
            "SparkListenerExecutorRemoved",
        ];

        Self {
            rng,
            base_timestamp,
            app_ids,
            event_types,
            current_event_id: 0,
        }
    }

    pub fn generate_event_batch(&mut self, batch_size: usize) -> Vec<(i64, String, Value)> {
        (0..batch_size)
            .map(|_| {
                self.current_event_id += 1;
                let app_id = self.app_ids[self.rng.gen_range(0..self.app_ids.len())].clone();
                let event = self.generate_single_event(&app_id);
                (self.current_event_id, app_id, event)
            })
            .collect()
    }

    fn generate_single_event(&mut self, app_id: &str) -> Value {
        let event_type = self.event_types[self.rng.gen_range(0..self.event_types.len())];
        let timestamp = self.base_timestamp + self.rng.gen_range(0..86400000); // Within 24 hours

        match event_type {
            "SparkListenerApplicationStart" => self.generate_application_start(timestamp, app_id),
            "SparkListenerApplicationEnd" => self.generate_application_end(timestamp, app_id),
            "SparkListenerJobStart" => self.generate_job_start(timestamp, app_id),
            "SparkListenerJobEnd" => self.generate_job_end(timestamp, app_id),
            "SparkListenerStageSubmitted" => self.generate_stage_submitted(timestamp, app_id),
            "SparkListenerStageCompleted" => self.generate_stage_completed(timestamp, app_id),
            "SparkListenerTaskStart" => self.generate_task_start(timestamp, app_id),
            "SparkListenerTaskEnd" => self.generate_task_end(timestamp, app_id),
            "SparkListenerExecutorAdded" => self.generate_executor_added(timestamp, app_id),
            "SparkListenerExecutorRemoved" => self.generate_executor_removed(timestamp, app_id),
            _ => json!({
                "Event": event_type,
                "Timestamp": timestamp,
            }),
        }
    }

    fn generate_application_start(&mut self, timestamp: u64, app_id: &str) -> Value {
        json!({
            "Event": "SparkListenerApplicationStart",
            "Timestamp": timestamp,
            "App Name": format!("Test Application {}", app_id),
            "App ID": app_id,
            "Driver": {
                "Host": "localhost",
                "Port": self.rng.gen_range(4040..9999)
            },
            "Executor Infos": {},
            "User": "spark"
        })
    }

    fn generate_application_end(&mut self, timestamp: u64, app_id: &str) -> Value {
        json!({
            "Event": "SparkListenerApplicationEnd",
            "Timestamp": timestamp,
            "App ID": app_id,
        })
    }

    fn generate_job_start(&mut self, timestamp: u64, _app_id: &str) -> Value {
        let job_id = self.rng.gen_range(0..1000);
        json!({
            "Event": "SparkListenerJobStart",
            "Timestamp": timestamp,
            "Job ID": job_id,
            "Stage Infos": [],
            "Properties": {}
        })
    }

    fn generate_job_end(&mut self, timestamp: u64, _app_id: &str) -> Value {
        let job_id = self.rng.gen_range(0..1000);
        json!({
            "Event": "SparkListenerJobEnd",
            "Timestamp": timestamp,
            "Job ID": job_id,
            "Job Result": {
                "Result": "JobSucceeded"
            }
        })
    }

    fn generate_stage_submitted(&mut self, timestamp: u64, _app_id: &str) -> Value {
        let stage_id = self.rng.gen_range(0..100);
        let _job_id = self.rng.gen_range(0..1000);
        json!({
            "Event": "SparkListenerStageSubmitted",
            "Timestamp": timestamp,
            "Stage Info": {
                "Stage ID": stage_id,
                "Stage Attempt ID": 0,
                "Stage Name": format!("Stage {}", stage_id),
                "Number of Tasks": self.rng.gen_range(1..1000),
                "Parent IDs": [],
                "Details": "",
                "Accumulables": []
            },
            "Properties": {}
        })
    }

    fn generate_stage_completed(&mut self, timestamp: u64, _app_id: &str) -> Value {
        let stage_id = self.rng.gen_range(0..100);
        json!({
            "Event": "SparkListenerStageCompleted",
            "Timestamp": timestamp,
            "Stage Info": {
                "Stage ID": stage_id,
                "Stage Attempt ID": 0,
                "Stage Name": format!("Stage {}", stage_id),
                "Number of Tasks": self.rng.gen_range(1..1000),
                "Status": "COMPLETE",
                "Parent IDs": [],
                "Details": "",
                "Accumulables": []
            }
        })
    }

    fn generate_task_start(&mut self, timestamp: u64, _app_id: &str) -> Value {
        let task_id = self.rng.gen_range(0..10000);
        let stage_id = self.rng.gen_range(0..100);
        json!({
            "Event": "SparkListenerTaskStart",
            "Timestamp": timestamp,
            "Stage ID": stage_id,
            "Stage Attempt ID": 0,
            "Task Info": {
                "Task ID": task_id,
                "Index": task_id % 100,
                "Attempt": 0,
                "Launch Time": timestamp,
                "Executor ID": format!("executor-{}", self.rng.gen_range(1..10)),
                "Host": format!("worker-{}.example.com", self.rng.gen_range(1..20)),
                "Locality": vec!["PROCESS_LOCAL", "NODE_LOCAL", "RACK_LOCAL", "ANY"][self.rng.gen_range(0..4)],
                "Speculative": false,
                "Getting Result Time": 0,
                "Finish Time": 0,
                "Failed": false,
                "Killed": false,
                "Accumulables": []
            }
        })
    }

    fn generate_task_end(&mut self, timestamp: u64, _app_id: &str) -> Value {
        let task_id = self.rng.gen_range(0..10000);
        let stage_id = self.rng.gen_range(0..100);
        let duration_ms = self.rng.gen_range(100..30000);
        let finish_time = timestamp + duration_ms;

        json!({
            "Event": "SparkListenerTaskEnd",
            "Timestamp": finish_time,
            "Stage ID": stage_id,
            "Stage Attempt ID": 0,
            "Task Type": "ResultTask",
            "Task End Reason": {
                "Reason": "Success"
            },
            "Task Info": {
                "Task ID": task_id,
                "Index": task_id % 100,
                "Attempt": 0,
                "Launch Time": timestamp,
                "Executor ID": format!("executor-{}", self.rng.gen_range(1..10)),
                "Host": format!("worker-{}.example.com", self.rng.gen_range(1..20)),
                "Locality": vec!["PROCESS_LOCAL", "NODE_LOCAL", "RACK_LOCAL", "ANY"][self.rng.gen_range(0..4)],
                "Speculative": false,
                "Getting Result Time": 0,
                "Finish Time": finish_time,
                "Failed": false,
                "Killed": false,
                "Accumulables": []
            },
            "Task Metrics": {
                "Executor Deserialize Time": self.rng.gen_range(0..100),
                "Executor Deserialize CPU Time": self.rng.gen_range(0..100000000),
                "Executor Run Time": duration_ms,
                "Executor CPU Time": self.rng.gen_range(100000000..1000000000),
                "Peak Execution Memory": self.rng.gen_range(1048576..1073741824), // 1MB to 1GB
                "Result Size": self.rng.gen_range(1000..100000),
                "JVM GC Time": self.rng.gen_range(0..1000),
                "Result Serialization Time": self.rng.gen_range(0..100),
                "Memory Bytes Spilled": self.rng.gen_range(0..1048576),
                "Disk Bytes Spilled": self.rng.gen_range(0..1073741824),
                "Shuffle Read Metrics": {
                    "Remote Blocks Fetched": self.rng.gen_range(0..100),
                    "Local Blocks Fetched": self.rng.gen_range(0..50),
                    "Remote Bytes Read": self.rng.gen_range(0..10485760), // Up to 10MB
                    "Remote Bytes Read To Disk": 0,
                    "Local Bytes Read": self.rng.gen_range(0..5242880), // Up to 5MB
                    "Total Bytes Read": self.rng.gen_range(0..15728640), // Up to 15MB
                    "Fetch Wait Time": self.rng.gen_range(0..1000)
                },
                "Shuffle Write Metrics": {
                    "Bytes Written": self.rng.gen_range(0..10485760), // Up to 10MB
                    "Records Written": self.rng.gen_range(0..10000),
                    "Write Time": self.rng.gen_range(0..5000)
                },
                "Input Metrics": {
                    "Bytes Read": self.rng.gen_range(0..52428800), // Up to 50MB
                    "Records Read": self.rng.gen_range(0..100000)
                },
                "Output Metrics": {
                    "Bytes Written": self.rng.gen_range(0..52428800), // Up to 50MB
                    "Records Written": self.rng.gen_range(0..100000)
                }
            }
        })
    }

    fn generate_executor_added(&mut self, timestamp: u64, _app_id: &str) -> Value {
        let executor_id = format!("executor-{}", self.rng.gen_range(1..10));
        json!({
            "Event": "SparkListenerExecutorAdded",
            "Timestamp": timestamp,
            "Executor ID": executor_id,
            "Executor Info": {
                "Host": format!("worker-{}.example.com", self.rng.gen_range(1..20)),
                "Total Cores": self.rng.gen_range(1..8),
                "Max Memory": self.rng.gen_range(1073741824..8589934592_u64), // 1GB to 8GB
                "Attributes": {},
                "Resources": {},
                "Resource Profile Id": 0
            }
        })
    }

    fn generate_executor_removed(&mut self, timestamp: u64, _app_id: &str) -> Value {
        let executor_id = format!("executor-{}", self.rng.gen_range(1..10));
        json!({
            "Event": "SparkListenerExecutorRemoved",
            "Timestamp": timestamp,
            "Executor ID": executor_id,
            "Removed Reason": "Normal executor shutdown"
        })
    }

    pub fn get_current_event_id(&self) -> i64 {
        self.current_event_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synthetic_data_generator() {
        let mut generator = SyntheticDataGenerator::new();
        let batch = generator.generate_event_batch(10);

        assert_eq!(batch.len(), 10);
        assert_eq!(generator.get_current_event_id(), 10);

        // Verify each event has the expected structure
        for (id, app_id, event) in batch {
            assert!(id > 0);
            assert!(!app_id.is_empty());
            assert!(event.get("Event").is_some());
            assert!(event.get("Timestamp").is_some());
        }
    }
}
