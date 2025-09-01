use std::process;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sysinfo::{Pid, System};
use tokio::time::interval;

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub timestamp: Instant,
    pub process_id: u32,
    pub cpu_usage: f32,
    pub memory_usage_bytes: u64,
    pub virtual_memory_bytes: u64,
    pub disk_read_bytes: u64,
    pub disk_written_bytes: u64,
    pub system_cpu_usage: f32,
    pub system_memory_total: u64,
    pub system_memory_used: u64,
}

#[derive(Debug, Clone)]
pub struct PerformanceSnapshot {
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub metrics: Vec<PerformanceMetrics>,
    pub max_cpu_usage: f32,
    pub max_memory_usage: u64,
    pub avg_cpu_usage: f32,
    pub avg_memory_usage: u64,
}

pub struct PerformanceMonitor {
    system: Arc<Mutex<System>>,
    metrics: Arc<Mutex<Vec<PerformanceMetrics>>>,
    start_time: Instant,
    is_monitoring: Arc<Mutex<bool>>,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();

        Self {
            system: Arc::new(Mutex::new(system)),
            metrics: Arc::new(Mutex::new(Vec::new())),
            start_time: Instant::now(),
            is_monitoring: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn start_monitoring(&self, sample_interval_ms: u64) {
        {
            let mut is_monitoring = self.is_monitoring.lock().unwrap();
            if *is_monitoring {
                return; // Already monitoring
            }
            *is_monitoring = true;
        }

        let system = Arc::clone(&self.system);
        let metrics = Arc::clone(&self.metrics);
        let is_monitoring = Arc::clone(&self.is_monitoring);
        let current_pid = process::id();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(sample_interval_ms));

            loop {
                interval.tick().await;

                {
                    let is_monitoring = is_monitoring.lock().unwrap();
                    if !*is_monitoring {
                        break;
                    }
                }

                // Collect metrics
                let metric = {
                    let mut sys = system.lock().unwrap();
                    sys.refresh_all();

                    let pid = Pid::from_u32(current_pid);
                    let process = sys.process(pid);
                    let cpu_usage = if let Some(proc) = process {
                        proc.cpu_usage()
                    } else {
                        0.0
                    };

                    let (memory_usage, virtual_memory) = if let Some(proc) = process {
                        (proc.memory(), proc.virtual_memory())
                    } else {
                        (0, 0)
                    };

                    let (disk_read, disk_written) = if let Some(proc) = process {
                        let disk_usage = proc.disk_usage();
                        (disk_usage.read_bytes, disk_usage.written_bytes)
                    } else {
                        (0, 0)
                    };

                    let system_cpu_usage = sys.global_cpu_info().cpu_usage();
                    let system_memory_total = sys.total_memory();
                    let system_memory_used = sys.used_memory();

                    PerformanceMetrics {
                        timestamp: Instant::now(),
                        process_id: current_pid,
                        cpu_usage,
                        memory_usage_bytes: memory_usage,
                        virtual_memory_bytes: virtual_memory,
                        disk_read_bytes: disk_read,
                        disk_written_bytes: disk_written,
                        system_cpu_usage,
                        system_memory_total,
                        system_memory_used,
                    }
                };

                {
                    let mut metrics_guard = metrics.lock().unwrap();
                    metrics_guard.push(metric);
                }
            }
        });
    }

    pub fn stop_monitoring(&self) -> PerformanceSnapshot {
        {
            let mut is_monitoring = self.is_monitoring.lock().unwrap();
            *is_monitoring = false;
        }

        let metrics_guard = self.metrics.lock().unwrap();
        let metrics = metrics_guard.clone();

        let max_cpu_usage = metrics.iter().map(|m| m.cpu_usage).fold(0.0f32, f32::max);

        let max_memory_usage = metrics
            .iter()
            .map(|m| m.memory_usage_bytes)
            .max()
            .unwrap_or(0);

        let avg_cpu_usage = if metrics.is_empty() {
            0.0
        } else {
            metrics.iter().map(|m| m.cpu_usage).sum::<f32>() / metrics.len() as f32
        };

        let avg_memory_usage = if metrics.is_empty() {
            0
        } else {
            metrics.iter().map(|m| m.memory_usage_bytes).sum::<u64>() / metrics.len() as u64
        };

        PerformanceSnapshot {
            start_time: self.start_time,
            end_time: Some(Instant::now()),
            metrics,
            max_cpu_usage,
            max_memory_usage,
            avg_cpu_usage,
            avg_memory_usage,
        }
    }

    pub fn get_current_metrics(&self) -> Vec<PerformanceMetrics> {
        let metrics_guard = self.metrics.lock().unwrap();
        metrics_guard.clone()
    }
}

impl PerformanceSnapshot {
    pub fn duration(&self) -> Duration {
        match self.end_time {
            Some(end) => end.duration_since(self.start_time),
            None => Instant::now().duration_since(self.start_time),
        }
    }

    pub fn print_summary(&self) {
        println!("\n=== Performance Summary ===");
        println!("Duration: {:.2}s", self.duration().as_secs_f64());
        println!("Max CPU Usage: {:.1}%", self.max_cpu_usage);
        println!("Avg CPU Usage: {:.1}%", self.avg_cpu_usage);
        println!(
            "Max Memory Usage: {:.2} MB",
            self.max_memory_usage as f64 / 1024.0 / 1024.0
        );
        println!(
            "Avg Memory Usage: {:.2} MB",
            self.avg_memory_usage as f64 / 1024.0 / 1024.0
        );

        if let (Some(first), Some(last)) = (self.metrics.first(), self.metrics.last()) {
            let disk_read_diff = last.disk_read_bytes - first.disk_read_bytes;
            let disk_written_diff = last.disk_written_bytes - first.disk_written_bytes;
            println!(
                "Disk Read: {:.2} MB",
                disk_read_diff as f64 / 1024.0 / 1024.0
            );
            println!(
                "Disk Written: {:.2} MB",
                disk_written_diff as f64 / 1024.0 / 1024.0
            );
        }

        println!("Samples Collected: {}", self.metrics.len());
        println!("===========================\n");
    }

    pub fn to_csv_string(&self) -> String {
        let mut csv = String::from("timestamp,cpu_usage,memory_usage_mb,virtual_memory_mb,disk_read_mb,disk_written_mb,system_cpu,system_memory_usage_percent\n");

        for metric in &self.metrics {
            let timestamp_ms = metric.timestamp.duration_since(self.start_time).as_millis();
            let memory_mb = metric.memory_usage_bytes as f64 / 1024.0 / 1024.0;
            let virtual_mb = metric.virtual_memory_bytes as f64 / 1024.0 / 1024.0;
            let disk_read_mb = metric.disk_read_bytes as f64 / 1024.0 / 1024.0;
            let disk_written_mb = metric.disk_written_bytes as f64 / 1024.0 / 1024.0;
            let system_memory_percent =
                (metric.system_memory_used as f64 / metric.system_memory_total as f64) * 100.0;

            csv.push_str(&format!(
                "{},{:.1},{:.2},{:.2},{:.2},{:.2},{:.1},{:.1}\n",
                timestamp_ms,
                metric.cpu_usage,
                memory_mb,
                virtual_mb,
                disk_read_mb,
                disk_written_mb,
                metric.system_cpu_usage,
                system_memory_percent
            ));
        }

        csv
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_performance_monitor() {
        let monitor = PerformanceMonitor::new();

        // Start monitoring
        monitor.start_monitoring(100).await;

        // Let it collect some data
        sleep(Duration::from_millis(300)).await;

        // Stop and get results
        let snapshot = monitor.stop_monitoring();

        assert!(snapshot.metrics.len() > 0);
        assert!(snapshot.duration().as_millis() > 200);

        snapshot.print_summary();
    }
}
