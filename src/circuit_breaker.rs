use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,   // Normal operation
    Open,     // Failing fast
    HalfOpen, // Testing if service recovered
}

/// Configuration for circuit breaker
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Failure threshold to open circuit
    pub failure_threshold: u64,
    /// Success threshold to close circuit from half-open
    pub success_threshold: u64,
    /// Time to wait before moving to half-open
    pub timeout_duration: Duration,
    /// Window size for failure counting
    pub window_duration: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            timeout_duration: Duration::from_secs(60),
            window_duration: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Circuit breaker implementation for external dependencies
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<RwLock<CircuitState>>,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    last_failure_time: AtomicI64,
    last_window_reset: AtomicI64,
    name: String,
}

impl CircuitBreaker {
    pub fn new(name: String, config: CircuitBreakerConfig) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Self {
            config,
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            last_failure_time: AtomicI64::new(now),
            last_window_reset: AtomicI64::new(now),
            name,
        }
    }

    #[allow(dead_code)]
    pub fn with_defaults(name: String) -> Self {
        Self::new(name, CircuitBreakerConfig::default())
    }

    /// Execute a function with circuit breaker protection
    pub async fn call<F, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        // Check if circuit is open
        if self.is_open().await {
            debug!("Circuit breaker {} is open, failing fast", self.name);
            return Err(CircuitBreakerError::CircuitOpen);
        }

        // Execute the function
        let start = Instant::now();
        let result = f.await;
        let duration = start.elapsed();

        match result {
            Ok(value) => {
                self.record_success().await;
                debug!("Circuit breaker {} success in {:?}", self.name, duration);
                Ok(value)
            }
            Err(e) => {
                self.record_failure().await;
                warn!("Circuit breaker {} failure in {:?}", self.name, duration);
                Err(CircuitBreakerError::CallFailed(e))
            }
        }
    }

    async fn is_open(&self) -> bool {
        let state = *self.state.read().await;
        match state {
            CircuitState::Open => {
                // Check if timeout has passed
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let last_failure = self.last_failure_time.load(Ordering::Relaxed);

                if now - last_failure > self.config.timeout_duration.as_millis() as i64 {
                    // Move to half-open
                    let mut state_guard = self.state.write().await;
                    *state_guard = CircuitState::HalfOpen;
                    info!("Circuit breaker {} moved to half-open", self.name);
                    false
                } else {
                    true
                }
            }
            CircuitState::HalfOpen => false,
            CircuitState::Closed => false,
        }
    }

    async fn record_success(&self) {
        let state = *self.state.read().await;

        match state {
            CircuitState::HalfOpen => {
                let success_count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                if success_count >= self.config.success_threshold {
                    let mut state_guard = self.state.write().await;
                    *state_guard = CircuitState::Closed;
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);
                    info!("Circuit breaker {} closed after recovery", self.name);
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::Open => {
                // Should not happen, but handle it
                debug!("Success recorded for open circuit breaker {}", self.name);
            }
        }
    }

    async fn record_failure(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Check if we need to reset the window
        let last_window = self.last_window_reset.load(Ordering::Relaxed);
        if now - last_window > self.config.window_duration.as_millis() as i64 {
            self.failure_count.store(0, Ordering::Relaxed);
            self.last_window_reset.store(now, Ordering::Relaxed);
            debug!("Circuit breaker {} window reset", self.name);
        }

        let failure_count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.last_failure_time.store(now, Ordering::Relaxed);

        let state = *self.state.read().await;
        if state == CircuitState::Closed && failure_count >= self.config.failure_threshold {
            let mut state_guard = self.state.write().await;
            *state_guard = CircuitState::Open;
            warn!(
                "Circuit breaker {} opened after {} failures",
                self.name, failure_count
            );
        }
    }

    /// Get current state for monitoring
    #[allow(dead_code)]
    pub async fn get_state(&self) -> CircuitState {
        *self.state.read().await
    }

    /// Get failure count for monitoring
    #[allow(dead_code)]
    pub fn get_failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::Relaxed)
    }

    /// Get success count for monitoring
    #[allow(dead_code)]
    pub fn get_success_count(&self) -> u64 {
        self.success_count.load(Ordering::Relaxed)
    }

    /// Force circuit to open (for testing)
    #[cfg(test)]
    pub async fn force_open(&self) {
        let mut state = self.state.write().await;
        *state = CircuitState::Open;
    }

    /// Force circuit to close (for testing)
    #[cfg(test)]
    #[allow(dead_code)]
    pub async fn force_close(&self) {
        let mut state = self.state.write().await;
        *state = CircuitState::Closed;
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);
    }
}

/// Circuit breaker errors
#[derive(Debug, thiserror::Error)]
pub enum CircuitBreakerError<E> {
    #[error("Circuit breaker is open")]
    CircuitOpen,
    #[error("Call failed: {0}")]
    CallFailed(E),
}

impl<E> CircuitBreakerError<E> {
    pub fn is_circuit_open(&self) -> bool {
        matches!(self, CircuitBreakerError::CircuitOpen)
    }

    pub fn into_inner(self) -> Option<E> {
        match self {
            CircuitBreakerError::CallFailed(e) => Some(e),
            CircuitBreakerError::CircuitOpen => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_circuit_breaker_normal_operation() {
        let circuit_breaker = CircuitBreaker::with_defaults("test".to_string());

        // Normal operation should work
        let result = circuit_breaker.call(async { Ok::<i32, String>(42) }).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_on_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_duration: Duration::from_millis(100),
            window_duration: Duration::from_secs(10),
        };
        let circuit_breaker = CircuitBreaker::new("test".to_string(), config);

        // Cause failures to open circuit
        for _ in 0..3 {
            let result = circuit_breaker
                .call(async { Err::<i32, String>("error".to_string()) })
                .await;
            assert!(result.is_err());
        }

        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);

        // Next call should fail fast
        let result = circuit_breaker.call(async { Ok::<i32, String>(42) }).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_circuit_open());
    }

    #[tokio::test]
    async fn test_circuit_breaker_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout_duration: Duration::from_millis(50),
            window_duration: Duration::from_secs(10),
        };
        let circuit_breaker = CircuitBreaker::new("test".to_string(), config);

        // Open circuit
        circuit_breaker.force_open().await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);

        // Wait for timeout
        sleep(Duration::from_millis(60)).await;

        // Should allow calls (half-open)
        let result = circuit_breaker.call(async { Ok::<i32, String>(42) }).await;
        assert!(result.is_ok());

        // After enough successes, should close
        let result = circuit_breaker.call(async { Ok::<i32, String>(42) }).await;
        assert!(result.is_ok());

        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
    }
}
