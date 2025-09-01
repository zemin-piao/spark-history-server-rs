#!/bin/bash

set -e

echo "🚀 Spark History Server - Load Testing Suite"
echo "============================================="
echo ""

# Check if cargo is available
if ! command -v cargo &> /dev/null; then
    echo "❌ Error: cargo not found. Please install Rust."
    exit 1
fi

# Ensure we're in the project directory
if [ ! -f "Cargo.toml" ]; then
    echo "❌ Error: Please run this script from the project root directory."
    exit 1
fi

# Create results directory
mkdir -p load_test_results
cd load_test_results

echo "📋 Available Load Tests:"
echo "1. Full Pipeline Test (10M events) - Tests complete file processing pipeline"
echo "2. Write Performance Test - Tests DuckDB write performance with various batch sizes"
echo "3. API Load Test - Tests API endpoint performance under load"  
echo "4. Comprehensive Benchmark Suite - Runs all tests with multiple data sizes"
echo ""

# Function to run a specific test and capture output
run_test() {
    local test_name=$1
    local test_function=$2
    local output_file=$3
    
    echo "🔄 Running: $test_name"
    echo "   Output will be saved to: $output_file"
    echo ""
    
    # Run the test and capture both stdout and stderr
    if cargo test $test_function --release -- --nocapture > "$output_file" 2>&1; then
        echo "✅ $test_name completed successfully"
        
        # Extract key performance metrics if available
        if grep -q "events/sec" "$output_file"; then
            echo "📊 Key metrics:"
            grep -E "(events/sec|Duration:|Peak memory|Max Memory|Throughput)" "$output_file" | head -5 | sed 's/^/   /'
        fi
    else
        echo "❌ $test_name failed. Check $output_file for details."
    fi
    echo ""
}

# Function to run all tests
run_all_tests() {
    echo "🏃 Running all load tests..."
    echo "This may take 30-60 minutes depending on your system."
    echo ""
    
    # Start timestamp
    start_time=$(date)
    echo "Started at: $start_time" | tee test_summary.txt
    echo "" | tee -a test_summary.txt
    
    # 1. Comprehensive benchmark (covers multiple scenarios)
    run_test "Comprehensive Benchmark Suite" "run_comprehensive_benchmark_suite" "comprehensive_benchmark.log"
    
    # 2. Full pipeline test with 10M events  
    run_test "Full Pipeline Test (10M events)" "test_full_pipeline_10m_events" "full_pipeline_10m.log"
    
    # 3. Write performance with different batch sizes
    run_test "Write Performance (Batch Size Testing)" "test_write_performance_batch_sizes" "write_performance_batches.log"
    
    # 4. API load testing
    run_test "API Load Test" "test_api_load_performance" "api_load_test.log"
    
    # 5. Concurrent user simulation
    run_test "Concurrent User Simulation" "test_concurrent_user_simulation" "concurrent_users.log"
    
    # 6. API stress test with scaling
    run_test "API Stress Test (Scaling)" "test_stress_test_scaling" "api_stress_test.log"
    
    # 7. Compression performance test
    run_test "Compression Performance Test" "test_compression_performance" "compression_test.log"
    
    # End timestamp and summary
    end_time=$(date)
    echo "Completed at: $end_time" | tee -a test_summary.txt
    echo "" | tee -a test_summary.txt
    
    echo "📈 Generating summary report..."
    
    # Generate summary from JSON reports if they exist
    if [ -f "comprehensive_benchmark_report.json" ]; then
        echo "Comprehensive benchmark report available: comprehensive_benchmark_report.json" | tee -a test_summary.txt
    fi
    
    if [ -f "../write_performance_metrics.csv" ]; then
        echo "Write performance CSV: ../write_performance_metrics.csv" | tee -a test_summary.txt
    fi
    
    if [ -f "../full_pipeline_performance_metrics.csv" ]; then
        echo "Full pipeline CSV: ../full_pipeline_performance_metrics.csv" | tee -a test_summary.txt
    fi
    
    # Count total events processed across all tests
    echo "" | tee -a test_summary.txt
    echo "📊 Performance Summary:" | tee -a test_summary.txt
    echo "Check individual log files for detailed metrics:" | tee -a test_summary.txt
    ls -la *.log | tee -a test_summary.txt
    
    echo ""
    echo "🎉 All load tests completed!"
    echo "📋 Results are in the 'load_test_results' directory"
    echo "📊 Check 'test_summary.txt' for an overview"
}

# Function to run individual tests
run_individual_test() {
    echo "Select which test to run:"
    echo "1) Comprehensive Benchmark Suite (recommended)"
    echo "2) Full Pipeline Test (10M events)"  
    echo "3) Write Performance Test"
    echo "4) API Load Test"
    echo "5) Concurrent User Simulation"
    echo "6) API Stress Test"
    echo "7) Compression Performance Test"
    echo ""
    read -p "Enter choice (1-7): " choice
    
    case $choice in
        1) run_test "Comprehensive Benchmark Suite" "run_comprehensive_benchmark_suite" "comprehensive_benchmark.log" ;;
        2) run_test "Full Pipeline Test (10M events)" "test_full_pipeline_10m_events" "full_pipeline_10m.log" ;;
        3) run_test "Write Performance Test" "test_write_performance_batch_sizes" "write_performance.log" ;;
        4) run_test "API Load Test" "test_api_load_performance" "api_load_test.log" ;;
        5) run_test "Concurrent User Simulation" "test_concurrent_user_simulation" "concurrent_users.log" ;;
        6) run_test "API Stress Test" "test_stress_test_scaling" "api_stress_test.log" ;;
        7) run_test "Compression Performance Test" "test_compression_performance" "compression_test.log" ;;
        *) echo "Invalid choice" ;;
    esac
}

# Check command line arguments
if [ "$1" == "--all" ]; then
    run_all_tests
elif [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    echo "Usage: $0 [--all|--help]"
    echo ""
    echo "Options:"
    echo "  --all    Run all load tests (may take 30-60 minutes)"
    echo "  --help   Show this help message"
    echo ""
    echo "If no arguments provided, you'll be prompted to select individual tests."
else
    echo "Would you like to:"
    echo "1) Run ALL load tests (recommended for comprehensive analysis)"
    echo "2) Run individual tests"
    echo ""
    read -p "Enter choice (1-2): " main_choice
    
    case $main_choice in
        1) run_all_tests ;;
        2) run_individual_test ;;
        *) echo "Invalid choice" ;;
    esac
fi

echo ""
echo "📁 All results are saved in: $(pwd)"
echo "🔍 Key files to check:"
echo "   - test_summary.txt (overview)"
echo "   - comprehensive_benchmark_report.json (detailed JSON report)"  
echo "   - *.csv (performance metrics)"
echo "   - *.log (detailed test outputs)"