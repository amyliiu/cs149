#include "tasksys.h"
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // Use atomic counter for dynamic task assignment
    std::atomic<int> next_task(0);
    
    // Worker thread function
    auto worker = [&]() {
        while (true) {
            // Atomically get next task index
            int task_id = next_task.fetch_add(1);
            
            // Check if we're done
            if (task_id >= num_total_tasks) {
                break;
            }
            
            // Execute the task
            runnable->runTask(task_id, num_total_tasks);
        }
    };
    
    // Spawn worker threads
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads_; i++) {
        threads.push_back(std::thread(worker));
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    shutdown_ = false;
    next_task_ = 0;
    num_total_tasks_ = 0;
    tasks_completed_ = 0;
    runnable_ = nullptr;

    threads_.resize(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
        threads_[i] = std::thread([this]() {
            while (!shutdown_) {
                // Try to grab a task first (before checking runnable)
                int task_id = next_task_.load();
                
                // Check if there might be work
                if (task_id < num_total_tasks_.load() && runnable_ != nullptr) {
                    // Atomically grab the task
                    task_id = next_task_.fetch_add(1);
                    
                    // Double-check task is valid and we have a runnable
                    if (task_id < num_total_tasks_.load()) {
                        IRunnable* local_runnable = runnable_;  // Cache locally
                        if (local_runnable != nullptr) {
                            local_runnable->runTask(task_id, num_total_tasks_.load());
                            tasks_completed_.fetch_add(1);
                        }
                    }
                }
                // Keep spinning (checking for work)
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    shutdown_ = true;
    for (auto& thread : threads_) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    next_task_ = 0;
    tasks_completed_ = 0;
    num_total_tasks_ = num_total_tasks;
    runnable_ = runnable;
    
    // Spin wait until all tasks are completed
    while (tasks_completed_.load() < num_total_tasks) {
        // Busy wait (spinning)
    }
    
    // Clear runnable pointer
    runnable_ = nullptr;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    shutdown_ = false;
    next_task_ = 0;
    num_total_tasks_ = 0;
    runnable_ = nullptr;
    tasks_completed_ = 0;
    work_available_ = false;

    threads_.resize(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
        threads_[i] = std::thread([this]() {
            while (true) {
                int task_id;
                int total_tasks;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    // Wait for work
                    cv_work_.wait(lock, [this]() { 
                        return work_available_ || shutdown_; 
                    });
                    
                    if (shutdown_) {
                        return;  // Exit thread
                    }
                    
                    // Grab a task
                    task_id = next_task_.fetch_add(1);
                    total_tasks = num_total_tasks_.load();
                    
                    // If no more tasks left to grab, mark work as unavailable
                    if (next_task_.load() >= total_tasks) {
                        work_available_ = false;
                    }
                }
                
                // Execute task if valid
                if (task_id < total_tasks) {
                    runnable_->runTask(task_id, total_tasks);
                    
                    // Increment completed counter
                    int completed = tasks_completed_.fetch_add(1) + 1;
                    
                    // If all tasks done, notify main thread
                    if (completed >= total_tasks) {
                        cv_done_.notify_one();
                    }
                }
            }
        });
    }
}


TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
    }
    cv_work_.notify_all();  // Wake up all threads so they can exit

    // Wait for all threads to complete
    for (auto& thread : threads_) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    {
        std::unique_lock<std::mutex> lock(mutex_);
        runnable_ = runnable;
        num_total_tasks_ = num_total_tasks;
        tasks_completed_ = 0;
        next_task_ = 0;
        work_available_ = true;  // Mark work as available
        cv_work_.notify_all();  // Notify while holding lock!
    }

    // wait for completion
    std::unique_lock<std::mutex> lock(mutex_);
    cv_done_.wait(lock, [this]() { return tasks_completed_.load() >= num_total_tasks_; });

    //clear runnable
    runnable_ = nullptr;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
