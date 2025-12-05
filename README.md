# MapReduce System (Go)

This project implements a distributed MapReduce system in Go, consisting of a Coordinator and multiple Workers. It is designed to handle Map and Reduce tasks in parallel, manage fault tolerance, and support distributed execution without a shared file system.

## System Architecture

### 1. Coordinator
The Coordinator manages the entire lifecycle of the MapReduce job.
- **State Management:** Tracks the state of all Map and Reduce tasks (*Idle*, *In-progress*, *Completed*).
- **Task Assignment:**
    - Assigns Map tasks to workers first.
    - Once all Map tasks are completed, it proceeds to the Reduce phase.
    - Assigns Reduce tasks to workers.
    - Signals workers to exit when all tasks are done.
- **Fault Tolerance:**
    - Tracks the start time of every task.
    - Periodically checks (background ticker) for tasks that have been *In-progress* for more than 10 seconds.
    - If a task times out, it is reset to *Idle* to be reassigned to a different worker.

### 2. Worker
Workers are long-running processes that request tasks from the Coordinator via RPC.
- **Map Phase:**
    - Reads input files and executes the application-specific `Map` function.
    - Bucketing: Partitions output into `nReduce` buckets using hashing.
    - Storage: Writes intermediate data to local disk using JSON encoding.
    - Atomic Writes: Uses `ioutil.TempFile` and `os.Rename` to prevent partial writes during crashes.
    - (Advanced) Serving: Starts an HTTP File Server to serve intermediate files to other workers (since no shared file system is used).
- **Reduce Phase:**
    - Receives locations (IP:Port) of intermediate files from the Coordinator.
    - Fetches intermediate data via **HTTP GET** requests from Map workers.
    - Sorts data, executes the `Reduce` function, and writes final output to `mr-out-Y`.

### 3. RPC Communication
- Protocol: Uses **TCP** (instead of Unix Domain Sockets) to allow execution across different machines.
- Worker → Coordinator: asks for tasks; reports task completion (sending Task ID and Type).
- Coordinator → Worker: assigns tasks (sending Task ID, Type, Filename, `nReduce`, or Map worker address list).

---

## Getting Started

### Prerequisites
- Go (Golang) installed.
- Source code structured with `src/main` and `src/mrapps`.

### 1. Build the Plugin
Compile the MapReduce application (e.g., WordCount) as a Go plugin.

```bash
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
```

### 2. Local Testing
Clean previous outputs and run the coordinator and workers locally.

```bash
rm mr-out*
go run mrcoordinator.go pg-*.txt
```
Start Workers (in separate terminals):

```bash
# Worker 1
go run mrworker.go 127.0.0.1:1234 wc.so

# Worker 2 (to test parallelism/recovery)
go run mrworker.go 127.0.0.1:1234 wc.so
```

### 3. Verification
After execution, verify the output matches the sequential reference.
```bash
cat mr-out-* | sort | more
```

---
## Cloud Testing
To run on a distributed environment (e.g., AWS/Cloud VMs) where workers do not share a file system:

### 1. Deploy: Copy the binary and source files to the remote machines.
### 2. Coordinator: Start on the primary node.
```bash
./mrcoordinator pg-*.txt
```
### 3. Workers: Start on worker nodes, pointing to the Coordinator's IP.
```bash
./mrworker <COORDINATOR_IP>:1234 wc.so
```
_Note: Workers will verify their own public IP and open a random port to serve intermediate files via HTTP._
