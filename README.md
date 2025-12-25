# velocity

**velocity** is a high-performance, multithreaded in-memory key-value store written in modern **C++23**.

It leverages **ASIO Coroutines**, **Abseil's Flat Hash Map**, and **Lock Striping** architecture to achieve over **200,000 Requests Per Second (RPS)** on commodity hardware (4-core CPU).

![C++](https://img.shields.io/badge/Language-C%2B%2B23-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## Key Features

* **Extreme Performance:** Capable of handling **210k+ RPS** on a 4-core machine using **TCP_NODELAY**.
* **Sharded Architecture:** Uses **64-way Lock Striping** to minimize thread contention (~98% lock-free probability).
* **Cache Friendly:** Implements **alignas(64) padding** to eliminate False Sharing between CPU cores.
* **Multithreaded:** Runs a dedicated `io_context` pool utilizing all available hardware cores.
* **Zero-Copy Parsing:** Custom protocol parser using `std::string_view` to eliminate unnecessary memory allocations.
* **Coroutine-based:** Uses C++20/23 `co_await` for clean, asynchronous, non-blocking network I/O.
* **Smart Storage:** Built on top of Google's `absl::flat_hash_map` for O(1) lookups and cache locality.

## Architecture

velocity avoids the common "Global Lock" bottleneck found in simple multithreaded servers.

### 1. Sharding & Lock Striping
Instead of a single mutex protecting the entire database, the key space is hashed and distributed across **64 independent shards**.
* **Reads (get):** Use `std::shared_lock` (Multiple readers allowed per shard).
* **Writes (set):** Use `std::unique_lock` (Exclusive access to only 1/64th of the DB).

### 2. False Sharing Prevention
Each shard struct is padded to 64 bytes (`alignas(64)`) to ensure that mutexes for adjacent shards sit on different CPU Cache Lines, preventing expensive cache invalidations.

## Build Instructions

### Prerequisites
* **C++ Compiler:** GCC 11+ or Clang 14+ (Must support C++20/23).
* **CMake:** Version 3.20 or higher.
* **Make** or **Ninja**.

### Dependencies
The project automatically downloads **Abseil** via CMake `FetchContent`. You need **ASIO** installed on your system.

**On Fedora/RedHat:**
```bash
sudo dnf install asio-devel
```

On Ubuntu/Debian:

```bash
sudo apt install libasio-dev
```
Compiling
Clone the repository

```bash
git clone https://github.com/Rishi-Rijal/velocity.git
cd velocity
```
Create build directory

```bash
mkdir build && cd build
```
Configure (Release mode!)

```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
```
Build

```bash
make -j$(nproc)
```
Usage
Starting the Server
Run the executable. It will automatically detect your CPU cores and spawn worker threads.

```bash
./velocity
```
Output:

```plaintext
Server running on port 8999
Starting 4 worker threads...
```
Client
You can connect using netcat, telnet, or the included benchmark tool.

Using Netcat:

```terminal
nc localhost 8999
```
Commands:

```terminal
set user:101 rishi
# (No output means success)

get user:101
rishi
```
Protocol:

set <key> <value>: Stores the value.

get <key>: Returns the value or (empty).

Benchmarking
To run the built-in load tester:

Bash

# Ensure server is running first
```bash
./benchmark
```
Contributing
Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

License
This project is licensed under the MIT License.
