# Raft

A distributed systems project implementing the Raft consensus algorithm in Go.

This project implements a fault-tolerant replicated state machine using the Raft protocol. Multiple servers coordinate through RPC communication to maintain a consistent replicated log, allowing the system to continue operating even in the presence of server crashes or network failures.

## Features

- Leader election with randomized election timeouts
- Heartbeat-based failure detection
- Log replication across distributed nodes
- Commit indexing and replicated state machine consistency
- Fault tolerance under network partitions and server failures
- Concurrent RPC handling using Go goroutines and channels

## Technologies

- Go
- RPC communication
- Concurrency with goroutines and mutexes
- Distributed systems concepts

## Overview

Raft is a distributed consensus algorithm designed to maintain consistency across replicated servers. Servers elect a leader responsible for coordinating log replication and ensuring that committed entries remain consistent across the cluster.

This implementation focuses on the core mechanisms of consensus, replication, and fault tolerance in distributed systems.
