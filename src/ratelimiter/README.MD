# 🛡️ The Rate Limiting Handbook

**Context:** Traffic Engineering & Backend Systems (Netflix/High-Scale).

## Part I: The Algorithms (Single Node)
*Mathematical logic used to decide "Yes/No" for a request on a single machine.*

### 1. Token Bucket
**The Metaphor:** An allowance. You get paid tokens every second. You spend them per request.
* **Behavior:** Allows **Bursts** up to `maxCapacity`.
* **Implementation:**
    * **State:** `currentTokens` (double), `lastRefillTime` (nanos).
    * **Logic (Lazy Refill):** Do not use background threads. Calculate refill only when a request arrives.
    * **Math:** Use `double` for tokens and `nanoseconds` for duration to avoid the "Staircase Effect" (stuttering traffic).
    * **Thread Safety:** Requires `synchronized` block or `ReentrantLock` to ensure atomicity between reading `currentTokens` and updating it.
* **Code Pitfall:** Capturing `Instant.now()` twice (once for calc, once for set) causes "Time Leaks." Capture it once.

### 2. Leaky Bucket
**The Metaphor:** A bucket with a hole. Requests add water; gravity drains it.
* **Variant A: The Meter (What we coded)**
    * **Behavior:** Mathematically the inverse of Token Bucket.
    * **Logic:** `water = max(0, water - leaked)`. Check `if (water + 1 <= capacity)`.
    * **Use Case:** Binary Rate Limiting (Allow/Deny).
* **Variant B: The Queue (Traffic Shaper)**
    * **Behavior:** Requests enter a FIFO queue and are processed at a strictly constant rate.
    * **Use Case:** Preventing database overload (smoothing spikes into a flat line).
    * **Trade-off:** Increases latency for the user (they wait in the queue).

### 3. Fixed Window Counter
**The Metaphor:** A fresh counter for every clock-second (e.g., `12:00:00`).
* **Behavior:** Simple counting.
* **The Flaw (Double Dip):** If a user sends 10 requests at `12:00:00.999` and 10 at `12:00:01.001`, they processed 20 requests in 2ms. This allows 2x the rate limit at boundaries.
* **Use Case:** Low-memory constraints where occasional bursts are acceptable.

### 4. Sliding Window Log
**The Metaphor:** A strictly tracked list of timestamps.
* **Behavior:** "Strictly no more than N requests in the last T seconds."
* **Implementation:** `Map<User, Deque<Instant>>`.
* **Logic:**
    1.  **Prune:** Pop timestamps from head if `< (now - window)`.
    2.  **Count:** Check `deque.size()`.
    3.  **Add:** Push `now` to tail.
* **Pros:** 100% Accuracy. Solves the "Double Dip."
* **Cons:** **High Memory.** Storing 1 million `Instant` objects per user is expensive. `O(N)` operations.

---

## Part II: Distributed Architectures (Cluster Scale)
*How to synchronize limits across 100+ servers.*

### Level 1: Centralized (Redis + Lua)
**Architecture:** Every app server calls Redis for every request.
* **Atomicity:** Use **Lua Scripts** to ensure `get` + `math` + `set` happen atomically.
* **Pros:** Strict consistency. One "Global Truth."
* **Cons:** **High Latency.** Adds network Round-Trip Time (RTT) to every API call. Becomes a Single Point of Failure (SPOF).

### Level 2: Distributed Batching (Token Reservation)
**Architecture:** App servers act as "Wholesalers."
* **Mechanism:**
    1.  App Server checks local RAM. If empty...
    2.  Calls Redis: "Reserve **50 tokens**."
    3.  Redis deducts 50.
    4.  App Server serves the next 50 requests from RAM (Zero Latency).
* **Trade-offs:**
    * **Accuracy:** If an app server crashes, reserved tokens are lost ("Leak").
    * **Starvation:** An idle server might hoard tokens while an active server starves.
* **Verdict:** The standard for High-Performance systems (Netflix/Stripe) where Latency > Perfect Accuracy.

### Level 3: Sticky Sharding (Consistent Hashing)
**Architecture:** Route `UserId` to a specific App Server.
* **Mechanism:**
    * Load Balancer/Mesh calculates `hash(userId)`.
    * Routes to "Home Node."
    * Home Node keeps counters in simple Java `HashMap` (RAM).
* **Pros:** Zero Latency. No Redis bottleneck.
* **Cons:**
    * **The "Taylor Swift" Problem (Hot Partition):** If one user is viral, that single node gets 100% of the traffic and dies.
    * **Rebalancing:** When a node adds/removes, counters are reset.

---

## Part III: Comparison Cheat Sheet

| Algorithm | Complexity | Memory | Accuracy | Best For |
| :--- | :--- | :--- | :--- | :--- |
| **Token Bucket** | Low | Low (2 vars) | Good (Avg Rate) | General API Limiting |
| **Leaky Bucket (Queue)** | High | Medium (Queue) | Perfect (Constant) | DB Protection / Background Jobs |
| **Fixed Window** | Low | Very Low | Low (Double Dip) | Simple/Low-Criticality Apps |
| **Sliding Log** | Medium | **High** (`O(N)`) | **Perfect** | Strict billing / Security |
| **Redis Batching** | High | Low | Medium | **High Scale Distributed Systems** |
