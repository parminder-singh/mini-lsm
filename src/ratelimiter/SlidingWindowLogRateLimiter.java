package ratelimiter;

import java.time.Duration;
import java.time.Instant;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SlidingWindowLogRateLimiter {

    private final int maxRequests;
    private final Duration windowSize;
    private Map<String, Deque<Instant>> userRequestsMap = new ConcurrentHashMap<>();

    public SlidingWindowLogRateLimiter(final int maxRequests, final Duration windowSize) {
        this.maxRequests = maxRequests;
        this.windowSize = windowSize;
    }

    public boolean isAdmissible(final String userId) {
        final Deque<Instant> userRequests = userRequestsMap.computeIfAbsent(userId, (id) -> new LinkedList<>());
        synchronized (userRequests) {
            pruneStaleRequests(userRequests);
            if(userRequests.size() + 1 > maxRequests) {
                return false;
            }
            else {
                userRequests.addLast(Instant.now());
                return true;
            }
        }
    }

    private void pruneStaleRequests(final Deque<Instant> requestQueue) {
        final Instant now = Instant.now();
        while(!requestQueue.isEmpty()) {
            if(requestQueue.peekFirst().isBefore(now.minus(windowSize))) {
                requestQueue.pollFirst();
            } else {
                break;
            }
        }
    }
}
