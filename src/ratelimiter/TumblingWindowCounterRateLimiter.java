package ratelimiter;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TumblingWindowCounterRateLimiter {

    final int limit;
    final Duration windowSize;
    final Map<String, UserRequests> windowCounterMap = new ConcurrentHashMap<>();

    public TumblingWindowCounterRateLimiter(final int limit, Duration windowSize){
        this.limit = limit;
        this.windowSize = windowSize;
    }

    public boolean isAdmissible(String userId) {
        final Instant now = Instant.now();
        UserRequests userRequests = windowCounterMap.computeIfAbsent(userId, (id) -> new UserRequests(0, now));
        synchronized (userRequests) {
            if(now.isAfter(userRequests.getWindowStart().plus(windowSize))) {
                userRequests.setCount(1);
                userRequests.setWindowStart(now);
                return true;
            } else if (userRequests.getCount() + 1 > limit){
                return false;
            } else {
                userRequests.setCount(1 + userRequests.getCount());
                return true;
            }
        }
    }

    private static class UserRequests {
        private int count;
        private Instant windowStart;

        public UserRequests(int count, Instant windowStart) {
            this.count = count;
            this.windowStart = windowStart;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public Instant getWindowStart() {
            return windowStart;
        }

        public void setWindowStart(Instant windowStart) {
            this.windowStart = windowStart;
        }
    }
}
