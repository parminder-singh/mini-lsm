package ratelimiter;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TokenBucketRateLimiter {

    final double maxCapacity;
    final RefillRate refillRate;
    final double refillsPerNanoSec;

    final Map<String, UserBucket>  bucketMap = new ConcurrentHashMap<>();

    public TokenBucketRateLimiter(final double maxCapacity, final RefillRate refillRate) {
        this.maxCapacity = maxCapacity;
        this.refillRate = refillRate;
        this.refillsPerNanoSec = (refillRate.value() / refillRate.unit().getDuration().toNanos());
    }

    public boolean isAdmissible(final String userId) {
        final UserBucket userBucket = bucketMap.computeIfAbsent(userId, (id) -> new UserBucket(maxCapacity, Instant.now()));
        synchronized (userBucket) {
            updateCapacity(userBucket);
            if(userBucket.getTokenCount() < 1.0) return false;
            else {
                userBucket.setTokenCount(userBucket.getTokenCount() - 1.0);
                return true;
            }
        }
    }

    private void updateCapacity(final UserBucket userBucket) {
        final double availableCapacity = userBucket.getTokenCount();
        final Instant lastUpdateTime = userBucket.getLastUpdateTime();
        final Instant now = Instant.now();
        double updatedCapacity = Math.min(
                maxCapacity,
                availableCapacity + refillsPerNanoSec * lastUpdateTime.until(now, ChronoUnit.NANOS)
        );
        userBucket.setLastUpdateTime(now);
        userBucket.setTokenCount(updatedCapacity);
    }

    private static final class UserBucket {
        private double tokenCount;
        private Instant lastUpdateTime;

        public UserBucket(final double tokenCount, final Instant lastUpdateTime) {
            this.tokenCount = tokenCount;
            this.lastUpdateTime = lastUpdateTime;
        }

        public Instant getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void setLastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public double getTokenCount() {
            return tokenCount;
        }

        public void setTokenCount(double tokenCount) {
            this.tokenCount = tokenCount;
        }
    }

    public record RefillRate(double value, ChronoUnit unit) {

    }
}
