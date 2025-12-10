package ratelimiter;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeakBucketRateLimiter {

    final double maxCapacity;
    final DrainageRate drainageRate;
    final double drainagePerNanoSecond;

    final Map<String, UserBucket>  bucketMap = new ConcurrentHashMap<>();

    public LeakBucketRateLimiter(final double maxCapacity, final DrainageRate drainRate) {
        this.maxCapacity = maxCapacity;
        this.drainageRate = drainRate;
        this.drainagePerNanoSecond = (drainRate.value() / drainRate.unit().getDuration().toNanos());
    }

    public boolean isAdmissible(final String userId) {
        final UserBucket userBucket = bucketMap.computeIfAbsent(userId, (id) -> new UserBucket(0, Instant.now()));
        synchronized (userBucket) {
            updateVolumeHeld(userBucket);
            if(userBucket.getVolumeHeld() + 1 > maxCapacity) return false;
            else {
                userBucket.setVolumeHeld(userBucket.getVolumeHeld() + 1.0);
                return true;
            }
        }
    }

    private void updateVolumeHeld(final UserBucket userBucket) {
        final double volumeHeld = userBucket.getVolumeHeld();
        final Instant lastUpdateTime = userBucket.getLastUpdateTime();
        final Instant now = Instant.now();
        double updatedVolume = Math.max(
                0,
                volumeHeld - drainagePerNanoSecond * lastUpdateTime.until(now, ChronoUnit.NANOS)
        );
        userBucket.setLastUpdateTime(now);
        userBucket.setVolumeHeld(updatedVolume);
    }

    private static final class UserBucket {
        private double volumeHeld;
        private Instant lastUpdateTime;

        public UserBucket(final double volumeHeld, final Instant lastUpdateTime) {
            this.volumeHeld = volumeHeld;
            this.lastUpdateTime = lastUpdateTime;
        }

        public Instant getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void setLastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public double getVolumeHeld() {
            return volumeHeld;
        }

        public void setVolumeHeld(double volumeHeld) {
            this.volumeHeld = volumeHeld;
        }
    }

    public record DrainageRate(double value, ChronoUnit unit) {

    }
}
