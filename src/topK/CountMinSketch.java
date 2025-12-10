package topK;

import java.util.Random;
import java.util.concurrent.atomic.AtomicIntegerArray;

class CountMinSketch {

    private final int width;
    private final int depth;

    private final AtomicIntegerArray counts;

    private final int[] salts;

    public CountMinSketch(final double errorFactor, final double confidence) {
        this.width = (int) Math.ceil(Math.E/errorFactor);
        this.depth = (int)  Math.ceil(Math.log(1/(1-confidence)));
        this.counts = new AtomicIntegerArray(width * depth);
        this.salts = new int[depth];
        Random random = new Random();
        for(int i = 0; i < depth; i++) salts[i] = random.nextInt();
    }

    public int add(String item) {
        int min = Integer.MAX_VALUE;
        for(int i = 0; i < depth; i++) {
            int hash =  hash(item, salts[i]);
            int count = counts.incrementAndGet( i* width + hash);
            if(count < min) min = count;
        }
        return min;
    }

    public int estimate(String item) {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < depth; i++) {
            int hash = hash(item, salts[i]);
            int count = counts.get( i* width + hash);
            if (count == 0) return 0; // short circuit
            if (count < min) min = count;
        }
        return min;
    }

    private int hash(final String item, int salt) {
        return  ((item.hashCode() ^ salt) & 0x7FFFFFFF) % width;
    }
}