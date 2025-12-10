import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.IntStream;

public class ConsistentHash {

    public static final int REPLICATION_FACTOR = 100;

    private NavigableMap<Integer, String> nodeRing = new TreeMap<>();

    public String getNode(final String key) {
        final int hash = hash(key);
        if (Objects.isNull(nodeRing.ceilingKey(hash))) {
            return nodeRing.firstEntry().getValue();
        } else {
            return nodeRing.ceilingEntry(hash).getValue();
        }
    }

    private void addNode(String nodeId) {
        for (int vNode : getVNodes(nodeId)) {
            nodeRing.put(vNode, nodeId);
        }
    }

    private void removeNode(String nodeId) {
        for (int vNode : getVNodes(nodeId)) {
            nodeRing.remove(vNode);
        }
    }

    private int[] getVNodes(String nodeId) {
        return IntStream.range(0, REPLICATION_FACTOR)
                .map(i -> hash(String.format("%s-%d", nodeId, i)))
                .toArray();
    }

    private int hash(String key) {
        return Hashing.murmur3_32_fixed().hashString(key, Charset.defaultCharset()).asInt();
    }
}