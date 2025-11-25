import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class MiniLsm {

    private static final int WAL_FLUSH_THRESHOLD = 4 * 1024 * 1024; //4MB

    private static final int INDEX_ITEM_DISTANCE = 4 * 1024; //4KB

    final SortedMap<String, String> memtable; //easy to flush as a SS table

    final Path dataDir;

    //latest files first
    final SortedMap<String, SortedMap<String, Long>> sstIndices;

    File writeAheadLog;

    FileOutputStream walOutputStream;


    public MiniLsm(final Path dataDir) throws IOException {
        this.dataDir = Files.createDirectories(dataDir);
        this.writeAheadLog = getWriteAheadLogFile();
        this.walOutputStream = new FileOutputStream(writeAheadLog);
        this.memtable = buildMemtable();
        this.sstIndices = buildAllSparseIndices();
    }

    public void put(String key, String value) throws IOException {
        walOutputStream.write(encode(key, value));
        walOutputStream.getFD().sync(); //ensures we write to the disk
        memtable.put(key, value);

        if (writeAheadLog.length() >= WAL_FLUSH_THRESHOLD) {
            flushWriteAheadLog();
            memtable.clear();
            writeAheadLog = getWriteAheadLogFile();
            walOutputStream = new FileOutputStream(writeAheadLog);
        }
    }

    public String get(String key) throws IOException {
        if (memtable.containsKey(key)) {
            return memtable.get(key);
        }
        for (Map.Entry<String, SortedMap<String, Long>> index : sstIndices.entrySet()) {
            final String searchResult = searchSortedStringTable(key, index.getKey(), index.getValue());
            if (searchResult != null) {
                return searchResult;
            }
        }
        return null;
    }

    // flush the current memtable as sorted string table file
    private void flushWriteAheadLog() throws IOException {
        final File sstFile = dataDir.resolve("sst-" + System.currentTimeMillis() + ".log").toFile();
        final SortedMap<String, Long> sstIndex = new TreeMap<>();
        long nextIndexPoint  = 0; long currIndex = 0;
        try (final BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(sstFile))) {
            for (Map.Entry<String, String> entry : memtable.entrySet()) {
                byte[] dataBytes = encode(entry.getKey(), entry.getValue());

                //building SST index on the fly
                if(currIndex >= nextIndexPoint) {
                    sstIndex.put(entry.getKey(), currIndex);
                    nextIndexPoint +=  INDEX_ITEM_DISTANCE;
                }

                outputStream.write(dataBytes);
                currIndex += dataBytes.length;
            }
            sstIndices.put(sstFile.getName(), sstIndex);
            outputStream.flush();
        }
        walOutputStream.close();
        writeAheadLog.delete();
    }

    //for storge in file
    private byte[] encode(String key, String value) {

        final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        final byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

        // key length + val length + data
        final ByteBuffer bb = ByteBuffer.allocate(4 + 4 + keyBytes.length + valBytes.length);
        bb.putInt(keyBytes.length);
        bb.putInt(valBytes.length);
        bb.put(keyBytes);
        bb.put(valBytes);

        return bb.array();
    }

    //search within a single string sorted table
    private String searchSortedStringTable(
            final String key,
            final String fileName,
            final SortedMap<String, Long> index) throws IOException {

        final TreeMap<String, Long> treeIndex = new TreeMap<>(index);
        if (treeIndex.floorKey(key) == null) return null;  // required key smaller than first key

        long offset = treeIndex.get(treeIndex.floorKey(key));
        try (RandomAccessFile file = new RandomAccessFile(dataDir.resolve(fileName).toFile(), "r")) {

            //seek to the start of the floor key entry
            file.seek(offset);

            while (file.getFilePointer() < file.length()) {

                //read current key
                final int keyLen = file.readInt();
                final byte[] keyBytes = new byte[keyLen];
                file.readFully(keyBytes);
                final String currKey = new String(keyBytes, StandardCharsets.UTF_8);

                //find val length to read or skip over
                final int valLen = file.readInt();
                if (key.equals(currKey)) {
                    final byte[] valBytes = new byte[valLen];
                    file.readFully(valBytes);
                    return new String(valBytes, StandardCharsets.UTF_8);
                } else {
                    file.skipBytes(valLen);
                }
            }
        }
        return null;
    }

    private File getWriteAheadLogFile() {
        return this.dataDir.resolve("write_head.log").toFile();
    }


    private SortedMap<String, SortedMap<String, Long>> buildAllSparseIndices() throws IOException {
        try (Stream<Path> files = Files.find(dataDir, 1, (p, a) -> p.getFileName().toString().startsWith("sst-"))) {
            final SortedMap<String, SortedMap<String, Long>> sparseIndices = new TreeMap<>(Comparator.reverseOrder());
            files.forEach(file -> {
                try {
                    sparseIndices.put(file.getFileName().toString(), buildSparseIndex(file.toFile()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return sparseIndices;
        }
    }

    private SortedMap<String, Long> buildSparseIndex(File sstFile) throws IOException {
        final SortedMap<String, Long> result = new TreeMap<>();
        try (RandomAccessFile raf = new RandomAccessFile(sstFile, "r")) {
            long nextIndexPoint = 0;
            while (raf.getFilePointer() < raf.length()) {
                long currOffset = raf.getFilePointer();
                if (currOffset >= nextIndexPoint) {
                    result.put(parseNextString(raf), currOffset);
                    nextIndexPoint = currOffset + INDEX_ITEM_DISTANCE;
                } else {
                    seekEntry(raf);
                }
            }
        }
        return result;
    }


    private SortedMap<String, String> buildMemtable() throws IOException {
        final SortedMap<String, String> result = new TreeMap<>();
        if (this.writeAheadLog.exists()) {
            try (RandomAccessFile raf = new RandomAccessFile(this.writeAheadLog, "r")) {
                while (raf.getFilePointer() < raf.length()) {
                    String key = parseNextString(raf);
                    String val = parseNextString(raf);
                    result.put(key, val);
                }
            }
        }
        return result;
    }

    private String parseNextString(final RandomAccessFile raf) throws IOException {
        final int strLen = raf.readInt();
        final byte[] strBytes = new byte[strLen];
        raf.readFully(strBytes);
        return new String(strBytes, StandardCharsets.UTF_8);
    }

    private void seekEntry(final RandomAccessFile raf) throws IOException {
        final int keyLen = raf.readInt();
        raf.skipBytes(keyLen);
        final int valLen = raf.readInt();
        raf.skipBytes(valLen);
    }

}
