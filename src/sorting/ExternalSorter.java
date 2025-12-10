package sorting;

import com.google.common.base.Stopwatch;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Gatherers;
import java.util.stream.LongStream;

public class ExternalSorter {

    private static final long INPUT_FILE_SIZE = 20 * 1024 * 1024;
    private static final long INPUT_FILE_COUNT = 1000;

    /*
        Size of int on file can be up to 3 times the size in memory (12 bytes vs 4 bytes).
        Setting memory limit smaller the (FILE_COUNT * FILE_SIZE)/3 to force k-way merging.
     */
    private static final long MEM_LIMIT = (INPUT_FILE_COUNT * INPUT_FILE_SIZE) / 100;

    private static final Path DATA_DIR = Path.of(System.getProperty("user.dir"), "external-sort");

    private static final ExecutorService executorService = Executors.newFixedThreadPool(300);

    static void main(final String[] args) throws IOException {

        final Stopwatch timer = Stopwatch.createStarted();

        final TestDataGenerator testDataGenerator = new TestDataGenerator(
                DATA_DIR.toString(),
                "test_data_",
                INPUT_FILE_SIZE, //20 MiB,
                INPUT_FILE_COUNT
        );


        // generate test data
        final List<String> dataFiles = testDataGenerator.generate();
        System.out.println("Time taken generate test data ... " + timer.elapsed());


        //chuck and store sorted file. Assumes we have N node with up to M memory
        timer.reset();
        timer.start();
        final List<String> sortedDataFiles = splitAndSortTestData(dataFiles);
        System.out.println("Time taken to compute intermedia sorted file ... " + timer.elapsed());

        timer.reset();
        timer.start();
        System.out.println("Merging intermediaries...");
        int currPhase = 0; // only so we can use it in lambda expression
        final File intermediaryPathDir = Path.of(sortedDataFiles.getFirst()).getParent().toFile();

        //loop till we are only left with a single file
        while (Objects.requireNonNull(intermediaryPathDir.list()).length != 1) {

            //locate all files to be sorted in current phase
            final List<File> currPhaseFiles = findFilesForMerge(intermediaryPathDir, currPhase);

            //merge all files in current phase
            CompletableFuture.allOf(
                    constructMergeTasks(currPhaseFiles, currPhase + 1).stream()
                            .map(runnable -> CompletableFuture.runAsync(runnable, executorService))
                            .toArray(CompletableFuture[]::new)
            ).join();

            currPhase++;
        }

        System.out.println("Time taken to compute final sorted files ... " + timer.elapsed());

        executorService.shutdown();
    }

    private static List<File> findFilesForMerge(final File dir, final int phaseNumber) {
        //locate all files to be sorted in current phase
        return Arrays.stream(
                        Objects.requireNonNull(
                                dir.list((d, name) -> name.startsWith("phase_" + phaseNumber))
                        )
                )
                .map(fileName -> new File(dir.toString(), fileName))
                .toList();
    }

    private static List<Runnable> constructMergeTasks(final List<File> files, final int nextPhase) {
        return files.stream()
                .gather(Gatherers.windowFixed(300))
                .<Runnable>map(window -> () -> mergeFiles(window, nextPhase))
                .toList();
    }

    private static void mergeFiles(final List<File> files, final int nextPhase) {
        final PriorityQueue<HeapNode> minHeap = new PriorityQueue<>(Comparator.comparing(HeapNode::count));
        try {

            //bootstrapping heap
            for (final File file : files) {
                final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
                final HeapNode heapNode = getHeapNode(file.toPath(), dis);
                if (heapNode != null) minHeap.offer(heapNode);
            }

            final Path sortedFilePath = getSortedFilePath(nextPhase);
            try (final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(sortedFilePath.toFile())))) {
                while (!minHeap.isEmpty()) {
                    final HeapNode currMin = minHeap.poll();
                    dos.writeInt(currMin.count());
                    final HeapNode candidate = getHeapNode(currMin.filePath(), currMin.br());
                    if (candidate != null) {
                        minHeap.offer(candidate);
                    }
                }
            }
        } catch (final Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private static HeapNode getHeapNode(final Path filePath, final DataInputStream dis) throws IOException {
        try {
            final int val = dis.readInt();
            return new HeapNode(val, filePath, dis);
        } catch (EOFException ex) {
            dis.close();
            Files.delete(filePath);
            return null;
        }
    }

    private static List<String> splitAndSortTestData(final List<String> files) {
        
        final List<CompletableFuture<List<String>>> sortingFutures = files.stream()
                .map(testFile -> CompletableFuture.supplyAsync(() -> {
                            try {
                                return sortFile(Path.of(testFile));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, executorService)
                ).toList();

        final CompletableFuture<Void> allOfSortingFutures = CompletableFuture.allOf(
                sortingFutures.toArray(new CompletableFuture[0])
        );

        return allOfSortingFutures.thenApply(
                v -> sortingFutures.stream()
                        .map(CompletableFuture::join)
                        .flatMap(List::stream)
                        .toList()
        ).join();
    }

    private static List<String> sortFile(final Path unsortedFilePath) throws IOException {
        List<String> sortedFileNames = new ArrayList<>();
        try (final BufferedReader br = new BufferedReader(new FileReader(unsortedFilePath.toFile()))) {
            while (true) {
                final int[] batch = readBatch(br);
                if (batch.length == 0) break;
                Arrays.sort(batch);
                final Path sortedFilePath = getSortedFilePath(0);
                try (final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(sortedFilePath.toFile())))) {
                    for (int val : batch) {
                        dos.writeInt(val);
                    }
                    sortedFileNames.add(sortedFilePath.toString());
                }
            }
        }
        System.out.println("Deleting file..." + unsortedFilePath);
        Files.delete(unsortedFilePath);
        return sortedFileNames;
    }

    private static Path getSortedFilePath(int phaseNum) throws IOException {
        final Path intermediarySortedFilesDir = Path.of(DATA_DIR.toString(), "intermediary");
        Files.createDirectories(intermediarySortedFilesDir);
        final String fileName = String.format("phase_%d_sorted_%d", phaseNum, Math.abs(ThreadLocalRandom.current().nextInt()));
        return Path.of(intermediarySortedFilesDir.toString(), fileName);
    }

    private static int[] readBatch(final BufferedReader br) throws IOException {
        int currentBatchSize = 0;
        final int maxBatchSize = (int) (4 * MEM_LIMIT / INPUT_FILE_COUNT); // N files being sorted in parallel and 4 bytes per integer
        String currLine;
        final int[] batch = new int[maxBatchSize];
        while (currentBatchSize < maxBatchSize && ((currLine = br.readLine()) != null)) {
            batch[currentBatchSize++] = Integer.parseInt(currLine);
        }
        return Arrays.copyOf(batch, currentBatchSize);
    }

    private record HeapNode(int count, Path filePath, DataInputStream br) {
    }

    private static class TestDataGenerator {

        private final File directory;
        private final String namePrefix;
        private final long requestedFileSize;
        private final long requestedFileCount;
        private final AtomicInteger nameSuffixCounter = new AtomicInteger(0);

        public TestDataGenerator(
                final String directory,
                final String namePrefix,
                final long fileSize,
                final long fileCount
        ) {
            this.directory = new File(directory);
            this.namePrefix = namePrefix;
            this.requestedFileSize = fileSize;
            this.requestedFileCount = fileCount;
        }

        public List<String> generate() throws IOException {

            if (!Files.exists(directory.toPath())) {
                System.out.println("Creating directory path ... " + directory.toPath());
                Files.createDirectories(directory.toPath());
            }

            final List<CompletableFuture<String>> promises = LongStream.range(0, requestedFileCount)
                    .mapToObj(i -> CompletableFuture.supplyAsync(this::writeInts, executorService)).
                    toList();

            final CompletableFuture<Void> combined = CompletableFuture.allOf(promises.toArray(new CompletableFuture[0]));

            return combined.thenApply(
                    v -> promises.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList())
            ).join();
        }

        //returns file name
        public String writeInts() {
            final String suffix = this.nameSuffixCounter.getAndIncrement() + ".txt";
            final String fileName = this.namePrefix + suffix;
            final Path filePath = new File(directory, fileName).toPath();
            System.out.println("Writing file ... " + filePath);
            try (final PrintWriter pw = new PrintWriter(Files.newBufferedWriter(filePath))) {
                long currSize = 0;
                while (currSize < requestedFileSize) {
                    for (int val : getRandomInts()) {
                        final String line = String.valueOf(val);
                        currSize += line.length() + 1; //UTF-8 consumes a single byte on disk for numbers
                        pw.println(line);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return filePath.toString();
        }

        private int[] getRandomInts() {
            int[] randoms = new int[1024]; // Max overshoot of 12KB (max 10 digits + sign + nl)
            for (int i = 0; i < 1024; i++) {
                randoms[i] = ThreadLocalRandom.current().nextInt();
            }
            return randoms;
        }
    }

}
