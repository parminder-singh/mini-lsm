package topK;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TumblingWindowLeaderAnalyzer {

    private final int leaderBoardSize;
    private final AtomicReference<FrequencyLeaderBoard> currLeaderBoardRef;
    private final ScheduledExecutorService executorService;

    public TumblingWindowLeaderAnalyzer(final int leaderBoardSize, final Duration window) {
        this.leaderBoardSize = leaderBoardSize;
        this.currLeaderBoardRef = new AtomicReference<>(new FrequencyLeaderBoard(leaderBoardSize));
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(
                this::rotateAndAnalyze,
                window.toSeconds(),
                window.toSeconds(),
                TimeUnit.SECONDS
        );

    }

    public void add(String item) {
        currLeaderBoardRef.get().add(item);
    }

    public void rotateAndAnalyze() {
        final FrequencyLeaderBoard oldLeaderBoard = currLeaderBoardRef.getAndSet(new FrequencyLeaderBoard(leaderBoardSize));
        for (FrequencyLeaderBoard.LeaderBoardItem item : oldLeaderBoard.getLeaderBoard()) {
            System.out.println("Item: %s Frequency: %s" + item);
        }
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if(!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

}
