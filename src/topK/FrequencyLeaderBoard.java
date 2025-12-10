package topK;

import java.util.*;
import java.util.stream.Collectors;


public class FrequencyLeaderBoard {

    public static double ERROR_FACTOR = Math.pow(10, -5); // error of up-to 10 per 1M
    public static double CONFIDENCE = 0.9999; //error in 1 out of every 10K estimates goes beyond 10

    private final CountMinSketch cms = new CountMinSketch(ERROR_FACTOR, CONFIDENCE);
    private final int leaderBoardSize;

    private final TreeSet<LeaderBoardItem> leaderBoard;

    public FrequencyLeaderBoard(final int leaderBoardSize) {
        this.leaderBoardSize = leaderBoardSize;
        final Comparator<LeaderBoardItem> leaderBoardItemComparator = Comparator
                .comparingInt(LeaderBoardItem::count)
                .thenComparing(LeaderBoardItem::item);
        this.leaderBoard = new TreeSet<>(leaderBoardItemComparator);
    }

    public synchronized void add(String item) {
        int itemCount = cms.add(item);
        if (leaderBoard.size() < leaderBoardSize || leaderBoard.first().count() < itemCount) {
            leaderBoard.removeIf(leaderBoardItem -> leaderBoardItem.item.equals(item)); //remove existing entry for the key
            leaderBoard.add(new LeaderBoardItem(item, itemCount));
            if (leaderBoard.size() > leaderBoardSize) {
                leaderBoard.pollFirst(); //remove the smallest item if overflow
            }
        }
    }

    public synchronized List<LeaderBoardItem> getLeaderBoard() {
        return leaderBoard.stream()
                .sorted(Comparator.comparingInt(LeaderBoardItem::count).reversed())
                .collect(Collectors.toList());
    }

    public record LeaderBoardItem(String item, int count) {
    }
}
