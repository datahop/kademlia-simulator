package peersim.kademlia.das;



public class GossipTopicMap {
    private static final int SIZE = 512;
    private final int groupSize;

    public GossipTopicMap(int groupSize) {
        if (groupSize <= 0 || groupSize > SIZE) {
            throw new IllegalArgumentException("Group size must be between 1 and " + SIZE);
        }
        this.groupSize = groupSize;
    }

    public String getRowTopic(int row) {
        validateIndex(row);
        return "row" + ((row / groupSize) + 1);
    }

    public String getColumnTopic(int column) {
        validateIndex(column);
        return "column" + ((column / groupSize) + 1);
    }

    private void validateIndex(int index) {
        if (index < 0 || index >= SIZE) {
            throw new IllegalArgumentException("Index must be between 0 and " + (SIZE - 1));
        }
    }
}