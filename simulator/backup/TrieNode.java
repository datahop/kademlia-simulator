package peersim.kademlia;

public class TrieNode {

  // private static final Logger logger =
  // Logger.getLogger(Thread.currentThread().getStackTrace()[0].getClassName()  );
  private int count;
  private TrieNode zero;
  private TrieNode one;
  // Lower-bound storage
  private double last_modifier;
  private long last_timestamp;

  public TrieNode() {
    count = 0;
    zero = null;
    one = null;
    last_modifier = 0;
    last_timestamp = 0;
  }

  // Gettter and Setters
  public double getLastModifier() {
    return last_modifier;
  }

  public long getLastTimestamp() {
    return last_timestamp;
  }

  public void setLastTimestamp(long timestamp) {
    last_timestamp = timestamp;
  }

  public void setLastModifier(double modifier) {
    last_modifier = modifier;
  }

  private TrieNode zero() {
    return zero;
  }

  private TrieNode one() {
    return one;
  }

  private int count() {
    return count;
  }

  private void increment() {
    count += 1;
  }

  private void decrement() {
    count -= 1;
  }

  private boolean isLeaf() {
    if (one == null && zero == null) return true;
    return false;
  }
  // returns a similarity score for the given (binary) ip address
  // using the trie with the given root node
  public static double getSimilarityScore(TrieNode root, String ip) {

    int score = 0;
    TrieNode currNode = root;

    for (int length = 0; length < 32; length++) {

      double expected = root.count() / Math.pow(2, length);
      if (currNode.count() > expected) score += 1;

      char bit = ip.charAt(length);
      if (bit == '0') {
        currNode = currNode.zero();
      } else if (bit == '1') {
        currNode = currNode.one();
      }
      if (currNode == null) break;
    }

    return (1.0 * score) / 32;
  }

  // adds an ip to the trie whose root node is given and
  // returns similarity score
  public static void addIp(TrieNode root, String ip) {

    TrieNode currNode = root;
    // logger.info("Adding ip: " + ip);

    for (int length = 0; length < 32; length++) {

      currNode.increment();

      // String prefix = ip.substring(0, length);
      // logger.info("Increment prefix: " + prefix + " to " + currNode.count());

      char bit = ip.charAt(length);

      if (bit == '0') {
        if (currNode.zero() == null) {
          currNode.zero = new TrieNode();
        }
        currNode = currNode.zero();
      } else if (bit == '1') {
        if (currNode.one() == null) {
          currNode.one = new TrieNode();
        }
        currNode = currNode.one();
      } else {
        System.out.println("invalid ip address: " + ip);
        System.exit(-1);
      }
    }
    currNode.increment();
  }

  // Removes an ip address from the trie whose root node is the root
  public static void removeIp(TrieNode root, String ip) {

    // logger.info("Removing ip: " + ip);
    TrieNode currNode = root;
    TrieNode prevNode = root;
    boolean delete = false;
    int length;

    boolean zero = false;
    for (length = 0; length < 32; length++) {

      // String prefix = ip.substring(0, length);

      // logger.info("Before Decrement prefix: " + prefix + " to " + currNode.count());
      currNode.decrement();
      // logger.info("After Decrement prefix: " + prefix + " to " + currNode.count());

      if (currNode.count() == 0) {
        zero = true;
        if (currNode != root) {
          char bit = ip.charAt(length - 1);
          if (bit == '0') {
            prevNode.zero = null;
          } else { // if (bit == '1')
            prevNode.one = null;
          }
        }
      } else {
        if (zero) { // came across a node with 1 count (0 after decrement) so all its descendants
          // must be 1 (0 after decrement)
          assert (currNode.count() == 0)
              : "count must be greater than zero, but count = " + currNode.count() + " " + length;
        }
      }
      prevNode = currNode;

      // advance currNode
      char bit = ip.charAt(length);
      if (bit == '0') {
        currNode = currNode.zero();
      } else {
        currNode = currNode.one();
      }
    }
    currNode.decrement();
    if (currNode.count() == 0) {
      char bit = ip.charAt(length - 1);
      if (bit == '0') {
        prevNode.zero = null;
      } else { // if (bit == '1')
        prevNode.one = null;
      }
    }
  }

  // Returns the trie node that is the longest prefix match of a given ip address
  // The returned node is used to store a given IP's lower bound
  public static TrieNode getLowerBoundNode(TrieNode root, String ip) {
    TrieNode currNode = root;
    for (int length = 0; length < 32; length++) {
      char bit = ip.charAt(length);
      if (bit == '0') {
        if (currNode.zero() == null) {
          break;
        } else {
          currNode = currNode.zero();
        }
      } else { // (bit == '1') {
        if (currNode.one() == null) {
          break;
        } else {
          currNode = currNode.one();
        }
      }
    }

    return currNode;
  }
}
