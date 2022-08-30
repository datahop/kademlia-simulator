package peersim.kademlia;

import java.math.BigInteger;
import peersim.core.Network;
import peersim.core.Node;

/**
 * Some utility and mathematical function to work with BigInteger numbers and strings.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class Util {

  /**
   * Given two numbers, returns the length of the common prefix, i.e. how many digits (in base 2)
   * have in common from the leftmost side of the number
   *
   * @param b1 BigInteger
   * @param b2 BigInteger
   * @return int
   *     <p>public static final int prefixLen(BigInteger b1, BigInteger b2) {
   *     <p>String s1 = Util.put0(b1); String s2 = Util.put0(b2);
   *     <p>int i = 0; for (i = 0; i < s1.length(); i++) { if (s1.charAt(i) != s2.charAt(i)) return
   *     i; }
   *     <p>return i; }
   */

  /**
   * return the distance between two number which is defined as (a XOR b)
   *
   * @param a BigInteger
   * @param b BigInteger
   * @return BigInteger
   *     <p>public static final BigInteger distance(BigInteger a, BigInteger b) { return a.xor(b); }
   */

  /**
   * convert a BigInteger into a String (base 2) and lead all needed non-significative zeroes in
   * order to reach the canonical length of a nodeid
   *
   * @param b BigInteger
   * @return String
   */
  public static final String put0(BigInteger b) {
    if (b == null) return null;
    String s = b.toString(2); // base 2
    while (s.length() < KademliaCommonConfig.BITS) {
      s = "0" + s;
    }
    return s;
  }
  /**
   * Measures the log-distance between two BigInteger values using the length of the differing
   * suffix in bits
   *
   * @param BigInteger a
   * @param BigInteger b
   * @return int result
   */
  public static int distance(BigInteger a, BigInteger b) {

    BigInteger x = a.xor(b);

    return x.bitLength();
  }

  /**
   * Search through the network the Node having a specific node Id, by performing binary serach (we
   * concern about the ordering of the network).
   *
   * @param searchNodeId BigInteger
   * @return Node
   */
  public static Node nodeIdtoNode(BigInteger searchNodeId, int pid) {
    if (searchNodeId == null) return null;

    int inf = 0;
    int sup = Network.size() - 1;
    int m;

    while (inf <= sup) {
      m = (inf + sup) / 2;

      BigInteger mId = ((KademliaProtocol) Network.get(m).getProtocol(pid)).getNode().getId();

      if (mId.equals(searchNodeId)) return Network.get(m);

      if (mId.compareTo(searchNodeId) < 0) inf = m + 1;
      else sup = m - 1;
    }

    // perform a traditional search for more reliability (maybe the network is not ordered)
    BigInteger mId;
    for (int i = Network.size() - 1; i >= 0; i--) {
      mId = ((KademliaProtocol) Network.get(i).getProtocol(pid)).getNode().getId();
      if (mId.equals(searchNodeId)) return Network.get(i);
    }

    return null;
  }
}
