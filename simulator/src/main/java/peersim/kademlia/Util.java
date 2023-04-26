package peersim.kademlia;

import java.math.BigInteger;
import java.util.Set;

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
   */
  public static final int prefixLen(BigInteger b1, BigInteger b2) {
    String s1 = Util.put0(b1);
    String s2 = Util.put0(b2);
    int i = 0;
    for (i = 0; i < s1.length(); i++) {
      if (s1.charAt(i) != s2.charAt(i)) return i;
    }
    return i;
  }

  /**
   * Given a BigInteger b and an index i, flip the bit at index i of b and return the updated
   * BigInteger value.
   *
   * @param b BigInteger
   * @param index int
   * @return BigInteger
   */
  public static final BigInteger flipBit(BigInteger b, int index) {
    if (index >= KademliaCommonConfig.BITS) {
      throw new IndexOutOfBoundsException("Invalid bit index: " + index);
    }

    if (b.testBit(index)) { // check if bit at index is set to 1
      b = b.clearBit(index); // if yes, clear the bit
    } else {
      b = b.setBit(index); // if no, set the bit
    }

    return b;
  }

  /**
   * Given a target key (BigInteger) and a set of closest peers, compute and return the minimum of
   * the common prefix lengths shared by the peers in the set and the target.
   *
   * @param targetKey BigInteger
   * @param closest
   * @return BigInteger
   */
  public static final int getMinCplWithSet(BigInteger targetKey, Set<BigInteger> closestSet) {
    int curr_minCPL = KademliaCommonConfig.BITS;
    for (BigInteger n : closestSet) {
      int cpl = Util.prefixLen(n, targetKey);
      if (cpl < curr_minCPL) {
        curr_minCPL = cpl;
      }
    }
    return curr_minCPL;
  }

  /**
   * return the distance between two number which is defined as (a XOR b)
   *
   * @param a BigInteger
   * @param b BigInteger
   * @return BigInteger
   *     <p>public static BigInteger xorDistance(BigInteger a, BigInteger b) { return a.xor(b); }
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
  public static int logDistance(BigInteger a, BigInteger b) {

    BigInteger x = a.xor(b);

    return x.bitLength();
  }

  /**
   * return the distance between two number wich is defined as (a XOR b)
   *
   * @param a BigInteger
   * @param b BigInteger
   * @return BigInteger
   */
  public static BigInteger xorDistance(BigInteger a, BigInteger b) {
    return a.xor(b);
  }
}
