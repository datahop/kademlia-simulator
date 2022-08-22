package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
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
   */
  public static final int prefixLen(BigInteger b1, BigInteger b2) {

    String s1 = Util.put0(b1);
    String s2 = Util.put0(b2);
    int i = 0;
    for (i = 0; i < s1.length(); i++) {
      if (s1.charAt(i) != s2.charAt(i)) {
        return i;
      }
    }
    return i;
  }

  /**
   * return the distance between two number wich is defined as (a XOR b)
   *
   * @param a BigInteger
   * @param b BigInteger
   * @return BigInteger
   */
  public static final BigInteger distance(BigInteger a, BigInteger b) {
    // assert false : "Shouldn't be used anymore";
    return a.xor(b);
  }

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
   * calculate the log base 2 of a BigInteger value
   *
   * @param BigInteger value
   * @return double result
   */
  public static double log2(BigInteger x) {
    return Math.log10(x.doubleValue()) / Math.log10(2);
  }

  /**
   * calculate the log base 2 of a BigInteger value
   *
   * @param BigInteger value
   * @return double result
   */
  /*public static int logDistance(BigInteger a,BigInteger b)
  {
      int lz = 0;

      byte[] abyte = a.toByteArray();
      byte[] bbyte = b.toByteArray();

      if (abyte[0] == 0) {
          byte[] tmp = new byte[abyte.length - 1];
          System.arraycopy(abyte, 1, tmp, 0, tmp.length);
          abyte = tmp;
      }

      if (bbyte[0] == 0) {
          byte[] tmp = new byte[bbyte.length - 1];
          System.arraycopy(bbyte, 1, tmp, 0, tmp.length);
          bbyte = tmp;
      }



      for(int i = 0;i<abyte.length;i++)
      {
          //System.out.println("lz "+lz);
          byte x = (byte) (abyte[i] ^ bbyte[i]);
          //System.out.println("xor "+Byte.toUnsignedInt(x)+" "+Byte.toUnsignedInt(abyte[i])+" "+Byte.toUnsignedInt(bbyte[i]));
          if(x==0) {
              lz+=8;
          } else {
              lz += leadingZeros8(Byte.toUnsignedInt(x));
              break;
          }
      }
      return abyte.length*8 - lz;
  }*/

  /**
   * Measures the logdistance between two BigInteger values using the length of the differing suffix
   * in bits
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
   * Return the 64 bit prefix of any BigInteger
   *
   * @param BigInteger value
   * @return BigInteger result
   */
  public static BigInteger prefix(BigInteger address) {

    String prefix = address.toString(16).substring(0, 16);

    return new BigInteger(prefix, 16);
  }

  /**
   * Search through the network the Node having a specific node Id, by performing binary search (we
   * concern about the ordering of the network).
   *
   * @param searchNodeId BigInteger
   * @return Node
   */
  public static Node nodeIdtoNode(BigInteger searchNodeId) {
    if (searchNodeId == null) return null;

    int inf = 0;
    int sup = Network.size() - 1;
    int m;

    // System.out.println("nodeIdtoNode "+kademliaid);

    while (inf <= sup) {
      m = (inf + sup) / 2;
      Node n = Network.get(m);

      BigInteger mId = n.getKademliaProtocol().node.getId();

      if (mId.equals(searchNodeId)) return Network.get(m);

      if (mId.compareTo(searchNodeId) < 0) inf = m + 1;
      else sup = m - 1;
    }

    // perform a traditional search for more reliability (maybe the network is not ordered)
    for (int i = Network.size() - 1; i >= 0; i--) {
      Node n = Network.get(i);

      BigInteger mId = n.getKademliaProtocol().node.getId();

      if (mId.equals(searchNodeId)) return Network.get(i);
    }

    return null;
  }

  private static int leadingZeros8(int x) {

    int[] len8tab =
        new int[] {
          0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
          5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
          6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
          7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
          7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
          8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
          8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
          8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
          8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8
        };

    return 8 - len8tab[x];
  }

  public static String bigIntegetSetToString(HashSet<BigInteger> set) {
    if (set.size() == 0) return "";

    String result = "\"";
    boolean first = true;
    for (BigInteger n : set) {
      if (first) {
        result += n;
        first = false;
      } else {
        result += " " + n;
      }
    }
    result += "\"";
    return result;
  }

  public static boolean isNetworkConnected() {
    ArrayList<Set<BigInteger>> groups = new ArrayList<Set<BigInteger>>();

    // form groups of "node neighbor sets" that are disjoint
    for (int i = 0; i < Network.size(); i++) {
      Node node = Network.get(i);
      KademliaProtocol prot = (KademliaProtocol) (node.getKademliaProtocol());
      BigInteger id = prot.node.getId();
      Set<BigInteger> neighbours = prot.routingTable.getAllNeighbours();

      boolean added = false;
      for (Set<BigInteger> group : groups) {
        HashSet<BigInteger> intersection = new HashSet<BigInteger>(group);
        intersection.retainAll(neighbours);
        if (intersection.size() > 0) {
          group.addAll(neighbours);
          added = true;
          break;
        }
      }
      if (!added) {
        groups.add(neighbours);
      }
    }

    // Try merging the groups, if more than one.
    // If groups are mergeable, then we should be done in n^2 iterations
    // where n is the number of groups.
    int max_iterations = groups.size();
    System.out.println("Merging groups");
    while ((groups.size() > 1) && (max_iterations >= 0)) {
      boolean merged = false;
      for (int i = 1; i < groups.size(); i++) {
        HashSet<BigInteger> intersection = new HashSet<BigInteger>(groups.get(0));
        intersection.retainAll(groups.get(i));
        if (intersection.size() > 0) {
          groups.get(0).addAll(groups.get(i));
          groups.remove(i);
          merged = true;
          break;
        }
      }
      max_iterations--;
    }
    if (groups.size() == 1) {
      return true;
    } else {
      System.out.println("We have " + groups.size() + " disconnected groups");
      return false;
    }
  }

  public static final boolean compareAddr(String addr1, String addr2) {

    String[] split1 = addr1.split("\\.");
    String[] split2 = addr2.split("\\.");

    // System.out.println("Split 1 "+addr1+" "+split1+ " "+split1.length);
    // System.out.println("Split 2 "+addr2+" "+split2+ " "+split2.length);

    if (split1.length != 4 || split2.length != 4) return false;

    if (split1[0].equals(split2[0]) && split1[1].equals(split2[1]) && split1[2].equals(split2[2]))
      return true;

    return false;
  }

  public static Topic generateRandomTopic(UniformRandomGenerator urg) {

    int random_topic = urg.randomInt();
    String topic = new String("t_random" + random_topic);
    return new Topic(topic);
  }
}
