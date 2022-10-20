package peersim.kademlia;

import java.math.BigInteger;
import java.util.Random;

// _________________________________________________________________________________________________
/**
 * This initializator assign to the Nodes a nodeId (stored in the protocol MSPastryProtocol) by
 * using this 128-bit (32 byte) random generator.
 *
 * <p><b>Warning:</b> this implementation is not serialized and is not thread-safe
 *
 * <p>Title: MSPASTRY
 *
 * <p>Description: MsPastry implementation for PeerSim
 *
 * <p>Copyright: Copyright (c) 2007
 *
 * <p>Company: The Pastry Group
 *
 * @author Elisa Bisoffi, Manuel Cortella
 * @version 1.0
 */
public final class UniformRandomGenerator {

  // ______________________________________________________________________________________________
  private final Random rnd;
  private final int bits;

  // ______________________________________________________________________________________________
  private final BigInteger nextRand() {
    return new BigInteger(bits, rnd);
  }

  // ______________________________________________________________________________________________
  /**
   * initialized this random generator with the specified random seeder and the number of desider
   * bits to generate
   *
   * @param aBits int
   * @param r Random
   */
  public UniformRandomGenerator(int aBits, Random r) {
    bits = aBits;
    rnd = r;
  }

  // ______________________________________________________________________________________________
  /**
   * instanciate the random generator with the given seed
   *
   * @param aSeed long
   * @param aBits number of bits of the number-to-be-generateed
   */
  public UniformRandomGenerator(int aBits, long aSeed) {
    this(aBits, new Random(aSeed));
  }

  public BigInteger getMaxID() {
    BigInteger max = this.generate();
    for (int i = 0; i < bits; i++) {
      max = max.setBit(i);
    }
    return max;
  }

  public BigInteger getMinID() {
    BigInteger min = this.generate();
    for (int i = 0; i < bits; i++) {
      min = min.clearBit(i);
    }
    return min;
  }

  // ______________________________________________________________________________________________
  /**
   * Returns a unique 128-bit random number. The number is also put into an internal store to check
   * it will be never returned again
   *
   * @return BigInteger
   */
  public final BigInteger generate() {
    return nextRand();
  }
  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
