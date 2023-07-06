package peersim.kademlia.das;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import peersim.config.Configuration;
import peersim.config.ParsedProperties;
import peersim.core.CommonState;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.UniformRandomGenerator;

public class SampleTest {

  protected static void setUpBeforeClass() {
    String[] array = new String[] {"config/dasprotocol.cfg"};
    Configuration.setConfig(new ParsedProperties(array));
    CommonState.setEndTime(Long.parseLong("100"));
    CommonState.setTime(Long.parseLong("0"));
  }

  @Test
  public void nodeAssigment() {

    int n = 100;
    int redundancy = 2;
    int mSize = 100;
    int idSize = 256;
    KademliaCommonConfig.BITS = idSize;
    // BigInteger[] nodes = new BigInteger[n];
    UniformRandomGenerator urg = new UniformRandomGenerator(idSize, 1);
    HashMap<BigInteger, Integer> nodeMap = new HashMap<>();
    BigInteger rad = urg.getMaxID().divide(BigInteger.valueOf(n));
    // System.out.println("Max id:" + urg.getMaxID() + " " + rad);
    for (int i = 0; i < n; i++) {
      nodeMap.put(rad.multiply(BigInteger.valueOf(i)), 0);
      // nodeMap.put(urg.generate(), 0);
    }

    Block b = new Block(mSize, 0);
    int samplesWithinRegion = 0; // samples that are within at least one node's region
    int totalSamples = 0;

    BigInteger MAX_KEY =
        BigInteger.ONE.shiftLeft(KademliaCommonConfig.BITS).subtract(BigInteger.ONE);

    BigInteger radius =
        MAX_KEY.divide(BigInteger.valueOf(n)).multiply(BigInteger.valueOf(redundancy));

    while (b.hasNext()) {
      Sample s = b.next();
      boolean assigned = false;
      for (BigInteger id : nodeMap.keySet()) {
        Integer i = nodeMap.get(id);

        if (s.isInRegionByRow(id, radius)) {

          i++;
          totalSamples++;
          assigned = true;
          nodeMap.put(id, i);
        }
      }
      if (assigned) samplesWithinRegion++;
      else System.out.println("Not assigned !");
      totalSamples++;
    }

    assertEquals(samplesWithinRegion, mSize * mSize);
    System.out.println("Samples assgined:" + totalSamples + " " + samplesWithinRegion);

    // assertEquals(1, 2);
  }
}
