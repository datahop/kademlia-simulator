package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.HashMap;
import peersim.core.CommonState;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.UniformRandomGenerator;

public class Parcel {

  HashMap<BigInteger, Sample> samples;
  BigInteger parcelId;
  int size;

  public Parcel(int size) {
    samples = new HashMap<>();
    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    parcelId = urg.generate();
    this.size = size;
  }

  public Parcel(BigInteger id, int size) {
    samples = new HashMap<>();
    parcelId = id;
    this.size = size;
  }

  public void addSample(Sample s) {
    samples.put(s.getId(), s);
  }

  public Sample getSample(BigInteger id) {
    return samples.get(id);
  }

  public int getSize() {
    return size;
  }

  public BigInteger getId() {
    return parcelId;
  }
}
