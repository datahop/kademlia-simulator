package peersim.kademlia;

import java.math.BigInteger;

public class LookupInfo {

  private BigInteger address;
  private Topic topic;
  private boolean radiusLookup;

  public LookupInfo(BigInteger address, Topic topic, boolean radiusLookup) {
    this.address = address;
    this.topic = topic;
    this.radiusLookup = radiusLookup;
  }

  public BigInteger getAddress() {
    return address;
  }

  public Topic getTopic() {
    return topic;
  }

  public boolean getRadiusLookup() {
    return radiusLookup;
  }
}
