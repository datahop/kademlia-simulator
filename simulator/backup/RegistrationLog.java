package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RegistrationLog {

  private BigInteger registrant;
  private Map<BigInteger, Long> registered;
  private Map<BigInteger, Long> registeredWaiting;

  private long registrationInitTime;
  private Map<BigInteger, List<Long>> discovered;
  private Map<BigInteger, Long> minTimes;

  public static final long MAX_VALUE = 0x7fffffffffffffffL;

  public RegistrationLog(BigInteger node, long currTime) {
    this.registrant = node;
    registered = new HashMap<BigInteger, Long>();
    discovered = new HashMap<BigInteger, List<Long>>();
    registeredWaiting = new HashMap<BigInteger, Long>();
    minTimes = new HashMap<BigInteger, Long>();
    this.registrationInitTime = currTime;
  }

  public BigInteger getRegistrant() {
    return registrant;
  }

  // node is regisrar at which this registrant is registering at
  public void addRegistrar(BigInteger node, long currentTime, long waitingTime) {
    registered.put(node, currentTime);
    registeredWaiting.put(node, waitingTime);
  }

  // node is the one discovered this registrant
  public void addDiscovered(BigInteger node, long currentTime) {
    // System.out.println("Add discovered "+currentTime+" "+registered.get(node));
    // assert(registered.get(node) != null ) : "node is not registered, but discovered";
    if (registered.get(node) != null) {
      if (discovered.get(node) != null) {
        List<Long> times = discovered.get(node);
        times.add(currentTime - registered.get(node));
        discovered.put(node, times);
      } else {
        List<Long> times = new ArrayList<Long>();
        times.add(currentTime - registered.get(node));
        discovered.put(node, times);
      }
      if (minTimes.get(node) != null) {
        if (minTimes.get(node) > (currentTime - registered.get(node)))
          minTimes.put(node, currentTime - registered.get(node));
      } else {
        minTimes.put(node, currentTime - registered.get(node));
      }
    }
  }

  public long getMinRegisterTime() {
    long regTime = MAX_VALUE;
    if (registeredWaiting.size() > 0) {
      for (Long time : registeredWaiting.values())
        if (time.longValue() < regTime) regTime = time.longValue();
      return regTime;
    } else return 0;
  }

  /*public long getMinDiscoveryTime() {
  	long discTime=MAX_VALUE;
  	if(discovered.size()>0) {
  		for(Long time : discovered.values())
  			if (time.longValue()<discTime)discTime=time.longValue();
  		return discTime;
  	} else
  		return 0;
  }*/

  public long getAvgFirstDiscoveryTime() {

    if (minTimes.size() == 0) return 0;
    long times = 0;
    int count = 0;
    for (Long list : minTimes.values()) {
      times += list;
      count++;
    }
    return times / count;
  }

  public int getTimesDiscovered() {

    int count = 0;
    for (List<Long> times : discovered.values()) {
      count += times.size();
    }
    return count;
  }

  public double getAvgRegisterTime() {
    long regTime = 0;
    if (registeredWaiting.size() > 0) {
      for (Long time : registeredWaiting.values()) regTime += time.longValue();
      return (1.0 * regTime) / registeredWaiting.size();
    } else return 0;
  }

  /*public double getAvgDiscoveryTime() {
  	long discTime=0;
  	if(discovered.size()>0) {
  		for(Long time : discovered.values())
  			discTime+=time.longValue();
  		return (1.0*discTime)/discovered.size();
  	} else
  		return 0;
  }*/

  public Map<BigInteger, Long> getRegistered() {
    return registered;
  }

  public Map<BigInteger, List<Long>> getDiscovered() {
    return discovered;
  }
}
