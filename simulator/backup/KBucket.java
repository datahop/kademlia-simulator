package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import peersim.core.CommonState;
import peersim.core.Node;

/**
 * This class implements a kademlia k-bucket. Function for the management of the neighbours update
 * are also implemented
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KBucket implements Cloneable {

  // k-bucket array
  // protected TreeMap<BigInteger, Long> neighbours = null;
  protected List<BigInteger> neighbours;

  // protected HashSet<String> addresses;
  // protected HashMap<BigInteger,String> addrMap;
  // replacementList
  // protected TreeMap<BigInteger, Long> replacements = null;
  protected List<BigInteger> replacements;

  protected RoutingTable rTable;

  protected int k, maxReplacements;
  protected HashMap<Integer, Integer> ipValues;
  //	int sameIps;

  // protected KademliaProtocol prot;
  // empty costructor
  public KBucket(RoutingTable rTable, int k, int maxReplacements) {
    // neighbours = new TreeMap<BigInteger, Long>();
    // System.out.println("New bucket "+prot.kademliaid);
    // this.prot = prot;
    this.k = k;
    this.maxReplacements = maxReplacements;
    this.rTable = rTable;
    neighbours = new ArrayList<BigInteger>();
    replacements = new ArrayList<BigInteger>();
    //		sameIps = 0;
    ipValues = new HashMap<Integer, Integer>();
    // addresses = new HashSet<String>();
    // addrMap = new HashMap<>();
  }

  public int occupancy() {
    return neighbours.size();
  }
  // add a neighbour to this k-bucket

  public boolean addNeighbour(BigInteger node) {
    // long time = CommonState.getTime();
    // KademliaNode kad= Util.nodeIdtoNode(node).getKademliaProtocol().getNode();

    // InetAddress ip = new InetAddress(kad.getAddr());
    // System.out.println(kad+" "+kad.getAddr());

    // Random random = new Random();

    // String ipString = InetAddresses.fromInteger(random.nextInt()).getHostAddress();

    String ipString = Util.nodeIdtoNode(node).getKademliaProtocol().getNode().getAddr();

    int ipValue = parseIp(ipString);

    if (ipValues.get(ipValue) == null) ipValues.put(ipValue, 1);
    else if (ipValues.get(ipValue) == KademliaCommonConfig.MAX_ADDRESSES_BUCKET) return false;
    else ipValues.put(ipValue, ipValues.get(ipValue) + 1);

    if (neighbours.contains(node)) return false;
    // System.out.println("IP string "+ipString+" "+ipValue+" "+sameIps+"
    // "+KademliaCommonConfig.MAX_ADDRESSES_BUCKET);
    /*for(BigInteger n : neighbours) {
    	if(n.compareTo(node)==0) {
    		return false;
    	} else {
    		String otherNodeIpString = Util.nodeIdtoNode(n).getKademliaProtocol().getNode().getAddr();
    		int otherIpValue = parseIp(otherNodeIpString);
    		if(ipValue == otherIpValue) {
    			//System.out.println("Same domain "+ipString+" "+ipValue+" "+otherNodeIpString+" "+otherIpValue);
    			if(sameIps == KademliaCommonConfig.MAX_ADDRESSES_BUCKET)
    				return false;
    			else
    				sameIps++;
    		}
    	}
    }*/

    if (neighbours.size() < k) { // k-bucket isn't full
      // neighbours.put(node, time); // add neighbour to the tail of the list
      neighbours.add(node);
      // addresses.add(ipString);
      // addrMap.put(node, ipString);
      removeReplacement(node);
      return true;
    } else {
      addReplacement(node);
      return false;
    }
  }

  // add a replacement node in the list
  public void addReplacement(BigInteger node) {
    // long time = CommonState.getTime();

    /*for(BigInteger r : replacements) {
    	if(r.compareTo(node)==0)return;
    }*/
    if (replacements.contains(node)) return;
    replacements.add(0, node);

    if (replacements.size() > maxReplacements) { // k-bucket isn't full
      // replacements.put(node, time); // add neighbour to the tail of the list
      replacements.remove(replacements.size() - 1);
    }
  }
  // remove a neighbour from this k-bucket
  public void removeNeighbour(BigInteger node) {
    neighbours.remove(node);

    String ipString = Util.nodeIdtoNode(node).getKademliaProtocol().getNode().getAddr();
    int ipValue = parseIp(ipString);

    /*for(BigInteger n : neighbours) {
    	String otherNodeIpString = Util.nodeIdtoNode(n).getKademliaProtocol().getNode().getAddr();
    	int otherIpValue = parseIp(ipString);
    	if(ipValue==otherIpValue) {
    		if(sameIps>0)sameIps--;
    		break;
    	}

    }*/
    if (ipValues.get(ipValue) != null) {
      if (ipValues.get(ipValue) == 1) ipValues.remove(ipValue);
      else ipValues.put(ipValue, ipValues.get(ipValue) - 1);
    }
    //	addresses.remove(addrMap.get(node));
    //	addrMap.remove(node);
  }

  // remove a replacement node in the list
  public void removeReplacement(BigInteger node) {
    replacements.remove(node);
  }

  public Object clone() {
    KBucket dolly = new KBucket(rTable, k, maxReplacements);
    for (BigInteger node : neighbours) {
      // System.out.println("clone kbucket");
      dolly.neighbours.add(new BigInteger(node.toByteArray()));
    }
    return dolly;
  }

  public String toString() {
    String res = "{\n";

    for (BigInteger node : neighbours) {
      res += node + "\n";
    }

    return res + "}";
  }

  public void replace() {
    if ((replacements.size() > 0) && (neighbours.size() < k)) {
      // Random rand = new Random();
      // BigInteger n = replacements.get(rand.nextInt(replacements.size()));
      BigInteger n = replacements.get(CommonState.r.nextInt(replacements.size()));
      neighbours.add(n);
      replacements.remove(n);
    }
  }

  public void checkAndReplaceLast() {
    if (neighbours.size() == 0 || CommonState.getTime() == 0)
      // Entry has moved, don't replace it.
      return;

    // System.out.println("Replace node "+neighbours.get(neighbours.size()-1)+" at
    // "+CommonState.getTime());

    Node node = Util.nodeIdtoNode(neighbours.get(neighbours.size() - 1));
    // System.out.println("Replace node "+neighbours.get(neighbours.size()-1)+" at
    // "+CommonState.getTime());
    KademliaProtocol remote = node.getKademliaProtocol();

    if (remote.routingTable != null) remote.routingTable.sendToFront(rTable.nodeId);

    // System.out.println("checkAndReplaceLast "+remote.getNode().getId()+" at
    // "+CommonState.getTime()+" at "+rTable.nodeId);

    if (node.getFailState() != Node.OK) {
      // Still the last entry.
      neighbours.remove(neighbours.size() - 1);
      if (replacements.size() > 0) {
        // Random rand = new Random();
        // BigInteger n = replacements.get(rand.nextInt(replacements.size()));
        BigInteger n = replacements.get(CommonState.r.nextInt(replacements.size()));
        neighbours.add(n);
        replacements.remove(n);
      }
    }
  }

  public List<BigInteger> getNeighbours() {
    return neighbours;
  }

  public int getAddresses(String ip) {
    int ipValue = parseIp(ip);

    if (ipValues.get(ipValue) != null) return ipValues.get(ipValue).intValue();
    else return 0;
  }

  private static int parseIp(String address) {
    int result = 0;

    // iterate over each octet
    String[] parts = address.split(Pattern.quote("."));
    for (int i = 0; i < 3; i++) {
      // shift the previously parsed bits over by 1 byte
      result = result << 8;
      // set the low order bits to the current octet
      result |= Integer.parseInt(parts[i]);
    }
    return result;
  }
}
