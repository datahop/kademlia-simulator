package peersim.kademlia;

import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
// import inet.ipaddr.ipv4.IPv4AddressTrie;

public class IpModifier {

  protected static final int baseMultiplier = 30;
  TrieNode root;

  public IpModifier() {
    root = new TrieNode();
  }
  // TODO instead of toBinaryString() conversion, check bits of addresses with & operator
  // this conversion will likely be slow
  public double getModifier(String address) {

    IPAddressString addr = new IPAddressString(address);
    IPv4Address ipv4Addr = addr.getAddress().toIPv4();
    String binAddr = ipv4Addr.toBinaryString();

    return TrieNode.getSimilarityScore(root, binAddr);
  }

  // adds the address and returns similarity score
  public void newAddress(String address) {
    IPAddressString addr = new IPAddressString(address);
    IPv4Address ipv4Addr = addr.getAddress().toIPv4();
    String binAddr = ipv4Addr.toBinaryString();

    TrieNode.addIp(root, binAddr);
  }

  public void removeAddress(String address) {
    IPAddressString addr = new IPAddressString(address);
    IPv4Address ipv4Addr = addr.getAddress().toIPv4();
    String binAddr = ipv4Addr.toBinaryString();

    TrieNode.removeIp(root, binAddr);
  }

  public TrieNode getLowerBound(String address) {

    IPAddressString addr = new IPAddressString(address);
    IPv4Address ipv4Addr = addr.getAddress().toIPv4();
    String binAddr = ipv4Addr.toBinaryString();

    TrieNode n = TrieNode.getLowerBoundNode(root, binAddr);

    return n;
  }
}
