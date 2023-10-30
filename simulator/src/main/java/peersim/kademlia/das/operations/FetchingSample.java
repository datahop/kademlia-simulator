package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import peersim.kademlia.das.Sample;

public class FetchingSample {

  public Sample s;
  public boolean downloaded;
  public List<Node> beingFetchedFrom;
  public List<Node> askedNodes;
  public boolean beingFetched;

  public FetchingSample(Sample s) {
    this.s = s;
    downloaded = false;
    beingFetchedFrom = new ArrayList<>();
    askedNodes = new ArrayList<>();
    beingFetched = false;
  }

  public BigInteger getId() {
    return s.getId();
  }

  public Sample getSample(){
    return s;
  }

  public BigInteger getIdByColumn() {
    return s.getIdByColumn();
  }

  public boolean isDownloaded() {
    return downloaded;
  }

  public void setDownloaded() {
    this.downloaded = true;
  }

  public void addFetchingNode(Node n) {
    beingFetchedFrom.add(n);
    addAskedNode(n);
    if (beingFetchedFrom.size() > 0) beingFetched = true;
  }

  public void removeFetchingNode(Node n) {
    beingFetchedFrom.remove(n);
    if (beingFetchedFrom.size() == 0) beingFetched = false;
  }

  public List<Node> beingFetchedFrom() {
    return beingFetchedFrom;
  }

  public void addAskedNode(Node n) {
    askedNodes.add(n);
  }

  public List<Node> askNodes() {
    return askedNodes;
  }
}
