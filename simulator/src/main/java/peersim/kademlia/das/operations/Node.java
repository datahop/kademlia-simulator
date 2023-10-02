package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class Node implements Comparable<Node> {

  private BigInteger id;
  private List<FetchingSample> samples;
  private int aggressiveness;
  private boolean beingAsked;

  public Node(BigInteger id) {
    this.id = id;
    samples = new ArrayList<>();
    aggressiveness = 0;
    beingAsked = false;
  }

  public void addSample(FetchingSample s) {
    samples.add(s);
  }

  public BigInteger getId() {
    return id;
  }

  public List<FetchingSample> getSamples() {
    return samples;
  }

  public void setBeingAsked(boolean value) {
    this.beingAsked = value;
  }

  public boolean isBeingAsked() {
    return beingAsked;
  }

  public int getScore() {
    int score = 0;

    for (FetchingSample s : samples) {

      if (!s.isDownloaded() && s.beingFetchedFrom.size() < aggressiveness) {
        score += 1;
      }
    }
    return score;
  }

  public void setAgressiveness(int agr) {
    aggressiveness = agr;
  }

  @Override
  public int compareTo(Node n) {
    if (this.getScore() < n.getScore()) return 1;
    else if (this.getScore() > n.getScore()) return -1;
    else return 0;
  }
}
