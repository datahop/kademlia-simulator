package peersim.kademlia;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Topic implements Comparable<Topic> {
  // ID of the node storing the topic - this is used to enable sorted collections,
  // priority queues etc.
  // FIXME: do we need this hostID attribute in Topic - the TopicRegistration has that
  protected BigInteger hostID;
  protected BigInteger topicID;
  protected String topic;
  /*
   * Create a new topic. The topicID is created using SHA-256 hash of the string.
   * All the node IDs are positive, so to make it uniform, we create only positive
   * topic IDs as well.
   */

  public Topic() {
    this.hostID = new BigInteger("0");
    this.topic = "";
    this.topicID = new BigInteger("0");
  }

  private void setTopicID() {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(topic.getBytes(StandardCharsets.UTF_8));
      this.topicID = new BigInteger(1, hash);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  public Topic(String topic) {
    this.hostID = new BigInteger("0");
    this.topic = topic;
    setTopicID();
  }

  public Topic(BigInteger hostID, String topic) {
    this.hostID = hostID;
    this.topic = topic;
    setTopicID();
  }

  public Topic(Topic t) {
    this.hostID = t.hostID;
    this.topicID = t.topicID;
    this.topic = t.topic;
  }

  public int compareTo(Topic t) {
    assert this.hostID.equals(t.hostID) : "Comparing topics with different hostID";

    // System.out.println("Topic 1 "+this.hostID+" "+this.topicID+" "+Util.logDistance(this.hostID,
    // this.topicID));

    // System.out.println("Topic 2 "+t.hostID+" "+t.topicID+" "+Util.logDistance(t.hostID,
    // t.topicID));

    /*if(Util.logDistance(this.hostID, this.topicID) < Util.logDistance(t.hostID, t.topicID))
    	return -1;
    else if(Util.logDistance(this.hostID, this.topicID)==Util.logDistance(t.hostID, t.topicID))
    	return 0;

    else
    	return -1;*/
    return this.hostID.xor(this.topicID).compareTo(t.hostID.xor(t.topicID));
  }

  public String toString() {
    return "[hostID=" + this.hostID + "][topicID=" + this.topicID + "][topic=" + this.topic + "]";
  }

  public void setHostID(BigInteger hostID) {
    this.hostID = hostID;
  }

  public BigInteger getHostID() {
    return this.hostID;
  }

  public String getTopic() {
    return this.topic;
  }

  public BigInteger getTopicID() {
    return this.topicID;
  }

  @Override
  public int hashCode() {
    return this.topic.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    // If the object is compared with itself then return true
    if (o == this) {
      return true;
    }

    /* Check if o is an instance of Topic or not
    "null instanceof [type]" also returns false */
    if (!(o instanceof Topic)) {
      return false;
    }

    // typecast o to Complex so that we can compare data members
    Topic r = (Topic) o;

    // if ((this.hostID == r.hostID) && (this.getTopic() == r.getTopic()))
    if (this.hostID == null) {
      if (this.getTopic().equals(r.getTopic())) return true;
    } else {
      if ((this.hostID.compareTo(r.hostID) == 0) && (this.getTopic().equals(r.getTopic())))
        return true;
    }

    return false;
  }
}
