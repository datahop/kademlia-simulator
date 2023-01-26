package peersim.kademlia.das;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Sample {

  private int sampleId;
  private long blockId;
  private BigInteger id;
  private int size;

  public Sample(long blockId, int sampleId) {

    try {

      size = 512;
      MessageDigest digest = MessageDigest.getInstance("SHA-256");

      String idName = String.valueOf(blockId) + "_" + String.valueOf(sampleId);
      byte[] hash = digest.digest(idName.getBytes(StandardCharsets.UTF_8));
      this.id = new BigInteger(1, hash);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  public int getSampleId() {
    return sampleId;
  }

  public long getBlockId() {
    return blockId;
  }

  public int getSize() {
    return size;
  }

  public BigInteger getId() {
    return id;
  }
}
