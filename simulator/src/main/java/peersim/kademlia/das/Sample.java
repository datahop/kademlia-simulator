package peersim.kademlia.das;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Sample {

  private int row, column;
  private long blockId;
  private BigInteger id;

  public Sample(long blockId, int row, int column) {

    try {

      MessageDigest digest = MessageDigest.getInstance("SHA-256");

      String idName =
          String.valueOf(blockId) + "_" + String.valueOf(row) + "x" + String.valueOf(column);
      byte[] hash = digest.digest(idName.getBytes(StandardCharsets.UTF_8));
      this.id = new BigInteger(1, hash);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  public int getRow() {
    return this.row;
  }

  public int getColumn() {
    return this.column;
  }

  public long getBlockId() {
    return blockId;
  }

  public BigInteger getId() {
    return id;
  }
}
