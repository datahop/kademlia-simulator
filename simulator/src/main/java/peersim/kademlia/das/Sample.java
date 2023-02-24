package peersim.kademlia.das;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import peersim.kademlia.KademliaCommonConfig;

public class Sample {

  /** Row and column numbers of a sample within a block */
  private int row, column;
  /** The unique ID of the block that this sample belongs to */
  private long blockId;
  /** The key of a sample in the DHT keyspace (after mapping) */
  private BigInteger id;
  /** Block that this sample is part of */
  private Block block;

  /** Initialise a sample instance and map it to the keyspace */
  public Sample(long blockId, int row, int column, Block b) {

    this.id = null;
    this.block = b;
    this.row = row;
    this.column = column;
    this.blockId = blockId;
    computeID();
  }

  /**
   * Sample numbering to map each sample to an integer in the range 1 to SIZE*SIZE Samples are
   * ordered by row
   */
  public int sampleNumberByRow() {
    return this.row * this.block.getSize() + this.column;
  }

  /**
   * Sample numbering to map each sample to an integer in the range 1 to SIZE*SIZE Samples are
   * ordered by column
   */
  public int sampleNumberByColumn() {
    return this.column * this.block.getSize() + this.row;
  }

  /** Map this sample to the DHT keyspace */
  public void computeID() {
    if (KademliaCommonConfig.MAPPING_FN == KademliaCommonConfig.SAMPLE_MAPPING_RANDOM) {
      try {
        String idName =
            String.valueOf(blockId) + "_" + String.valueOf(row) + "x" + String.valueOf(column);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(idName.getBytes(StandardCharsets.UTF_8));
        this.id = new BigInteger(1, hash);
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
    } else if (KademliaCommonConfig.MAPPING_FN
        == KademliaCommonConfig.SAMPLE_MAPPING_REGION_BASED) {
      this.id = Block.INTER_SAMPLE_GAP.multiply(BigInteger.valueOf(this.sampleNumberByRow()));
    } else {
      System.out.println("Error: invalid selection for sample mapping function");
      System.exit(1);
    }
  }

  /** Given the peerID of a node, determine if this sample falls within the region of the node. */
  public boolean isInRegion(BigInteger peerID, BigInteger radius) {
    /** (peerID - radius) < this.id < (peerID + radius) */
    if ((this.id.compareTo(peerID.subtract(radius)) == 1)
        && (this.id.compareTo(peerID.add(radius)) == -1)) {
      return true;
    } else {
      return false;
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
