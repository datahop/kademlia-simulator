package peersim.kademlia.das;

public class Block {

  private Sample[][] blockSamples;

  private long blockId;

  private static long ID_GENERATOR = 0;

  public Block() {

    this.blockId = (ID_GENERATOR++);
    blockSamples = new Sample[512][512];

    for (int i = 0; i < blockSamples.length; i++) {

      for (int j = 0; j < blockSamples[0].length; j++) {

        blockSamples[i][j] = new Sample(blockId, i * j);
      }
    }
  }
}
