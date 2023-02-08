package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.Iterator;
import peersim.core.CommonState;

public class Block implements Iterator<Sample> {

  private Sample[][] blockSamples;

  private long blockId;

  private static long ID_GENERATOR = 0;

  private int row, column;

  private int SIZE;

  public Block() {

    SIZE = 512;
    this.blockId = (ID_GENERATOR++);
    blockSamples = new Sample[SIZE][SIZE];
    row = column = 0;
    for (int i = 0; i < blockSamples.length; i++) {

      for (int j = 0; j < blockSamples[0].length; j++) {

        blockSamples[i][j] = new Sample(blockId, i * j);
      }
    }
  }

  public Block(int size) {

    SIZE = size;
    this.blockId = (ID_GENERATOR++);
    blockSamples = new Sample[SIZE][SIZE];
    row = column = 0;
    for (int i = 0; i < blockSamples.length; i++) {

      for (int j = 0; j < blockSamples[0].length; j++) {

        blockSamples[i][j] = new Sample(blockId, i * j);
      }
    }
  }

  public long getBlockId() {
    return this.blockId;
  }

  public Sample[][] getSamples() {
    return this.blockSamples;
  }

  public BigInteger[] getNRandomSamplesIds(int n) {

    BigInteger[] samples = new BigInteger[n];
    for (int i = 0; i < samples.length; i++) {
      int r = CommonState.r.nextInt(SIZE);
      int c = CommonState.r.nextInt(SIZE);
      samples[i] = this.blockSamples[r][c].getId();
    }
    return samples;
  }

  public Sample getSample(int row, int column) {
    return this.blockSamples[row][column];
  }

  @Override
  public boolean hasNext() {

    // System.out.println("Column " + column + " row " + row);
    if (row < SIZE) return true;

    return false;
  }

  @Override
  public Sample next() {

    Sample s = blockSamples[row][column];

    column++;
    if (column == SIZE) {
      row++;
      column = 0;
    }

    if (column > SIZE - 1) return null;
    return s;
  }

  public void initIterator() {
    column = row = 0;
  }
}
