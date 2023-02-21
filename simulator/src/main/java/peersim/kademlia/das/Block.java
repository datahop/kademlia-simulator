package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.Iterator;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.kademlia.KademliaCommonConfig;

public class Block implements Iterator<Sample> {

  /** The square matrix of samples */
  private Sample[][] blockSamples;

  /** Block identifier */
  private long blockId;

  private static long ID_GENERATOR = 0;

  /** Row and column numbers used by the iterator */
  private int row, column;

  /** block square matrix dimension (number of samples per row/column) */
  private int SIZE;

  /** Maximum key in the keyspace of BITS length */
  public static BigInteger MAX_KEY;

  /** allow to call the service initializer only once */
  private static boolean _ALREADY_INITIALISED = false;

  /** gap between two samples in the keyspace */
  public static BigInteger INTER_SAMPLE_GAP;

  /** number of samples in a block */
  private int numSamples;

  public Block() {

    SIZE = 512;
    this.numSamples = this.SIZE * this.SIZE;
    _init();

    this.blockId = (ID_GENERATOR++);
    blockSamples = new Sample[SIZE][SIZE];
    row = column = 0;
    for (int i = 0; i < blockSamples.length; i++) {

      for (int j = 0; j < blockSamples[0].length; j++) {

        blockSamples[i][j] = new Sample(blockId, i, j, this);
      }
    }
  }

  public Block(int size) {

    SIZE = size;
    this.numSamples = this.SIZE * this.SIZE;
    _init();

    this.blockId = (ID_GENERATOR++);
    blockSamples = new Sample[SIZE][SIZE];
    row = column = 0;
    for (int i = 0; i < blockSamples.length; i++) {

      for (int j = 0; j < blockSamples[0].length; j++) {

        blockSamples[i][j] = new Sample(blockId, i, j, this);
      }
    }
  }

  /** Compute the radius of the region containing the desired number of copies of each sample */
  public BigInteger computeRegionRadius(int numberOfCopiesPerSample) {

    double totalSamples = ((1.0 * this.numSamples) / Network.size()) * numberOfCopiesPerSample;
    if (totalSamples < 1.0) totalSamples = 1.0;
    totalSamples = totalSamples / 2.0;

    // System.out.println("INTER_SAMPLE_GAP: " + INTER_SAMPLE_GAP.toString(2));

    BigInteger reg_size = INTER_SAMPLE_GAP.multiply(BigInteger.valueOf(((long) totalSamples)));
    // System.out.println("Radius: " + reg_size.toString(2));

    return reg_size;
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

  public int getSize() {
    return this.SIZE;
  }

  public int getNumSamples() {
    return this.numSamples;
  }

  private void _init() {

    // execute once
    if (_ALREADY_INITIALISED) return;

    MAX_KEY = BigInteger.ZERO.setBit(KademliaCommonConfig.BITS).subtract(BigInteger.ONE);

    try {
      INTER_SAMPLE_GAP = MAX_KEY.divide(BigInteger.valueOf(this.numSamples));
    } catch (ArithmeticException e) {
      e.printStackTrace();
    }

    _ALREADY_INITIALISED = true;
  }
}
