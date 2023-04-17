package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.kademlia.KademliaCommonConfig;

public class Block implements Iterator<Sample>, Cloneable {

  /** The square matrix of samples */
  private Sample[][] blockSamples;

  /** Block identifier */
  private long blockId;

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

  // private TreeSet<BigInteger> samples;
  private TreeSet<BigInteger> samplesByRow;
  private TreeSet<BigInteger> samplesByColumn;

  public Block(long id) {

    SIZE = 512;
    this.numSamples = this.SIZE * this.SIZE;
    _init();

    samplesByRow = new TreeSet<>();
    samplesByColumn = new TreeSet<>();

    this.blockId = id;
    blockSamples = new Sample[SIZE][SIZE];
    row = column = 0;

    for (int i = 0; i < blockSamples.length; i++) {
      for (int j = 0; j < blockSamples[0].length; j++) {
        blockSamples[i][j] = new Sample(blockId, i + 1, j + 1, this);
        samplesByRow.add(blockSamples[i][j].getIdByRow());
        samplesByColumn.add(blockSamples[i][j].getIdByColumn());
      }
    }
  }

  public Block(int size, long id) {

    SIZE = size;
    this.numSamples = this.SIZE * this.SIZE;
    _init();
    samplesByRow = new TreeSet<>();
    samplesByColumn = new TreeSet<>();

    this.blockId = id;
    blockSamples = new Sample[SIZE][SIZE];
    row = column = 0;

    for (int i = 0; i < blockSamples.length; i++) {
      for (int j = 0; j < blockSamples[0].length; j++) {
        blockSamples[i][j] = new Sample(blockId, i + 1, j + 1, this);
        samplesByRow.add(blockSamples[i][j].getIdByRow());
        samplesByColumn.add(blockSamples[i][j].getIdByColumn());
      }
    }
  }

  public Block(Sample[][] blockSamples, int size, long id) {
    SIZE = size;
    this.numSamples = this.SIZE * this.SIZE;
    _init();
    this.blockSamples = blockSamples;
    row = column = 0;
    this.blockId = id;
    // samples = new TreeSet<>();
    samplesByRow = new TreeSet<>();
    samplesByColumn = new TreeSet<>();

    for (int i = 0; i < blockSamples.length; i++) {
      for (int j = 0; j < blockSamples[0].length; j++) {
        blockSamples[i][j] = new Sample(blockId, i, j, this);
        samplesByRow.add(blockSamples[i][j].getIdByRow());
        samplesByColumn.add(blockSamples[i][j].getIdByColumn());
      }
    }
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    initIterator();
    Block dolly = new Block(this.blockSamples, this.SIZE, this.blockId);
    return dolly;
  }

  /** Compute the radius of the region containing the desired number of copies of each sample */
  public BigInteger computeRegionRadius(int numberOfCopiesPerSample) {

    /**
     * Calculate the radius by dividing Id space by number of nodes in the network, and multiplying
     * by number of copies per sample The result is divided by 2 to calculate the radius (instead of
     * diameter)
     */
    BigInteger radius =
        MAX_KEY
            .divide(BigInteger.valueOf(Network.size()))
            .multiply(BigInteger.valueOf(numberOfCopiesPerSample));
    radius = radius.shiftRight(1);
    return radius;
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

  public Sample[] getNRandomSamples(int n) {

    Sample[] samples = new Sample[n];
    for (int i = 0; i < samples.length; i++) {
      int r = CommonState.r.nextInt(SIZE);
      int c = CommonState.r.nextInt(SIZE);
      samples[i] = this.blockSamples[r][c];
    }
    return samples;
  }

  public Sample getSample(int row, int column) {
    return this.blockSamples[row][column];
  }

  @Override
  public boolean hasNext() {

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

  public BigInteger[] getSamplesByRadius(BigInteger peerId, BigInteger radius) {
    BigInteger top = peerId.add(radius);
    BigInteger bottom = peerId.subtract(radius);

    Collection subSet = samplesByRow.subSet(bottom, true, top, true);
    return (BigInteger[]) subSet.toArray(new BigInteger[0]);
  }

  public BigInteger[] getSamplesByRadiusByRow(BigInteger peerId, BigInteger radius) {
    BigInteger top = peerId.add(radius);
    BigInteger bottom = peerId.subtract(radius);

    Collection subSet = samplesByRow.subSet(bottom, true, top, true);
    return (BigInteger[]) subSet.toArray(new BigInteger[0]);
  }

  public BigInteger[] getSamplesByRadiusByColumn(BigInteger peerId, BigInteger radius) {
    BigInteger top = peerId.add(radius);
    BigInteger bottom = peerId.subtract(radius);

    Collection subSet = samplesByColumn.subSet(bottom, true, top, true);
    return (BigInteger[]) subSet.toArray(new BigInteger[0]);
  }

  public BigInteger[] getSamplesIdsByRow(int row) {
    BigInteger[] samples = new BigInteger[this.SIZE];
    for (int i = 0; i < samples.length; i++) {
      samples[i] = this.blockSamples[row - 1][i].getIdByRow();
    }
    return samples;
  }

  public BigInteger[] getSamplesIdsByColumn(int column) {
    BigInteger[] samples = new BigInteger[this.SIZE];
    for (int i = 0; i < samples.length; i++) {
      samples[i] = this.blockSamples[i][column - 1].getIdByColumn();
    }
    return samples;
  }

  private void _init() {

    // execute once
    if (_ALREADY_INITIALISED) return;
    MAX_KEY = BigInteger.ONE.shiftLeft(KademliaCommonConfig.BITS).subtract(BigInteger.ONE);

    try {
      INTER_SAMPLE_GAP = MAX_KEY.divide(BigInteger.valueOf(this.numSamples));
    } catch (ArithmeticException e) {
      e.printStackTrace();
    }

    _ALREADY_INITIALISED = true;
  }
}
