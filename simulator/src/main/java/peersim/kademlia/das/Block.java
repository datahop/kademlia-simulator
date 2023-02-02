package peersim.kademlia.das;

import java.util.Iterator;

public class Block implements Iterator<Sample>  {

  private Sample[][] blockSamples;

  private long blockId;

  private static long ID_GENERATOR = 0;

  private int row,column;

  private static final int SIZE=512;

  public Block() {

    this.blockId = (ID_GENERATOR++);
    blockSamples = new Sample[SIZE][SIZE];
    row=column=0;
    for (int i = 0; i < blockSamples.length; i++) {

      for (int j = 0; j < blockSamples[0].length; j++) {

        blockSamples[i][j] = new Sample(blockId, i * j);
      }
    }
  }

  public long getBlockId(){
    return this.blockId;
  }

  public Sample[][] getSamples(){
    return this.blockSamples;
  }

  public Sample getSample(int row, int column){
    return this.blockSamples[row][column];
  }

  @Override
  public boolean hasNext() {
    
    if(row==SIZE-1&&column<SIZE-1)
      return false;
    
    return true;
  }

  @Override
  public Sample next() {

    if(column==SIZE-1&&row==SIZE-1)
      return null;

    column++;
    if(column==SIZE-1)
      row++;

    return blockSamples[row][column];
    
  }

}
