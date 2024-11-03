package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.*;

import peersim.core.CommonState;
import peersim.core.Network;

public class SearchTable extends SearchTableV1 {

  private HashMap<BigInteger, List<BigInteger>> validatorsSamples;
  private HashMap<BigInteger, Integer> validatorsRow;
  private HashMap<BigInteger, Integer> validatorsColumn;
  private HashMap<Integer, List<BigInteger>> rowsValidator;
  private HashMap<Integer, List<BigInteger>> columnsValidator;

  public SearchTable() {
    validatorsSamples = new HashMap<>();
    validatorsRow = new HashMap<>();
    validatorsColumn = new HashMap<>();
    rowsValidator = new HashMap<>();
    columnsValidator = new HashMap<>();
  }

  public void assignSamples(Block b, int r) {
    /*  BigInteger radiusValidator = b.computeRegionRadius(r, this.getValidatorsIndexed().size());

    while (b.hasNext()) {
       BigInteger radiusUsed = radiusValidator;
       boolean inRegion = false;
       Sample s = b.next();
       while (!inRegion) {

         List<BigInteger> idsValidators = this.getValidatorNodesbySample(s.getIdByRow(), radiusUsed);
         if (idsValidators.size() > 0) {
           inRegion = true;
           validatorsSamples.put(s.getIdByRow(), idsValidators);
           rowsValidator.put(s.getRow(), idsValidators);
           for (BigInteger id : idsValidators) {
             validatorsRow.put(id, s.getRow());
           }
         }
         idsValidators = this.getValidatorNodesbySample(s.getIdByColumn(), radiusUsed);
         if (idsValidators.size() > 0) {
           inRegion = true;
           validatorsSamples.put(s.getIdByColumn(), idsValidators);
           columnsValidator.put(s.getRow(), idsValidators);
           for (BigInteger id : idsValidators) {
             validatorsColumn.put(id, s.getColumn());
           }
         }
         if (!inRegion) radiusUsed = radiusUsed.multiply(BigInteger.valueOf(2));
       }
     }*/
    int nodesPerRow =
        Network.size() / (b.getSize() * KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);

    int row = 1;
    int counter = 0;
    for (int i = 0; i < Network.size(); i++) {
      counter++;
      BigInteger id = Network.get(i).getDASProtocol().getKademliaId();
      if (rowsValidator.get(row) != null) {
        rowsValidator.get(row).add(id);
      } else {
        List<BigInteger> list = new ArrayList<>();
        list.add(id);
        rowsValidator.put(row, list);
      }
      validatorsRow.put(id, row);
      if (counter == nodesPerRow) {
        row++;
        counter = 0;
      }
      if (row > b.getSize()) break;
    }

    int column = 1;
    counter = 0;
    for (int i = 0; i < Network.size(); i++) {
      counter++;
      BigInteger id = Network.get(i).getDASProtocol().getKademliaId();
      if (columnsValidator.get(column) != null) {
        columnsValidator.get(column).add(id);
      } else {
        List<BigInteger> list = new ArrayList<>();
        list.add(id);
        columnsValidator.put(column, list);
      }
      validatorsColumn.put(id, column);

      if (counter == nodesPerRow) {
        column++;
        counter = 0;
      }
      if (column > b.getSize()) break;
    }

    for (int i : rowsValidator.keySet()) {
      System.out.println("Row " + i + " nodes " + rowsValidator.get(i).size());
    }

    for (int i : columnsValidator.keySet()) {
      System.out.println("Column " + i + " nodes " + columnsValidator.get(i).size());
    }
    
    for (int r=1;r<=b.getSize();r++) {
      Sample s = b.getSample(row, column)
      List<BigInteger> vals = rowsValidator.get(s.getRow());
      validatorsSamples.put(s.getId(),vals.get(CommonState.r.nextInt(vals.size())) )
    }
  }

  public List<BigInteger> getNodesBySample(BigInteger sampleId) {
    return validatorsSamples.get(sampleId);
  }

  public int getValidatorRow(BigInteger id) {
    return validatorsRow.get(id);
  }

  public int getValidatorColumn(BigInteger id) {
    return validatorsColumn.get(id);
  }
}
