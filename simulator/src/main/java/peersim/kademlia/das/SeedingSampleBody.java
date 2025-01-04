package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.List;

public class SeedingSampleBody {
  public List<BigInteger>
      validatorList; // Assuming sample is an array of integers, you can change the type as needed
  public Sample[] samplesList;
  public boolean isRow; // true if it's a row

  public SeedingSampleBody(Sample[] samples, List<BigInteger> validators, boolean isRow) {
    this.samplesList = samples;
    this.validatorList = validators;
    this.isRow = isRow;
  }

  public List<BigInteger> getValidators() {
    return this.validatorList;
  }

  public Sample[] getsamplesList() {
    return this.samplesList;
  }

  public boolean getIsRow() {
    return this.isRow;
  }
}
