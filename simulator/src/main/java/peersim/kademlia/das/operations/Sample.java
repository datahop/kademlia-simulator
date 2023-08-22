package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class Sample {

    public BigInteger id;
    public boolean downloaded;
    public List<Node> beingFetchedFrom;
    public List<Node> askedNodes;

    public Sample(BigInteger id){
        this.id = id;
        downloaded=false;
        beingFetchedFrom= new ArrayList<>();
        askedNodes = new ArrayList<>();
    }

    public boolean isDownloaded(){
        return downloaded;
    }

    public void addFetchingNode(Node n){
        beingFetchedFrom.add(n);
    }

    public void removeFetchingNode(Node n){
        beingFetchedFrom.remove(n);
    }
    public List<Node> beingFetchedFrom(){
        return beingFetchedFrom;
    }

    public void addAskedNode(Node n){
        askedNodes.add(n);
    }

    public List<Node> askNodes(){
        return askedNodes;
    }
    
}
