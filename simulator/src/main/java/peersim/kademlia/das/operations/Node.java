package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;



public class Node implements Comparable<Node> {
    
    private BigInteger id;
    private List<Sample> samples;
    private int aggressiveness;
    public Node(BigInteger id){
        this.id=id;
        samples = new ArrayList<>();
        aggressiveness=1;
    }

    public void addSample(BigInteger id){
        Sample s = new Sample(id);
        samples.add(s);
    }
    
    public BigInteger getId(){
        return id;
    }

    public List<Sample> getSamples(){
        return samples;
    }

    public int getScore(){
        int score = 0;
        for(Sample s : samples){
            if(!s.isDownloaded() && s.beingFetchedFrom.size()< aggressiveness){
                score+=1;
            }
        }
        return score;
    }

    public void incrementAgressiveness(){
        aggressiveness+=1;
    }

    @Override
    public int compareTo(Node n) {
        if(this.getScore()>n.getScore())
            return 1;
        else if(this.getScore()<n.getScore())
            return -1;
        else return 0;
    }
}
