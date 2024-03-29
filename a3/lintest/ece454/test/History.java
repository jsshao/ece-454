package ece454.test;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 * @author Wojciech Golab, Shankha Subhra Chatterjee
 */
public class History {
    public static final String INITIAL_VALUE = "";

    private String key;
    private SortedMap<String, Cluster> clusterMap;
    private SortedSet<Cluster> clusterSet;

    public History(String key) {
        this.key = key;
        clusterMap = new TreeMap();
        clusterSet = new TreeSet();

	Cluster initCluster = new Cluster(key, INITIAL_VALUE);
	Operation initWrite = new Operation(key, INITIAL_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, "W");
	initCluster.addOperation(initWrite);
	clusterMap.put(INITIAL_VALUE, initCluster);
	clusterSet.add(initCluster);
    }

    public String getKey() {
        return key;
    }

    public void addOperation(Operation op) {
        String value = op.getValue();
	if (value == null && op.isRead())
	    return;

        Cluster clu = clusterMap.get(value);
        if (clu == null) {
            clu = new Cluster(key, value);
            clusterMap.put(value, clu);
        } else {
            // sort order of cluster may change
            clusterSet.remove(clu);
        }
        clu.addOperation(op);
        clusterSet.add(clu);
    }

    public void deleteOld(long timeThreshold) {
        deleteOldClusters(timeThreshold);
    }

    public void deleteOldClusters(long timeThreshold) {
        List<Cluster> deleteList = new ArrayList();
        for (Cluster c : clusterSet) {
            if (c.getRight() < timeThreshold) {
                deleteList.add(c);
                clusterMap.remove(c.getValue());
            } else {
                break;
            }
        }
        clusterSet.removeAll(deleteList);
    }

    public List<Long> logScores(ScoreFunction sfn, PrintWriter out) {
        List<Long> ret = new ArrayList();
        for (Cluster a : clusterSet) {
            long tempScore = a.getScore();
            SortedSet<Cluster> tail = clusterSet.tailSet(a);
            for (Cluster b : tail) {
                if (a == b) {
                    long newScore = Collections.max(sfn.getScores(a, b));
                    if (newScore > 0) {
                        tempScore = Math.max(tempScore, newScore);
                    }
                } else if (a.overlaps(b)) {
                    long newScore = Collections.max(sfn.getScores(a, b));
                    if (newScore > 0) {
                        tempScore = Math.max(tempScore, newScore);
                        if (b.getScore() < newScore) {
                            b.setScore(newScore);
                        }
                    }
                } else {
                    break;
                }
            }
            a.setScore(tempScore);            
            // Log scores          
            out.println("Key = " + a.getKey() + ", Value = " + a.getValue() + ", Score = " + tempScore);
            
            ret.add(tempScore);
        }
        return ret;
    }
}

