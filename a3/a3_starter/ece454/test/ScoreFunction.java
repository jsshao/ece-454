package ece454.test;

import java.util.List;

/**
 *
 * @author Wojciech Golab
 */
public interface ScoreFunction {

    public List<Long> getScores(Cluster a, Cluster b);
}

