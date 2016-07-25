package ece454.test;

import java.io.IOException;
import java.util.List;

/**
 *
 * @author Shankha Subhra Chatterjee
 */
public class Main {
    public static void main(String[] args) {      
        
        if (args.length != 2) {
            System.out.println("Incorrect usage! 2 arguments expected, 1) fully qualified input file name 2) fully qualified output filename");
            return;
        }       
        LogParser t = new LogParser();       
        Analyzer a = new Analyzer(args[1]);        
        try
        {
            List<Operation> ops = t.parse(args[0]);
            ops.stream().forEach((o) -> {
                a.processOperation(o);
            });
            a.computeMetrics();
        }catch (IOException e)
        {
            System.out.println(e.getMessage());
        }
        System .out.println("Scores calculated successfully!");
    }
}

