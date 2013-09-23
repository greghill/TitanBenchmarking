import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.*;
import java.util.ArrayList;
import java.util.ArrayDeque;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;


public class TitanBench {

    public static long total_reqs = 0;
    public static long total_time = 0;
    public static long total_2reqs = 0;
    public static long total_2time = 0;
    public static long[] reqtimes = new long[200];
    public static final String ID = "vertex_id";
    public static final String VISIT = "visit";
    public static TitanGraph graph;

    public static void start() {
        BaseConfiguration config = new BaseConfiguration();
        config.setProperty("storage.backend", "cassandrathrift");
        config.setProperty("storage.hostname", "127.0.0.1");//"128.84.227.111");
        graph = TitanFactory.open(config);
    }

    public static Vertex getVertex(Integer id) {
    	for (Vertex v: graph.getVertices(ID, id)) {
            return v;
        }
        System.err.println("Vertex " + id + " not found");
        System.exit(-1);
        return graph.addVertex(null);
    }

    public static Integer getId(TitanVertex v) {
        for (TitanProperty p: v.getProperties()) {
            return p.getValue(Integer.class);
        }
        return null;
    }

    public static boolean req_single(Integer n1, Integer n2, Integer req) {
        return req_single(n1, n2, req, -1);
    }

    public static boolean req_single(Integer n1, Integer n2, Integer req, final int max_hops) {
        //System.out.println("reachability " + n1 + " to " + n2);
        int hops = 0;
        if (max_hops >= 0)
            total_2reqs++;
        else
            total_reqs++;

        long start = System.nanoTime();
        Object targetId = getVertex(n2).getId();
        ArrayDeque<Vertex> exploringDepth = new ArrayDeque<Vertex>();
        ArrayDeque<Vertex> nextDepth = new ArrayDeque<Vertex>();
        exploringDepth.add(getVertex(n1));
        while (!exploringDepth.isEmpty() && hops != max_hops){
            while (!exploringDepth.isEmpty()) {
                Vertex v = exploringDepth.remove();
                for (Vertex nbr: v.getVertices(Direction.OUT, "nbr")) {
                    if (nbr.getId().equals(targetId)) { // found target
                        reqtimes[req] = System.nanoTime()-start;
                        /*
                        if (max_hops >= 0)
                            total_2time += net;
                        else
                            total_time += net;
                            */
                        hops++;
                        //System.out.println("found in " +hops+" hops");
                        return true;
                    }
                    else if (!req.equals((Integer) nbr.getProperty(VISIT))) {
                        nbr.setProperty(VISIT, req);
                        nextDepth.add(nbr);
                    }
                }
            }
            // finished this depth, swap to next one
            ArrayDeque<Vertex> temp = exploringDepth;
            exploringDepth = nextDepth;
            nextDepth = temp;
            hops++;
        }
        //System.out.println("not found in " +hops+" hops");
        reqtimes[req] = System.nanoTime()-start;
        /*
        long net = System.nanoTime()-start;
        if (max_hops >= 0)
            total_2time += net;
        else
            total_time += net;
            */
        return false;
    }

    public static void requests() {
        BufferedReader file;

        // requests

        try {
            file = new BufferedReader(new FileReader("requests2.rec"));
            String line;
            int cnt = 0;
            while ((line = file.readLine()) != null) {
                String[] arr = line.split(" ");
                System.out.println("Processing request " + cnt);
                Integer source = Integer.valueOf(arr[0]);
                Integer dest = Integer.valueOf(arr[1]);
                /*
                String reachabile = arr[2];
                String tworeachabile = arr[3];
                String fourreachabile = arr[4];
                */
                req_single(source, dest, cnt++);
                /*
                if (req_single(source, dest, cnt++))
                    System.out.println(reachabile.equals("True"));
                else
                    System.out.println(reachabile.equals("False"));
                    */

/*
                if (req_single(source, dest, cnt++, 2))
                    System.out.println(tworeachabile.equals("True"));
                else
                    System.out.println(tworeachabile.equals("False"));

                if (req_single(source, dest, cnt++, 4))
                    System.out.println(fourreachabile.equals("True"));
                else
                    System.out.println(fourreachabile.equals("False"));
                    */
            }
            file.close();
        } catch (java.io.FileNotFoundException e) {
            System.err.println("FileNotFoundException: " + e.getMessage());
            System.exit(-1);
        } catch (java.io.IOException e) {
            System.err.println("Caught IOException: " + e.getMessage());
            System.exit(-1);
        }

    }
    public static void writeTimes(){
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));
            for ( int i = 0; i < reqtimes.length; i++)
            {      
                writer.write(reqtimes[i]/1e6 + " \n");
            }
            writer.close();
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }
    
    public static void main (String[] args) {
    	start();
        requests();
//        System.out.println("finished. "+total_reqs+" requests took average " + total_time/(1e6*total_reqs) + " each");
        //System.out.println("finished. "+total_2reqs+" n hop requests took average " + total_2time/(1e6*total_2reqs) + " each");
        writeTimes();
    }
}
