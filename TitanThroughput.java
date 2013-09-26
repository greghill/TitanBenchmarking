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
import java.util.Random;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.ArrayDeque;
import java.lang.Thread;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;


public class TitanThroughput implements Runnable {

    public static final Random rand = new Random();
    public static final int EXISTING_GRAPH_SIZE = 10000;
    public static final int OPS_PER_CLIENT = 1000;
    public static final int PERCENT_READS = 50;
    public static final int NUM_CLIENTS = 5;
    public static final int NUM_NEW_EDGES = 2;
    public static final String INDEX_NAME = "search";
    public static final String ID = "vertex_id";
    public static final String VISIT = "visit";

    public static int node_id = EXISTING_GRAPH_SIZE + 1;
    //public static ArrayList<Integer> vertex_ids = new ArrayList<Integer>();//OPS_PER_CLIENT*(100-PERCENT_READS)*(NUM_NEW_EDGES+1)/100);
    TitanGraph graph;
    public final int proc;

    public TitanThroughput(int proc) {
    	String directory = "titan_graph";
        BaseConfiguration config = new BaseConfiguration();
        if (proc == 0) {
            Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
            // configuring local backend
            storage.setProperty("storage.backend", "cassandrathrift");
            storage.setProperty("storage.hostname", "127.0.0.1");//"128.84.227.111");
            storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, directory);
            /*
            // configuring elastic search index
            Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
            index.setProperty(INDEX_BACKEND_KEY, "elasticsearch");
            index.setProperty("client-only", false);
            index.setProperty(STORAGE_DIRECTORY_KEY, directory + File.separator + "es");
            */
        }
        config.setProperty("storage.backend", "cassandrathrift");
        config.setProperty("storage.hostname", "127.0.0.1");
        graph = TitanFactory.open(config);
        this.proc = proc;
        System.out.println("proc " + proc + " created");
        if (proc == 0) { // first proc add some nodes
            graph.makeType().name(ID).dataType(Integer.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
            graph.makeType().name(VISIT).dataType(Integer.class).unique(Direction.OUT).makePropertyKey();
            graph.makeType().name("nbr").makeEdgeLabel();
            graph.commit();
            for(int i = 0; i < NUM_NEW_EDGES ; i++) {
                int node = TitanThroughput.getNewNodeId();
                Vertex v = graph.addVertex(null);
                v.setProperty(ID, node);
            }
            graph.commit();
        }
    }

    public Vertex getVertex(Integer id) {
    	for (Vertex v: graph.getVertices(ID, id)) {
            return v;
        }
        System.err.println("Vertex " + id + " not found");
        System.exit(-1);
        return graph.addVertex(null);
    }

    public Integer getId(TitanVertex v) {
        for (TitanProperty p: v.getProperties()) {
            return p.getValue(Integer.class);
        }
        return null;
    }

    public boolean req_single(Integer n1, Integer n2, Integer req, final int max_hops) {
        //System.out.println("reachability " + n1 + " to " + n2);
        int hops = 0;
        Object targetId = getVertex(n2).getId();
        ArrayDeque<Vertex> exploringDepth = new ArrayDeque<Vertex>();
        ArrayDeque<Vertex> nextDepth = new ArrayDeque<Vertex>();
        exploringDepth.add(getVertex(n1));
        while (!exploringDepth.isEmpty() && hops != max_hops){
            while (!exploringDepth.isEmpty()) {
                Vertex v = exploringDepth.remove();
                for (Vertex nbr: v.getVertices(Direction.OUT, "nbr")) {
                    if (nbr.getId().equals(targetId)) { // found target
                        graph.rollback();
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
        graph.rollback();
        return false;
    }

    public static synchronized int getNewNodeId() {
        return node_id++;
    }

    public static synchronized ArrayList<Integer> getRandomNodes(int size) {
        ArrayList<Integer> toRet = new ArrayList<Integer>(size);
        while (toRet.size() < size) {
            Integer toAdd = rand.nextInt(EXISTING_GRAPH_SIZE);
            if (!toRet.contains(toAdd))
                toRet.add(toAdd);
        }
        return toRet;
    }

    public void run() {
        int num_ops = 0;
        while (num_ops < OPS_PER_CLIENT) {
            System.out.println("proc " + proc + " done " + num_ops + " ops");
            // do writes
            for (int j = 0; j < 100-PERCENT_READS; j++) {
                Vertex v = graph.addVertex(null);
                v.setProperty(ID, TitanThroughput.getNewNodeId());

                Vertex v2 = graph.addVertex(null);
                v2.setProperty(ID, TitanThroughput.getNewNodeId());

                v.addEdge("nbr", v2);

                graph.commit();
                num_ops++;
            }
            // do reads
            for (int j = 0; j < PERCENT_READS; j++) {
                ArrayList<Integer> sourcedest =  TitanThroughput.getRandomNodes(2);
                boolean r = req_single(sourcedest.get(0), sourcedest.get(1), num_ops, 2); // calls rollback on transactio for us
            //    System.out.println("two hop reachability from  " + sourcedest.get(0)+ " to " + sourcedest.get(1) + " has reachable " + r);
                num_ops++;
            }
        }
    }

    public static void main (String[] args) {
        ArrayList<Thread> threads = new ArrayList<Thread>(NUM_CLIENTS);
        long start = System.nanoTime();
        for (int i = 0; i < NUM_CLIENTS; i++) {
            threads.add(i, new Thread(new TitanThroughput(i)));
            threads.get(i).start();
        }
        try {
            for (int i = 0; i < NUM_CLIENTS; i++) {
                threads.get(i).join();
                System.out.println("finished " + i);
            }
            long end = System.nanoTime();
            System.out.println("took sum " + (end-start)/1e6 + " milliseconds");
            double div = NUM_CLIENTS * OPS_PER_CLIENT;
            System.out.println("or " + (end-start)/(div * 1e6) + " milliseconds");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
