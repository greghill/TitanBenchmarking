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

    public static int node_id = 1;
    public static final Random rand = new Random();
    public static final int OPS_PER_CLIENT = 1000;
    public static final int PERCENT_READS = 90;
    public static final int NUM_CLIENTS = 10;
    public static final int NUM_NEW_EDGES = 10;
    public static final String INDEX_NAME = "search";
    public static final String ID = "vertex_id";
    public static final String VISIT = "visit";
    public static double[] stats = new double[NUM_CLIENTS];
    public static ArrayList<Integer> vertex_ids = new ArrayList<Integer>(OPS_PER_CLIENT*(100-PERCENT_READS)*(NUM_NEW_EDGES+1)/100);
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
                TitanThroughput.addNewNode(node);
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

    public HashSet<Vertex> getTwoNeighbors(int start) {
        HashSet<Vertex> toRet = new HashSet<Vertex>();
        for (Vertex nbr: getVertex(start).getVertices(Direction.OUT, "nbr")) {
            toRet.add(nbr);
            for (Vertex fof : nbr.getVertices(Direction.OUT, "nbr")) {
                toRet.add(fof);
            }
        }
        return toRet;
    }

    public static synchronized int getNewNodeId() {
        return node_id++;
    }

    public static synchronized void addNewNode(int id) {
        vertex_ids.add(id);
    }

    public static synchronized ArrayList<Integer> getRandomNodes(int size) {
        ArrayList<Integer> toRet = new ArrayList<Integer>(size);
        ArrayList<Integer> idxs = new ArrayList<Integer>(size);
        if (vertex_ids.size() < size) {
            return null;
        }
        while (idxs.size() < size) {
            Integer toAdd = rand.nextInt(vertex_ids.size());
            if (!idxs.contains(toAdd))
                idxs.add(toAdd);
        }
        for(Integer i : idxs)
            toRet.add(vertex_ids.get(i));

        return toRet;
    }

    public void run() {
        int num_ops = 0;
        long start = System.nanoTime();
        while (num_ops < OPS_PER_CLIENT) {
            System.out.println("proc " + proc + " done " + num_ops + " ops");
            // do writes
            for (int j = 0; j < 100-PERCENT_READS; j++) {
                int node = TitanThroughput.getNewNodeId();
                ArrayList<Integer> out_nbrs = TitanThroughput.getRandomNodes(NUM_NEW_EDGES/2);
                ArrayList<Integer> in_nbrs = TitanThroughput.getRandomNodes(NUM_NEW_EDGES/2);
                //System.out.println("node " + node + " with nieghbors " + out_nbrs + " and " + in_nbrs);
                Vertex v = graph.addVertex(null);
                v.setProperty(ID, node);
                for (Integer nbr : out_nbrs)
                    v.addEdge("nbr", getVertex(nbr));
                for (Integer nbr : in_nbrs)
                    getVertex(nbr).addEdge("nbr", v);
                graph.commit();
                num_ops++;
                TitanThroughput.addNewNode(node);
            }
            // do reads
            for (int j = 0; j < PERCENT_READS; j++) {
                int source = TitanThroughput.getRandomNodes(1).get(0);
                //String toPrint = "neighbors of " + source + " are:";
                getTwoNeighbors(source);
                graph.commit();
                num_ops++;
                /*
                for (Vertex v : fof) {
                    toPrint += " " + v.getId();
                }
                System.out.println(toPrint);
                */
            }
        }
        long end = System.nanoTime();
        stats[proc] = (end-start) / 1e6;
    }

    public static void writeTimes() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("throughputresults.txt"));
            double op_mult = (PERCENT_READS + (100-PERCENT_READS)*(1+NUM_NEW_EDGES)) /100.;
            double div = op_mult * NUM_CLIENTS * OPS_PER_CLIENT;
            double toRet = 0;
            for (double d : stats) {
                toRet += d;
            }
            writer.write(toRet/div + " \n");
            writer.close();
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void main (String[] args) {
        ArrayList<Thread> threads = new ArrayList<Thread>(NUM_CLIENTS);
        for (int i = 0; i < NUM_CLIENTS; i++) {
            threads.add(i, new Thread(new TitanThroughput(i)));
            threads.get(i).start();
        }
        try {
            for (int i = 0; i < NUM_CLIENTS; i++) {
                threads.get(i).join();
                System.out.println("finished " + i);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        writeTimes();
    }
}
