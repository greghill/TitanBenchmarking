import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanGraphQuery;
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


public class TitanTriangleCount implements Runnable {

    public static final Random rand = new Random();
    public static final int EXISTING_GRAPH_SIZE = 10000;
    public static final int OPS_PER_CLIENT = 1000;
    public static final int PERCENT_READS = 50;
    public static final int NUM_CLIENTS = 1;
    public static final int NUM_NEW_EDGES = 2;
    public static final String INDEX_NAME = "search";
    public static final String ID = "vertex_id";
    public static final String VISIT = "visit";

    public static int node_id = EXISTING_GRAPH_SIZE + 1;
    TitanGraph graph;
    public final int proc;

    public TitanTriangleCount(int proc) {
    	String directory = "titan_graph";
        BaseConfiguration config = new BaseConfiguration();
        config.setProperty("storage.backend", "cassandrathrift");
        config.setProperty("storage.hostname", "127.0.0.1");
        graph = TitanFactory.open(config);
        this.proc = proc;
        System.out.println("proc " + proc + " created");
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

    public int countTrianglesOne(Vertex v) {
        int toRet = 0;
        for (Vertex nbr : v.getVertices(Direction.OUT, "nbr")) {
            toRet++;
        }
        return toRet;
    }

    public int countTriangles() {
        int toRet = 0;
        TitanGraphQuery vertexRange = graph.query().has(ID, com.thinkaurelius.titan.core.attribute.Cmp.GREATER_THAN, "12"); // change to INTERVAL XXX
        for (Vertex v : vertexRange.vertices()) {
            toRet += countTrianglesOne(v);
        }
        return toRet;
    }
    public static synchronized int getNewNodeId() {
        return node_id++;
    }

    public void run() {
        System.out.println("one thread found " +  countTriangles() + " triangles");
    }

    public static void main (String[] args) {
        ArrayList<Thread> threads = new ArrayList<Thread>(NUM_CLIENTS);
        long start = System.nanoTime();
        for (int i = 0; i < NUM_CLIENTS; i++) {
            threads.add(i, new Thread(new TitanTriangleCount(i)));
            threads.get(i).start();
        }
        try {
            for (int i = 0; i < NUM_CLIENTS; i++) {
                threads.get(i).join();
                System.out.println("finished " + i);
            }
            long end = System.nanoTime();
            double div = NUM_CLIENTS * OPS_PER_CLIENT;
            System.out.println("took " + (end-start)/1e6 + " milliseconds or " + (end-start)/(div * 1e6) + " milliseconds per op");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
