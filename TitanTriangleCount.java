import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.gremlin.java.*;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.*;
import java.util.Random;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.lang.Thread;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;


public class TitanTriangleCount implements Runnable {

    public static final Random rand = new Random();
    public static final int EXISTING_GRAPH_SIZE = 10000;
    public static final int NUM_CLIENTS = 1;
    public static final int NODES_PER_CLIENT = EXISTING_GRAPH_SIZE/NUM_CLIENTS;
    public static final int NODES_PER_BATCH = 10000;
    public static final String INDEX_NAME = "search";
    public static final String ID = "vertex_id";
    public static final String VISIT = "visit";

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

/*
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
    */

    public int countTrianglesOne(Vertex v) {
        int triangles = 0;
        HashMap<Vertex, HashSet<Vertex>> fof = new HashMap<Vertex, HashSet<Vertex>>();
        for (Vertex nbr : v.getVertices(Direction.OUT, "nbr")) {
            if (!fof.containsKey(nbr)) {
                HashSet<Vertex> toAdd = new HashSet<Vertex>();
                for (Vertex twonbr : nbr.getVertices(Direction.OUT, "nbr")){
                    toAdd.add(twonbr);
                }
                fof.put(nbr, toAdd);
            }
        }

        ArrayList<Vertex> friends = new ArrayList<Vertex>();
        for (Vertex vert : fof.keySet()) {
            friends.add(vert);
        }

        for (int  i = 0; i < friends.size()-1; i++) {
            for(int j = i+1; j<friends.size();j++) {
                if (fof.get(friends.get(i)).contains(friends.get(j)) || fof.get(friends.get(j)).contains(friends.get(i))) {
                    triangles++;
                }
            }
        }
        return triangles;
    }

    public int countTriangles() {
        int toRet = 0;
        int start = (proc * NODES_PER_CLIENT);
        int end = start + NODES_PER_BATCH;
        while (end <= (proc+1)*NODES_PER_CLIENT) {
            System.out.println("client " + proc + " counting from " + start+ " to " +end);
            Interval<Integer> range = new Interval<Integer>(start, end);
            //new GremlinPipeline();
            TitanGraphQuery vertexRange = graph.query().has(ID, com.thinkaurelius.titan.core.attribute.Cmp.INTERVAL, range);
            for (Vertex v : vertexRange.vertices()) {
                toRet += countTrianglesOne(v);
            }
            start += NODES_PER_BATCH;
            end += NODES_PER_BATCH;
        }
        return toRet;
    }

    public void run() {
            System.out.println("client " +proc + " found " +  countTriangles() + " triangles");
            graph.rollback();
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
            System.out.println("took " + (end-start)/1e6 + " milliseconds ");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
