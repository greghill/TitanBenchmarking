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
import java.lang.Thread;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;


public class TitanTriangleCount implements Runnable {

    public static final Random rand = new Random();
    public static final int EXISTING_GRAPH_SIZE = 10000;
    public static final int NUM_CLIENTS = 2;
    public static final int NODES_PER_CLIENT = EXISTING_GRAPH_SIZE/NUM_CLIENTS;
    public static final int NODES_PER_BATCH = 5000;
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
        Hashmap<Vertex, HashSet<Vertex>> fof = new Hashmap<Vertex, HashSet<Vertex>>;
        for (Vertex nbr : v.getVertices(Direction.OUT, "nbr")) {
            if (!fof.contains(nbr)) {
                HashSet<Vertex> toAdd = new HashSet<Vertex>();
                for (Vertex 2nbr : nbr.getVertices(Direction.OUT, "nbr")){
                    toAdd.add(2nbr);
                }
                fof.insert(nbr, toAdd);
            }
        }

        ArrayList<Vertex> nbrSet = new ArrayList<Vertex>(nbrs);
        for (Vertex v : fof.keySet()) {
            nbrSet.add(v);
        }

        for (int  i = 0; i < nbrSet.size()-1; i++) {
            for(int j = i+1; j<nbrSet.size();j++) {
                if (fof[friends[i]].contains(friends[j]) || fof[friends[j]].contains(friends[i])) {
                    triangles++;
                }
            }
        }


    inline uint64_t calc_triangles(std::unordered_map<uint64_t, std::unordered_set<uint64_t>>& fof, std::vector<uint64_t>& friends) {
        uint64_t triangles = 0;
        for (uint64_t  i = 0; i < friends.size()-1; i++) {
            for(uint64_t j = i+1; j<friends.size();j++) {
                if (fof[friends[i]].count(friends[j]) || fof[friends[j]].count(friends[i])) {
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
        if (proc != 0) {
            System.out.println("client " +proc + " found " +  countTriangles() + " triangles");
            graph.rollback();
        }
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
