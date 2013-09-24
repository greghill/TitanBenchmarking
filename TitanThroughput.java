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
import java.util.Collection;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.lang.Thread;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;


public class TitanThroughput implements Runnable {

    public static final int NUM_CLIENTS = 10;
    public static final String ID = "vertex_id";
    public static final String VISIT = "visit";
    TitanGraph graph;

    public TitanThroughput() {
        BaseConfiguration config = new BaseConfiguration();
        config.setProperty("storage.backend", "cassandrathrift");
        config.setProperty("storage.hostname", "127.0.0.1");
        graph = TitanFactory.open(config);
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

    public Collection<Vertex> getTwoNeighbors(Vertex start) {
        ArrayList<Vertex> friends = new ArrayList<Vertex>();
        ArrayList<Vertex> toRet = new ArrayList<Vertex>();
        for (Vertex nbr: start.getVertices(Direction.OUT, "nbr"))
            friends.add(nbr);
        for (Vertex friend : friends) {
            for (Vertex fof : friend.getVertices(Direction.OUT, "nbr"))
                toRet.add(fof);
        }
        return toRet;
    }

    public void writeTimes(){
        /*
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
        */
    }

    public void run() {
        int node = getNewNodeId();
        ArrayList<Integer> out_neighbors = getNeighbors();
        ArrayList<Integer> in_neighbors = getNeighbors();
        Vertex v = graph.addVertex(null);
        v.setProperty(ID, node);
        for (Integer nbr : out_nbrs)
            v.addEdge("nbr", getVertex(nbr));
        for (Integer nbr : in_nbrs)
            getVertex(nbr).addEdge("nbr", v);
        graph.commit();

        node = getRandomNode();
        getTwoNeighbors(getVertex(node));
    }
    
    public static void main (String[] args) {
        ArrayList<Thread> threads = new ArrayList<Thread>(NUM_CLIENTS);
        for (int i = 0; i < NUM_CLIENTS; i++) {
            threads.add(i, new Thread(new TitanThroughput()));
            threads.get(i).run();
        }
        try {
            for (int i = 0; i < NUM_CLIENTS; i++) {
                threads.get(i).join();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
