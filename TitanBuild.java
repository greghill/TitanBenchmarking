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


public class TitanBuild {

    public static int BATCH_SIZE = 1000;
    public static final String INDEX_NAME = "search";
    public static final String ID = "vertex_id";
    public static final String VISIT = "visit";
    public static ArrayList<Vertex> nodes;
    public static TitanGraph graph;

    public static void create() {
    	String directory = "titan_graph";
        BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        // configuring local backend
        storage.setProperty("storage.backend", "cassandrathrift");
        storage.setProperty("storage.hostname", "127.0.0.1");//"128.84.227.111");
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, directory);
        // configuring elastic search index
        Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
        index.setProperty(INDEX_BACKEND_KEY, "elasticsearch");
        //index.setProperty("local-mode", true);
        index.setProperty("client-only", false);
        index.setProperty(STORAGE_DIRECTORY_KEY, directory + File.separator + "es");

        config.setProperty("storage.backend", "cassandrathrift");
        config.setProperty("storage.hostname", "127.0.0.1");
        graph = TitanFactory.open(config);
        TitanBuild.load();
    }

    public static Vertex getVertex(Integer id) {
    	for (Vertex v: graph.getVertices(ID, id)) {
            return v;
        }
        System.err.println("Vertex " + id + " not found");
        System.exit(-1);
        return graph.addVertex(null);
    }

    public static void load() {

        BufferedReader file;
        nodes = new ArrayList<Vertex>();
        graph.makeType().name(ID).dataType(Integer.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
        graph.makeType().name(VISIT).dataType(Integer.class).unique(Direction.OUT).makePropertyKey();
        graph.makeType().name("nbr").makeEdgeLabel();
        graph.commit();

        // vertices

        try {
            file = new BufferedReader(new FileReader("graph2.rec"));
            String line;
            int cnt = 0;
            while ((line = file.readLine()) != null) {
                String[] arr = line.split(" ");
                Vertex v = graph.addVertex(null);
                v.setProperty(ID, Integer.valueOf(arr[0]));
                nodes.add(v);
                cnt++;
                if (cnt % BATCH_SIZE == 0) {
                    System.out.println("Adding node " + cnt);
                    graph.commit();
                }
                //System.out.println("Adding node " + arr[0]);
            }
            file.close();
            file = new BufferedReader(new FileReader("graph2.rec"));
            cnt = 0;
            while ((line = file.readLine()) != null) {
                String[] arr = line.split(" ");
                //System.out.print("Adding edges for node " + arr[0] + ":");
                for (int i = 1; i < arr.length; i++) {
                    try {
                        nodes.get(cnt).addEdge("nbr", getVertex(Integer.valueOf(arr[i])));
                    } catch (java.lang.IllegalArgumentException e) {
                        continue;
                    }
                    //System.out.print(" " + arr[i]);
                }
                //System.out.print("\nAnd again -- ");
                for (Edge e: getVertex(Integer.valueOf(arr[0])).getEdges(Direction.OUT, "nbr")) {
                    TitanVertex v = (TitanVertex) e.getVertex(Direction.IN);
                    for (TitanProperty p: v.getProperties()) {
                        //System.out.print(" " + p.getValue(Integer.class));
                    }
                }
                //System.out.println();
                cnt++;
                if (cnt % BATCH_SIZE == 0) {
                    System.out.println("Adding edges for node " + cnt);
                    graph.commit();
                }
            }
            file.close();
        } catch (java.io.FileNotFoundException e) {
            System.err.println("FileNotFoundException: " + e.getMessage());
            System.exit(-1);
        } catch (java.io.IOException e) {
            System.err.println("Caught IOException: " + e.getMessage());
            System.exit(-1);
        }

        // commit the transaction to disk
        graph.commit();
    }

    public static void main (String[] args) {
        System.out.println("making graph now");
    	create();
    }
}
