package ru.sfedu.test;

import java.util.Map;
import java.util.Optional;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphApp.class);

    protected String propFileName;
    protected Configuration conf;
    protected Graph graph;
    protected GraphTraversalSource g;
    protected boolean supportsTransactions;
    protected boolean supportsSchema;

    /**
     * Constructs a graph app using the given properties.
     *
     * @param fileName location of the properties file
     */
    public GraphApp(final String fileName) {
        propFileName = fileName;
    }

    /**
     * Opens the graph instance. If the graph instance does not exist, a new
     * graph instance is initialized.
     */
    public GraphTraversalSource openGraph() throws ConfigurationException {
        LOGGER.info("opening graph");
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                        .configure(params.properties()
                                .setFileName(propFileName));

        conf = builder.getConfiguration();

        graph = GraphFactory.open(conf);
        g = graph.traversal();
        return g;
    }

    /**
     * Closes the graph instance.
     */
    public void closeGraph() throws Exception {
        LOGGER.info("closing graph");
        try {
            if (g != null) {
                g.close();
            }
            if (graph != null) {
                graph.close();
            }
        } finally {
            g = null;
            graph = null;
        }
    }

    /**
     * Drops the graph instance. The default implementation does nothing.
     */
    public void dropGraph() throws Exception {
    }

    /**
     * Creates the graph schema. The default implementation does nothing.
     */
    public void createSchema() {
    }

    /**
     * Adds the vertices, edges, and properties to the graph.
     */
    public void createTestGraphElements() {
        try {
            // naive check if the graph was previously created
            if (g.V().has("label", "A").hasNext()) {
                if (supportsTransactions) {
                    g.tx().rollback();
                }
                return;
            }
            LOGGER.info("creating elements");

            final Vertex A = g.addV("vertex").property("label", "A").next();
            final Vertex B = g.addV("vertex").property("label", "B").next();
            final Vertex C = g.addV("vertex").property("label", "C").next();
            final Vertex D = g.addV("vertex").property("label", "D").next();
            final Vertex E = g.addV("vertex").property("label", "E").next();
            final Vertex F = g.addV("vertex").property("label", "F").next();
            final Vertex G = g.addV("vertex").property("label", "G").next();
            final Vertex H = g.addV("vertex").property("label", "H").next();
            final Vertex I = g.addV("vertex").property("label", "I").next();

            g.V(A).as("a").V(B).addE("adjacent").property("distance", 7).from("a").next();
            g.V(A).as("a").V(C).addE("adjacent").property("distance", 10).from("a").next();

            g.V(B).as("a").V(G).addE("adjacent").property("distance", 27).from("a").next();
            g.V(B).as("a").V(F).addE("adjacent").property("distance", 9).from("a").next();

            g.V(C).as("a").V(F).addE("adjacent").property("distance", 8).from("a").next();
            g.V(C).as("a").V(E).addE("adjacent").property("distance", 31).from("a").next();

            g.V(F).as("a").V(H).addE("adjacent").property("distance", 11).from("a").next();

            g.V(E).as("a").V(D).addE("adjacent").property("distance", 32).from("a").next();

            g.V(G).as("a").V(I).addE("adjacent").property("distance", 15).from("a").next();

            g.V(H).as("a").V(D).addE("adjacent#1").property("distance", 17).from("a").next();
            g.V(H).as("a").V(I).addE("adjacent").property("distance", 15).from("a").next();

            //g.V(D).as("a").V(H).addE("adjacent#2").property("distance", 17).from("a").next();
            g.V(D).as("a").V(I).addE("adjacent").property("distance", 21).from("a").next();

            if (supportsTransactions) {
                g.tx().commit();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    /**
     * Runs some traversal queries to get data from the graph.
     */
    public void readTestGraphElements() {
        try {
            if (g == null) {
                return;
            }

            LOGGER.info("reading elements");

            final Optional<Map<Object, Object>> v = g.V().has("label", "A").valueMap().tryNext();
            if (v.isPresent()) {
                LOGGER.info(v.get().toString());
            } else {
                LOGGER.warn("A not found");
            }

        } finally {
            // the default behavior automatically starts a transaction for
            // any graph interaction, so it is best to finish the transaction
            // even for read-only graph query operations
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    public void createGeneratedGraphElements() {
        try {
            // naive check if the graph was previously created
            if (g.V().has("source", "A").hasNext()) {
                if (supportsTransactions) {
                    g.tx().rollback();
                }
                return;
            }
            LOGGER.info("creating elements");

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            g.io(classLoader.getResource("ca-AstroPh.xml").getFile()).read().iterate();
            //g.io(classLoader.getResource("facebook_large.xml").getFile()).read().iterate();

            LOGGER.info("Graph: nodes - " + g.V().count().next() + "; edges - " + g.E().count().next());

            if (supportsTransactions) {
                g.tx().commit();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    public void readGeneratedGraphElements() {
        try {
            if (g == null) {
                return;
            }

            LOGGER.info("reading elements");

            final Optional<Map<Object, Object>> v = g.V().has("source", "A").valueMap().tryNext();
            if (v.isPresent()) {
                LOGGER.info(v.get().toString());
            } else {
                LOGGER.warn("A not found");
            }

        } finally {
            // the default behavior automatically starts a transaction for
            // any graph interaction, so it is best to finish the transaction
            // even for read-only graph query operations
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    /**
     * Deletes elements from the graph structure. When a vertex is deleted,
     * its incident edges are also deleted.
     */
    public void deleteTestGraphElements() {
        try {
            if (g == null) {
                return;
            }
            LOGGER.info("deleting elements");
            // note that this will succeed whether or not H exists
            g.V().has("label", "H").drop().iterate();
            if (supportsTransactions) {
                g.tx().commit();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    public void runClusteringComputer() {
        try {
            if (g == null) {
                return;
            }

            LOGGER.info("run clustering");

            AntInspiredClusteringVertexProgram antp = AntInspiredClusteringVertexProgram.build()
                    .antHillThreshold(450)
                    .columnSize(50)
                    .q0(0.1)
                    .antsNumber(200)
                    .foragingAnts(0.9)
                    //.weightProperty("weight")
                    .iterations(40)
                    .alpha(1.0)
                    .beta(1.0)
                    //.rho(0.9)
                    //.mu(0.9)
                    //.tau0(1000)
                    //.theta0(1000)
                    .antNewHillThreshold(300)
                    .newHillDistance(10)
                    .antBuddyHillThreshold(100)
                    .buddyHillDistance(1)
                    .create();

            ComputerResult result = graph.compute().persist(GraphComputer.Persist.EDGES).program(antp).submit().get();

            LOGGER.info("Runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());

//            LOGGER.info("run clustering");
//
//            PeerPressureVertexProgram ppvp = PeerPressureVertexProgram.build()
//                    .distributeVote(false)
//                    .create();
//
//            ComputerResult result = graph.compute().persist(GraphComputer.Persist.EDGES).program(ppvp).submit().get();
//
//            LOGGER.info("Runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());

            result.graph().traversal().io("ca-AstroPh-clustered.graphml").with(IO.writer,IO.graphml).write().iterate();

            LOGGER.info("run modularity calculation");

            GraphModularityVertexProgram gmvm = GraphModularityVertexProgram.build()
                    //.weightProperty("weight")
                    .clusterProperty(AntInspiredClusteringVertexProgram.CLUSTER)
                    //.clusterProperty(PeerPressureVertexProgram.CLUSTER)
                    .create();

            ComputerResult result1 = result.graph().compute().program(gmvm).submit().get();

            LOGGER.info("Runtime = " + result1.memory().getRuntime() + "ms; iteration: " + result1.memory().getIteration());
            LOGGER.info("MODULARITY : " + result1.memory().get(GraphModularityVertexProgram.MODULARITY).toString());

            LOGGER.info("run ratiocut calculation");

            RatioCutVertexProgram rcvm = RatioCutVertexProgram.build()
                    //.weightProperty("weight")
                    .clusterProperty(AntInspiredClusteringVertexProgram.CLUSTER)
                    //.clusterProperty(PeerPressureVertexProgram.CLUSTER)
                    .create();

            ComputerResult result2 = result.graph().compute().program(rcvm).submit().get();

            LOGGER.info("Runtime = " + result2.memory().getRuntime() + "ms; iteration: " + result2.memory().getIteration());
            LOGGER.info("Ratio cut : " + result2.memory().get(RatioCutVertexProgram.RATIO_CUT).toString());

            LOGGER.info("run normalizedcut calculation");

            NormalizedCutVertexProgram ncvm = NormalizedCutVertexProgram.build()
                    .weightProperty("weight")
                    .clusterProperty(AntInspiredClusteringVertexProgram.CLUSTER)
                    //.clusterProperty(PeerPressureVertexProgram.CLUSTER)
                    .create();

            ComputerResult result3 = result.graph().compute().program(ncvm).submit().get();

            LOGGER.info("Runtime = " + result3.memory().getRuntime() + "ms; iteration: " + result3.memory().getIteration());
            LOGGER.info("Normalized cut : " + result3.memory().get(NormalizedCutVertexProgram.NORMALIZED_CUT).toString());

            if (supportsTransactions) {
                g.tx().commit();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    public void runApp() {
        try {
            // open and initialize the graph
            openGraph();

            // define the schema before loading data
            if (supportsSchema) {
                createSchema();
            }

            // build the graph structure
            //createTestGraphElements();
            createGeneratedGraphElements();
            // read to see they were made
            //readTestGraphElements();
            readGeneratedGraphElements();

            runClusteringComputer();

            //runTestGraphShortestPathComputer();
            //runGeneratedGraphShortestPathComputer();

            // delete some graph elements
            //deleteTestGraphElements();

            //runTestGraphShortestPathComputer();

            // close the graph
            closeGraph();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
