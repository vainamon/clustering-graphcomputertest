/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ru.sfedu.test;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

public class AntInspiredClusteringVertexProgram implements VertexProgram<AntInspiredClusteringVertexProgram.Ant> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AntInspiredClusteringVertexProgram.class);

    @SuppressWarnings("WeakerAccess")
    public static final String CLUSTER = "ru.sfedu.test.AntInspiredClusteringVertexProgram.cluster";
    private static final String ANTHILL = "ru.sfedu.test.AntInspiredClusteringVertexProgram.anthill";

    private static final String WEIGHT_TRAVERSAL = "ru.sfedu.test.AntInspiredClusteringVertexProgram.weightTraversal";
    private static final String CLUSTER_TRAVERSAL = "ru.sfedu.test.AntInspiredClusteringVertexProgram.clusterTraversal";

    private static final String WORKER_ANTS_NUMBER = "ru.sfedu.test.AntInspiredClusteringVertexProgram.workerAntsNumber";
    private static final String FORAGING_ANTS_PROPORTION = "ru.sfedu.test.AntInspiredClusteringVertexProgram.foragingAntsProportion";
    private static final String ALPHA = "ru.sfedu.test.AntInspiredClusteringVertexProgram.alpha";
    private static final String BETA = "ru.sfedu.test.AntInspiredClusteringVertexProgram.beta";
    private static final String Q0 = "ru.sfedu.test.AntInspiredClusteringVertexProgram.q0";
    private static final String RHO = "ru.sfedu.test.AntInspiredClusteringVertexProgram.rho";
    private static final String MU = "ru.sfedu.test.AntInspiredClusteringVertexProgram.mu";
    private static final String TAU0 = "ru.sfedu.test.AntInspiredClusteringVertexProgram.tau0";
    private static final String THETA0 = "ru.sfedu.test.AntInspiredClusteringVertexProgram.theta0";
    private static final String ANT_HILL_THRESHOLD = "ru.sfedu.test.AntInspiredClusteringVertexProgram.antHillThreshold";
    private static final String ANT_BUDDYHILL_THRESHOLD = "ru.sfedu.test.AntInspiredClusteringVertexProgram.antBuddyHillThreshold";
    private static final String ANT_NEWHILL_THRESHOLD = "ru.sfedu.test.AntInspiredClusteringVertexProgram.antNewHillThreshold";
    private static final String BUDDYHILL_DISTANCE = "ru.sfedu.test.AntInspiredClusteringVertexProgram.buddyHillDistance";
    private static final String NEWHILL_DISTANCE = "ru.sfedu.test.AntInspiredClusteringVertexProgram.newHillDistance";
    private static final String COLUMN_SIZE = "ru.sfedu.test.AntInspiredClusteringVertexProgram.columnSize";
    private static final String ITERATIONS = "ru.sfedu.test.AntInspiredClusteringVertexProgram.iterations";

    private static final String EDGE_PHEROMONE = "ru.sfedu.test.AntInspiredClusteringVertexProgram.edgePheromone";
    private static final String VERTEX_PHEROMONES = "ru.sfedu.test.AntInspiredClusteringVertexProgram.vertexPheromones";

    private static final String VOTE_TO_HALT = "ru.sfedu.test.AntInspiredClusteringVertexProgram.voteToHalt";

    public static final PureTraversal<Edge, Number> DEFAULT_WEIGHT_TRAVERSAL = new PureTraversal<>(
            __.<Edge>start().<Number>constant(1.0d).asAdmin()); // todo: new ConstantTraversal<>(1)
    private static final PureTraversal<Element, Object> DEFAULT_CLUSTER_TRAVERSAL
            = new PureTraversal<>(__.values(CLUSTER).asAdmin());

    private PureTraversal<Edge, Number> weightTraversal = DEFAULT_WEIGHT_TRAVERSAL.clone();
    private PureTraversal<Element, Object> clusterTraversal = DEFAULT_CLUSTER_TRAVERSAL.clone();

    private Integer workerAntsNumber;
    private Number alpha;
    private Number beta;
    private static Number rho;
    private static Number mu;
    private Number q0;
    private Number tau0;
    private Number theta0;
    private Number foragingAntsProportion;
    private Integer iterations;
    private Integer anthillThreshold;
    private Integer antBuddyHillThreshold;
    private Integer antNewHillThreshold;
    private Integer buddyHillDistance;
    private Integer newHillDistance;
    private Integer columnSize;

    final long time = System.currentTimeMillis();

    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            VertexComputeKey.of(CLUSTER, false),
            VertexComputeKey.of(ANTHILL, true),
            VertexComputeKey.of(VERTEX_PHEROMONES, true))
    );

    private final Set<MemoryComputeKey> memoryComputeKeys = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true)
    ));

    private AntInspiredClusteringVertexProgram() {

    }

    private static Pair<Integer, Double> applyEdgePheromone(Pair<Integer, Double> oldValue, Pair<Integer, Double> newValue) {
        return new Pair<>(newValue.getValue0(),
                oldValue.getValue1()
                        * Math.pow(1 - rho.doubleValue(), newValue.getValue0() - oldValue.getValue0())
                        + newValue.getValue1());
    }

    private static Pair<Integer, Double> applyVertexPheromone(Pair<Integer, Double> oldValue, Pair<Integer, Double> newValue) {
        return new Pair<>(newValue.getValue0(),
                oldValue.getValue1()
                        * Math.pow(1 - mu.doubleValue(), newValue.getValue0() - oldValue.getValue0())
                        + newValue.getValue1());
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {

        if (configuration.containsKey(WEIGHT_TRAVERSAL))
            this.weightTraversal = PureTraversal.loadState(configuration, WEIGHT_TRAVERSAL, graph);

        if (configuration.containsKey(CLUSTER_TRAVERSAL))
            this.clusterTraversal = PureTraversal.loadState(configuration, CLUSTER_TRAVERSAL, graph);

        if (configuration.containsKey(ALPHA))
            this.alpha = (Number) configuration.getProperty(ALPHA);
        else
            this.alpha = 1;

        if (configuration.containsKey(BETA))
            this.beta = (Number) configuration.getProperty(BETA);
        else
            this.beta = 2;

        if (configuration.containsKey(RHO))
            rho = (Number) configuration.getProperty(RHO);
        else
            rho = 0.1;

        if (configuration.containsKey(MU))
            mu = (Number) configuration.getProperty(MU);
        else
            mu = 0.1;

        if (configuration.containsKey(Q0))
            this.q0 = (Number) configuration.getProperty(Q0);
        else
            this.q0 = 0.9;

        if (configuration.containsKey(TAU0))
            this.tau0 = (Number) configuration.getProperty(TAU0);
        else
            this.tau0 = 1.0;

        if (configuration.containsKey(THETA0))
            this.theta0 = (Number) configuration.getProperty(THETA0);
        else
            this.theta0 = 1.0;

        if (configuration.containsKey(FORAGING_ANTS_PROPORTION))
            this.foragingAntsProportion = (Number) configuration.getProperty(FORAGING_ANTS_PROPORTION);
        else
            this.foragingAntsProportion = 0.5;

        this.workerAntsNumber = configuration.getInteger(WORKER_ANTS_NUMBER, 2);
        this.iterations = configuration.getInteger(ITERATIONS, 10);
        this.anthillThreshold = configuration.getInteger(ANT_HILL_THRESHOLD, 10);
        this.antBuddyHillThreshold = configuration.getInteger(ANT_BUDDYHILL_THRESHOLD, Integer.MAX_VALUE);
        this.antNewHillThreshold = configuration.getInteger(ANT_NEWHILL_THRESHOLD, Integer.MAX_VALUE);
        this.buddyHillDistance = configuration.getInteger(BUDDYHILL_DISTANCE, 2);
        this.newHillDistance = configuration.getInteger(NEWHILL_DISTANCE, 4);
        this.columnSize = configuration.getInteger(COLUMN_SIZE, 10);
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);

        this.weightTraversal.storeState(configuration, WEIGHT_TRAVERSAL);
        this.clusterTraversal.storeState(configuration, CLUSTER_TRAVERSAL);

        configuration.setProperty(WORKER_ANTS_NUMBER, this.workerAntsNumber);

        if (this.alpha != null)
            configuration.setProperty(ALPHA, alpha);
        if (this.beta != null)
            configuration.setProperty(BETA, beta);
        if (rho != null)
            configuration.setProperty(RHO, rho);
        if (mu != null)
            configuration.setProperty(MU, mu);
        if (this.q0 != null)
            configuration.setProperty(Q0, q0);
        if (this.tau0 != null)
            configuration.setProperty(TAU0, tau0);
        if (this.theta0 != null)
            configuration.setProperty(THETA0, theta0);
        if (this.foragingAntsProportion != null)
            configuration.setProperty(FORAGING_ANTS_PROPORTION, foragingAntsProportion);
        if (this.iterations != null)
            configuration.setProperty(ITERATIONS, iterations);
        if (this.anthillThreshold != null)
            configuration.setProperty(ANT_HILL_THRESHOLD, anthillThreshold);
        if (this.antBuddyHillThreshold != null)
            configuration.setProperty(ANT_BUDDYHILL_THRESHOLD, antBuddyHillThreshold);
        if (this.antNewHillThreshold != null)
            configuration.setProperty(ANT_NEWHILL_THRESHOLD, antNewHillThreshold);
        if (this.buddyHillDistance != null)
            configuration.setProperty(BUDDYHILL_DISTANCE, buddyHillDistance);
        if (this.newHillDistance != null)
            configuration.setProperty(NEWHILL_DISTANCE, newHillDistance);
        if (this.columnSize != null)
            configuration.setProperty(COLUMN_SIZE, columnSize);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return VERTEX_COMPUTE_KEYS;
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return memoryComputeKeys;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return Collections.emptySet();
    }

    @Override
    public VertexProgram<Ant> clone() {
        try {
            final AntInspiredClusteringVertexProgram clone = (AntInspiredClusteringVertexProgram) super.clone();
            if (null != this.weightTraversal)
                clone.weightTraversal = this.weightTraversal.clone();
            if (null != this.clusterTraversal)
                clone.clusterTraversal = this.clusterTraversal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<Ant> messenger, final Memory memory) {

        boolean voteToHalt = true;

        if (memory.isInitialIteration()) {
            final long edgesCount = IteratorUtils.count(vertex.edges(Direction.BOTH));

            if (edgesCount > anthillThreshold) {
                createAntHill(vertex, 0, HillType.NEW, vertex.id().toString());

                try {
                    spawnAnts(vertex, messenger,
                            vertex.id().toString(),
                            vertex.id().toString(),
                            (int) edgesCount,
                            memory.getIteration());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                voteToHalt = false;
            }

            vertex.property(VertexProperty.Cardinality.single, CLUSTER, vertex.id().toString());
        } else {
            final Iterator<Ant> antsIterator = messenger.receiveMessages();

            final HashMap<String, ArrayList<Pair<Double, Ant>>> workerAntsWithChoices = new HashMap<>();
            final ArrayList<Pair<Double, Ant>> foragerAntsWithChoices = new ArrayList<>();
            final HashMap<Integer, ArrayList<Pair<Double, Ant>>> hillWorkerAntsWithChoices = new HashMap<>();
            final HashMap<Integer, ArrayList<Pair<Double, Ant>>> hillForagerAntsWithChoices = new HashMap<>();

            final Pair<HillType, String> hillInfo = (Pair<HillType, String>)
                    vertex.property(ANTHILL).orElse(new Pair<>(HillType.NONE, ""));

            boolean isHill = !hillInfo.getValue0().equals(HillType.NONE);

            final int iteration = memory.getIteration();

            final HashMap<String, Pair<Integer, Double>> vertexPheromones =
                    vertex.<HashMap<String, Pair<Integer, Double>>>property(VERTEX_PHEROMONES).orElseGet(HashMap::new);

            while (antsIterator.hasNext()) {
                final Ant ant = antsIterator.next();

                vertexPheromones.putIfAbsent(ant.hillId(), new Pair<>(0, 0.0));

                vertexPheromones.merge(ant.hillId(), new Pair<>(iteration, mu.doubleValue() * theta0.doubleValue()),
                        AntInspiredClusteringVertexProgram::applyVertexPheromone);

                // collect ants in maps
                if (isHill && (vertex.id().toString().equals(ant.buddyId()))) {
                    if (ant.type().equals(AntType.WORKER)) {
                        ArrayList<Pair<Double, Ant>> ants
                                = hillWorkerAntsWithChoices.getOrDefault(ant.column(), new ArrayList<>());

                        ants.add(new Pair<>(Math.random(), ant));
                        hillWorkerAntsWithChoices.put(ant.column(), ants);
                    } else {
                        ArrayList<Pair<Double, Ant>> ants
                                = hillForagerAntsWithChoices.getOrDefault(ant.column(), new ArrayList<>());

                        ants.add(new Pair<>(Math.random(), ant));
                        hillForagerAntsWithChoices.put(ant.column(), ants);
                    }
                } else {
                    if ((!isHill) || (hillInfo.getValue1().equals(ant.hillId()))) {
                        if (ant.type().equals(AntType.WORKER)) {
                            ArrayList<Pair<Double, Ant>> ants
                                    = workerAntsWithChoices.getOrDefault(ant.hillId(), new ArrayList<>());

                            ants.add(new Pair<>(Math.random(), ant));
                            workerAntsWithChoices.put(ant.hillId(), ants);
                        } else
                            foragerAntsWithChoices.add(new Pair<>(Math.random(), ant));
                    }
                }
            }

            // move hill ants
            if (!hillForagerAntsWithChoices.isEmpty() || !hillWorkerAntsWithChoices.isEmpty()) {
                final Iterator<Edge> edges = vertex.edges(Direction.BOTH);
                final long edgesCount = IteratorUtils.count(vertex.edges(Direction.BOTH));

                int column = 0;

                while (edges.hasNext()) {
                    final List<Edge> columnEdges =
                            IteratorUtils.list(
                                    IteratorUtils.limit(edges,
                                            (edgesCount - (column + 1) * columnSize) < columnSize / 2 ?
                                                    (int) (edgesCount - column * columnSize) : columnSize)
                            );

                    if (!hillWorkerAntsWithChoices.isEmpty()) {
                        try {
                            moveAntsForward(hillWorkerAntsWithChoices.getOrDefault(column, new ArrayList<>()),
                                    calculateProbabilities(columnEdges, vertex.id().toString(), false, iteration),
                                    vertex,
                                    messenger,
                                    iteration);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    if (!hillForagerAntsWithChoices.isEmpty()) {
                        try {
                            moveAntsForward(hillForagerAntsWithChoices.getOrDefault(column, new ArrayList<>()),
                                    calculateProbabilities(columnEdges, null, true, iteration),
                                    vertex,
                                    messenger,
                                    iteration);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    column++;
                }
            }

            // move other ants
            if (!foragerAntsWithChoices.isEmpty() || !workerAntsWithChoices.isEmpty()) {
                final List<Edge> edges = IteratorUtils.list(vertex.edges(Direction.BOTH));
                final long edgesCount = IteratorUtils.count(vertex.edges(Direction.BOTH));

                if (!foragerAntsWithChoices.isEmpty()) {
                    final Iterator<Pair<Double, Ant>> foragerAntsIterator = foragerAntsWithChoices.iterator();
                    // create new buddy hills if
                    while (!isHill && foragerAntsIterator.hasNext()) {
                        Pair<Double, Ant> foragerAnt = foragerAntsIterator.next();

                        if ((foragerAnt.getValue1().distance >= newHillDistance)
                                && (edgesCount > antNewHillThreshold)) {
                            createAntHill(vertex, memory.getIteration(),
                                    HillType.NEW, vertex.id().toString());

                            try {
                                spawnAnts(vertex, messenger,
                                        vertex.id().toString(),
                                        vertex.id().toString(),
                                        (int) edgesCount,
                                        memory.getIteration());
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            foragerAntsWithChoices.clear();
                            workerAntsWithChoices.clear();

                            isHill = true;
                        } else if ((foragerAnt.getValue1().distance >= buddyHillDistance)
                                && (foragerAnt.getValue1().distance < newHillDistance)
                                && (edgesCount > antBuddyHillThreshold)) {
                            createAntHill(vertex, memory.getIteration(),
                                    HillType.BUDDY, foragerAnt.getValue1().hillId());

                            try {
                                spawnAnts(vertex, messenger,
                                        foragerAnt.getValue1().hillId(),
                                        vertex.id().toString(),
                                        (int) edgesCount,
                                        memory.getIteration());
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            foragerAntsWithChoices.removeIf(antI -> !antI.getValue1().hillId().equals(foragerAnt.getValue1().hillId()));

                            workerAntsWithChoices.keySet().removeIf(key -> !key.equals(foragerAnt.getValue1().hillId()));

                            isHill = true;
                        }
                    }

                    try {
                        moveAntsForward(foragerAntsWithChoices,
                                calculateProbabilities(edges, null, true, iteration),
                                vertex,
                                messenger,
                                iteration);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    workerAntsWithChoices.forEach(
                            (key, value) -> {
                                try {
                                    moveAntsForward(value,
                                            calculateProbabilities(edges, key, false, iteration),
                                            vertex,
                                            messenger,
                                            iteration);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                    );
                }
            }

            // update hill pheromones and cluster label
            String[] maxPheromoneHillId = new String[1];
            double[] maxPheromone = new double[1];

            maxPheromoneHillId[0] = vertex.id().toString();
            maxPheromone[0] = Double.MIN_VALUE;

            vertexPheromones.keySet().forEach(key -> {
                vertexPheromones.merge(key, new Pair<>(iteration, 0.0),
                        AntInspiredClusteringVertexProgram::applyVertexPheromone);

                if (vertexPheromones.get(key).getValue1() > maxPheromone[0]) {
                    maxPheromoneHillId[0] = key;
                    maxPheromone[0] = vertexPheromones.get(key).getValue1();
                }
            });

            vertex.property(VertexProperty.Cardinality.single, VERTEX_PHEROMONES, vertexPheromones);
            vertex.property(VertexProperty.Cardinality.single, CLUSTER, maxPheromoneHillId[0]);

            if (iteration < iterations)
                voteToHalt = false;
        }

        memory.add(VOTE_TO_HALT, voteToHalt);
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.get(VOTE_TO_HALT);

        memory.set(VOTE_TO_HALT, true);

        return voteToHalt;
    }

    @Override
    public String toString() {

        final List<String> options = new ArrayList<>();
        final Function<String, String> shortName = name -> name.substring(name.lastIndexOf(".") + 1);

        if (!this.weightTraversal.equals(DEFAULT_WEIGHT_TRAVERSAL)) {
            options.add(shortName.apply(WEIGHT_TRAVERSAL) + "=" + this.weightTraversal.get());
        }

        return StringFactory.vertexProgramString(this, String.join(", ", options));
    }

    //////////////////////////////

    private void createAntHill(final Vertex vertex,
                               final int iteration,
                               final HillType type,
                               final String hillId) {

        vertex.property(VertexProperty.Cardinality.single, ANTHILL, new Pair<>(type, hillId));

        final HashMap<String, Pair<Integer, Double>> vertexPheromones =
                vertex.<HashMap<String, Pair<Integer, Double>>>property(VERTEX_PHEROMONES).orElseGet(HashMap::new);

        vertexPheromones.putIfAbsent(hillId, new Pair<>(iteration, theta0.doubleValue()));

        vertex.property(VertexProperty.Cardinality.single, VERTEX_PHEROMONES, vertexPheromones);
    }

    private Pair<Edge, List<Pair<Edge, Double>>> calculateProbabilities(final List<Edge> edgesList,
                                                                        final String hillId,
                                                                        final boolean predecessor,
                                                                        final int iteration) throws InterruptedException {

        Pair<Edge, List<Pair<Edge, Double>>> result = new Pair<>(null, new ArrayList<>());

        double probabilitiesSum = 0.0;

        double maxProbability = 0;

        Edge maxEdge = null;

        Iterator<Edge> edgesIterator = edgesList.iterator();

        if (edgesIterator.hasNext()) {
            double tau;

            while (edgesIterator.hasNext()) {
                final Edge edge = edgesIterator.next();

                if (hillId != null) {
                    synchronized (edge.id()) {
                        final Map<String, Pair<Integer, Double>> pheromones =
                                (Map<String, Pair<Integer, Double>>) edge.property(EDGE_PHEROMONE).orElseGet(HashMap::new);

                        pheromones.putIfAbsent(hillId, new Pair<>(0, tau0.doubleValue()));

                        pheromones.merge(hillId, new Pair<>(iteration, 0.0),
                                AntInspiredClusteringVertexProgram::applyEdgePheromone);

                        tau = pheromones.get(hillId).getValue1();

                        edge.property(EDGE_PHEROMONE, pheromones);
                    }

                    final double probability = Math.pow(tau, alpha.doubleValue()) *
                            Math.pow(1 / getWeight(edge).doubleValue(), beta.doubleValue());

                    if (probability > maxProbability) {
                        maxEdge = edge;
                        maxProbability = probability;
                    }

                    probabilitiesSum += probability;
                }
            }

            result = result.setAt0(maxEdge);

            edgesIterator = edgesList.iterator();

            while (edgesIterator.hasNext()) {
                final Edge edge = edgesIterator.next();

                if (hillId != null) {
                    synchronized (edge.id()) {
                        final Map<String, Pair<Integer, Double>> pheromones =
                                (Map<String, Pair<Integer, Double>>) edge.property(EDGE_PHEROMONE).value();

                        tau = pheromones.get(hillId).getValue1();
                    }

                    Double edgeProbability = probabilitiesSum == 0 ?
                            1.0 :
                            (Math.pow(tau, alpha.doubleValue()) * Math.pow(1 / getWeight(edge).doubleValue(), beta.doubleValue()))
                                    / probabilitiesSum;

                    result.getValue1().add(Pair.with(edge, edgeProbability));
                } else {
                    Double edgeProbability = edgesList.size() > 1 ?
                            (predecessor ?
                                    1.0 / (Double.valueOf(edgesList.size()) - 1.0)
                                    : 1.0 / Double.valueOf(edgesList.size()))
                            : 1.0;

                    result.getValue1().add(Pair.with(edge, edgeProbability));
                }
            }
        }

        return result;
    }

    private void spawnAnts(final Vertex vertex,
                           final Messenger<Ant> messenger,
                           final String hillId,
                           final String buddyId,
                           final int edgesCount,
                           final int iteration) throws InterruptedException {
        Iterator<Edge> edges = vertex.edges(Direction.BOTH);
        int column = 0;

        while (edges.hasNext()) {
            final List<Edge> columnEdges =
                    IteratorUtils.list(
                            IteratorUtils.limit(edges,
                                    (edgesCount - (column + 1) * columnSize) < columnSize / 2 ?
                                            (edgesCount - column * columnSize) : columnSize)
                    );

            final ArrayList<Pair<Double, Ant>> workerAntsWithChoices = new ArrayList<>();
            final ArrayList<Pair<Double, Ant>> foragerAntsWithChoices = new ArrayList<>();

            final int finalColumn = column;

            IntStream.rangeClosed(1, workerAntsNumber).forEach(i -> {
                        final Ant newAnt = Ant.of(String.valueOf(i), hillId, buddyId, finalColumn);
                        workerAntsWithChoices.add(new Pair<>(Math.random(), newAnt));
                    }
            );

            int foragingAntsNumber = (int) (workerAntsNumber * foragingAntsProportion.doubleValue());

            foragingAntsNumber = foragingAntsNumber == 0 ? 1 : foragingAntsNumber;

            IntStream.rangeClosed(1, foragingAntsNumber).forEach(i -> {
                        final Ant newAnt = Ant.of(String.valueOf(i), hillId, buddyId, finalColumn);
                        newAnt.setType(AntType.FORAGER);
                        foragerAntsWithChoices.add(new Pair<>(Math.random(), newAnt));
                    }
            );

            moveAntsForward(workerAntsWithChoices,
                    calculateProbabilities(columnEdges, vertex.id().toString(), false, iteration),
                    vertex,
                    messenger,
                    iteration);

            moveAntsForward(foragerAntsWithChoices,
                    calculateProbabilities(columnEdges, null, false, iteration),
                    vertex,
                    messenger,
                    iteration);

            column++;
        }
    }

    private void moveAntsForward(final ArrayList<Pair<Double, Ant>> antsWithChoices,
                                 final Pair<Edge, List<Pair<Edge, Double>>> edgesWithProbabilities,
                                 final Vertex vertex,
                                 final Messenger<Ant> messenger,
                                 final int iteration) {

        antsWithChoices.sort(Comparator.comparing(Pair::getValue0));

        final List<Pair<Edge, Double>> edgesProbabilities = edgesWithProbabilities.getValue1();

        if ((!edgesProbabilities.isEmpty()) && (!antsWithChoices.isEmpty())) {
            Double cumulativeProbability = 0.0;

            final Iterator<Pair<Double, Ant>> antsIterator = antsWithChoices.iterator();
            Iterator<Pair<Edge, Double>> edgesIterator = edgesProbabilities.iterator();
            Pair<Edge, Double> edgeProbability = null;

            do {
                final Pair<Double, Ant> antWithChoice = antsIterator.next();
                final double choice = Math.random();
                double antChoice = edgesWithProbabilities.getValue0() == null ?
                        antWithChoice.getValue0() :
                        choice < q0.doubleValue() ? -1.0 : antWithChoice.getValue0();

                do {
                    if ((edgesWithProbabilities.getValue0() == null) || (choice >= q0.doubleValue())) {
                        if ((edgesIterator.hasNext()) && (cumulativeProbability < antChoice)) {
                            edgeProbability = edgesIterator.next();

                            cumulativeProbability += edgeProbability.getValue1();
                        } else if ((edgesIterator.hasNext()) && (cumulativeProbability >= 1.0)) {
                            edgeProbability = edgesIterator.next();
                        }
                    }

                    final Edge edge = edgesWithProbabilities.getValue0() == null ?
                            edgeProbability.getValue0() :
                            choice < q0.doubleValue() ?
                                    edgesWithProbabilities.getValue0() : edgeProbability.getValue0();

                    if ((!edge.equals(antWithChoice.getValue1().getLastEdge())) && (cumulativeProbability >= antChoice)) {
                        Vertex otherV = edge.inVertex();

                        if (otherV.equals(vertex))
                            otherV = edge.outVertex();

                        if (antWithChoice.getValue1().type().equals(AntType.FORAGER))
                            antWithChoice.getValue1().setLastEdge(edge);

                        antWithChoice.getValue1().incDistance();

                        messenger.sendMessage(MessageScope.Global.of(otherV), antWithChoice.getValue1());

                        synchronized (edge.id()) {
                            final Map<String, Pair<Integer, Double>> pheromones =
                                    (Map<String, Pair<Integer, Double>>) edge.property(EDGE_PHEROMONE).orElseGet(HashMap::new);

                            pheromones.putIfAbsent(antWithChoice.getValue1().hillId(), new Pair<>(0, tau0.doubleValue()));

                            // local pheromone update
                            pheromones.merge(antWithChoice.getValue1().hillId(),
                                    new Pair<>(iteration, rho.doubleValue() * tau0.doubleValue()),
                                    AntInspiredClusteringVertexProgram::applyEdgePheromone);

                            edge.property(EDGE_PHEROMONE, pheromones);
                        }
                    } else if (cumulativeProbability >= antChoice) {
                        antChoice = cumulativeProbability + Math.ulp(cumulativeProbability);
                    } else if (!edgesIterator.hasNext()) {
                        edgesIterator = edgesProbabilities.iterator();
                    }
                } while ((antChoice > cumulativeProbability) && (edgesIterator.hasNext()));
            } while (antsIterator.hasNext());
        }
    }

    protected Number getWeight(final Edge edge) {
        return (null == this.weightTraversal ?
                1.0d : TraversalUtil.apply(edge, this.weightTraversal.get()).doubleValue());
    }

    protected String getCluster(final Vertex vertex) {
        return (null == this.clusterTraversal ?
                vertex.id().toString() : TraversalUtil.apply(vertex, this.clusterTraversal.get()).toString());
    }

    static class Ant {

        private final String id;
        private AntType type;
        private int distance = 0;
        private final String hillId;
        private final String buddyId;
        private final int column;
        private Edge lastEdge = null;

        private Ant(String id, String hillId, String buddyId, int column) {
            this.id = id;
            this.hillId = hillId;
            this.buddyId = buddyId;
            this.column = column;
            type = AntType.WORKER;
        }

        static Ant of(String id, String hillId, String buddyId, int column) {
            return new Ant(id, hillId, buddyId, column);
        }

        AntType type() {
            return type;
        }

        int distance() {
            return distance;
        }

        String id() {
            return id;
        }

        String hillId() {
            return hillId;
        }

        String buddyId() {
            return hillId;
        }

        int column() {
            return column;
        }

        public void setType(AntType type) {
            this.type = type;
        }

        private void setLastEdge(final Edge edge) {
            lastEdge = edge;
        }

        private Edge getLastEdge() {
            return lastEdge;
        }

        private void incDistance() { this.distance++; }

        private void resetDistance() { this.distance = 0; }

        @Override
        public String toString() {
            return "Ant " + id() + ": distance = " + distance + "; hill = " + hillId + "; type = " + type;
        }
    }

    enum AntType {
        WORKER,
        FORAGER
    }

    enum HillType {
        NONE,
        NEW,
        BUDDY
    }
    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    @SuppressWarnings("WeakerAccess")
    public static final class Builder extends AbstractVertexProgramBuilder<Builder> {


        private Builder() {
            super(AntInspiredClusteringVertexProgram.class);
        }

        public Builder weightProperty(final String distance) {
            return distance != null
                    ? weightTraversal(__.values(distance)) // todo: (Traversal) new ElementValueTraversal<>(distance)
                    : weightTraversal(DEFAULT_WEIGHT_TRAVERSAL.getPure());
        }

        public Builder weightTraversal(final Traversal<Edge, Number> distanceTraversal) {
            if (null == distanceTraversal) throw Graph.Exceptions.argumentCanNotBeNull("distanceTraversal");
            PureTraversal.storeState(this.configuration, WEIGHT_TRAVERSAL, distanceTraversal.asAdmin());
            return this;
        }

        public Builder alpha(final Number alpha) {
            if (null != alpha)
                this.configuration.setProperty(ALPHA, alpha);
            else
                this.configuration.clearProperty(ALPHA);
            return this;
        }

        public Builder beta(final Number beta) {
            if (null != beta)
                this.configuration.setProperty(BETA, beta);
            else
                this.configuration.clearProperty(BETA);
            return this;
        }

        public Builder rho(final Number rho) {
            if (null != rho)
                this.configuration.setProperty(RHO, rho);
            else
                this.configuration.clearProperty(RHO);
            return this;
        }

        public Builder mu(final Number mu) {
            if (null != mu)
                this.configuration.setProperty(MU, mu);
            else
                this.configuration.clearProperty(MU);
            return this;
        }

        public Builder q0(final Number q0) {
            if (null != q0)
                this.configuration.setProperty(Q0, q0);
            else
                this.configuration.clearProperty(Q0);
            return this;
        }

        public Builder tau0(final Number tau0) {
            if (null != tau0)
                this.configuration.setProperty(TAU0, tau0);
            else
                this.configuration.clearProperty(TAU0);
            return this;
        }

        public Builder theta0(final Number theta0) {
            if (null != theta0)
                this.configuration.setProperty(THETA0, theta0);
            else
                this.configuration.clearProperty(THETA0);
            return this;
        }

        public Builder foragingAnts(final Number proportion) {
            if (null != proportion)
                this.configuration.setProperty(FORAGING_ANTS_PROPORTION, proportion);
            else
                this.configuration.clearProperty(FORAGING_ANTS_PROPORTION);
            return this;
        }

        public Builder iterations(final Integer iterations) {
            if (null != iterations)
                this.configuration.setProperty(ITERATIONS, iterations);
            else
                this.configuration.clearProperty(ITERATIONS);
            return this;
        }

        public Builder antsNumber(final Integer ants) {
            if (null == ants) throw Graph.Exceptions.argumentCanNotBeNull("antsNumber");
            this.configuration.setProperty(WORKER_ANTS_NUMBER, ants);
            return this;
        }

        public Builder antHillThreshold(final Integer threshold) {
            if (null == threshold) throw Graph.Exceptions.argumentCanNotBeNull("antHillThreshold");
            this.configuration.setProperty(ANT_HILL_THRESHOLD, threshold);
            return this;
        }

        public Builder antBuddyHillThreshold(final Integer threshold) {
            if (null == threshold) throw Graph.Exceptions.argumentCanNotBeNull("antBuddyHillThreshold");
            this.configuration.setProperty(ANT_BUDDYHILL_THRESHOLD, threshold);
            return this;
        }

        public Builder antNewHillThreshold(final Integer threshold) {
            if (null == threshold) throw Graph.Exceptions.argumentCanNotBeNull("antNewHillThreshold");
            this.configuration.setProperty(ANT_NEWHILL_THRESHOLD, threshold);
            return this;
        }

        public Builder buddyHillDistance(final Integer threshold) {
            if (null == threshold) throw Graph.Exceptions.argumentCanNotBeNull("buddyHillDistance");
            this.configuration.setProperty(BUDDYHILL_DISTANCE, threshold);
            return this;
        }

        public Builder newHillDistance(final Integer threshold) {
            if (null == threshold) throw Graph.Exceptions.argumentCanNotBeNull("newHillDistance");
            this.configuration.setProperty(NEWHILL_DISTANCE, threshold);
            return this;
        }

        public Builder columnSize(final Integer size) {
            if (null == size) throw Graph.Exceptions.argumentCanNotBeNull("columnSize");
            this.configuration.setProperty(COLUMN_SIZE, size);
            return this;
        }
    }

    ////////////////////////////

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresGlobalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return false;
            }
        };
    }
}