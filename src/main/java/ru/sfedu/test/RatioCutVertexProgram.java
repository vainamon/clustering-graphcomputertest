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
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;

public class RatioCutVertexProgram implements VertexProgram<Pair<Serializable, Double>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RatioCutVertexProgram.class);

    @SuppressWarnings("WeakerAccess")
    public static final String RATIO_CUT = "ru.sfedu.test.RatioCutVertexProgram.ratioCut";

    public static final String CUT_EDGES_WEIGHT_COUNT_PROPERTY = "ru.sfedu.test.RatioCutVertexProgram.cutEdgesWeightProperty";

    private static final String EDGE_TRAVERSAL = "ru.sfedu.test.RatioCutVertexProgram.edgeTraversal";
    private static final String WEIGHT_TRAVERSAL = "ru.sfedu.test.RatioCutVertexProgram.weightTraversal";
    private static final String CLUSTER_TRAVERSAL = "ru.sfedu.test.RatioCutVertexProgram.clusterTraversal";

    private static final PureTraversal<Vertex, Edge> DEFAULT_EDGE_TRAVERSAL = new PureTraversal<>(bothE().asAdmin());

    private static final PureTraversal<Edge, Number> DEFAULT_WEIGHT_TRAVERSAL = new PureTraversal<>(
            __.<Edge>start().<Number>constant(1.0d).asAdmin()); // todo: new ConstantTraversal<>(1)

    private static final PureTraversal<Element, Object> DEFAULT_CLUSTER_TRAVERSAL
            = new PureTraversal<>(__.values(PeerPressureVertexProgram.CLUSTER).asAdmin());

    private PureTraversal<Vertex, Edge> edgeTraversal = DEFAULT_EDGE_TRAVERSAL.clone();
    private PureTraversal<Edge, Number> weightTraversal = DEFAULT_WEIGHT_TRAVERSAL.clone();
    private PureTraversal<Element, Object> clusterTraversal = DEFAULT_CLUSTER_TRAVERSAL.clone();

    private static final String DEFAULT_CLUSTER_PROPERTY = PeerPressureVertexProgram.CLUSTER;
    private static String clusterProperty = DEFAULT_CLUSTER_PROPERTY;

    private final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = Collections.singleton(VertexComputeKey.of(CUT_EDGES_WEIGHT_COUNT_PROPERTY, false));

    private final Set<MemoryComputeKey> memoryComputeKeys = new HashSet<>();

    private RatioCutVertexProgram() {

    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {

        if (configuration.containsKey(EDGE_TRAVERSAL))
            this.edgeTraversal = PureTraversal.loadState(configuration, EDGE_TRAVERSAL, graph);

        if (configuration.containsKey(WEIGHT_TRAVERSAL))
            this.weightTraversal = PureTraversal.loadState(configuration, WEIGHT_TRAVERSAL, graph);

        if (configuration.containsKey(CLUSTER_TRAVERSAL))
            this.clusterTraversal = PureTraversal.loadState(configuration, CLUSTER_TRAVERSAL, graph);
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);

        this.edgeTraversal.storeState(configuration, EDGE_TRAVERSAL);
        this.weightTraversal.storeState(configuration, WEIGHT_TRAVERSAL);
        this.clusterTraversal.storeState(configuration, CLUSTER_TRAVERSAL);
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
    public VertexProgram<Pair<Serializable, Double>> clone() {
        try {
            final RatioCutVertexProgram clone = (RatioCutVertexProgram) super.clone();

            if (null != this.edgeTraversal)
                clone.edgeTraversal = this.edgeTraversal.clone();
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
    public Set<MapReduce> getMapReducers() {
        Set<MapReduce> mapReducers = new HashSet<>();
        mapReducers.add(RatioCutMapReduce.build().clusterProperty(clusterProperty).create());
        return mapReducers;
    }


    @Override
    public void setup(final Memory memory) {
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<Pair<Serializable, Double>> messenger, final Memory memory) {

        if (memory.isInitialIteration()) {
            TraversalUtil.applyAll(vertex,
                    this.edgeTraversal.get()).
                    forEachRemaining(edge -> {
                        Vertex otherV = edge.inVertex();

                        if (otherV.equals(vertex))
                            otherV = edge.outVertex();

                        messenger.sendMessage(MessageScope.Global.of(otherV),
                                Pair.with(getCluster(vertex),
                                        getWeight(edge).doubleValue()
                                ));
                    });
        } else {
            final Iterator<Pair<Serializable, Double>> messagesIterator = messenger.receiveMessages();

            double cutEdgesWeight = 0.0d;

            while (messagesIterator.hasNext()) {
                final Pair<Serializable, Double> nextMsg = messagesIterator.next();

                if (!(nextMsg.getValue0().equals(getCluster(vertex))))
                    cutEdgesWeight += nextMsg.getValue1();
            }

            vertex.property(VertexProperty.Cardinality.single, CUT_EDGES_WEIGHT_COUNT_PROPERTY, cutEdgesWeight);
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        if (memory.isInitialIteration()) {
            return false;
        } else
            return true;
    }

    protected Number getWeight(final Edge edge) {
        return (null == this.weightTraversal ?
                1.0d : TraversalUtil.apply(edge, this.weightTraversal.get()).doubleValue());
    }

    protected Serializable getCluster(final Vertex vertex) {
        return (null == this.clusterTraversal ?
                vertex.id().toString() : TraversalUtil.apply(vertex, this.clusterTraversal.get()).toString());
    }

    @Override
    public String toString() {

        final List<String> options = new ArrayList<>();
        final Function<String, String> shortName = name -> name.substring(name.lastIndexOf(".") + 1);

        if (!this.edgeTraversal.equals(DEFAULT_EDGE_TRAVERSAL)) {
            options.add(shortName.apply(EDGE_TRAVERSAL) + "=" + this.edgeTraversal.get());
        }

        if (!this.weightTraversal.equals(DEFAULT_WEIGHT_TRAVERSAL)) {
            options.add(shortName.apply(WEIGHT_TRAVERSAL) + "=" + this.weightTraversal.get());
        }

        return StringFactory.vertexProgramString(this, String.join(", ", options));
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    @SuppressWarnings("WeakerAccess")
    public static final class Builder extends AbstractVertexProgramBuilder<Builder> {


        private Builder() {
            super(RatioCutVertexProgram.class);
        }

        public Builder weightProperty(final String weight) {
            return weight != null
                    ? weightTraversal(__.values(weight)) // todo: (Traversal) new ElementValueTraversal<>(weight)
                    : weightTraversal(DEFAULT_WEIGHT_TRAVERSAL.getPure());
        }

        public Builder weightTraversal(final Traversal<Edge, Number> weightTraversal) {
            if (null == weightTraversal) throw Graph.Exceptions.argumentCanNotBeNull("weightTraversal");
            PureTraversal.storeState(this.configuration, WEIGHT_TRAVERSAL, weightTraversal.asAdmin());
            return this;
        }

        public Builder clusterProperty(final String cluster) {
            clusterProperty = cluster;
            return cluster != null
                    ? clusterTraversal(__.values(cluster)) // todo: (Traversal) new ElementValueTraversal<>(cluster)
                    : clusterTraversal(DEFAULT_CLUSTER_TRAVERSAL.getPure());
        }

        public Builder clusterTraversal(final Traversal<Element, Object> clusterTraversal) {
            if (null == clusterTraversal) throw Graph.Exceptions.argumentCanNotBeNull("clusterTraversal");
            PureTraversal.storeState(this.configuration, CLUSTER_TRAVERSAL, clusterTraversal.asAdmin());
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
            public boolean requiresLocalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }
}