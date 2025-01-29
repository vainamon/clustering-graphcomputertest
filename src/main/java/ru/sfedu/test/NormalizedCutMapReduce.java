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

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration2.Configuration;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Danilov Igor
 */
public class NormalizedCutMapReduce extends StaticMapReduce<Serializable, Pair<Double, Double>, Serializable, Pair<Double, Double>, Double> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizedCutMapReduce.class);

    public static final String CUT_MEMORY_KEY = "ru.sfedu.test.NormalizedCutMapReduce.cutMemoryKey";

    private static final String CLUSTER_PROPERTY = "ru.sfedu.test.NormalizedCutMapReduce.clusterProperty";
    private static final String CUT_EDGES_WEIGHT_PROPERTY = "ru.sfedu.test.NormalizedCutMapReduce.cutEdgesWeightProperty";
    private static final String VERTEX_DEGREE_PROPERTY = "ru.sfedu.test.NormalizedCutMapReduce.vertexDegreeProperty";

    private static final String DEFAULT_MEMORY_KEY = NormalizedCutVertexProgram.NORMALIZED_CUT;
    private static final String DEFAULT_CLUSTER_PROPERTY = PeerPressureVertexProgram.CLUSTER;
    private static final String DEFAULT_CUT_EDGES_WEIGHT_PROPERTY = NormalizedCutVertexProgram.CUT_EDGES_WEIGHT_PROPERTY;
    private static final String DEFAULT_VERTEX_DEGREE_PROPERTY = NormalizedCutVertexProgram.VERTEX_DEGREE_PROPERTY;

    private String memoryKey = DEFAULT_MEMORY_KEY;
    private String clusterProperty = DEFAULT_CLUSTER_PROPERTY;
    private String cutEdgesWeightProperty = DEFAULT_CUT_EDGES_WEIGHT_PROPERTY;
    private String vertexDegreeProperty = DEFAULT_VERTEX_DEGREE_PROPERTY;

    private NormalizedCutMapReduce() {
    }

    private NormalizedCutMapReduce(final String memoryKey, final String clusterProperty,
                                   final String cutEdgesWeightProperty, final String vertexDegreeProperty) {
        this.memoryKey = memoryKey;
        this.clusterProperty = clusterProperty;
        this.cutEdgesWeightProperty = cutEdgesWeightProperty;
        this.vertexDegreeProperty = vertexDegreeProperty;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(CUT_MEMORY_KEY, this.memoryKey);
        configuration.setProperty(CLUSTER_PROPERTY, this.clusterProperty);
        configuration.setProperty(CUT_EDGES_WEIGHT_PROPERTY, this.cutEdgesWeightProperty);
        configuration.setProperty(VERTEX_DEGREE_PROPERTY, this.vertexDegreeProperty);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.memoryKey = configuration.getString(CUT_MEMORY_KEY, DEFAULT_MEMORY_KEY);
        this.clusterProperty = configuration.getString(CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY);
        this.cutEdgesWeightProperty = configuration.getString(CUT_EDGES_WEIGHT_PROPERTY, DEFAULT_CUT_EDGES_WEIGHT_PROPERTY);
        this.vertexDegreeProperty = configuration.getString(VERTEX_DEGREE_PROPERTY, DEFAULT_VERTEX_DEGREE_PROPERTY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Serializable, Pair<Double, Double>> emitter) {
        final Property<Serializable> cluster = vertex.property(this.clusterProperty);
        final Property<Double> cutEdgesWeight = vertex.property(this.cutEdgesWeightProperty);
        final Property<Double> vertexDegree = vertex.property(this.vertexDegreeProperty);

        if (cluster.isPresent()) {
            emitter.emit(cluster.value(), Pair.with(cutEdgesWeight.value(), vertexDegree.value()));
        }
    }

    @Override
    public void combine(final Serializable key, final Iterator<Pair<Double, Double>> values, final ReduceEmitter<Serializable, Pair<Double, Double>> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Pair<Double, Double>> values, final ReduceEmitter<Serializable, Pair<Double, Double>> emitter) {
        double cutEdgesWeight = 0.0d;
        double volume = 0.0d;

        while (values.hasNext()) {
            Pair<Double, Double> resultPair = values.next();

            cutEdgesWeight += resultPair.getValue0();
            volume += resultPair.getValue1();
        }

        emitter.emit(key, Pair.with(cutEdgesWeight, volume));
    }

    @Override
    public Double generateFinalResult(final Iterator<KeyValue<Serializable, Pair<Double, Double>>> keyValues) {
        Double normalizedCut = 0.0d;

        while (keyValues.hasNext()) {
            Pair<Double, Double> resultPair = keyValues.next().getValue();

            if (resultPair.getValue1() != 0)
                normalizedCut += resultPair.getValue0() / resultPair.getValue1();
        }

        return 0.5 * normalizedCut;
    }

    @Override
    public String getMemoryKey() {
        return this.memoryKey;
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.memoryKey);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {

        private String memoryKey = DEFAULT_MEMORY_KEY;
        private String clusterProperty = DEFAULT_CLUSTER_PROPERTY;
        private String cutEdgesWeightProperty = DEFAULT_CUT_EDGES_WEIGHT_PROPERTY;
        private String vertexDegreeProperty = DEFAULT_VERTEX_DEGREE_PROPERTY;

        private Builder() {

        }

        public Builder memoryKey(final String memoryKey) {
            this.memoryKey = memoryKey;
            return this;
        }

        public Builder clusterProperty(final String clusterProperty) {
            this.clusterProperty = clusterProperty;
            return this;
        }

        public Builder cutEdgesWeightProperty(final String cutEdgesWeightProperty) {
            this.cutEdgesWeightProperty = cutEdgesWeightProperty;
            return this;
        }

        public Builder vertexDegreeProperty(final String vertexDegreeProperty) {
            this.vertexDegreeProperty = vertexDegreeProperty;
            return this;
        }

        public NormalizedCutMapReduce create() {
            return new NormalizedCutMapReduce(this.memoryKey, this.clusterProperty,
                    this.cutEdgesWeightProperty, this.vertexDegreeProperty);
        }

    }
}