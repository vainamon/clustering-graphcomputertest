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
public class RatioCutMapReduce extends StaticMapReduce<Serializable, Double, Serializable, Pair<Double, Long>, Double> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RatioCutMapReduce.class);

    public static final String CUT_MEMORY_KEY = "ru.sfedu.test.RatioCutMapReduce.cutMemoryKey";

    private static final String CLUSTER_PROPERTY = "ru.sfedu.test.RatioCutMapReduce.clusterProperty";
    private static final String CUT_EDGES_WEIGHT_PROPERTY = "ru.sfedu.test.RatioCutMapReduce.cutEdgesWeightProperty";

    private static final String DEFAULT_MEMORY_KEY = RatioCutVertexProgram.RATIO_CUT;
    private static final String DEFAULT_CLUSTER_PROPERTY = PeerPressureVertexProgram.CLUSTER;
    private static final String DEFAULT_CUT_EDGES_WEIGHT_PROPERTY = RatioCutVertexProgram.CUT_EDGES_WEIGHT_COUNT_PROPERTY;

    private String memoryKey = DEFAULT_MEMORY_KEY;
    private String clusterProperty = DEFAULT_CLUSTER_PROPERTY;
    private String cutEdgesWeightProperty = DEFAULT_CUT_EDGES_WEIGHT_PROPERTY;

    private RatioCutMapReduce() {
    }

    private RatioCutMapReduce(final String memoryKey, final String clusterProperty, final String cutEdgesWeightProperty) {
        this.memoryKey = memoryKey;
        this.clusterProperty = clusterProperty;
        this.cutEdgesWeightProperty = cutEdgesWeightProperty;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(CUT_MEMORY_KEY, this.memoryKey);
        configuration.setProperty(CLUSTER_PROPERTY, this.clusterProperty);
        configuration.setProperty(CUT_EDGES_WEIGHT_PROPERTY, this.cutEdgesWeightProperty);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.memoryKey = configuration.getString(CUT_MEMORY_KEY, DEFAULT_MEMORY_KEY);
        this.clusterProperty = configuration.getString(CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY);
        this.cutEdgesWeightProperty = configuration.getString(CUT_EDGES_WEIGHT_PROPERTY, DEFAULT_CUT_EDGES_WEIGHT_PROPERTY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Serializable, Double> emitter) {
        final Property<Serializable> cluster = vertex.property(this.clusterProperty);
        final Property<Double> cutEdgesWeight = vertex.property(this.cutEdgesWeightProperty);

        if (cluster.isPresent()) {
            emitter.emit(cluster.value(), cutEdgesWeight.value());
        }
    }

    @Override
    public void combine(final Serializable key, final Iterator<Double> values, final ReduceEmitter<Serializable, Pair<Double, Long>> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Double> values, final ReduceEmitter<Serializable, Pair<Double, Long>> emitter) {
        long count = 0l;
        double cutEdgesWeight = 0.0d;

        while (values.hasNext()) {
            count++;
            cutEdgesWeight += values.next();
        }

        emitter.emit(key, Pair.with(cutEdgesWeight, count));
    }

    @Override
    public Double generateFinalResult(final Iterator<KeyValue<Serializable, Pair<Double, Long>>> keyValues) {
        Double ratioCut = 0.0d;

        while (keyValues.hasNext()) {
            Pair<Double, Long> resultPair = keyValues.next().getValue();

            if (resultPair.getValue1() != 0)
                ratioCut += resultPair.getValue0() / Double.valueOf(resultPair.getValue1());
        }

        return 0.5 * ratioCut;
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

        public RatioCutMapReduce create() {
            return new RatioCutMapReduce(this.memoryKey, this.clusterProperty, this.cutEdgesWeightProperty);
        }

    }
}