package com.github.ramiyer.nicety;

import com.google.common.base.Objects;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DirectedGraph<T>
{

    public static class Edge<T>
    {

        public final T source;

        public final T sink;

        public Edge (T source, T sink)
        {
            this.source = source;
            this.sink = sink;
        }

        @Override
        public boolean equals (Object other)
        {
            if (!(other instanceof Edge)) {
                return false;
            }
            Edge otherEdge = (Edge) other;
            return source.equals(otherEdge.source) && sink.equals(otherEdge.sink);
        }

        @Override
        public int hashCode ()
        {
            return Objects.hashCode(source, sink);
        }
    }

    private static <T> Set<T> generateConcurrentSet ()
    {
        return Collections.newSetFromMap(new ConcurrentHashMap<T, Boolean>());
    }

    private static class Vertex<T>
    {

        private final T key;

        private final Set<T> sourceEdges;

        private final Set<T> sinkEdges;

        Vertex (T key)
        {
            this.key = key;
            this.sourceEdges = generateConcurrentSet();
            this.sinkEdges = generateConcurrentSet();
        }
    }

    private final ConcurrentHashMap<T, Vertex<T>> vertexNodes;

    public DirectedGraph ()
    {
        this.vertexNodes = new ConcurrentHashMap<>();
    }

    /**
     * Public API methods.
     */

    /**
     * Retrieve the set of vertexNodes in the graph.
     *
     * @return set of vertexNodes
     */
    public Set<T> getVertexNodes ()
    {
        return vertexNodes.keySet();
    }

    /**
     * Retrieve the source edges for a particular node in the graph.
     *
     * @param key
     *         name of a graph node
     *
     * @return either a set of edges or null
     */
    public Set<T> getSourceEdges (T key)
    {
        Vertex<T> vertex = vertexNodes.get(key);
        if (vertex != null) {
            return vertex.sourceEdges;
        }
        else {
            return null;
        }
    }

    /**
     * Retrieve the sink edges for a particular node in the graph.
     *
     * @param key
     *         name of a graph node
     *
     * @return either a set of edges or null
     */
    public Set<T> getSinkEdges (T key)
    {
        Vertex<T> vertex = vertexNodes.get(key);
        if (vertex != null) {
            return vertex.sinkEdges;
        }
        else {
            return null;
        }
    }

    public Set<Edge<T>> getAllEdges (Set<T> keys)
    {
        Set<Edge<T>> retval = new HashSet<>();
        for (T key : keys) {
            Vertex<T> vertex = vertexNodes.get(key);
            if (vertex != null) {
                for (T sink : vertex.sinkEdges) {
                    Edge<T> edge = new Edge<>(key, sink);
                    retval.add(edge);
                }
                for (T source : vertex.sourceEdges) {
                    Edge<T> edge = new Edge<>(source, key);
                    retval.add(edge);
                }
            }
        }

        return retval;
    }

    /**
     * Add a new node to the graph.
     *
     * @param key
     *
     * @return true if the graph did not previously contain the node.
     */
    public boolean addNode (T key)
    {
        return (vertexNodes.putIfAbsent(key, new Vertex(key)) == null);
    }

    /**
     * Add a directed edge to the graph.
     *
     * @param source
     *         edge source
     * @param sink
     *         edge sink
     */
    public void addEdge (T source, T sink)
    {
        addEdgeHelper(source, sink, true);
        addEdgeHelper(sink, source, false);
    }

    /**
     * Remove a directed edge from the graph.
     *
     * @param source
     *         edge source
     * @param sink
     *         edge sink
     */
    public void removeEdge (T source, T sink)
    {
        Vertex<T> vertex = vertexNodes.get(source);

        if (vertex != null) {
            vertex.sinkEdges.remove(sink);
        }

        vertex = vertexNodes.get(sink);

        if (vertex != null) {
            vertex.sourceEdges.remove(source);
        }
    }

    /**
     * Remove a node from the graph and its associated edges.
     *
     * @param key
     *
     * @return true if the graph previously contained the node
     */
    public boolean removeNode (T key)
    {
        Vertex<T> vertex = vertexNodes.remove(key);

        if (vertex == null) {
            return false;
        }

        for (T sink : vertex.sinkEdges) {
            Vertex<T> sinkVertex = vertexNodes.get(sink);
            if (sinkVertex != null) {
                sinkVertex.sourceEdges.remove(key);
            }
        }

        for (T source : vertex.sourceEdges) {
            Vertex<T> sourceVertex = vertexNodes.get(source);
            if (sourceVertex != null) {
                sourceVertex.sinkEdges.remove(key);
            }
        }

        return true;
    }

    public Set<T> sinksClosure (T key)
    {
        Set<T> retval = new HashSet<>();
        Set<Vertex<T>> workSet = new HashSet<>();
        Vertex<T> root = vertexNodes.get(key);

        if (root != null) {
            workSet.add(root);
        }

        while (!workSet.isEmpty()) {
            Vertex<T> vertex = workSet.iterator().next();
            retval.add(vertex.key);
            workSet.remove(vertex);
            for (T sink : vertex.sinkEdges) {
                if (!retval.contains(sink)) {
                    Vertex<T> sinkVertex = vertexNodes.get(sink);
                    if (sinkVertex != null) {
                        workSet.add(sinkVertex);
                    }
                }
            }
        }

        return retval;
    }

    public Set<T> sourcesClosure (T key)
    {
        Set<T> retval = new HashSet<>();
        Set<Vertex<T>> workSet = new HashSet<>();
        Vertex<T> root = vertexNodes.get(key);

        if (root != null) {
            workSet.add(root);
        }

        while (!workSet.isEmpty()) {
            Vertex<T> vertex = workSet.iterator().next();
            retval.add(vertex.key);
            workSet.remove(vertex);
            for (T source : vertex.sourceEdges) {
                if (!retval.contains(source)) {
                    Vertex<T> sourceVertex = vertexNodes.get(source);
                    if (sourceVertex != null) {
                        workSet.add(sourceVertex);
                    }
                }
            }
        }

        return retval;
    }

    public Set<T> transitiveClosure (T key)
    {
        Set<T> retval = new HashSet<>();
        Set<Vertex<T>> workSet = new HashSet<>();
        Vertex<T> root = vertexNodes.get(key);

        if (root != null) {
            workSet.add(root);
        }

        while (!workSet.isEmpty()) {
            Vertex<T> vertex = workSet.iterator().next();
            retval.add(vertex.key);
            workSet.remove(vertex);
            for (T source : vertex.sourceEdges) {
                if (!retval.contains(source)) {
                    Vertex<T> sourceVertex = vertexNodes.get(source);
                    if (sourceVertex != null) {
                        workSet.add(sourceVertex);
                    }
                }
            }
            for (T sink : vertex.sinkEdges) {
                if (!retval.contains(sink)) {
                    Vertex<T> sinkVertex = vertexNodes.get(sink);
                    if (sinkVertex != null) {
                        workSet.add(sinkVertex);
                    }
                }
            }
        }

        return retval;
    }

    /**
     * Tarjan's Algorithm is a graph theory algorithm for finding the strongly
     * connected components of a graph. The algorithm takes a directed graph as input,
     * and produces a partition of the graph's vertices into the graph's strongly
     * connected components.
     * <p>
     * Any set of two or more strongly connected components is a set of vertexNodes that form
     * a cycle in the graph. A strongly connected set of a single node may or may
     * not be a cycle in a graph.
     */
    class TarjansAlgorithm
    {

        private int index;
        private final Map<T, Integer> indices;
        private final Map<T, Integer> lowlinks;
        private final Deque<Vertex<T>> stack;
        private final boolean forward;

        /**
         * Set membership for the stack object.
         * For constant-time stack membership test.
         */
        private final Set<Vertex<T>> inStack;

        private final Set<T> keys;
        private final Set<Set<T>> components;

        TarjansAlgorithm (boolean forward)
        {
            this(vertexNodes.keySet(), forward);
        }

        TarjansAlgorithm (Set<T> keys, boolean forward)
        {
            this.keys = keys;
            this.forward = forward;
            this.indices = new HashMap<>();
            this.lowlinks = new HashMap<>();
            this.stack = new ArrayDeque<>();
            this.inStack = new HashSet<>();
            this.components = new HashSet<>();
        }

        Set<Set<T>> generateComponents ()
        {
            for (T key : keys) {
                Vertex<T> vertex = vertexNodes.get(key);
                if (vertex != null && !indices.containsKey(key)) {
                    strongConnect(vertex);
                }
            }
            return components;
        }

        private void strongConnect (Vertex<T> vertex)
        {
            T key = vertex.key;
            indices.put(key, index);
            lowlinks.put(key, index);
            index++;
            stack.push(vertex);
            inStack.add(vertex);

            Iterator<T> iterator;

            if (forward) {
                iterator = vertex.sinkEdges.iterator();
            }
            else {
                iterator = vertex.sourceEdges.iterator();
            }

            while (iterator.hasNext()) {
                T next = iterator.next();
                Vertex<T> nextVertex = vertexNodes.get(next);
                if (nextVertex != null) {
                    if (!indices.containsKey(next)) {
                        strongConnect(nextVertex);
                        lowlinks.put(key, Math.min(lowlinks.get(key),
                                                   lowlinks.get(next)));
                    }
                    else if (inStack.contains(nextVertex)) {
                        lowlinks.put(key, Math.min(lowlinks.get(key),
                                                   indices.get(next)));
                    }
                }
            }

            int index = indices.get(key);
            int lowlink = lowlinks.get(key);

            if (index == lowlink) {
                Set<T> newComponent = new HashSet<>();
                T next;
                do {
                    Vertex<T> nextVertex = stack.pop();
                    inStack.remove(nextVertex);
                    next = nextVertex.key;
                    newComponent.add(next);
                }
                while (!next.equals(key));
                components.add(newComponent);
            }
        }
    }

    /**
     * returns all the cycles in the graph that
     * are forward reachable from the key.
     *
     * @param key
     *
     * @return a set of cycles in the graph
     */
    public Set<Set<T>> sinksCycles (T key)
    {
        Set<Set<T>> strongComponents = cyclesHelper(sinksClosure(key), true);
        return (removeNonCycles(strongComponents));
    }

    /**
     * returns all the cycles in the graph that
     * are backward reachable from the key.
     *
     * @param key
     *
     * @return a set of cycles in the graph
     */
    public Set<Set<T>> sourcesCycles (T key)
    {
        Set<Set<T>> strongComponents = cyclesHelper(sourcesClosure(key), false);
        return (removeNonCycles(strongComponents));
    }

    /**
     * returns all the cycles in the graph,
     *
     * @return a set of cycles in the graph
     */
    public Set<Set<T>> allCycles ()
    {
        Set<Set<T>> strongComponents = cyclesHelper(vertexNodes.keySet(), true);
        return (removeNonCycles(strongComponents));
    }

    public Set<Set<T>> stronglyConnectedComponents ()
    {
        Set<Set<T>> strongComponents = cyclesHelper(vertexNodes.keySet(), true);
        return strongComponents;
    }

    /**
     * Private helper methods.
     */

    private Set<Set<T>> cyclesHelper (Set<T> domain, boolean forward)
    {
        TarjansAlgorithm tarjan = new TarjansAlgorithm(domain, forward);
        Set<Set<T>> strongComponents = tarjan.generateComponents();
        return strongComponents;
    }

    private Set<Set<T>> removeNonCycles (Set<Set<T>> strongComponents)
    {
        Iterator<Set<T>> iterator = strongComponents.iterator();
        while (iterator.hasNext()) {
            Set<T> component = iterator.next();
            if (component.size() == 1) {
                T single = component.iterator().next();
                Vertex<T> vertex = vertexNodes.get(single);
                if (vertex == null || !vertex.sinkEdges.contains(single)) {
                    iterator.remove();
                }
            }
        }
        return strongComponents;
    }

    private void addEdgeHelper (T nodeGet, T nodePut, boolean sinkEdges)
    {
        Vertex<T> vertex = vertexNodes.get(nodeGet);
        if (vertex == null) {
            Vertex<T> newVertex = new Vertex(nodeGet);
            Vertex<T> prevVertex = vertexNodes.putIfAbsent(nodeGet, newVertex);
            vertex = (prevVertex == null) ? newVertex : prevVertex;
        }
        if (sinkEdges) {
            vertex.sinkEdges.add(nodePut);
        }
        else {
            vertex.sourceEdges.add(nodePut);
        }
    }

    /**
     * Methods with package level visibility. Intended for unit testing.
     */

    boolean testEdge (T source, T sink)
    {
        Vertex<T> vertex = vertexNodes.get(source);
        if (vertex == null) {
            return false;
        }
        return vertex.sinkEdges.contains(sink);
    }

    boolean testEdgeReverse (T source, T sink)
    {
        Vertex<T> vertex = vertexNodes.get(sink);
        if (vertex == null) {
            return false;
        }
        return vertex.sourceEdges.contains(source);
    }
}