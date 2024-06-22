/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.optimizer.join;

import ibd.query.Operation;
import ibd.query.binaryop.join.Join;
import ibd.query.binaryop.join.JoinPredicate;
import ibd.query.binaryop.join.NestedLoopJoin;
import ibd.query.binaryop.join.JoinTerm;
import ibd.query.sourceop.FullTableScan;

import java.util.*;

/**
 *
 * @author Sergio
 */
public class RafaelCarneiroPregardierQueryOptimizer implements QueryOptimizer{

    Graph graph = new Graph();
    List<Edge> edges = new ArrayList<>();
    List<Vertex> visitados = new ArrayList<>();

    private void dfs(Vertex v) {
        visitados.add(v);
        Iterator<Edge> it = v.getEdges();
        while (it.hasNext()) {
            Edge e = it.next();
            Vertex w = e.destination;
            if (!visitados.contains(w)) {
                dfs(w);
            }
            if (!edges.contains(e)) {
                edges.add(e);
            }
        }
    }

    private Vertex findVertexWithNoIncomingEdges() {
        for (Vertex v : graph.V) {
            boolean hasIncomingEdge = false;
            for (Vertex u : graph.V) {
                if (u != v) {
                    for (Iterator<Edge> it = u.getEdges(); it.hasNext(); ) {
                        Edge e = it.next();
                        if (e.destination == v) {
                            hasIncomingEdge = true;
                            break;
                        }
                    }
                }
                if (hasIncomingEdge) {
                    break;
                }
            }
            if (!hasIncomingEdge) {
                return v;
            }
        }
        return null;
    }


    private Vertex findStartVertex() throws Exception {
        Vertex startVertex = findVertexWithNoIncomingEdges();
        if (startVertex == null) {
            throw new Exception("No vertex with no incoming edges found");
        }
        return startVertex;
    }


    private JoinPredicate createJoinPredicate(Edge edge, Vertex vertex, boolean secondJoin) {
        JoinPredicate terms = new JoinPredicate();

        if (!secondJoin) {
            for (JoinTerm jt : edge.terms) {
                if (jt.getLeftTableAlias().equals(vertex.toString())) {
                    terms.addTerm(jt.getLeftTableAlias(), jt.getLeftColumn(), jt.getRightTableAlias(), jt.getRightColumn());
                } else {
                    terms.addTerm(jt.getRightTableAlias(), jt.getRightColumn(), jt.getLeftTableAlias(), jt.getLeftColumn());
                }
            }
        } else {
            for (JoinTerm jt : edge.terms) {
                if (jt.getRightTableAlias().equals(edge.destination.toString())) {
                    terms.addTerm(jt.getLeftTableAlias(), jt.getLeftColumn(), jt.getRightTableAlias(), jt.getRightColumn());
                } else {
                    terms.addTerm(jt.getRightTableAlias(), jt.getRightColumn(), jt.getLeftTableAlias(), jt.getLeftColumn());
                }
            }
        }
        return terms;
    }


    private JoinPredicate processEdges(Operation lastJoin, Edge e) {
            for (Iterator<Edge> it3 = e.destination.getEdges(); it3.hasNext(); ) {
                Edge e2 = it3.next();
                if (!edges.contains(e2)) {
                    edges.add(e2);
                }
            }

        return createJoinPredicate(e, e.destination, true);
    }

    
    @Override
    public Operation optimizeQuery(Operation query) throws Exception {
        buildGraph(query);
        Operation lastJoin = null;

        Vertex startVertex = findStartVertex();

        Iterator<Edge> it = startVertex.getEdges();
        Edge firstEdge = it.next();

        while (it.hasNext()) {
            Edge e = it.next();
            edges.add(e);
        }

        JoinPredicate terms = createJoinPredicate(firstEdge, startVertex, false);

        lastJoin = new NestedLoopJoin(startVertex.getScan(), firstEdge.destination.getScan(), terms);

        dfs(firstEdge.destination);

        while (!edges.isEmpty()) {
            Edge e = edges.getFirst();
            edges.remove(e);

            JoinPredicate terms2 = processEdges(lastJoin, e);
            lastJoin = new NestedLoopJoin(lastJoin, e.destination.getScan(), terms2);

        }

        return lastJoin;
    }

    private void buildGraph(Operation query) throws Exception {
        buildVertices(query);
        buildEdges(query);
    }


    private void buildEdges(Operation op) throws Exception {

        if (op instanceof NestedLoopJoin) {
            NestedLoopJoin join = (NestedLoopJoin) op;
            List<JoinTerm> terms = join.getTerms();
            for (JoinTerm term : terms) {
                Vertex v1 = graph.getVertexByName(term.getLeftTableAlias());
                Vertex v2 = graph.getVertexByName(term.getRightTableAlias());

                List<String> pks = v1.getScan().table.getPrototype().getPKColumns();
                if (pks.contains(term.getLeftColumn()) && pks.size() == 1) {
                    v2.addEdge(v1, term);
                }

                pks = v2.getScan().table.getPrototype().getPKColumns();
                if (pks.contains(term.getRightColumn()) && pks.size() == 1) {
                    v1.addEdge(v2, term);
                }
            }
            buildEdges(join.getLeftOperation());
            buildEdges(join.getRightOperation());
        }
    }

    private void buildVertices(Operation op) throws Exception {

        if (op instanceof FullTableScan) {
            FullTableScan ts = (FullTableScan) op;
            graph.addVertex(ts);
        } else if (op instanceof NestedLoopJoin) {
            NestedLoopJoin join = (NestedLoopJoin) op;
            buildVertices(join.getLeftOperation());
            buildVertices(join.getRightOperation());
        }
    }

}
