/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.optimizer.join;

import ibd.query.Operation;
import ibd.query.binaryop.join.NestedLoopJoin;
import ibd.query.binaryop.join.JoinTerm;
import ibd.query.sourceop.FullTableScan;
import java.util.List;

/**
 *
 * @author Sergio
 */
public class XXXQueryOptimizer implements QueryOptimizer{

    Graph graph = new Graph();

    @Override
    public Operation optimizeQuery(Operation query) throws Exception {
        buildGraph(query);
        Operation newQuery = null;
        //o código vai aqui
        return newQuery;
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
