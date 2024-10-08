/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop;

import ibd.query.ColumnDescriptor;
import ibd.query.Operation;
import ibd.query.UnpagedOperationIterator;
import ibd.query.ReferedDataSource;
import ibd.query.Tuple;
import ibd.query.unaryop.sort.Sort;

import java.sql.SQLOutput;
import java.util.Iterator;
import java.util.List;

import ibd.table.prototype.LinkedDataRow;
import ibd.table.prototype.Prototype;
import ibd.table.prototype.column.DoubleColumn;
import ibd.table.prototype.column.IntegerColumn;
import ibd.table.prototype.column.StringColumn;

/**
 * This operation groups tuples and computes an aggregated value (MIN. MAX, SUM,
 * AVG, COUNT) for each group
 *
 * @author Sergio
 */
public class AggregationRafaelCarneiroPregardier extends UnaryOperation {

    //defines the five types of aggregation
    public final static int MAX = 1;
    public final static int MIN = 2;
    public final static int COUNT = 3;
    public final static int AVG = 4;
    public final static int SUM = 5;

    //the alias identifying the tuples generated by this operation
    String alias;

    //the group by column is used to group the incoming tuples
    ColumnDescriptor groupByColumn;
    //the aggregated column is used to perform the selected aggregation
    ColumnDescriptor aggregateColumn;

    //the index of the tuple that contains the group by column
    //int groupByTupleIndex = -1;
    //the index of the tuple that contains the aggregated column
    //int aggregatedTupleIndex = -1;
    //the aggregation type
    int type;

    //the schema of the rows returned by this operation
    Prototype prototype = null;

    /**
     *
     * @param op the operation to be connected with this unary operation
     * @param alias the alias identifying the tuples generated by this operation
     * @param groupByCol the name of the column to be used to group tuples. The
     * name can be prefixed by the table name (e.g. tab.col)
     * @param aggregateCol the name of the column to be used to aggregate values
     * from the grouped tuples. The name can be prefixed by the table name (e.g.
     * tab.col)
     * @param type the type of aggregation to be performed (AVG, COUNT, MIN,
     * MAX, SUM)
     * @param isOrdered indicates if the incoming tuples are already ordered by
     * the groupByCol column
     * @throws Exception
     */
    public AggregationRafaelCarneiroPregardier(Operation op, String alias, String groupByCol, String aggregateCol, int type, boolean isOrdered) throws Exception {
        super(op);
        this.alias = alias;
        groupByColumn = new ColumnDescriptor(groupByCol);
        aggregateColumn = new ColumnDescriptor(aggregateCol);
        this.type = type;
        prototype = new Prototype();
        prototype.addColumn(new StringColumn(groupByColumn.getColumnName(), (short)30));
        if (type == AVG) {
            prototype.addColumn(new DoubleColumn(getStringType(type) + "(" + aggregateColumn.getColumnName() + ")"));
            //prototype.addColumn(new DoubleColumn(aggregateColumn.getColumnName()));
        } else {
            prototype.addColumn(new IntegerColumn(getStringType(type) + "(" + aggregateColumn.getColumnName() + ")"));
            //prototype.addColumn(new IntegerColumn(aggregateColumn.getColumnName()));
        }
        prototype.validateColumns();

        //If the incoming tupled are not already sorted, an intermediary sort operation is added
        if (!isOrdered) {
            Sort cs = new Sort(childOperation, new String[]{groupByColumn.getColumnName()});
            childOperation = cs;
        }
    }

    private String getStringType(int type) {
        return switch (type) {
            case AVG ->
                    "AVG";
            case MIN ->
                    "MIN";
            case MAX ->
                    "MAX";
            case COUNT ->
                    "COUNT";
            case SUM ->
                    "SUM";
            default ->
                    "";
        };
    }

    @Override
    public void prepare() throws Exception {

        //uses the table's names to set the tuple indexes 
        childOperation.setColumnLocation(groupByColumn);
        childOperation.setColumnLocation(aggregateColumn);

        //groupByTupleIndex = childOperation.getRowIndex(groupByColumn.getTableName());
        //aggregatedTupleIndex = childOperation.getRowIndex(aggregateColumn.getTableName());
        super.prepare();

    }

    /**
     * {@inheritDoc }
     * An aggregation defines its own schema. The schema of the rows coming into
     * an aggregation are combined into one, according to the type of
     * aggregation needed.
     *
     * @throws Exception
     */
    @Override
    public void setDataSourcesInfo() throws Exception {
        dataSources = new ReferedDataSource[1];
        dataSources[0] = new ReferedDataSource();
        dataSources[0].alias = alias;
        dataSources[0].prototype = prototype;

        childOperation.setDataSourcesInfo();
    }

    @Override
    public Iterator<Tuple> lookUp_(List<Tuple> processedTuples, boolean withFilterDelegation) {
        return new AggregationIterator(processedTuples, withFilterDelegation);
    }

    @Override
    public String toString() {
        String stringType = AggregationRafaelCarneiroPregardier.getType(type);
        return "Group by(" + groupByColumn.toString() + ")," + stringType + "(" + aggregateColumn + ")";
    }

    /**
     * this class produces resulting tuples from an aggregation over the child
     * operation
     */
    private class AggregationIterator extends UnpagedOperationIterator {

        // the iterator over the child operation
        Iterator<Tuple> tuples;

        // variables to hold the aggregate values
        int count = 0;
        int sum = 0;
        Comparable min = null;
        Comparable max = null;

        public AggregationIterator(List<Tuple> processedTuples, boolean withFilterDelegation) {
            super(processedTuples, withFilterDelegation, getDelegatedFilters());

            tuples = childOperation.lookUp(processedTuples, false); // returns all tuples from the child operation
        }

        private Comparable getValue(Tuple tp, ColumnDescriptor col) {
            return tp.rows[col.getColumnLocation().rowIndex].getValue(col.getColumnName());
        }

        @Override
        protected Tuple findNextTuple() {
            Comparable currentGroupByValue = null;
            Tuple tpCompare = null;

            while (tuples.hasNext()) {
                Tuple tp = tuples.next();

                if (tpCompare == null) {
                    tpCompare = tp;
                    currentGroupByValue = getValue(tp, groupByColumn);
                }

                // a tuple must satisfy the lookup filter
                if (tp == null || !lookup.match(tp)) {
                    continue;
                }

                // get the values for the groupByColumn and aggregateColumn
                Comparable groupByValue = getValue(tp, groupByColumn);
                Comparable aggregateValue = getValue(tp, aggregateColumn);


                if (currentGroupByValue != null && !currentGroupByValue.equals(groupByValue) || !tuples.hasNext()) {
                    // instantiate a new LinkedDataRow with the prototype schema
                    LinkedDataRow dataRow = new LinkedDataRow(prototype, false);

                    // set these values in the dataRow
                    dataRow.setValue(groupByColumn.getColumnName(), currentGroupByValue);
                    String aggregateColumnName = getStringType(type) + "(" + aggregateColumn.getColumnName() + ")";

                    if (!tuples.hasNext() && tp != null) {
                        count++;
                        sum += ((Number) getValue(tp, aggregateColumn)).intValue();
                        if (min == null || getValue(tpCompare, aggregateColumn).compareTo(min) < 0) {
                            min = getValue(tp, aggregateColumn);
                        }
                        if (max == null || getValue(tpCompare, aggregateColumn).compareTo(max) > 0) {
                            max = getValue(tp, aggregateColumn);
                        }
                    }
                    switch (type) {
                        case COUNT:
                            dataRow.setValue(aggregateColumnName, count);
                            break;
                        case SUM:
                            dataRow.setValue(aggregateColumnName, sum);
                            break;
                        case MIN:
                            dataRow.setValue(aggregateColumnName, min);
                            break;
                        case MAX:
                            dataRow.setValue(aggregateColumnName, max);
                            break;
                        case AVG:
                            dataRow.setValue(aggregateColumnName, (double) sum / count);
                            break;
                    }

                    sum = ((Number) aggregateValue).intValue();
                    count = 1;
                    min = aggregateValue;
                    max = aggregateValue;

                    // create a new Tuple and set the dataRow in it
                    Tuple tuple = new Tuple();
                    tuple.setSingleSourceRow(alias, dataRow);

                    // return the created Tuple
                    return tuple;
                }

                // update the aggregate values
                count++;
                sum += ((Number) aggregateValue).intValue();
                if (min == null || aggregateValue.compareTo(min) < 0) {
                    min = aggregateValue;
                }
                if (max == null || aggregateValue.compareTo(max) > 0) {
                    max = aggregateValue;
                }
            }
            return null;
        }
    }

    public static final String getType(int type) {
        switch (type) {
            case AVG -> {
                return "AVG";
            }
            case MAX -> {
                return "MAX";
            }
            case MIN -> {
                return "MIN";
            }
            case COUNT -> {
                return "COUNT";
            }
            case SUM -> {
                return "SUM";
            }
        }
        return "";
    }
}