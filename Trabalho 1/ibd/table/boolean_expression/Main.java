/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table.boolean_expression;

import ibd.table.ComparisonTypes;
import ibd.table.Directory;
import ibd.table.Table;
import java.util.List;
import ibd.table.prototype.Prototype;
import ibd.table.prototype.DataRow;
import ibd.table.prototype.column.IntegerColumn;
import ibd.table.prototype.column.StringColumn;

/**
 *
 * @author Sergio
 */
public class Main {

    public Prototype createSchema() {
        Prototype pt = new Prototype();
        pt.addColumn(new IntegerColumn("id", true));
        pt.addColumn(new IntegerColumn("year"));
        pt.addColumn(new StringColumn("title"));
        pt.addColumn(new StringColumn("genre"));
        pt.addColumn(new IntegerColumn("cost"));
        return pt;
    }

    public void addMovie(Table table, int id, String title, int year, int cost, String genre) throws Exception {
        DataRow rowData = new DataRow();
        rowData.setInt("id", id);
        rowData.setInt("year", year);
        rowData.setInt("cost", cost);
        rowData.setString("title", title);
        rowData.setString("genre", genre);
        table.addRecord(rowData);

    }

    public void getAll(Table table) throws Exception {
        List<DataRow> list = table.getAllRecords();
        for (DataRow rowData : list) {
            System.out.println(rowData.toString());
        }
    }

    public void getRows(Table table, String col, Comparable comp, int type) throws Exception {
        List<DataRow> list = table.getRecords(col, comp, type);
        for (DataRow rowData : list) {
            System.out.println(rowData.toString());
        }
    }

    public void getRows(Table table, Expression expression, ExpressionSolver solver) throws Exception {
        System.out.println("result list:");
        for (DataRow r : table.getAllRecords()) {
            if (solver.solve(expression, r)) {
                System.out.println(r.toString());
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Main main = new Main();

        //defining the table schema
        Prototype schema = main.createSchema();

        //creating of the table’s file
        Table table = Directory.getTable(schema, "c:\\Users\\naoca\\Documents\\GitHub\\Implementacao-de-Banco-de-Dados\\Trabalho 1\\ibd", "movie", 4096, true);
        //adding movies
        main.addMovie(table, 1, "Interstelar", 2014, 165, "Sci-Fi");
        main.addMovie(table, 2, "Os Infiltrados", 2006, 40, "Drama");
        main.addMovie(table, 3, "Avatar", 2009, 237, "Sci-Fi");
        main.addMovie(table, 4, "O Aviador", 2004, 110, "Drama");
        main.addMovie(table, 5, "O Terminal", 2004, 60, "Drama");
        main.addMovie(table, 6, "Guerra dos Mundos", 2005, 132, "Sci-Fi");
        main.addMovie(table, 7, "Minority Report", 2002, 102, "Sci-Fi");

        //print movies from 2004, without using a boolean expression
        main.getRows(table, "year", 2004, ComparisonTypes.EQUAL);

        //defining the expression
        SingleExpression se1 = new SingleExpression("year", ComparisonTypes.EQUAL, 2004);
        SingleExpression se2 = new SingleExpression("year", ComparisonTypes.EQUAL, 2014);
//        NegationExpression ne = new NegationExpression(se2);
        CompositeExpression ce = new CompositeExpression(CompositeExpression.AND);
        ce.addExpression(se1);
        ce.addExpression(se2);

        //printing the rows that satisfy the expression
        main.getRows(table, ce, new ExpressionSolverRafaelCarneiroPregardier());

        SingleExpression se3 = new SingleExpression("genre", ComparisonTypes.EQUAL, "Sci-Fi");
        CompositeExpression ce2 = new CompositeExpression(CompositeExpression.AND);
        ce2.addExpression(se1);
        ce2.addExpression(se3);

        main.getRows(table, ce2, new ExpressionSolverRafaelCarneiroPregardier());


        //operation using and & or
        CompositeExpression ce3 = new CompositeExpression(CompositeExpression.OR);
        ce3.addExpression(se1);
        ce3.addExpression(se2);
        ce3.addExpression(se3);

        main.getRows(table, ce3, new ExpressionSolverRafaelCarneiroPregardier());

        //operation using not
        NegationExpression ne = new NegationExpression(se1);

        main.getRows(table, ne, new ExpressionSolverRafaelCarneiroPregardier());
    }

}
