/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import java.util.List;
import ibd.table.prototype.Prototype;
import ibd.table.prototype.DataRow;
import ibd.table.prototype.column.Column;
import ibd.table.prototype.column.IntegerColumn;
import ibd.table.prototype.column.StringColumn;

/**
 *
 * @author Sergio
 */
public class MainRafaelCarneiroPregardier {

    /**
     * Cretes the schema of the person table
     * @return
     */
    public Prototype createSchema(){
        Prototype schema = new Prototype();
        schema.addColumn(new IntegerColumn("id", true));
        schema.addColumn(new StringColumn("name"));
        schema.addColumn(new IntegerColumn("salary"));
        schema.addColumn(new StringColumn("dept"));
        return schema;
    }
    
    /**
     * Adds a person row into a table
     * @param table the target table
     * @param id the id of the person
     * @param name the name of the person
     * @param salary the salary of the person
     * @param dept the departament of the person
     * @throws Exception
     */
    public void addPerson(Table table, int id, String name, int salary, String dept) throws Exception {
        DataRow row = new DataRow();
        row.setInt("id", id);
        row.setString("name", name);
        row.setInt("salary", salary);
        row.setString("dept", dept);
        table.addRecord(row);
    }

    /**
     * Find the average salary considering people that belong to a specific department
     * @param table the target table
     * @param dept the department
     * @return the average salary
     * @throws Exception
     */
    public double getAvgSalary(Table table, String dept) throws Exception{
        List<DataRow> list = table.getRecords("dept", dept, ComparisonTypes.EQUAL);
        double sum = 0;
        for (DataRow rowData : list) {
            sum += rowData.getInt("salary");
        }
        return sum / list.size();
    }
    
    /**
     * Print information of all rows from the table
     * @param table the target table
     * @throws Exception
     */
    public void getAll(Table table) throws Exception{
        List<DataRow> list = table.getAllRecords();
        for (DataRow rowData : list) {
            System.out.println(rowData.toString());
        }
    }
    
    /**
     * Print information from people of a specific department
     * @param table the target table
     * @param dept the department 
     * @throws Exception
     */
    public void getSalario(Table table, String dept) throws Exception{
        List<DataRow> list = table.getRecords("dept", dept, ComparisonTypes.EQUAL);
        for (DataRow rowData : list) {
            System.out.println(rowData.toString());
        }
    }
    
    
    
    /**
     * Prints the average salary of people from a specific department
     * @param table the target table
     * @param dept the department
     * @throws Exception
     */
    public void printAvgSalary(Table table, String dept) throws Exception{
        double avg = getAvgSalary(table, dept);
        System.out.println("The average salary of dept "+ dept +": "+avg);
    }
    
    

    public static void main(String[] args) throws Exception {

        MainRafaelCarneiroPregardier main = new MainRafaelCarneiroPregardier();

        Prototype schema = main.createSchema();
            
        Table table = Directory.getTable(schema, "C:\\Users\\naoca\\Documents\\GitHub\\Implementacao-de-Banco-de-Dados\\Trabalho 1\\ibd", "person", 4096, true);

        main.addPerson(table, 1, "Ana", 2000, "finances");
        main.addPerson(table, 2, "Joao", 1500, "sells");
        main.addPerson(table, 3, "Miguel", 4500, "sells");
        main.addPerson(table, 4, "Carlos", 3000, "finances");
        
         table.flushDB();
        //main.getSalary(table, "financas");
        //main.getAll(table);
        main.printAvgSalary(table, "sells");

       

    }

}
