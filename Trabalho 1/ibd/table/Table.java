/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import java.util.List;
import ibd.table.prototype.DataRow;
import ibd.table.prototype.Prototype;

public abstract class Table  {

    public static final int DEFULT_PAGE_SIZE = 4096;

    public String tableKey;

    public abstract DataRow getRecord(DataRow rowdata) throws Exception;

    public abstract List<DataRow> getRecords(String col, Comparable comp, int comparisonType) throws Exception;
    
    public abstract List<DataRow> getAllRecords() throws Exception;
    
    public abstract List<DataRow> getRecords(DataRow rowData);

    public abstract DataRow addRecord(DataRow rowdata) throws Exception;

    public abstract DataRow updateRecord(DataRow rowdata) throws Exception;
        
    public abstract DataRow removeRecord(DataRow rowdata) throws Exception;

    public abstract void flushDB() throws Exception;

    public abstract int getRecordsAmount() throws Exception;
    
    public abstract void printStats() throws Exception;
    
    public abstract void open() throws Exception;
    
    public abstract void create(Prototype prototype, int pageSize) throws Exception;
    
    public abstract void close() throws Exception;
    
    public abstract Prototype getPrototype();
    
}
