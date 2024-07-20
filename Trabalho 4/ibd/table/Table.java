/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import ibd.table.lookup.RowLookupFilter;
import ibd.table.prototype.BasicDataRow;
import java.util.List;
import ibd.table.prototype.LinkedDataRow;
import ibd.table.prototype.Prototype;

public abstract class Table  {

    public static final int DEFULT_PAGE_SIZE = 4096;

    public String tableKey;

    public abstract LinkedDataRow getRecord(BasicDataRow rowdata) throws Exception;

    public abstract List<LinkedDataRow> getRecords(String col, Comparable comp, int comparisonType) throws Exception;
    
    public abstract List<LinkedDataRow> getAllRecords() throws Exception;
    
    public abstract List<LinkedDataRow> getRecords(BasicDataRow rowData);

    public abstract LinkedDataRow addRecord(BasicDataRow rowdata) throws Exception;

    public abstract LinkedDataRow updateRecord(BasicDataRow rowdata) throws Exception;
    
    public abstract LinkedDataRow updateRecord(LinkedDataRow rowdata) throws Exception;
        
    public abstract LinkedDataRow removeRecord(BasicDataRow rowdata) throws Exception;

    public abstract void flushDB() throws Exception;

    public abstract int getRecordsAmount() throws Exception;
    
    public abstract void printStats() throws Exception;
    
    public abstract void open() throws Exception;
    
    public abstract void create(Prototype prototype, int pageSize) throws Exception;
    
    public abstract void close() throws Exception;
    
    public abstract Prototype getPrototype();
    
    public abstract LinkedDataRow getRecord(LinkedDataRow rowdata) throws Exception;
    
    public abstract List<LinkedDataRow> getRecords(LinkedDataRow rowData);
    
    public abstract boolean contains(LinkedDataRow pkRow);
    
    public abstract List<LinkedDataRow> getFilteredRecords(RowLookupFilter filter) throws Exception;
    
    public abstract AllRowsIterator getAllRecordsIterator() throws Exception;
    public abstract FilteredRowsIterator getFilteredRecordsIterator(RowLookupFilter filter) throws Exception;
    public abstract List<LinkedDataRow> getRecords(LinkedDataRow pkRow, RowLookupFilter rowFilter);
}
