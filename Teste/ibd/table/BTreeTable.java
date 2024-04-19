package ibd.table;

import ibd.index.btree.table.BPlusTreeFileTable;
import ibd.index.btree.DictionaryPair;
import ibd.index.btree.Key;
import ibd.index.btree.Value;
import ibd.persistent.PersistentPageFile;
import ibd.persistent.cache.Cache;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import ibd.table.prototype.DataRow;

import java.util.ArrayList;
import java.util.List;
import ibd.table.prototype.Prototype;

public class BTreeTable extends Table {

    //the b-tree that stores the table's content
    BPlusTreeFileTable tree = null;

    //the optional cache for the tree's pages
    protected Cache cache = null;

    //the location of the table
    String folder;

    //the file name uses to store the table's contents
    String name;

    //prevents the file from being reopened
    boolean loaded = false;

    /**
     *
     * @param folder: the location of the table
     * @param name: the file name uses to store the table's contents
     * @throws Exception
     */
    public BTreeTable(String folder, String name) throws Exception {

        this.folder = folder;
        this.name = name;

    }

    /**
     * Creates a table
     *
     * @param prototype: the schema of the table
     * @param pageSize: the size of the file pages
     * @throws Exception
     */
    @Override
    public void create(Prototype prototype, int pageSize) throws Exception {
        if (loaded) {
            return;
        }
        //defines the paged file that the BTree will use
        PersistentPageFile p = new PersistentPageFile(pageSize, Paths.get(folder + "\\" + name), true);
        open(p, prototype);
    }

    /**
     * Open an existing table
     *
     * @throws Exception if the file does not exists or if the provided file is
     * not a valid table
     */
    @Override
    public void open() throws Exception {
        if (loaded) {
            return;
        }

        Path fileName = Paths.get(folder + "\\" + name);
        boolean exists = Files.exists(fileName);
        if (!exists) {
            throw new Exception("The file " + fileName + "does not exists");
        }
        //defines the paged file thatthe BTree will use
        PersistentPageFile p = new PersistentPageFile(-1, fileName, false);
        open(p, null);
    }

    private void open(PersistentPageFile p, Prototype prototype) throws Exception {

        //LRUCache lru = new LRUCache(5000000, p);
        //defines the buffer management to be used, if any.
        cache = new ibd.persistent.cache.LRUCache(5000000);
        //cache = new MidPointCache(5000000);
        cache.setPageFile(p);

        //creates a B+ Tree instance using the defined buffer manager, if any
        if (cache != null) {
            tree = new BPlusTreeFileTable(cache, prototype);
        } else {
            tree = new BPlusTreeFileTable(p, prototype);
        }
        tree.open();

        loaded = true;
    }

    /**
     * Closes the table
     */
    @Override
    public void close() {
        tree.flush();
        tree.close();
    }

    /**
     * Flushes the table's content to disk
     *
     * @throws Exception
     */
    @Override
    public void flushDB() throws Exception {
        tree.flush();
    }

    /**
     * Adds a row to the table
     *
     * @param dataRow: the row to be added
     * @return the added row or null if no row was added
     */
    @Override
    public DataRow addRecord(DataRow dataRow) {
        //assigns the metadata to the row columns(data type, constraints, ...)
        dataRow.setMetadata(tree.prototype);

        //sets the key to be a byte array corresponding to the row's primary key
        DataRow pkRow = tree.prototype.createPKRow(dataRow);
        Key key = tree.createKey();
        key.setKeys(new DataRow[]{pkRow});

        //sets the value to be a byte array corresponding to the row's content
        Value value = tree.createValue();
        byte bytes[] = tree.prototype.convertToArray(dataRow);
        value.set(0, bytes);

        //tries to insert the row into the b-tree
        boolean ok = tree.insert(key, value);
        if (!ok) {
            return null;
        }

        //this.tree.flush();
        return dataRow;
    }

    /**
     * Updates a row in the table
     *
     * @param dataRow: the row to be updated
     * @return the updated row or null if no row was updated
     */
    @Override
    public DataRow updateRecord(DataRow dataRow) {
        //assigns the metadata to the row columns(data type, constraints, ...)
        dataRow.setMetadata(tree.prototype);

        //sets the key to be a byte array corresponding to the row's primary key
        DataRow pkRow = tree.prototype.createPKRow(dataRow);
        Key key = tree.createKey();
        key.setKeys(new DataRow[]{pkRow});

        //sets the value to be a byte array corresponding to the row's content
        Value value = tree.createValue();
        byte bytes[] = tree.prototype.convertToArray(dataRow);
        value.set(0, bytes);

        //tries to update the row in the b-tree
        Value v = tree.update(key, value);
        if (v == null) {
            return null;
        }

        //this.tree.flush();
        return dataRow;
    }

    /**
     * Removes a row from the table
     *
     * @param dataRow the row to be removed
     * @return the removed row or null if no row was removed
     */
    @Override
    public DataRow removeRecord(DataRow dataRow) {
        //assigns the metadata to the row columns(data type, constraints, ...)
        dataRow.setMetadata(tree.prototype);

        //sets the key to be a byte array corresponding to the row's primary key
        DataRow pkRow = tree.prototype.createPKRow(dataRow);
        Key key = tree.createKey();
        key.setKeys(new DataRow[]{pkRow});

        //tries to remove the row from the b-tree
        Value value = tree.delete(key);
        if (value == null) {
            return null;
        }

        //converts the rows's byte array stored in the b-tree back to a row format
        byte bytes_[] = (byte[]) value.get(0);
        dataRow = tree.prototype.convertBinaryToRowData(bytes_, null, true, false);

        //this.tree.flush();
        return dataRow;
    }

    /**
     * Returns all rows from the table
     *
     * @return all rows from the table
     * @throws Exception
     */
    @Override
    public List<DataRow> getAllRecords() throws Exception {
        //returns all b-trees leaf entries
        List<DictionaryPair> values = tree.searchAll();
        List<DataRow> rows = new ArrayList();
        //traverses all entries
        for (DictionaryPair value : values) {
            //for each entry, converts the rows's byte array stored in the b-tree back to a row format
            Value v = value.getValue();
            byte bytes_[] = (byte[]) v.get(0);
            DataRow rowData = tree.prototype.convertBinaryToRowData(bytes_, null, true, false);
            rows.add(rowData);
        }
        return rows;
    }

    /**
     * Returns a row that satisfies a primary key search condition.
     *
     * @param dataRow the row whose primary key is used to do the search
     * @return the row that satisfy the search condition or null if no row
     * satisfies the condition.
     */
    @Override
    public DataRow getRecord(DataRow dataRow) {
        //assigns the metadata to the row columns(data type, constraints, ...)
        dataRow.setMetadata(tree.prototype);

        //sets the key to be a byte array corresponding to the row's primary key
        DataRow pkRow = tree.prototype.createPKRow(dataRow);
        Key key = tree.createKey();
        key.setKeys(new DataRow[]{pkRow});

        //performs the sarch over the b-tree
        Value v = tree.search(key);
        if (v == null) {
            return null;
        }

        //converts the rows's byte array stored in the b-tree back to a row format
        byte bytes_[] = (byte[]) v.get(0);
        dataRow = tree.prototype.convertBinaryToRowData(bytes_, null, true, false);
        return dataRow;
    }

    /**
     * Returns a list of rows that satisfies a primary key search condition.
     * This function can return more than one row if the search uses a prefix of
     * the primary key instead of the whole primary key
     *
     * @param dataRow the row whose primary key is used to do the search
     * @return the list of rows that satisfy the search condition
     */
    @Override
    public List<DataRow> getRecords(DataRow dataRow) {
        //assigns the metadata to the row columns(data type, constraints, ...)
        dataRow.setMetadata(tree.prototype);

        List<DataRow> rows = new ArrayList();

        //sets the key to be a byte array corresponding to the row's primary key
        DataRow pkRow = tree.prototype.createPKRow(dataRow);
        Key key = tree.createKey();
        key.setKeys(new DataRow[]{pkRow});

        //performs the sarch over the b-tree
        List<Value> values = tree.partialSearch(key);
        for (Value value : values) {
            //converts the rows's byte array stored in the b-tree back to a row format
            byte bytes_[] = (byte[]) value.get(0);
            dataRow = tree.prototype.convertBinaryToRowData(bytes_, null, true, false);
            rows.add(dataRow);
        }
        return rows;

    }

    /**
     * Returns all rows that satisfy a single column comparison
     *
     * @param col: the column name
     * @param comparable: the comparable value to be compared against
     * @param comparisonType: the comparison type (<,>,...)
     * @return all rows that satisfy a single column comparison
     * @throws Exception
     */
    @Override
    public List<DataRow> getRecords(String col, Comparable comparable, int comparisonType) throws Exception {
        //not implemented yet
        List<DataRow> rows = new ArrayList();
        List<DataRow> allRows = getAllRecords();
        for (DataRow row : allRows) {
            if (ComparisonTypes.match(row.getValue(col), comparable, comparisonType)) {
                rows.add(row);
            }
        }
        return rows;

    }

    /**
     * Prints statistics concerning the table's usage
     *
     * @throws Exception
     */
    @Override
    public void printStats() throws Exception {
        //System.out.println("largest used page id:"+p.getNextPageID());
    }

    /**
     * Returns the number of rows stored in the table
     *
     * @return the number of rows stored in the table
     * @throws Exception
     */
    @Override
    public int getRecordsAmount() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Prototype getPrototype() {
        return tree.prototype;
    }

}
