package ibd.table.prototype;

import ibd.table.prototype.column.Column;
import ibd.table.prototype.metadata.BooleanMetadata;
import ibd.table.prototype.metadata.DoubleMetadata;
import ibd.table.prototype.metadata.FloatMetadata;
import ibd.table.prototype.metadata.IntegerMetadata;
import ibd.table.prototype.metadata.LongMetadata;
import ibd.table.prototype.metadata.Metadata;
import ibd.table.prototype.metadata.StringMetadata;
import ibd.table.prototype.query.fields.BinaryField;
import ibd.table.prototype.query.fields.Field;
import ibd.table.util.UtilConversor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * This class defines the content of a table's row.
 * The content must satisfy the table's schema (the prototype)
 * @author Sergio
 */
public class DataRow implements Iterable<Map.Entry<String, Field>>, Comparable<DataRow> {

    //the mapping between the column name and the column value
    private Map<String, Field> data;
    
    //the mapping between the column name and the column value for columns whose data type is still unknown
    private Map<String, Comparable> unknownData;
    
    //the mapping between the column name and the column definition
    private Map<String, Column> metadata;
    
    //the size in bytes of the row
    private long byteSize = 0;

    //the schema of the table
    Prototype prototype = null;

    long len = 0;
    private byte checkSum = 0;
    

    public DataRow() {
        data = new TreeMap<>();
        metadata = new HashMap<>();
        unknownData = new TreeMap<>(); 
    }

    protected DataRow(DataRow cloneData) {
        data = new TreeMap<>(cloneData.data);
        metadata = new HashMap<>(cloneData.metadata);
        byteSize = cloneData.byteSize;
    }

    private void applyChecksum(BData data) {
        if (data != null && data.length() > 0) {
            checkSum ^= data.getData()[0];
        }
    }

    public void setField(String column, Field field) {
        valid = false;
        Field currentField = this.data.get(column);
        if (currentField != null) {
            applyChecksum(currentField.getBData());
            byteSize -= currentField.bufferByteSize();
        }
        if (field == null) {
            return;
        }
        this.data.put(column, field);
        applyChecksum(field.getBData());
        byteSize += field.bufferByteSize();
    }

    public void setField(String column, Field field, Column metadata) {
        setField(column, field);
        setMetadata(column, metadata);
    }

    public void setData(String column, byte[] data) {
        BData bdata = new BData(data);
        this.setField(column, new BinaryField(new Metadata((short) (data.length >> 8 + 1), Metadata.LSHIFT_8_SIZE_COLUMN), bdata));
    }

    public void setInt(String column, int data) {
        BData bdata = new BData(UtilConversor.intToByteArray(data));
        this.setField(column, Field.createField(IntegerMetadata.generic, bdata));
    }

    public void setLong(String column, long data) {
        BData bdata = new BData(UtilConversor.longToByteArray(data));
        this.setField(column, Field.createField(LongMetadata.generic, bdata));
    }

    public void setString(String column, String data) {
        BData bdata = new BData(UtilConversor.stringToByteArray(data));
        this.setField(column, Field.createField(new StringMetadata((short) (data.length() + 1)), bdata));
    }

    public void setFloat(String column, float data) {
        BData bdata = new BData(UtilConversor.floatToByteArray(data));
        this.setField(column, Field.createField(FloatMetadata.generic, bdata));
    }

    public void setDouble(String column, double data) {
        BData bdata = new BData(UtilConversor.doubleToByteArray(data));
        this.setField(column, Field.createField(DoubleMetadata.generic, bdata));
    }

    public void setBoolean(String column, boolean data) {
        BData bdata = new BData(new byte[]{(byte) (data ? 1 : 0)});
        this.setField(column, Field.createField(BooleanMetadata.generic, bdata));
    }

    public void setValue(String column, Comparable data) {
        
        if (prototype==null){
            unknownData.put(column, data);
            return;
        }
        
        Column col = prototype.getColumn(column);

        
        if (col==null){
            unknownData.put(column, data);
            return;
        }
        
        switch (col.getType()) {
            case "STRING" ->
                setString(column, (String) data);
            case "BOOLEAN" ->
                setBoolean(column, (Boolean) data);
            case "INTEGER" ->
                setInt(column, (Integer) data);
            case "FLOAT" ->
                setFloat(column, (Float) data);
            case "DOUBLE" ->
                setDouble(column, (Double) data);
            case "LONG" ->
                setLong(column, (Long) data);
        }
    }

    public void setInt(String column, int data, Column meta) {
        BData bdata = new BData(UtilConversor.intToByteArray(data));
        this.setField(column, Field.createField(meta, bdata));
        this.setMetadata(column, meta);
    }

    public void setLong(String column, Long data, Column meta) {
        BData bdata = new BData(UtilConversor.longToByteArray(data));
        this.setField(column, Field.createField(meta, bdata));
        this.setMetadata(column, meta);
    }

    public void setString(String column, String data, Column meta) {
        BData bdata = new BData(UtilConversor.stringToByteArray(data));
        this.setField(column, Field.createField(meta, bdata));
        this.setMetadata(column, meta);
    }

    public void setFloat(String column, float data, Column meta) {
        BData bdata = new BData(UtilConversor.floatToByteArray(data));
        this.setField(column, Field.createField(meta, bdata));
        this.setMetadata(column, meta);
    }

    public void setDouble(String column, double data, Column meta) {
        BData bdata = new BData(UtilConversor.doubleToByteArray(data));
        this.setField(column, Field.createField(meta, bdata));
        this.setMetadata(column, meta);
    }

    public void setBoolean(String column, boolean data, Column meta) {
        BData bdata = new BData(new byte[]{(byte) (data ? 1 : 0)});
        this.setField(column, Field.createField(meta, bdata));
        this.setMetadata(column, meta);
    }

    public Field unset(String column) {
        Field f = this.data.get(column);
        setField(column, null);
        setMetadata(column, null);
        this.data.remove(column);
        return f;
    }

    public Column getMetadata(String column) {
        return metadata.get(column);
    }

    public void setMetadata(String column, Column meta) {
        this.metadata.put(column, meta);
    }

    public boolean containsColumn(String column) {
        return this.data.containsKey(column);
    }

    public Field getField(String column) {
        return this.data.get(column);
    }

    public byte[] getData(String column) {
        if (!this.data.containsKey(column)) {
            return null;
        }
        BData data = this.data.get(column).getBData();
        if (data == null) {
            return null;
        }
        return data.getData();
    }

    public BData getBData(String column) {
        if (!this.data.containsKey(column)) {
            return null;
        }
        BData data = this.data.get(column).getBData();
        return data;
    }

    public Integer getInt(String column) {
        if (!this.data.containsKey(column)) {
            return null;
        }
        return this.data.get(column).getInt();
    }

    public Long getLong(String column) {
        if (!this.data.containsKey(column)) {
            return null;
        }
        return this.data.get(column).getLong();
    }

    public Float getFloat(String column) {
        if (!this.data.containsKey(column)) {
            return null;
        }
        return this.data.get(column).getFloat();
    }

    public Double getDouble(String column) {
        if (!this.data.containsKey(column)) {
            return null;
        }
        return this.data.get(column).getDouble();
    }

    public String getString(String column) {
        if (!this.data.containsKey(column)) {
            return null;
        }
        return this.data.get(column).getString();
    }

    public Boolean getBoolean(String column) {
        if (!this.data.containsKey(column)) {
            return null;
        }
        return this.data.get(column).getBoolean();
    }

    public Comparable getValue(String column) {
        if (!this.data.containsKey(column)) {
            return null;
        }
        return (Comparable) this.data.get(column).getBufferedData();
    }

    public String getAsString(String column) {
        Column col = metadata.get(column);

        switch (col.getType()) {
            case "STRING" -> {
                return getString(column).toString();
            }
            case "BOOLEAN" -> {
                return getBoolean(column).toString();
            }
            case "INTEGER" -> {
                return getInt(column).toString();
            }
            case "FLOAT" -> {
                return getFloat(column).toString();
            }
            case "DOUBLE" -> {
                return getDouble(column).toString();
            }
            case "LONG" -> {
                return getLong(column).toString();
            }
        }
        return "";
    }

    public int size() {
        return this.data.size();
    }

    public long getByteSize() {
        return this.byteSize;
    }

    public int len() {
        //int len =  8 + this.getString("nome").getBytes().length + 2;
        return (int) this.len;

    }

    private boolean valid = false;

    protected boolean isValid() {
        return valid;
    }

    protected void setValid() {
        valid = true;
    }

    @Override
    public Iterator<Map.Entry<String, Field>> iterator() {
        return data.entrySet().iterator();
    }

    @Override
    public int compareTo(DataRow r) {
        //int val = checkSum - r.checkSum;
//        if (val != 0) {
//            return val;
//        }
//        val = data.size() - r.data.size();
//        if (val != 0) {
//            return val;
//        }
        int val = 0;
        for (Map.Entry<String, Field> entry
                : this) {
            Field f = r.getField(entry.getKey());
            if (f == null) {
                continue;
            }
            val = entry.getValue().compareTo(f);
            if (val != 0) {
                return val;
            }
        }
        return data.size() - r.data.size();
        //if (r.data.size()> data.size()) return -1;
        //return 0;
    }

    public int partialMatch(DataRow r) {
        int val = 0;
        for (Map.Entry<String, Field> entry
                : this) {
            Field f = r.getField(entry.getKey());
            if (f == null) {
                continue;
            }
            val = entry.getValue().compareTo(f);
            if (val != 0) {
                return val;
            }
        }
        return 0;
    }

    public DataRow clone() {
        return new DataRow(this);
    }
    
    @Override
    public String toString() {
        String str = "row(";
        for (String col : data.keySet()) {
            switch (ibd.table.util.Util.typeOfColumn(getMetadata(col))) {
                case "boolean":
                    str += col+":"+getBoolean(col);
                    break;
                case "long":
                    str += col+":"+getLong(col);
                    break;
                case "int":
                    str += col+":"+getInt(col);
                    break;
                case "float":
                    str += col+":"+getFloat(col);
                    break;
                case "double":
                    str += col+":"+getDouble(col);
                    break;
                case "null":
                    str += col+":"+"Null" + ",";
                    break;
                case "string":
                default:
                    str += col+":"+getString(col);
                    break;
            }
            str += ", ";
        }
        str += ")";
        return str;
    }

    private void adjustField(Column col, Object value) {
        switch (col.getType()) {
            case Column.BOOLEAN_TYPE -> {
                this.setBoolean(col.getName(), (Boolean)value, col);
                return;
            }
            case Column.DOUBLE_TYPE -> {
                this.setDouble(col.getName(), (Double)value, col);
                return;
            }
            case Column.FLOAT_TYPE -> {
                this.setFloat(col.getName(), (Float)value, col);
                return;
            }
            case Column.INTEGER_TYPE -> {
                this.setInt(col.getName(), (Integer)value, col);
                return;
            }
            case Column.LONG_TYPE -> {
                this.setLong(col.getName(), (Long)value, col);
                return;
            }
            case Column.STRING_TYPE -> {
                this.setString(col.getName(), (String)value, col);
                return;
            }

        }
    }

    public void setMetadata(Prototype prototype) {
        //byteSize = 0;
        
        //create fields for the values with unknown datatype using provided the column names and the prototype
        for (Entry<String, Comparable> entry : unknownData.entrySet()) {
            Column col = prototype.getColumn(entry.getKey());
            if (col==null) continue;
            adjustField(col, entry.getValue());
        }
        
        for (String colName : data.keySet()) {

            if (metadata.containsKey(colName)) {
                continue;
            }
            Column col = prototype.getColumn(colName);
            if (col == null) {
                continue;
            }
            metadata.put(colName, col);
            Field f = data.get(colName);
            if (f.getType().equals(Column.UNKNOWN_TYPE)) {
                adjustField(col, f);
            }
            //byteSize+=col.getSizeinBytes();
        }
        //byteSize = prototype.getSizeInBytes();
        this.prototype = prototype;
        len = prototype.maxRecordSize();
    }

    

}
