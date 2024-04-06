package ibd.table.prototype;

import ibd.exceptions.DataBaseException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import ibd.table.prototype.column.Column;
import ibd.table.prototype.query.fields.Field;
import ibd.table.util.UtilConversor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A prototype defines the schema of a table
 *
 * @author Sergio
 */
public class Prototype implements Iterable<Column> {

    public static final Long KB = (long) 1024;
    public static final Long MB = 1024 * KB;
    public static final Long GB = 1024 * MB;
    public static final Long TB = 1024 * GB;

    //the list of columns of this schema
    private final ArrayList<Column> columns;

    //a dictionary mapping the column's names to the columns
    private final HashMap<String, Column> columnsDic;

    //indicates if the rows following this schema have a fixed size
    private boolean stat;

    //the size in bytes of a row header
    private int headerSize = 0;

    //the size in bytes of the prumary key values
    private int primaryKeySize;

    private HashMap<Integer, Integer> headerPosition;

    public Prototype() {
        columns = new ArrayList<>();
        columnsDic = new HashMap<>();
        stat = true;
    }

    /**
     * add a column to the prototype
     *
     * @param c the column to be added
     */
    public void addColumn(Column c) {
        if (c == null) {
            throw new DataBaseException("Prototype->addColumn", "Column passada é nula");
        }
        columns.add(c);
        if (c.isDinamicSize()) {
            stat = false;
        }
        columnsDic.put(c.getName(), c);
    }

    /**
     * add a column to the prototype
     *
     * @param name the name of the column
     * @param size the size of the data type
     * @param flags the attributes of the column
     */
    public void addColumn(String name, short size, short flags) {
        Column c = new Column(name, size, flags);
        addColumn(c);
    }

    /**
     *
     * @return true if the rows that follow this prototype are have a fixed size
     */
    public boolean isStatic() {
        return stat;
    }

    /**
     *
     * @param i the position of a column
     * @return the column at the position i
     */
    public Column getColumn(int i) {
        try {
            return columns.get(i);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    /**
     * Returns a column, given its name
     *
     * @param name: the name of the column
     * @return the column found, or null if no column was found
     */
    public Column getColumn(String name) {
        return columnsDic.get(name);
    }

    /**
     *
     * @return the number of column of this prototype
     */
    public short size() {
        return (short) columns.size();
    }

    /**
     * Verifies if the defined prototype is valid, according to several table
     * schema rules If the prototype is valid, the columns are reorganized to
     * ease manipulation Also, the size of the rows following this prototype are
     * computed
     */
    public void validateColumns() {
        DataBaseException ex = null;
        if (size() == 0) {
            String error = "Não é valido uma tabela com nenhuma coluna!";
            ex = new DataBaseException("Prototype->ValidateColumns", error);
            ex.addValidation("Min:1");
            throw ex;
        }
        for (short x = 0; x < size(); x++) {
            Column col = getColumn(x);
            int namelen = col.getName().length();
            if (namelen > 240 || namelen < 1) {
                String error = "Coluna " + x + " tem nome de tamanhos inválido!";
                ex = new DataBaseException("Prototype->ValidateColumns", error);
                ex.addValidation("Max:240");
                ex.addValidation("Min:1");
                throw ex;
            }
            if (col.isPrimaryKey() && col.canBeNull()) {
                String error = "Coluna " + col.getName() + " não pode ser nula e ao mesmo tempo ser primary key!";
                ex = new DataBaseException("Prototype->ValidateColumns", error);
                ex.addValidation("NULL not in PRIMARY KEY");
                throw ex;
            }
            if (col.isPrimaryKey() && col.isDinamicSize()) {
                String error = "Coluna " + col.getName() + " não pode ser dinamica ao mesmo tempo que é primary key!";
                ex = new DataBaseException("Prototype->ValidateColumns", error);
                ex.addValidation("DINAMIC not in PRIMARY KEY");
                throw ex;
            }
            for (short y = (short) (x + 1); y < size(); y++) {
                Column col2 = getColumn(y);
                if (col.getName().equalsIgnoreCase(col2.getName())) {
                    String error = "Coluna " + x + " e a coluna " + y + " tem nomes iguais!";
                    ex = new DataBaseException("Prototype->ValidateColumns", error);
                    throw ex;
                }
            }
        }

        columns.sort(new Comparator<Column>() {
            @Override
            public int compare(Column o1, Column o2) {
                if (o1.isPrimaryKey() && !o2.isPrimaryKey()) {
                    return -1;
                }
                if (!o1.isPrimaryKey() && o2.isPrimaryKey()) {
                    return 1;
                }
                if (o1.isPrimaryKey() && o2.isPrimaryKey()) {
                    return Integer.compare(columns.indexOf(o1), columns.indexOf(o2));
                } else {
                    if (o1.isDinamicSize() && !o2.isDinamicSize()) {
                        return 1;
                    }
                    if (!o1.isDinamicSize() && o2.isDinamicSize()) {
                        return -1;
                    }
                    if (o1.getSize() == o2.getSize()) {
                        return o1.getName().compareTo(o2.getName());
                    } else {
                        return Integer.compare(o1.getSize(), o2.getSize());
                    }
                }
            }
        });
        calculateSizes();
    }

    @Override
    public Iterator<Column> iterator() {
        return columns.iterator();
    }

    /**
     *
     * @return an unmodifiable copy of the columns list
     */
    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    /**
     *
     * @return the size in bytes considering all columns of the prototypr
     */
    public int getSizeInBytes() {
        int size = Integer.BYTES;
        for (int i = 0; i < columns.size(); i++) {
            size += columns.get(i).getSizeinBytes();
        }
        return size;
    }

    /**
     *
     * @return a list with the name of the primary key columns of this table
     */
    public List<String> getPKColumns() {
        List<String> cols = new ArrayList();
        for (Column column : columns) {
            if (column.isPrimaryKey()) {
                cols.add(column.getName());
            }

        }
        return cols;
    }

    /**
     * Checks if a provided list of columns is a prefix of the prototype's
     * primary key
     *
     * @param cols the columns to be checked
     * @return boolean if the list is a prefix
     */
    public boolean isPKPrefix(List<String> cols) {
        int count = 0;
        for (Column column : columns) {
            if (column.isPrimaryKey()) {
                if (!cols.contains(column.getName())) {
                    break;
                } else {
                    count++;
                }
            } else {
                break;
            }

        }
        return cols.size() == count;
    }

    /**
     * calculate the header dize and the primairy key sizes. needs to be
     * calculcated only once, after all columns are defined
     */
    private void calculateSizes() {
        int headerSizeAux = 1;
        this.headerPosition = new HashMap<>();
        /*
        0 - 256
                0-253 -> ja é o tamanho => 1 byte
                254 -> deve ler um short => 3 byes
                255 -> deve ler um int   => 5 bytes => unico pior caso
                5 bytes
                30 = nome -> 14 => 1 byte
                44 = email -> 20 => 1 byte
                64 = descricao -> 500 => 3 bytes
                //
                12 bytes
                int == 4 bytes
                [int 30,int 44,int 64]
                30 = nome -> 14
                44 = email -> 20
                64 = descricao -> 500
         */
        int aux = 1;
        int sizePk = 0;
        for (Column c : columns) {
            if (c.canBeNull()) {
                this.headerPosition.put(columns.indexOf(c), (headerSizeAux - 1) * 8 + aux);
                aux++;
            }
            if (aux >= 8) {
                headerSizeAux++;
                aux = 0;
            }
            if (c.isPrimaryKey()) {
                sizePk += c.getSize();
            }
        }
        this.headerSize = headerSizeAux;
        primaryKeySize = sizePk;
    }

    /**
     *
     * @return the maximum record size, considering the maximum size of the
     * dynamic columns and the control bytes overhead
     */
    public int maxRecordSize() {
        int size = headerSize;
        for (Column c : columns) {
            size += c.getSize();
            if (c.isDinamicSize()) {
                size += 4;
            }
        }
        return size;
    }

    /**
     *
     * @return the size of the primary key columns
     */
    public int getPrimaryKeySize() {
        return primaryKeySize;
    }

    /**
     * Verifies if the data row satisfies the table's prototype
     *
     * @param dataRow: the data row
     */
    private void validateRowData(DataRow dataRow) {
        if (dataRow.isValid()) {
            return;
        }
        for (Column c : columns) {
            byte[] data = dataRow.getData(c.getName());
            if (data == null) {
                if (!c.canBeNull()) {
                    throw new DataBaseException("RecordTranslateApi->convertToRecord", "Coluna " + c.getName() + " não pode ser nula!");
                } else if (c.isPrimaryKey()) {
                    throw new DataBaseException("RecordTranslateApi->convertToRecord", "Coluna " + c.getName() + " não pode ser nula!");
                }
            } else {
                if (c.isDinamicSize()) {
                    if (data.length > c.getSize()) {
                        throw new DataBaseException("RecordTranslateApi->convertToRecord", "Dado passado para a coluna " + c.getName() + " é maior que o limite: " + c.getSize());
                    }
                } else {
                    if (data.length > c.getSize()) {
                        throw new DataBaseException("RecordTranslateApi->convertToRecord", "Dado passado para a coluna " + c.getName() + " é diferente do tamanho fixo: " + c.getSize());
                    }
                }
            }
        }
        dataRow.setValid();
    }

    /**
     * Creates a data row from a specified byte array using the columns metadata
     *
     * @param data: the byte array
     * @param meta: the columns to be considered
     * @param hasHeader: indicates if the byte array contains header information
     * @param onlyPrimaryKey:indicates if only primary key columns need to be
     * extracted from the byte array
     * @return the created data row
     */
    public synchronized DataRow convertBinaryToRowData(byte[] data, Map<String, Column> meta, boolean hasHeader, boolean onlyPrimaryKey) {
        DataRow row = new DataRow();

        int selecteds = 0;
        int offset = (hasHeader) ? this.headerSize : 0;

        byte[] header = new byte[headerSize];
        if (hasHeader) {
            System.arraycopy(data, 0, header, 0, this.headerSize);
        }

        int headerPointer = 1;

        for (Column c : columns) {
            if (meta != null && selecteds >= meta.size()) {
                break;
            }
            if (onlyPrimaryKey && !c.isPrimaryKey()) {
                break;
            }
            boolean checkColumn = meta == null || meta.containsKey(c.getName());
            if (checkColumn) {
                selecteds++;
            }
            if (c.canBeNull() && hasHeader) {
                try {
                    if ((header[headerPointer / 8] & (1 << headerPointer % 8)) != 0) {
                        //campo é nulo
                        continue;
                    }
                } finally {
                    headerPointer++;
                }
            }
            int size = c.getSize();
            if (c.isDinamicSize()) {
                size = UtilConversor.byteArrayToInt(Arrays.copyOfRange(data, offset, offset + 4));
                offset += 4;
                if (checkColumn) {
                    byte[] arr = Arrays.copyOfRange(data, offset, offset + size);
                    row.setField(c.getName(), Field.createField(c, new BData(arr)), c);
                }

            } else {
                if (checkColumn) {
                    byte[] arr = Arrays.copyOfRange(data, offset, offset + c.getSize());
                    row.setField(c.getName(), Field.createField(c, new BData(arr)), c);
                }
            }
            offset += size;
        }
        return row;
    }

    /**
     * Generates a byte array from the values of a data row
     *
     * @param dataRow: the data row
     * @return the generated byte array
     */
    public byte[] convertToArray(DataRow dataRow) {
        this.validateRowData(dataRow);
        byte[] header = new byte[this.headerSize];
        ArrayList<byte[]> dados = new ArrayList<>();
        int size = this.headerSize;
        header[0] |= 1;
        for (Column c : columns) {
            byte[] data = dataRow.getData(c.getName());
            if (data == null) {
                if (c.canBeNull()) {
                    int posHeader = headerPosition.get(columns.indexOf(c));
                    header[posHeader / 8] |= 1 << (posHeader % 8);
                }
            } else {
                if (c.isDinamicSize()) {
                    ByteBuffer buff = ByteBuffer.allocate(4);
                    buff.order(ByteOrder.LITTLE_ENDIAN);
                    buff.putInt(data.length);
                    byte[] indice = buff.array();
                    dados.add(indice);
                    dados.add(data);
                    size += indice.length;
                    size += data.length;
                } else {
                    dados.add(data);
                    size += c.getSize();
                    if (data.length < c.getSize()) {
                        dados.add(new byte[c.getSize() - data.length]);
                    }
                }
            }
        }
        byte[] bufferRecord = new byte[size];
        int offset = this.headerSize;
        System.arraycopy(header, 0, bufferRecord, 0, header.length);
        for (byte[] data : dados) {
            System.arraycopy(data, 0, bufferRecord, offset, data.length);
            offset += data.length;
        }
        return bufferRecord;
    }

    /**
     * Generates a a byte array containing the values for the primary key
     * columns of the data row
     *
     * @param dataRow the data row
     * @return the generated byte array
     */
    public byte[] convertPrimaryKeyToByteArray(DataRow dataRow) {
        ArrayList<byte[]> dados = new ArrayList<>();
        int size = 0;
        for (Column c : columns) {
            if (!c.isPrimaryKey()) {
                continue;
            }
            byte[] data = dataRow.getData(c.getName());

            {
                dados.add(data);
                size += c.getSize();
                if (data.length < c.getSize()) {
                    dados.add(new byte[c.getSize() - data.length]);
                }
            }
        }
        byte[] bufferRecord = new byte[size];
        int offset = 0;
        for (byte[] data : dados) {
            System.arraycopy(data, 0, bufferRecord, offset, data.length);
            offset += data.length;
        }
        return bufferRecord;
    }

    /**
     * Generate a data row containing only the primary key values from an
     * original data row
     *
     * @param dataRow the original data row
     * @return the generated data row
     */
    public DataRow createPKRow(DataRow dataRow) {
        DataRow keyData = new DataRow();

        for (Column column : columns) {
            if (!dataRow.containsColumn(column.getName())) {
                continue;
            }
            if (column.isPrimaryKey()) {
                switch (column.getType()) {
                    case "STRING":
                        keyData.setString(column.getName(), dataRow.getString(column.getName()), column);
                        break;
                    case "INTEGER":
                        keyData.setInt(column.getName(), dataRow.getInt(column.getName()), column);
                        break;
                    case "LONG":
                        keyData.setLong(column.getName(), dataRow.getLong(column.getName()), column);
                        break;
                    case "FLOAT":
                        keyData.setFloat(column.getName(), dataRow.getFloat(column.getName()), column);
                        break;
                    default:
                        throw new AssertionError();
                }

            }
        }

        return keyData;
    }

}
