package ibd.table.prototype.query.fields;

import ibd.table.prototype.BData;
import ibd.table.prototype.metadata.Metadata;

import java.util.Arrays;
import ibd.table.prototype.column.Column;

public class BinaryField extends Field<byte[]>{
    public BinaryField(Metadata metadata, BData data) {
        super(metadata, data);
    }
    
    @Override
    public String getType(){
        return Column.BINARY_TYPE;
    }

    @Override
    protected byte[] constructData() {
        return data.getData();
    }

    @Override
    public int compareTo(Field f) {
        if(f==null)return 0;
        return Arrays.compare(getBufferedData(),f.data.getData());
    }
}
