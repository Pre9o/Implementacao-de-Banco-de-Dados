package ibd.table.prototype.query.fields;

import ibd.table.prototype.BData;
import ibd.table.prototype.column.Column;
import ibd.table.prototype.metadata.BooleanMetadata;
import ibd.table.prototype.metadata.Metadata;
import ibd.table.prototype.metadata.StringMetadata;

public class BooleanField extends Field<Boolean>{
    public BooleanField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    public BooleanField(boolean value) {
        super(BooleanMetadata.generic,value);
    }
    
    @Override
    public String getType(){
        return Column.BOOLEAN_TYPE;
    }

    @Override
    protected Boolean constructData() {
        return data.getBoolean();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        if(!f.metadata.isBoolean())return NOT_DEFINED;
        return getBufferedData().compareTo(f.getBoolean());
    }
}
