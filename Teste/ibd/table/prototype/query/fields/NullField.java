package ibd.table.prototype.query.fields;

import ibd.table.prototype.column.Column;
import ibd.table.prototype.metadata.IntegerMetadata;
import ibd.table.prototype.metadata.Metadata;

public class NullField extends Field<Object>{
    public static final NullField generic = new NullField(IntegerMetadata.generic);

    public NullField(Metadata metadata) {
        super(metadata, null);
    }

    public NullField() {
        super(IntegerMetadata.generic, null);
    }
    
    @Override
    public String getType(){
        return Column.NULL_TYPE;
    }
    
    @Override
    protected Object constructData() {
        return null;
    }

    @Override
    public int compareTo(Field f) {
        if(f==null)return 0;
        return NULL_COMPARE * -1;
    }
}
