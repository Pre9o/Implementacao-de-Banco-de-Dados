package ibd.table.prototype.query.fields;

import ibd.table.prototype.BData;
import ibd.table.prototype.column.Column;
import ibd.table.prototype.metadata.FloatMetadata;
import ibd.table.prototype.metadata.LongMetadata;
import ibd.table.prototype.metadata.Metadata;

public class LongField extends Field<Long>{
    public LongField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    public LongField(long value) {
        super(LongMetadata.generic, value);
    }
    
    @Override
    public String getType(){
        return Column.LONG_TYPE;
    }
    
    @Override
    protected Long constructData() {
        return data.getLong();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        if(f.metadata.isString() || f.metadata.isFloat())return f.compareTo(this);
        if(!f.metadata.isInt())return NOT_DEFINED;
        Long val;
        if(f.metadata.getSize()==8){
            val = f.getLong();
        } else {
            val = f.getInt().longValue();
        }
        return getBufferedData().compareTo(val);
    }
}
