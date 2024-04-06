package ibd.table.prototype.query.fields;

import ibd.table.prototype.BData;
import ibd.table.prototype.column.Column;
import ibd.table.prototype.metadata.FloatMetadata;
import ibd.table.prototype.metadata.IntegerMetadata;
import ibd.table.prototype.metadata.Metadata;

public class FloatField extends Field<Float>{
    private Integer value = null;
    public FloatField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    public FloatField(float value) {
        super(FloatMetadata.generic, value);
    }
    
    @Override
    public String getType(){
        return Column.FLOAT_TYPE;
    }
    
    @Override
    protected Float constructData() {
        return data.getFloat();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        if(f.metadata.isString())return f.compareTo(this);
        if(!f.metadata.isFloat() && !f.metadata.isInt())return NOT_DEFINED;
        if(f.metadata.isFloat() && f.metadata.getSize() == 8)return f.compareTo(this);
        Float val;
        if(f.metadata.isInt()){
            if(f.metadata.getSize()==8){
                val = f.getLong().floatValue();
            } else {
                val = f.getInt().floatValue();
            }
        }else{
            val = f.getFloat();
        }
        return getBufferedData().compareTo(val);
    }
}
