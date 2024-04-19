package ibd.table.prototype.query.fields;

import ibd.table.prototype.BData;
import ibd.table.prototype.column.Column;
import ibd.table.prototype.metadata.BooleanMetadata;
import ibd.table.prototype.metadata.DoubleMetadata;
import ibd.table.prototype.metadata.Metadata;

public class DoubleField extends Field<Double>{
    public DoubleField(Metadata metadata, BData data) {
        super(metadata, data);
    }
    public DoubleField(double value) {
        super(DoubleMetadata.generic,value);
    }
    
    @Override
    public String getType(){
        return Column.DOUBLE_TYPE;
    }

    @Override
    protected Double constructData() {
        return data.getDouble();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        if(f.metadata.isString())return f.compareTo(this);
        if(!f.metadata.isFloat() && !f.metadata.isInt())return NOT_DEFINED;
        Double val;
        if(f.metadata.isInt()){
            if(f.metadata.getSize()==8){
                val = f.getLong().doubleValue();
            } else {
                val = f.getInt().doubleValue();
            }
        }else{
            if(f.metadata.getSize() == 4)
                val = f.getFloat().doubleValue();
            else
                val = f.getDouble();
        }
        return getBufferedData().compareTo(val);
    }
}
