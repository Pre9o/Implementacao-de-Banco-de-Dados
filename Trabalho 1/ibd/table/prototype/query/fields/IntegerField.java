package ibd.table.prototype.query.fields;

import ibd.exceptions.DataBaseException;
import ibd.table.prototype.BData;
import ibd.table.prototype.column.Column;
import ibd.table.prototype.metadata.IntegerMetadata;
import ibd.table.prototype.metadata.Metadata;
import ibd.table.util.Util;

public class IntegerField extends Field<Integer> {

    public IntegerField(Metadata col, BData data) {
        super(col, data);
        if(Util.typeOfColumn(col)!="int")throw new DataBaseException("IntField->Constructor","IntField needs to be int");
    }
    public IntegerField(int value) {
        super(IntegerMetadata.generic, value);
    }
    
    @Override
    public String getType(){
        return Column.INTEGER_TYPE;
    }

    @Override
    protected Integer constructData() {
        return data.getInt();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        if(f.metadata.isString() || f.metadata.isFloat())return f.compareTo(this);
        if(!f.metadata.isInt())return NOT_DEFINED;
        if(f.metadata.getSize()==8)return f.compareTo(this);
        return getBufferedData().compareTo(f.getInt());
    }

}
