package ibd.table.prototype.query.fields;

import ibd.table.prototype.BData;
import ibd.table.prototype.column.Column;
import ibd.table.prototype.metadata.IntegerMetadata;
import ibd.table.prototype.metadata.Metadata;
import ibd.table.prototype.metadata.StringMetadata;
import ibd.table.util.Util;

public class StringField extends Field<String>{
    public StringField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    public StringField(String str) {
        super(new StringMetadata((short)str.length()), str);
    }
    
    @Override
    public String getType(){
        return Column.STRING_TYPE;
    }

    @Override
    protected String constructData() {
        return data.getString();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        String val;
        if(!f.metadata.isString()){
            switch (Util.typeOfColumn(f.metadata)){
            case "boolean":
            case "string":
            case "long":
            case "int":
            case "double":
            case "float":
                val = String.valueOf(f.getBoolean());
                break;
            case "null":
            default:
                val = null;
            }
        }else{
            val = f.getString();
        }
        return getBufferedData().compareTo(val);
    }
}
