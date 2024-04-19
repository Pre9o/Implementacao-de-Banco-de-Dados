package ibd.table.prototype.column;

import ibd.table.prototype.metadata.Metadata;

public class FloatColumn extends Column{
    
    public FloatColumn(String name,int size,short flags) {
        super(name, size, flags);
    }
    
    public FloatColumn(String name) {
        super(name, (short) 4, Metadata.FLOATING_POINT);
    }
    
    @Override
    public String getType() {
        return Column.FLOAT_TYPE;
    }
}
