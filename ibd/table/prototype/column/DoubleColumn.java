package ibd.table.prototype.column;

import ibd.table.prototype.metadata.Metadata;

public class DoubleColumn extends Column{
    
    public DoubleColumn(String name,int size,short flags) {
        super(name, size, flags);
    }
    
    public DoubleColumn(String name) {
        super(name, (short) 8, Metadata.FLOATING_POINT);
    }
    
    @Override
    public String getType() {
        return Column.DOUBLE_TYPE;
    }
}
