package ibd.table.prototype.column;

import ibd.table.prototype.metadata.Metadata;

public class BooleanColumn extends Column {

    public BooleanColumn(String name, int size, short flags) {
        super(name, size, flags);
    }

    public BooleanColumn(String name) {
        super(name, (short) 1, Metadata.BOOLEAN);
    }

    @Override
    public String getType() {
        return Column.BOOLEAN_TYPE;
    }
}
