package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

    private int vectorsize;
    private int fromIndex;
    private int toIndex;
    private ColumnStore store;

    public Scan(Store store, int vectorsize) {
        this.store = (ColumnStore) store;
        this.vectorsize = vectorsize;
    }

    @Override
    public void open() {
        fromIndex = 0;
    }

    @Override
    public DBColumn[] next() {
        if(fromIndex > store.getColumnSize()) {
          return null;
        }
        int toIndex = Math.min(store.getColumnSize(), fromIndex + vectorsize);
        DBColumn[] res = store.getColumnsVector(fromIndex, toIndex);
        fromIndex += vectorsize;
        return res;
    }

    @Override
    public void close() {
        fromIndex = 0;
    }
}
