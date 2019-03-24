package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

	private Store store;
	private int vectorsize;
	private int index;

	public Scan(Store store, int vectorsize) {
		this.store = store;
		this.vectorsize = vectorsize;
	}

	@Override
	public void open() {
		this.index = 0;
	}

	@Override
	public DBColumn[] next() {
		
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
