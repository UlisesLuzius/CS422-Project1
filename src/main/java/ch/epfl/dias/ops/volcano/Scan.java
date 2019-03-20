package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	// TODO: Add required structures
	private Store store;
	private int index;

	public Scan(Store store) {
		this.store = store;
	}

	@Override
	public void open() {
		this.index = 0;
	}

	@Override
	public DBTuple next() {
		DBTuple tuple = this.store.getRow(index);
		this.index++;
		return tuple;
	}

	@Override
	public void close() {
		this.index = 0;
	}
}