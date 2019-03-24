package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	VectorOperator child;
	int[] fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		this.child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
