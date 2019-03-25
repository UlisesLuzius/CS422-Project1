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
		DBColumn[] columns = child.next();
		if(columns == null) {
			return null;
		}
		DBColumn[] res = new DBColumn[fieldNo.length];
		for(int i = 0; i < fieldNo.length; i++) {
			res[i] = columns[fieldNo[i]];
		}
		return res;
	}

	@Override
	public void close() {
		this.child.close();
	}
}
