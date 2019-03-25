package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	private VolcanoOperator child;
	private int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		this.child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple tuple = child.next();

		if (tuple.eof) {
			return tuple;
		} else {
			Object[] fields = new Object[fieldNo.length];
			DataType[] newTypes = new DataType[fieldNo.length];
			DataType[] oldTypes = tuple.getTypes();

			for (int i = 0; i < fieldNo.length; i++) {
				fields[i] = tuple.getFieldAsObject(fieldNo[i]);
				newTypes[i] = oldTypes[fieldNo[i]];
			}
			return new DBTuple(fields, newTypes);
		}
	}

	@Override
	public void close() {
		this.child.close();
	}
}
