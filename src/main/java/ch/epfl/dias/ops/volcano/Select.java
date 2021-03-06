package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public void open() {
		this.child.open();
	}

	@Override
	public DBTuple next() {
		boolean select = false;
		DBTuple tuple = this.child.next();

		while (!tuple.eof && !(select = filter(tuple.getFieldAsInt(fieldNo), this.op, this.value))) {
			tuple = child.next();
		}
		return tuple;
	}

	public boolean filter(int field, BinaryOp op, int value) {
		switch (op) {
		case LT:
			return field < value;
		case LE:
			return field <= value;
		case EQ:
			return field == value;
		case NE:
			return field != value;
		case GT:
			return field > value;
		case GE:
			return field >= value;
		default:
			return false;
		}
	}

	@Override
	public void close() {
		this.child.close();
	}
}
