package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements VectorOperator {

	private VectorOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;
	private int idx;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public void open() {
		this.child.open();
		idx = 0;
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] columns = child.next();
		if (columns == null) {
			return null;
		}
		DBColumn[] res = new DBColumn[columns.length];
		int matchs = 0;
		int vectorsize = columns[fieldNo].size();
		while (columns != null && matchs < vectorsize) {
			while (idx < columns[fieldNo].size() && matchs < columns[fieldNo].size()) {
				if (filter((Integer) columns[fieldNo].get(idx), op, value)) {
					matchs++;
					for (int i = 0; i < columns.length; i++) {
						res[i].add(columns[i].get(idx));
					}
				}
				idx++;
			}
			if(matchs < vectorsize) {
				columns = child.next();
				idx = 0;
			}
		}
		return columns;
	}

	private boolean filter(int field, BinaryOp op, int value) {
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
		idx = 0;
	}
}
