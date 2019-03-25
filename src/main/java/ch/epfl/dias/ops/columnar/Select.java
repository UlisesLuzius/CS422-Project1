package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.DataType;

public class Select implements ColumnarOperator {

	private ColumnarOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] columns = this.child.execute();
		DBColumn column = columns[fieldNo];

		// if (column.getType() != DataType.INT)
		// return null;

		DBColumn[] res = new DBColumn[columns.length];
		for (int i = 0; i < columns.length; i++) {
			res[i] = new DBColumn(columns[i].getType());
		}

		Integer[] columnValues = column.getAsInteger();

		ArrayList<Integer> indices = new ArrayList<Integer>();
		for (int i = 0; i < columnValues.length; i++) {
			int v = columnValues[i];
			switch (op) {
			case LT:
				if (v < value)
					indices.add(i);
				break;
			case LE:
				if (v <= value)
					indices.add(i);
				break;
			case EQ:
				if (v == value)
					indices.add(i);
				break;
			case NE:
				if (v != value)
					indices.add(i);
				break;
			case GT:
				if (v > value)
					indices.add(i);
				break;
			case GE:
				if (v >= value)
					indices.add(i);
				break;
			}
		}

		for (int j = 0; j < res.length; j++) {
			for (int i = 0; i < indices.size(); i++) {
				res[j].add(columns[j].get(i));
			}
		}

		return res;
	}
}
