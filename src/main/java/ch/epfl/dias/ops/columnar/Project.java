package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements ColumnarOperator {

	private ColumnarOperator child;
	private int[] columns;

	public Project(ColumnarOperator child, int[] columns) {
		this.child = child;
		this.columns = columns;
	}

	public DBColumn[] execute() {
		DBColumn[] columns = this.child.execute();
		DBColumn[] result = new DBColumn[this.columns.length];

		for (int i = 0; i < this.columns.length; i++) {
			result[i] = columns[this.columns[i]];
		}

		return result;
	}
}
