package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements ColumnarOperator {

	private ColumnarOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn column = this.child.execute()[fieldNo];
		switch (this.agg) {
		case COUNT:
			break;
		case SUM:
			break;
		case MIN:
			break;
		case MAX:
			break;
		case AVG:
			break;
		}
	}

	public DBColumn[] count(DBColumn column) {
		DBColumn result = new DBColumn(DataType.INT);
		result.add(column.getAsObject().length);

		return new DBColumn[] { result };
	}

	public DBColumn[] sum(DBColumn column) {

		switch (this.dt) {
		case INT:

			break;
		case DOUBLE:
			break;
		}
	}

	public DBColumn[] min(DBColumn column) {

	}

	public DBColumn[] max(DBColumn column) {

	}

	public DBColumn[] avg(DBColumn column) {

	}
}
