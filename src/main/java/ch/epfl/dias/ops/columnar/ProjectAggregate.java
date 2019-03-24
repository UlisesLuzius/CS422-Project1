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
			return this.count(column);
		case SUM:
			return this.sum(column);
		case MIN:
			return this.min(column);
		case MAX:
			return this.max(column);
		case AVG:
			return this.avg(column);
		default:
			return null;
		}

	}

	public DBColumn[] count(DBColumn column) {
		DBColumn result = new DBColumn(DataType.INT);
		result.add(column.getAsObject().length);

		return new DBColumn[] { result };
	}

	public DBColumn[] sum(DBColumn column) {
		DBColumn result = new DBColumn(this.dt);
		double sum = 0.0;
		switch (this.dt) {
		case INT:
			Integer[] intValues = column.getAsInteger();
			for (int value : intValues) {
				sum += value;
			}
			result.add((int) sum);
			break;
		case DOUBLE:
			Double[] doubleValues = column.getAsDouble();
			for (double value : doubleValues) {
				sum += value;
			}
			result.add(sum);
			break;
		}
		return new DBColumn[] { result };
	}

	public DBColumn[] min(DBColumn column) {
		DBColumn result = new DBColumn(this.dt);
		double min = Double.MAX_VALUE;
		switch (this.dt) {
		case INT:
			Integer[] intValues = column.getAsInteger();
			for (int value : intValues) {
				min = Double.min(min, value);
			}
			result.add((int) min);
			break;
		case DOUBLE:
			Double[] doubleValues = column.getAsDouble();
			for (double value : doubleValues) {
				min = Double.min(min, value);
			}
			result.add(min);
			break;
		}
		result.add(min);
		return new DBColumn[] { result };
	}

	public DBColumn[] max(DBColumn column) {
		DBColumn result = new DBColumn(this.dt);
		double max = Double.MIN_VALUE;
		switch (this.dt) {
		case INT:
			Integer[] intValues = column.getAsInteger();
			for (int value : intValues) {
				max = Double.max(max, value);
			}
			result.add((int) max);
			break;
		case DOUBLE:
			Double[] doubleValues = column.getAsDouble();
			for (double value : doubleValues) {
				max = Double.min(max, value);
			}
			result.add(max);
			break;
		}
		result.add(max);
		return new DBColumn[] { result };
	}

	public DBColumn[] avg(DBColumn column) {
		DBColumn result = new DBColumn(this.dt);

		DBColumn top = sum(column)[0];
		DBColumn bottom = count(column)[0];

		double avg = 0.0;

		switch (this.dt) {
		case INT:
			avg = (double) top.getAsInteger()[0] / (double) bottom.getAsInteger()[0];
			break;
		case DOUBLE:
			avg = top.getAsDouble()[0] / bottom.getAsDouble()[0];
			break;
		}
		result.add(avg);
		return new DBColumn[] { result };
	}
}
