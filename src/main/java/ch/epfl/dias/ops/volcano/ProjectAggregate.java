package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	private int count;

	private int maxInt;
	private int minInt;
	private int sumInt;

	private double maxDouble;
	private double minDouble;
	private double sumDouble;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		this.child.open();
		DBTuple current = this.child.next();
		while (!current.eof) {
			update(current);
			current = this.child.next();
		}
	}

	@Override
	public DBTuple next() {
		switch (this.agg) {
		case COUNT:
			return new DBTuple(new Object[] { this.count }, new DataType[] { DataType.INT });
		case SUM:
			switch (this.dt) {
			case INT:
				return new DBTuple(new Object[] { this.sumInt }, new DataType[] { DataType.INT });
			case DOUBLE:
				return new DBTuple(new Object[] { this.sumDouble }, new DataType[] { DataType.DOUBLE });
			default:
				return null;
			}
		case MIN:
			switch (this.dt) {
			case INT:
				return new DBTuple(new Object[] { this.minInt }, new DataType[] { DataType.INT });
			case DOUBLE:
				return new DBTuple(new Object[] { this.minDouble }, new DataType[] { DataType.DOUBLE });
			default:
				return null;
			}
		case MAX:
			switch (this.dt) {
			case INT:
				return new DBTuple(new Object[] { this.maxInt }, new DataType[] { DataType.INT });
			case DOUBLE:
				return new DBTuple(new Object[] { this.minDouble }, new DataType[] { DataType.DOUBLE });
			default:
				return null;
			}
		case AVG:
			switch (this.dt) {
			case INT:
				return new DBTuple(new Object[] { this.sumInt / (double) this.count },
						new DataType[] { DataType.DOUBLE });
			case DOUBLE:
				return new DBTuple(new Object[] { this.sumDouble / (double) this.count },
						new DataType[] { DataType.DOUBLE });
			default:
				return null;
			}
		default:
			return null;
		}
	}

	private void update(DBTuple tuple) {
		switch (this.dt) {
		case INT:
			Integer intValue = tuple.getFieldAsInt(fieldNo);
			if (intValue != null) {
				this.maxInt = Math.max(this.maxInt, intValue);
				this.minInt = Math.min(this.minInt, intValue);
				this.sumInt += intValue;
				this.count++;
			}
			break;
		case DOUBLE:
			Double doubleValue = tuple.getFieldAsDouble(fieldNo);
			if (doubleValue != null) {
				this.maxDouble = Math.max(this.maxDouble, doubleValue);
				this.minDouble = Math.min(this.minDouble, doubleValue);
				this.sumDouble += doubleValue;
				this.count++;
			}
			break;
		case BOOLEAN:
			Boolean booleanValue = tuple.getFieldAsBoolean(fieldNo);
			if (booleanValue != null) {
				this.count++;
			}
			break;
		case STRING:
			String stringValue = tuple.getFieldAsString(fieldNo);
			if (stringValue != null) {
				this.count++;
			}
			break;
		}
	}

	@Override
	public void close() {
		this.child.close();
	}

}
