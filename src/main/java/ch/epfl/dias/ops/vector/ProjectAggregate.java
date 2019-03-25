package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VectorOperator {

	private final VectorOperator child;
	private final Aggregate agg;
	private final DataType dt;
	private final int fieldNo;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] col = child.next();
		if(child.next() == null) {
			return null;
		}
		
		Double res = 0.0;
		int count = 0;
		
		switch (agg) {
		case SUM:
			do{
				for (int i = 0; i < col[fieldNo].size(); i++) {
					res += (Double) col[fieldNo].get(i);
				}
			} while ((col = child.next()) != null);
			
		case MIN:
			res = Double.MAX_VALUE;
			do {
				for (int i = 0; i < col[fieldNo].size(); i++) {
					res = Math.min((Double) col[fieldNo].get(i), res);
				}
			} while ((col = child.next()) != null);
			break;
			
		case MAX:
			res = Double.MIN_VALUE;
			do {
				for (int i = 0; i < col[fieldNo].size(); i++) {
					res = Math.max((Double) col[fieldNo].get(i), res);
				}
			} while ((col = child.next()) != null);
			break;

		case COUNT:
			do {
				count += col[fieldNo].size();
			} while ((col = child.next()) != null);
			break;

		case AVG:
			do {
				count += col[fieldNo].size();
				for (int i = 0; i < col[fieldNo].size(); i++) {
					res += (Double) col[fieldNo].get(i);
				}
			} while ((col = child.next()) != null);
			break;
		}

		switch (agg) {
		case COUNT:
			return new DBColumn[] { new DBColumn(new Object[] { count }, DataType.INT) };
		case AVG:
			if(count == 0) {
				return new DBColumn[] { new DBColumn(new Object[] { 0.0 }, DataType.DOUBLE) };				
			} else {
				return new DBColumn[] { new DBColumn(new Object[] { res / count }, DataType.DOUBLE) };				
			}
		default:
			return new DBColumn[] { new DBColumn(new Object[] { res }, DataType.DOUBLE) };
		}
	}

	@Override
	public void close() {
		child.close();
	}

}
