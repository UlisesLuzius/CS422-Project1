package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	private VolcanoOperator leftChild;
	private VolcanoOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private HashMap<Integer, ArrayList<DBTuple>> hashMap;
	private ArrayList<DBTuple> currentRightTuples;
	private Iterator<DBTuple> iter;
	private DBTuple currentLeft;
	private boolean inTuple;
	private int rightIndex;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.hashMap = new HashMap<Integer, ArrayList<DBTuple>>();
		this.currentRightTuples = new ArrayList<DBTuple>();
		this.iter = this.currentRightTuples.iterator();
		this.rightIndex = 0;
	}

	@Override
	public void open() {
		this.leftChild.open();
		this.rightChild.open();

		this.buildHashMap();

	}

	public void buildHashMap() {
		DBTuple currentRight = rightChild.next();
		while (!currentRight.eof) {
			int currentValue = currentRight.getFieldAsInt(rightFieldNo);
			ArrayList<DBTuple> tuples = this.hashMap.getOrDefault(currentValue, new ArrayList<DBTuple>());
			tuples.add(currentRight);
			this.hashMap.put(currentValue, tuples);
			currentRight = rightChild.next();
		}
	}

	@Override
	public DBTuple next() {
		if (this.inTuple) {
			return this.nextInMap();
		} else {
			return this.nextBatch();
		}
	}

	private DBTuple nextBatch() {
		this.currentLeft = leftChild.next();
		if (this.currentLeft.eof) {
			return this.currentLeft;
		}
		this.inTuple = true;
		this.rightIndex = 0;
		this.currentRightTuples = hashMap.get(this.currentLeft.getFieldAsInt(leftFieldNo));
		this.iter = this.currentRightTuples.iterator();

		return nextInMap();
	}

	private DBTuple nextInMap() {
		if (iter.hasNext()) {
			return join(this.currentLeft, this.iter.next());
		} else {
			return this.nextBatch();
		}
	}

	@Override
	public void close() {
		this.leftChild.close();
		this.rightChild.close();
	}

	private DBTuple join(DBTuple left, DBTuple right) {
		Object[] leftFields = left.getFields();
		Object[] rightFields = right.getFields();
		int leftLength = leftFields.length;
		int rightLength = rightFields.length;

		DataType[] leftTypes = left.getTypes();
		DataType[] rightTypes = right.getTypes();

		Object[] finalFields = new Object[leftLength + rightLength - 1];
		DataType[] finalDataTypes = new DataType[leftLength + rightLength - 1];

		for (int i = 0; i < leftLength; i++) {
			finalFields[i] = leftFields[i];
			finalDataTypes[i] = leftTypes[i];
		}

		for (int i = 0; i < rightFieldNo; i++) {
			finalFields[leftLength + i] = rightFields[i];
			finalDataTypes[leftLength + i] = rightTypes[i];
		}
		for (int i = rightFieldNo + 1; i < rightLength; i++) {
			finalFields[leftLength + i - 1] = rightFields[i];
			finalDataTypes[leftLength + i - 1] = rightTypes[i];
		}
		return new DBTuple(finalFields, finalDataTypes);
	}
}
