package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	private VolcanoOperator leftChild;
	private VolcanoOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private HashMap<Integer, ArrayList<DBTuple>> hashMap;
	private ArrayList<DBTuple> tuples;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		this.leftChild.open();
		this.rightChild.open();
		
		DBTuple currentRight = rightChild.next();
		while(!currentRight.eof) {
			int currentValue = currentRight.getFieldAsInt(rightFieldNo);
			ArrayList<DBTuple> tuples = this.hashMap.getOrDefault(currentValue, new ArrayList<DBTuple>());
			tuples.add(currentRight);
			this.hashMap.put(currentValue, tuples);
		}
		
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
