package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

public class HashJoin implements VolcanoOperator {

	private final VolcanoOperator leftChild;
	private final VolcanoOperator rightChild;
	private final int leftFieldNo;
	private final int rightFieldNo;

	private HashMap<Integer, ArrayList<DBTuple>> hashMap;
	private Iterator<DBTuple> iter;
	private DBTuple leftTuple;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.hashMap = new HashMap<Integer, ArrayList<DBTuple>>();
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		buildHashMap();
		iter = Collections.emptyIterator();
	}

	public void buildHashMap() {
		DBTuple currentRight = rightChild.next();
		while (!currentRight.eof) {
			int currentValue = currentRight.getFieldAsInt(rightFieldNo);
			ArrayList<DBTuple> tuples = this.hashMap.getOrDefault(currentValue, new ArrayList<DBTuple>());
			tuples.add(currentRight);
			hashMap.put(currentValue, tuples);
			currentRight = rightChild.next();
		}
	}

	@Override
	public DBTuple next() {
		if (!iter.hasNext()) {
			leftTuple = leftChild.next();
			ArrayList<DBTuple> matchTuples = null;
			while (matchTuples == null && !leftTuple.eof) {
				matchTuples = hashMap.get(leftTuple.getFieldAsInt(leftFieldNo));
				leftTuple = leftChild.next();
			}
			
			if (leftTuple.eof) {
				return leftTuple;
			} else {
				iter = matchTuples.iterator();
			}
			
		}
		return new DBTuple(leftTuple, iter.next(), rightFieldNo);
	}

	@Override
	public void close() {
		this.leftChild.close();
		this.rightChild.close();
	}
}
