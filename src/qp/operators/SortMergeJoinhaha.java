package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.util.ArrayList;

/** Creating a Join:
 *  1. Update RandomOptimizer to allow SortMergeJoin
 *  2. Update JoinType and numJoinTypes to allow SortMergeJoin
 *  3. Update PlanCost
 */

/** 1. open(): sort left and sort right
 *  2. next(): scan through sored left and right, and returns equal tuples
 *  3. close(): close left and right
 */
public class SortMergeJoinhaha extends Join {

	int batchSize;
	Batch outBatch;
	ArrayList<Integer> leftIndices;
	ArrayList<Integer> rightIndices;
	ArrayList<Attribute> leftAttributes;
	ArrayList<Attribute> rightAttributes;
	Sort sortedLeft;
	Sort sortedRight;
	Batch leftBatch;
	ArrayList<Tuple> leftBlock;
	Batch rightBatch;
	ArrayList<Tuple> rightBlock;
	boolean leftEos = false;
	boolean rightEos = false;
	int lcurs;
	int rcurs;
	boolean eosl;
	boolean eosr;
	Tuple leftFirst;
	Tuple rightFirst;
	Tuple leftLast;
	Tuple rightLast;


	public SortMergeJoinhaha(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	public boolean open() {
		System.out.println("Opening SortMerge");
		/** select number of tuples per batch **/
		int tuplesize = schema.getTupleSize();
		batchSize = Batch.getPageSize() / tuplesize;

		/** find indices attributes of join conditions **/
		leftIndices = new ArrayList<>();
		rightIndices = new ArrayList<>();
		leftAttributes = new ArrayList<>();
		rightAttributes = new ArrayList<>();
		for (Condition con : conditionList) {
			Attribute leftattr = con.getLhs();
			Attribute rightattr = (Attribute) con.getRhs();
			leftAttributes.add(leftattr);
			rightAttributes.add(rightattr);
			leftIndices.add(left.getSchema().indexOf(leftattr));
			rightIndices.add(right.getSchema().indexOf(rightattr));
		}

		sortedLeft = new Sort(left, leftAttributes, numBuff);
		sortedLeft.open();
		sortedRight = new Sort(right, rightAttributes, numBuff);
		sortedRight.open();

		// Add condition to check whether both sortedLeft and sortedRight are open
		return true;
	}

	public Batch next() {
		if (leftEos || rightEos) {
			// Do we need to close left and right here??
			// Either left or right has reached the end, return nothing, stop getting a batch
			close();
			return null;
		}
		/**
		 * TODO: Think what is the situation here:
		 * 1. Get first runs from sortedLeft and sortedRight
		 * 2. Record first and last tuple of left and right of current left and right batches that are read into memory:
		 *      - If first tuple of right > last tuple of left (all tuples in right greater than all tuples in left):
		 *              No need to compare these batches -> advance to next left batches
		 *      - If first tuple of left > last tuple of right (all tuples in left greater than all tuples in right):
		 *              No need to compare these batches -> advance to next right batches
		 *      - Else: we start with comparison: need two cursors to track left and right batches that are in memory
		 *              Advance to next left tuple until current left tuple's sort key >= right tuple's sort key
		 *              Then advance to next right tuple until current right tuple's sort key >= left tuple's sort key
		 *              When left tuple's sort key = right tuple's sort key:
		 *                  output all tuples with same left tuple but different right tuples with same sort key value
		 *                  advance to next left tuple
		 *                  if left batches finish: advance to next left batches
		 *                  if right batches finish: advance to next right batches
		 */

		/**
		 * Use ceiling((numBuff - 2)/2 to read left
		 *  Use floor((numBuff-2)/2) ro read right
		 */
		if (leftBatch == null) {
			for (int i = 0; i < Math.ceil((numBuff-2)/2); i++) {
				leftBatch = sortedLeft.next();
				if (leftBatch == null) {
					leftEos = true;
					return outBatch;
				}
				/** Load all tuples in the page to the leftBlock */
				for (int j = 0; j < leftBatch.size(); j++) {
					Tuple leftTuple = leftBatch.get(j);
					leftBlock.add(leftTuple);
					if (j == 0) {
						leftFirst = leftTuple;
					}
					if (j == leftBatch.size()-1) {
						leftLast = leftTuple;
					}
				}
			}
		}

		if (rightBatch == null) {
			for (int i = 0; i < Math.floor((numBuff-2)/2); i++) {
				rightBatch = sortedRight.next();
				if (rightBatch == null) {
					rightEos = true;
					return outBatch;
				}
				/** Load all tuples in the page to the rightBlock */
				for (int j = 0; j < rightBatch.size(); j++) {
					Tuple rightTuple = rightBatch.get(j);
					rightBlock.add(rightTuple);
					if (j == 0) {
						rightFirst = rightTuple;
					}
					if (j == rightBatch.size()-1) {
						rightLast = rightTuple;
					}
				}
			}
		}

		// TODO: Handles situations when there is no comparison needed at all

		Batch outBatch = new Batch(batchSize);
		Tuple leftTuple = leftBlock.get(lcurs);
		Tuple rightTuple = rightBlock.get(rcurs);
		// I have leftBlock and rightBlock for comparison now
		while (!outBatch.isFull()) {
			// Advance to next left tuple until current left tuple's sort key >= right tuple's sort key
			// Then advance to next right tuple until current right tuple's sort key >= left tuple's sort key
			// When left tuple's sort key = right tuple's sort key
			int compareResult = compareLeftRight(leftTuple, rightTuple);
			while (compareResult != 1) {
				if  (compareResult == 0) {
					rcurs ++;
					rightTuple = rightBlock.get(rcurs);
					// TODO: handle when rcurs reached end of rightBlock
					compareResult = compareLeftRight(leftTuple, rightTuple);
				}
				if (compareResult == 2) {
					lcurs ++;
					leftTuple = leftBlock.get(lcurs);
					// TODO: handle when lcurs reached end of leftBlock
					compareResult = compareLeftRight(leftTuple, rightTuple);
				}
			}
			// Pair this leftTuple with all rightTuples with same sort key value,
			// Use a new function here, do not change rcurs
			computeAllPairs(leftTuple, rightTuple);
		}

		return outBatch;
	}

	private int compareLeftRight(Tuple left, Tuple right) {
		// TODO: 0: left larger; 1: equal; 2: right larger
		if (left.checkJoin(right, leftIndices, rightIndices)) {
			return 1;
		}
		else {

		}
		return 0;
	}

	private void computeAllPairs(Tuple left , Tuple right) {
		// TODO:
		// Add all rightTuples with same sort key value as current leftTuples
		// Do not change rcurs
	}

}