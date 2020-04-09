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

 // ASSUMPTIONS: 
 // 1. for left schema: .next(): the same value are always read into one batch
 // 2. for right schema: buffer size is enough to hold the largest partition of the right schema, 
 // 					if not enough, execution is terminated with an error message 

public class SortMergeJoin extends Join {

	// The number of tuples we can have per page
	int batchSize;

	// The buffer for the left input page
	Batch leftBatch;
	// The buffer for the right input page
	Batch rightBatch;
	// Everytime, we join a right partition with one left tuple
	ArrayList<Tuple> rightPartition;
	// The output page
	Batch outBatch;

	// The list of integer indices of join attributes from left table
	ArrayList<Integer> leftIndices;
	// The list of integer indices of join attributes from right table
	ArrayList<Integer> rightIndices;
	// The list of join attributes from left table
	ArrayList<Attribute> leftAttributes;
	// The list of join attributes from right table
	ArrayList<Attribute> rightAttributes;

	// The left table that is sorted
	Sort sortedLeft;
	// The right table that is sorted
	Sort sortedRight;

	// Cursor for left page
	int lcurs;
	// Cursor for right page
	int rcurs;
	// Cursor for right partition 
	int partitionEosr;

	// Indicates whether end of left table is reached
	boolean eosl = false;
	// Inddicates whether end of right table is reached
	boolean eosr = false;

	// The left tuple that is being read from left input page
	Tuple leftTuple;
	// The right tuple that is being read from right input page
	Tuple rightTuple;

	// The next left tuple from current left tuple
	Tuple nextLeftTuple;
	// The next right tuple from current right tuple
	Tuple nextTuple = null;

	/**
	 * Initialise a new join operator using Sort-Merge Join algorith
	 * @param jn jn is the base join operator
	 */
	public SortMergeJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	/**
	 * Sort left and right and store as files
	 */
	public boolean open() {
		/** select number of tuples per batch **/
		int tuplesize = schema.getTupleSize();
		batchSize = Batch.getPageSize() / tuplesize;

		// Get the attributes and indices of attributes of join conditions
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

		// Sort both left and right table
		sortedLeft = new Sort(left, leftAttributes, numBuff);
		sortedLeft.open();
		sortedRight = new Sort(right, rightAttributes, numBuff);
		sortedRight.open();

		return true;
	}

	/**
	 * Perform merging:
	 * 1. Read in left page, then read in one tuple from left page
	 * 2. Read in one partition from right table. A partition have the join keys of the same values. Then read in one tuple from right page
	 * 3. Compare their join keys and advance the one with smaller values until equality is found
	 * 4. Add join results (current left tuple join with all tuples in right partition), return output page whenever it is full
	 * 5. Continue joining results or get next left/right tuples to repeat step 3 - 4
	 */
	public Batch next() {
		// Returns null if either tables have reached the end
		if (eosl || eosr) {
			close();
			return null;
		}

		// When first calling .next(), leftBatch and rightBatch are both null
		if (leftBatch == null) {
			leftBatch = sortedLeft.next();
			if (leftBatch == null || leftBatch.isEmpty()) {
				eosl = true;
				return null;
			}
			leftTuple = leftBatch.get(lcurs);
		}
		if (rightBatch == null) {
			rightBatch = sortedRight.next();
			if (rightBatch == null || rightBatch.isEmpty()) {
				eosr = true;
				return outBatch;
			}
			rightPartition = getNextPartition();
			if (checkPartitionSize(rightPartition)) {
				System.exit(0); // if exceeds the buffers available, terminate the process
			}
			partitionEosr = 0;
			rightTuple = rightPartition.get(0);
		}

		// The output page
		Batch outBatch = new Batch(batchSize);
		while (!outBatch.isFull()) {
			int compareResult = Tuple.compareTuples(leftTuple, rightTuple, leftIndices, rightIndices);
			if (compareResult == 0) { // left and right tuples are equal
				outBatch.add(leftTuple.joinWith(rightTuple)); // add join result into outBatch, continues to next comparison
				if (partitionEosr < rightPartition.size() - 1) {// read next tuple from right partition
					partitionEosr++;
					rightTuple =  rightPartition.get(partitionEosr);
				}
				else if (partitionEosr == rightPartition.size()-1) {// finish reading right partition
					lcurs++;
					if (lcurs == leftBatch.size()) {// left cursor reached the size of leftBatch
						leftBatch = sortedLeft.next();
						if (leftBatch == null || leftBatch.isEmpty()) {
							eosl = true;
							break;
						}
						lcurs = 0;
					}
					nextLeftTuple = leftBatch.get(lcurs);
					if (nextLeftTuple == null) {
						eosl = true;
						break;
					}
					compareResult = Tuple.compareTuples(nextLeftTuple, leftTuple, leftIndices, leftIndices);
					leftTuple = nextLeftTuple;
					if (compareResult == 0) {// two consecutive tuples has equal values
						partitionEosr = 0;
						rightTuple = rightPartition.get(partitionEosr);
					} else {// we can get next partition for right
						rightPartition = getNextPartition();
						if (rightPartition == null || rightPartition.size() == 0) {
							eosr = true;
							break;
						}
						partitionEosr = 0;
						rightTuple = rightPartition.get(partitionEosr);
					}
				}
			}
			else if (compareResult > 0) { // left tuple is larger, advance right, remember for right we always advance by partition
				rightPartition = getNextPartition(); // get next right partition
				if (rightPartition == null || rightPartition.isEmpty() || rightPartition.size() == 0) {
					eosr = true;
					break;
				}
				partitionEosr = 0;
				rightTuple = rightPartition.get(partitionEosr);
			}
			else if (compareResult < 0) { // right tuple is larger, advance left
				lcurs++;
				if (lcurs == leftBatch.size()) { // no more tuples to read in leftBatch
					leftBatch = sortedLeft.next(); //read in new Batch
					if (leftBatch == null || leftBatch.isEmpty()) {
						eosl = true;
						break;
					}
					lcurs = 0; // set cursor back to 0
				}
				leftTuple = leftBatch.get(lcurs);
			}
		}
		return outBatch;
	}

	/**
	 * Keep reading in right tuples until there is a value change
	 * Remember we always get right by partition
	 * @return a right partition that is to be compared with left tuple
	 */

	private ArrayList<Tuple> getNextPartition() {
		ArrayList<Tuple> partition = new ArrayList<Tuple>();
		int compResult = 0;

		if (rightBatch == null || rightBatch.isEmpty()) {
			return partition;
		}

		if (nextTuple == null) {//if there is no nextTuple, it is the first time we create nextTuple
			if (rcurs == rightBatch.size()) { // get a batch
				rightBatch = sortedRight.next();
				if (rightBatch == null) { // if nothing more in right batch, reached the end of right table
					eosr = true;
					if (checkPartitionSize(partition)) {
						System.exit(0); // if exceeds the buffers available, terminate the process
					}
					return partition;
				}
				rcurs = 0;
			}
			nextTuple = rightBatch.get(rcurs); //get nextTuple
			if (nextTuple == null) {
				return partition;
			}
			rcurs++;
		}

		while (compResult == 0) {
			partition.add(nextTuple); // add in to partion, check if next partition has the same value

			// Get next right tuple: 1. get next right tuple using left cursor; 2. right tuple is the end of the batch size; get next batch
			if (rcurs == rightBatch.size()) {//current rightBatch has reached the end
				rightBatch = sortedRight.next(); //get next rightBatch
				if (rightBatch == null || rightBatch.isEmpty()) { //if next rightBatch is null
					eosr = true;
					return partition; //next Batch 
				}
				rcurs = 0;
			}
			nextTuple = rightBatch.get(rcurs);
			rcurs++;
			compResult = Tuple.compareTuples(partition.get(0), nextTuple, rightIndices, rightIndices);
		}
		if (checkPartitionSize(partition)) {
			System.exit(0); // if exceeds the buffers available, terminate the process
		}
		return partition;
	}

	/**
	 * To check wether a partition size has exceeded the size of buffers available
	 * @param partition
	 * @return boolean to indicate whether it has exceeded
	 */
	private boolean checkPartitionSize(ArrayList<Tuple> partition) {
		int numTuples = batchSize * (numBuff - 2);
		if (partition.size() > numTuples) {
			System.out.println("A partition size is larger than buffer size, try larger buffer size"); // Report an error message, stop the execution
			return true;
		}
		return false;
	}

	/**
	 * Close this join
	 */
	public boolean close() {
		sortedLeft.close();
		sortedRight.close();
		return true;
	}

}