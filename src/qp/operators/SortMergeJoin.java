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
public class SortMergeJoin extends Join {

	int batchSize;
	Batch outBatch;
	ArrayList<Integer> leftIndices;
	ArrayList<Integer> rightIndices;
	ArrayList<Attribute> leftAttributes;
	ArrayList<Attribute> rightAttributes;
	Sort sortedLeft;
	Sort sortedRight;
	Batch leftBatch;
	Batch rightBatch;
	int lcurs;
	int rcurs;
	boolean eosl = false;
	boolean eosr = false;
	ArrayList<Tuple> rightPartition;
	int partitionEosr;
	Tuple leftTuple;
	Tuple rightTuple;


	public SortMergeJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	public boolean open() {
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
		System.out.println("SMJ: Entering next()");
		// Read one left page and always read in partition into the memory
		if (eosl || eosr) {
			System.out.println("SMJ: Closing next()");
			close();
			return null;
		}
		if (leftBatch == null) {
			leftBatch = sortedLeft.next();
			//System.out.print("SMJ: left batch is: ");
			//Debug.PPrint(leftBatch);
			if (leftBatch == null) {
				eosl = true;
				return null;
			}
			// read in next left tuple
			System.out.println("first left Batch is");
			Debug.PPrint(leftBatch);
			leftTuple = leftBatch.get(lcurs);
		}
		if (rightBatch == null) {
			rightBatch = sortedRight.next();
			if (rightBatch == null) {
				eosr = true;
				return null;
			}
			System.out.println("SMJ: first rightBatch is ");
			Debug.PPrint(rightBatch);
			rightPartition = getNextPartition();
			//TODO: try & catch: Print error when next partition exceeds buffer number
			partitionEosr = 0;
			rightTuple = rightPartition.get(0);
			//System.out.println("SMJ: Right tuple is");
			//Debug.PPrint(rightTuple);
		}

		Batch outBatch = new Batch(batchSize);
		while (!outBatch.isFull()) {
			int compareResult = Tuple.compareTuples(leftTuple, rightTuple, leftIndices, rightIndices);
			if (compareResult == 0) { // left and right tuples are equal
				System.out.println("SMJ: Equal found");
				outBatch.add(leftTuple.joinWith(rightTuple)); // add join result into outBatch, continues to next comparison
				if (partitionEosr == rightPartition.size()-1) {//finish reading right partition
					// if next left tuple has the same key value as current left tuple, we don't advance right partition first
					Tuple lastLeftTuple = leftTuple;
					lcurs++;
					if (lcurs == leftBatch.size()) {//left cursor reached the size of leftBatch
						leftBatch = sortedLeft.next();
						if (leftBatch == null || leftBatch.isEmpty()) {
							eosl = true;
							break;
						}
						lcurs = 0;
					}
					/*
					System.out.println("SMJ: lcurs here is: ");
					System.out.println(lcurs);
					System.out.println("left Batch is: ");
					Debug.PPrint(leftBatch);
					*/
					leftTuple = leftBatch.get(lcurs);
					//lcurs++;
					if (leftTuple == null) {
						eosl = true;
						break;
					}
					System.out.println("SMJ: Checking two consecutive left tuples");
					Debug.PPrint(lastLeftTuple);
					Debug.PPrint(leftTuple);
					compareResult = Tuple.compareTuples(lastLeftTuple, leftTuple, leftIndices, rightIndices);

					if (compareResult == 0) {// two consecutive tuples has equal values
						partitionEosr = 0;
						rightTuple = rightPartition.get(partitionEosr);
					} else {// we can get next partition for right
						rightPartition = getNextPartition();
						if (rightPartition.size() == 0) {
							eosr = true;
							break;
						}
						partitionEosr = 0;
						rightTuple = rightPartition.get(partitionEosr);
					}
				}
				else if (partitionEosr < rightPartition.size() - 1) {//read next tuple from right partition
					partitionEosr++;
					System.out.println("partionEsor is " + partitionEosr);
					rightTuple =  rightPartition.get(partitionEosr);
				}
			}
			else if (compareResult > 0) { // left tuple is larger, advance right, remember for right we always advance by partition
				System.out.println("SMJ: Advance right");
				rightPartition = getNextPartition(); // get next right partition
				if (rightPartition.size() == 0) {
					eosr = true;
					break;
				}
				partitionEosr = 0;
				rightTuple = rightPartition.get(partitionEosr);
			}
			else if (compareResult < 0) { // right tuple is larger, advance left
				System.out.println("SMJ: Advance left ");
				lcurs++;
				if (lcurs == leftBatch.size()) { // no more tuples to read in leftBatch
					leftBatch = sortedLeft.next(); //read in new Batch
					if (leftBatch.isEmpty()) {
						eosl = true;
						break;
					}
					lcurs = 0; // set cursor back to 0
				}
				leftTuple = leftBatch.get(lcurs);
				Debug.PPrint(leftTuple);
			}
		}
		return outBatch;
	}

	private ArrayList<Tuple> getNextPartition() {
		System.out.println("Generatin next partition");
		/** 
			we keep reading in right tuples until there is a value change
			Remember we always get right by partition
		*/
		ArrayList<Tuple> partition = new ArrayList<Tuple>();
		int compResult = 0;
		Tuple next = null;

		/*
		//get next right tuple
		if (rightBatch == null) {//if current rightBatch is already null, we can't get any rightPartition
			return partition;
		}
		else if (rcurs == rightBatch.size()) {//current rightBatch has reached the end
			rightBatch = sortedRight.next(); //get next rightBatch
			//System.out.println("SMJ: current rightBatch has reached the end");
			if (rightBatch == null) { //if next rightBatch is null
				return null; 
			}
			Debug.PPrint(rightBatch);
			rcurs = 0;
		}
		//System.out.println("SMJ: Getting next right tuple from rightBatch");
		next = rightBatch.get(rcurs);
		//System.out.println("SMJ: Next right tuple is :");
		//Debug.PPrint(next);
		if (next == null) {
			return null;
		}
		rcurs++;
		compResult = 0;
		*/

		if (rightBatch == null || rightBatch.isEmpty()) {
			return partition;
		}
		else if (rcurs == rightBatch.size()) {
			rightBatch = sortedRight.next();
			if (rightBatch == null || rightBatch.isEmpty()) {
				checkPartitionSize(partition);
				return partition;
			}
			rcurs = 0;
		}
		next = rightBatch.get(rcurs);
		//rcurs++;

		while (compResult == 0) {
			rcurs++;
			System.out.println("SMJ: adding this tuple into partition");
			Debug.PPrint(next);
			partition.add(next);
			// get next right tuple
			//if (next == null) {//get next right tuple
			System.out.print("SMJ: rcurs is: ");
			System.out.println(rcurs);
			if (rcurs == rightBatch.size()) {//current rightBatch has reached the end
				rightBatch = sortedRight.next(); //get next rightBatch
				if (rightBatch == null || rightBatch.isEmpty()) { //if next rightBatch is null
					checkPartitionSize(partition);
					return partition;
				}
				rcurs = 0;
			}
			next = rightBatch.get(rcurs);
			//System.out.println("SMJ: checking two tuples");
			//Debug.PPrint(partition.get(0));
			//Debug.PPrint(next);
			compResult = Tuple.compareTuples(partition.get(0), next, rightIndices, rightIndices);
		}
		System.out.println("SMJ: Finish generating right partition");
		System.out.println();
		checkPartitionSize(partition);
		return partition;
	}

	// Do nothing, just report an error message
	private void checkPartitionSize(ArrayList<Tuple> partition) {
		//TODO: check if PartitionSize exceeds numBuff
		int numTuples = batchSize * (numBuff - 2);
		if (partition.size() > numTuples) {
			System.out.println("A partition size is larger than buffer size, might generate wrong results");
		}
	}

	public boolean close() {
		sortedLeft.close();
		sortedRight.close();
		return true;
	}

}