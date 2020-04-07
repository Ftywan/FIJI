package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
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

 //TODO: Probably need more experiements
 // 1. sometimes there is null pointer exception, but after I rerun there is no more??
 // 2. should I add a function to check left batch to see if all same valuese are read in????
 // ASSUMPTIONS: 
 // 1. for left schema: .next(): the same value are always read into one batch
 // 2. for right schema: buffer size is enough to hold the largest partition of the right schema, 
 // 					if not enough, execution is terminated with an error message 

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
	// Tuple lastLeftTuple;
	Tuple nextLeftTuple;
	static int fileNumber = 0; 
	String rightFileName; 
	Tuple nextTuple = null;



	public SortMergeJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	public boolean open() {
		System.out.println("SortMerge: Opening SortMerge");
		System.out.print("SortMerge: Left schema is: ");
		Debug.PPrint(left.getSchema());
		System.out.print("SortMerge: Right schema is: ");
		Debug.PPrint(right.getSchema());
		System.out.println();
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
		System.out.println("SMJ: eosl is " + eosl + "eosr is " + eosr);
		// Read one left page and always read in partition into the memory
		if (eosl || eosr) {
			System.out.println("SMJ: Closing next()");
			close();
			return null;
		}
		if (leftBatch == null) {
			leftBatch = sortedLeft.next();
			System.out.print("SMJ: left batch is: ");
			System.out.println("continue here4?");
			Debug.PPrint(leftBatch);
			if (leftBatch == null || leftBatch.isEmpty()) {
				eosl = true;
				return null;
			}
			// read in next left tuple
			//System.out.println("first left Batch is");
			//Debug.PPrint(leftBatch);
			leftTuple = leftBatch.get(lcurs);
		}
		if (rightBatch == null) {
			System.out.println("continue here2?");
			rightBatch = sortedRight.next();
			if (rightBatch == null || rightBatch.isEmpty()) {
				// System.out.println("SMJ: When do you set as true3???");
				eosr = true;
				return outBatch;
				//return null;
			}
			System.out.println("SMJ: first rightBatch is ");
			Debug.PPrint(rightBatch);
			rightPartition = getNextPartition();
			if (checkPartitionSize(rightPartition)) {
				System.exit(0);
			};
			partitionEosr = 0;
			rightTuple = rightPartition.get(0);
		}
		Batch outBatch = new Batch(batchSize);
		while (!outBatch.isFull()) {
			int compareResult = Tuple.compareTuples(leftTuple, rightTuple, leftIndices, rightIndices);
			if (compareResult == 0) { // left and right tuples are equal
				System.out.println("SMJ: Equal found, they are:============");
				Debug.PPrint(leftTuple);
				Debug.PPrint(rightTuple);
				outBatch.add(leftTuple.joinWith(rightTuple)); // add join result into outBatch, continues to next comparison
				if (partitionEosr < rightPartition.size() - 1) {//read next tuple from right partition
					partitionEosr++;
					System.out.println("partionEsor is " + partitionEosr);
					rightTuple =  rightPartition.get(partitionEosr);
				}
				else if (partitionEosr == rightPartition.size()-1) {//finish reading right partition
					System.out.println("partionEsor is " + partitionEosr);
					// if next left tuple has the same key value as current left tuple, we don't advance right partition first
					// lastLeftTuple = leftTuple;
					lcurs++;
					if (lcurs == leftBatch.size()) {//left cursor reached the size of leftBatch
						System.out.println("SM: Next left batch is:");
						leftBatch = sortedLeft.next();
						if (leftBatch == null || leftBatch.isEmpty()) {
							eosl = true;
							break;
						}
						Debug.PPrint(leftBatch);
						lcurs = 0;
					}
					/*
					System.out.println("SMJ: lcurs here is: ");
					System.out.println(lcurs);
					System.out.println("left Batch is: ");
					Debug.PPrint(leftBatch);
					*/
					// leftTuple = leftBatch.get(lcurs);
					nextLeftTuple = leftBatch.get(lcurs);
					//lcurs++;
					if (nextLeftTuple == null) {//should not need this
						eosl = true;
						break;
					}
					System.out.println("SMJ: Checking two consecutive left tuples");
					Debug.PPrint(leftTuple);
					Debug.PPrint(nextLeftTuple);
					compareResult = Tuple.compareTuples(nextLeftTuple, leftTuple, leftIndices, leftIndices);
					leftTuple = nextLeftTuple;
					if (compareResult == 0) {// two consecutive tuples has equal values
						partitionEosr = 0;
						rightTuple = rightPartition.get(partitionEosr);
					} else {// we can get next partition for right
						rightPartition = getNextPartition();
						if (rightPartition == null || rightPartition.size() == 0) {
							// System.out.println("SMJ: When do you set as true1???");
							eosr = true;
							break;
						}
						partitionEosr = 0;
						rightTuple = rightPartition.get(partitionEosr);
					}
				}
			}
			else if (compareResult > 0) { // left tuple is larger, advance right, remember for right we always advance by partition
				System.out.println("SMJ: Advance right");
				rightPartition = getNextPartition(); // get next right partition
				if (rightPartition == null || rightPartition.isEmpty()) {
					// System.out.println("SMJ: When do you set as true2???");
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
					System.out.println("SM: Next left batch is:");
					leftBatch = sortedLeft.next(); //read in new Batch
					if (leftBatch == null || leftBatch.isEmpty()) {
						eosl = true;
						break;
					}
					Debug.PPrint(leftBatch);
					lcurs = 0; // set cursor back to 0
				}
				leftTuple = leftBatch.get(lcurs);
				Debug.PPrint(leftTuple);
			}
		}
		System.out.println("SMJ: an outbatch is: ===========================================================");
		Debug.PPrint(outBatch);
		return outBatch;
	}

	// private ArrayList<Tuple> getNextPartition() {
	// 	System.out.println("SMJ: Generatin next partition");
	// 	/** 
	// 		we keep reading in right tuples until there is a value change
	// 		Remember we always get right by partition
	// 	*/
	// 	ArrayList<Tuple> partition = new ArrayList<Tuple>();
	// 	int compResult = 0;

	// 	// if (rightBatch == null || rightBatch.isEmpty()) {
	// 	// 	return partition;
	// 	// }

	// 	// else {//rightBatch has something: 1. get next tuple according to rcurs; 2. tuple has reached the end of partition: get new partition
	// 	if (nextTuple == null) {//if there is no nextTuple, it is the first time we create nextTuple
	// 		if (rcurs == rightBatch.size()) { // get a batch
	// 			rightBatch = sortedRight.next();
	// 			if (rightBatch == null) { // if nothing more in right batch, reached the end of right table
	// 				// System.out.println("SMJ: When do you tmd come here????");
	// 				eosr = true;
	// 				if (checkPartitionSize(partition)) {
	// 					System.exit(0);
	// 				}
	// 				return partition;
	// 			}
	// 			rcurs = 0;
	// 		}
	// 		nextTuple = rightBatch.get(rcurs); //get nextTuple
	// 		if (nextTuple == null) {
	// 			return partition;
	// 		}
	// 		// rcurs++;
	// 	}

	// 	while (compResult == 0) {
	// 		rcurs++;
	// 		//System.out.println("SMJ: adding this tuple into partition");
	// 		//Debug.PPrint(next);
	// 		partition.add(nextTuple); // add in to partion, check if next partition has the same value
	// 		Debug.PPrint(nextTuple);

	// 		// Get next right tuple: 1. get next right tuple using left cursor; 2. right tuple is the end of the batch size; get next batch
	// 		if (rcurs == rightBatch.size()) {//current rightBatch has reached the end
	// 			rightBatch = sortedRight.next(); //get next rightBatch
	// 			if (rightBatch == null || rightBatch.isEmpty()) { //if next rightBatch is null
	// 				// System.out.println("SMJ: When do you come here ahhhhhh");
	// 				eosr = true;
	// 				return partition; //next Batch 
	// 			}
	// 			rcurs = 0;
	// 		}
	// 		nextTuple = rightBatch.get(rcurs);
	// 		// rcurs++;
	// 		//System.out.println("SMJ: checking two tuples");
	// 		//Debug.PPrint(partition.get(0));
	// 		//Debug.PPrint(next);
	// 		compResult = Tuple.compareTuples(partition.get(0), nextTuple, rightIndices, rightIndices);
	// 	}
	// 	// System.out.println("SMJ: Finish generating right partition");
	// 	// System.out.println();
	// 	if (checkPartitionSize(partition)) {
	// 		System.exit(0);
	// 	};
	// 	return partition;
	// }

	// private ArrayList<Tuple> getNextPartition() {
    //     ArrayList<Tuple> partition = new ArrayList<>();
    //     int comparisionResult = 0;
    //     if (nextTuple == null) {
	// 		if (rightBatch == null) {
	// 			return null;
	// 		} else if (rcurs == rightBatch.size()) {
	// 			rightBatch = right.next();
	// 			rcurs = 0;
	// 		}
	
	// 		// Checks whether the right batch still has tuples left.
	// 		if (rightBatch == null || rightBatch.size() <= rcurs) {
	// 			return null;
	// 		}
	
	// 		// Reads the next tuple.
	// 		nextTuple = rightBatch.get(rcurs);
	// 		rcurs++;
    //         if (nextTuple == null) {
    //             return partition;
    //         }
    //     }

    //     // Continues until the next tuple carries a different value.
    //     while (comparisionResult == 0) {
    //         partition.add(nextTuple);

	// 		if (rightBatch == null) {
	// 			return null;
	// 		} else if (rcurs == rightBatch.size()) {
	// 			rightBatch = right.next();
	// 			rcurs = 0;
	// 		}
	
	// 		// Checks whether the right batch still has tuples left.
	// 		if (rightBatch == null || rightBatch.size() <= rcurs) {
	// 			return null;
	// 		}
	
	// 		// Reads the next tuple.
	// 		nextTuple = rightBatch.get(rcurs);
	// 		rcurs++;
    //         if (nextTuple == null) {
    //             break;
    //         }
    //         comparisionResult = Tuple.compareTuples(partition.get(0), nextTuple, rightIndices, rightIndices);
    //     }

    //     return partition;
	// }
	

	private ArrayList<Tuple> getNextPartition() {
        ArrayList<Tuple> partition = new ArrayList<>();
        int comparisionResult = 0;
        if (nextTuple == null) {
            nextTuple = readNextRightTuple();
            if (nextTuple == null) {
                return partition;
            }
        }

        // Continues until the next tuple carries a different value.
        while (comparisionResult == 0) {
            partition.add(nextTuple);

            nextTuple = readNextRightTuple();
            if (nextTuple == null) {
                break;
            }
            comparisionResult = Tuple.compareTuples(partition.get(0), nextTuple, rightIndices, rightIndices);
        }

        return partition;
	}
	
	private Tuple readNextRightTuple() {
        // Reads another batch if necessary.
        if (rightBatch == null) {
            return null;
        } else if (rcurs == rightBatch.size()) {
            rightBatch = right.next();
            rcurs = 0;
        }

        // Checks whether the right batch still has tuples left.
        if (rightBatch == null || rightBatch.size() <= rcurs) {
            return null;
        }

        // Reads the next tuple.
        Tuple next = rightBatch.get(rcurs);
        rcurs++;
        return next;
    }

	// Do nothing, just report an error message
	private boolean checkPartitionSize(ArrayList<Tuple> partition) {
		//TODO: check if PartitionSize exceeds numBuff
		int numTuples = batchSize * (numBuff - 2);
		if (partition.size() > numTuples) {
			System.out.println("A partition size is larger than buffer size, try larger buffer size");
			return true;
		}
		return false;
	}

	// private Tuple readNextRight() {
    //     // Reads another batch if necessary.
    //     if (rightBatch == null) {
    //         return null;
    //     } else if (rcurs == rightBatch.size()) {
    //         rightBatch = right.next();
    //         rcurs = 0;
    //     }

    //     // Checks whether the right batch still has tuples left.
    //     if (rightBatch == null || rightBatch.size() <= rcurs) {
    //         return null;
    //     }

    //     // Reads the next tuple.
    //     Tuple next = rightBatch.get(rcurs);
    //     rcurs++;
    //     return next;
    // }

	public boolean close() {
		sortedLeft.close();
		sortedRight.close();
		return true;
	}

}