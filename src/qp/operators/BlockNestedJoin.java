package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join{

	static int fileNumber = 0;          // To get unique file number for this operation
	int batchSize;                      // Number of tuples per out batch
	int tupleSize;                      // Number of byte per tuple
	ArrayList<Integer> leftIndex;       // Indices of the join attributes in left table
	ArrayList<Integer> rightIndex;      // Indices of the join attributes in right table
	String rightFileName;               // The file name where the right table is materialized
	Batch outputPage;                   // Buffer page for output
	Batch leftInputPage;                // Buffer page for left input stream
	Batch rightInputPage;               // Buffer page for right input stream
	ObjectInputStream in;               // File pointer to the right hand materialized file
	ArrayList<Tuple> block;             // Structure to simulate the block that containing several pages

	static int leftCursor;              // Cursor for left side block
	static int rightCursor;             // Cursor for right side buffer
	boolean endOfLeftStream;            // Whether end of stream (left table) is reached
	boolean endOfRightStream;           // Whether end of stream (right table) is reached

	// constructor similar to the Nested Join
	public BlockNestedJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	// initialization of the Block Nested Join
	public boolean open() {
		System.out.println("BNJ opening");
		tupleSize = schema.getTupleSize();
		batchSize = Batch.getPageSize() / tupleSize;

		leftIndex = new ArrayList<>();
		rightIndex = new ArrayList<>();

		// loading of the join attributes into two lists of attributes
		for (Condition condition : conditionList) {
			Attribute leftAttribute = condition.getLhs();
			Attribute rightAttribute = (Attribute) condition.getRhs();
			leftIndex.add(left.getSchema().indexOf(leftAttribute));
			rightIndex.add(right.getSchema().indexOf(rightAttribute));
		}

		/** currently both the cursors are at the starting positions
		 * the left stream is not to the end obviously
		 * the next step of the right stream is 0 so the current status is end
		 */
		leftCursor = 0;
		rightCursor = 0;
		endOfLeftStream = false;
		endOfRightStream = true;

		block = new ArrayList<>();

		// if the right table is not in the hard drive, materialize it
		Batch materializePage;
		if (! right.open()) {
			return false;
		} else {
			fileNumber ++;
			rightFileName = "BNJtemp-" + String.valueOf(fileNumber);
			try {
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rightFileName));
				while ((materializePage = right.next()) != null) {
					out.writeObject(materializePage);
				}
				out.close();
			} catch (IOException io){
				System.out.println("BlockNestedJoin: Error writing to temporary file");
				return false;
			}
			if (! right.close()) {
				return false;
			}
		}
		return left.open();
	}

	// return the next tuple of joined results
	public Batch next() {
		int i, j;
		System.out.println("next entered");
		if (endOfLeftStream) {
			return null;
		}
		outputPage = new Batch(batchSize); // the page for output
		while (! outputPage.isFull()) {
			/** a new block of data would be loaded
			 * only both are at the 0 position a new right page is needed to be read
			 * before that we will check the join-able pairs in the so-far loaded tuples
			 */
			if (leftCursor == 0 && endOfRightStream == true) {
				block.clear();
				/** NumBuffer - 2 pages in the buffer is available for caching the left table **/
				for (int a = 0; a < numBuff - 2; a++) {
					/** load each page into the buffer **/
					leftInputPage = (Batch) left.next();
					/** there is no data in the left table **/
					if (leftInputPage != null) {
						block.addAll(leftInputPage.getTuples());
					} else {
						break;
					}
				} // block loading finished

				/**
				 * end of left stream should not be triggered until there are no more tuples needed to be processed
				 * so if unable to read any tuples in the block, change the status
				 */
				if (block.isEmpty()) {
					endOfLeftStream = true;
					return outputPage;
				}

				// initiate reading the right table for the current block
				try {
					System.out.println("BNJ: entering this loop");
					in = new ObjectInputStream(new FileInputStream(rightFileName));
					endOfRightStream = false;
				} catch (IOException io) {
					System.err.println("BlockNestedJoin:error in reading the file");
					System.exit(1);
				}
			}

			// still under the progress of comparing the current left with the whole right table
			while (endOfRightStream == false) {
				System.out.println("endOdRightStream set to false");
				System.out.print("rightCursor is: ");
				System.out.println(rightCursor);
				try {
					if (rightCursor == 0 && leftCursor == 0) {
						rightInputPage = (Batch) in.readObject();
					}
					/**
					 * iterate through to find join-able pairs
					 * the left range of iteration is expanded to block size
					 */
					for (i = leftCursor; i < block.size(); i ++) {
						Tuple leftTuple = block.get(i);
						for (j = rightCursor; j < rightInputPage.size(); j ++) {
							Tuple rightTuple = rightInputPage.get(j);
							if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex)) {
								Tuple outTuple = leftTuple.joinWith(rightTuple);
								outputPage.add(outTuple);
								/** conditions of left and right cursors when the output buffer page is full **/
								if (outputPage.isFull()) {
									if (i == block.size() - 1 && j == rightInputPage.size() - 1) {
										leftCursor = 0;
										rightCursor = 0;
									} else if (i != block.size() - 1 && j == rightInputPage.size() - 1) {
										leftCursor = i + 1;
										//rightCursor = 0;
										rightCursor = j;
									} else if (i == block.size() - 1 && j != rightInputPage.size() - 1) {
										//leftCursor = i;
										leftCursor = 0;
										rightCursor = j + 1;
									} else {
										leftCursor = i;
										rightCursor = j + 1;
									}
									return outputPage;
								}
							}
						}
						rightCursor = 0;
					}
					leftCursor = 0;
				} catch (EOFException e) { // the right table is all processed
					try {
						in.close();
					} catch (IOException io ) {
						System.out.println("BlockNestedJoin: Error in reading temporary file");
					}
					endOfRightStream = true;
				} catch (IOException e) {
					System.out.println("BlockNestedJoin: Error in reading temporary file");
					System.exit(1);
				} catch (ClassNotFoundException e) {
					System.out.println("BlockNestedJoin: Error in de-serialising temporary file ");
					System.exit(1);
				}
			}
		}
		return outputPage;
	}

	public boolean close() {
		System.out.println("Closing BNL");
		File f = new File(rightFileName);
		f.delete(); // delete the intermediate table
		return true;
	}
}
