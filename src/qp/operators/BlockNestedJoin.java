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
	Batch outputPage;                 // Buffer page for output
	Batch leftInputPage;              // Buffer page for left input stream
	Batch rightInputPage;             // Buffer page for right input stream
	ObjectInputStream in;               // File pointer to the right hand materialized file
	ArrayList<Tuple> block;

	int leftCursor;                     // Cursor for left side buffer
	int rightCursor;                    // Cursor for right side buffer
	boolean endOfLeftStream;            // Whether end of stream (left table) is reached
	boolean endOfRightStream;           // Whether end of stream (right table) is reached

	public BlockNestedJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	public boolean open() {
		System.out.println("BNJ opening");
		tupleSize = schema.getTupleSize();
		batchSize = Batch.getPageSize() / tupleSize;

		leftIndex = new ArrayList<>();
		rightIndex = new ArrayList<>();

		for (Condition condition : conditionList) {
			Attribute leftAttribute = condition.getLhs();
			Attribute rightAttribute = (Attribute) condition.getRhs();
			leftIndex.add(left.getSchema().indexOf(leftAttribute));
			rightIndex.add(right.getSchema().indexOf(rightAttribute));
		}

		leftCursor = 0;
		rightCursor = 0;
		endOfLeftStream = false;
		endOfRightStream = true;

		block = new ArrayList<>();

		Batch materializePage;
		if (! right.open()) {
			return false;
		} else {
			fileNumber ++;
			rightFileName = "BNJtemp-" + fileNumber;
			try {
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rightFileName));
				while ((materializePage = right.next()) != null) {
					out.writeObject(materializePage);
				}
				out.close();
			} catch (IOException io){
				System.out.println("BlockedNestedJoin: Error writing to temporary file");
				return false;
			}
			if (! right.close()) {
				return false;
			}
		}
		return left.open();
	}

	public Batch next() {
		System.out.println("next entered");
		if (endOfLeftStream) {
			return null;
		}
		outputPage = new Batch(batchSize);
		while (! outputPage.isFull()) {
			/** a new block of data would be loaded **/
			if (leftCursor == 0 && endOfRightStream == true) {
				block.clear();
				/** NumBuffer - 2 pages in the buffer is available for caching the left table **/
				for (int i = 0; i < numBuff - 2; i++) {
					/** load each page into the buffer **/
					leftInputPage = (Batch) left.next();
					/** there is no data in the left table **/
					if (leftInputPage == null) {
						endOfLeftStream = true;
						break;
					}
					/** load all tuples in the page to the block **/
					for (int j = 0; j < leftInputPage.size(); j++) { //the number of elements in the input page can be smaller than the batch size
						Tuple leftTuple = leftInputPage.get(j);
						block.add(leftTuple);
					}
				} // block loading finished

				// initiate the reading for the right table
				try {
					in = new ObjectInputStream(new FileInputStream(rightFileName));
					endOfRightStream = false;
				} catch (IOException io) {
					System.err.println("BlockNestedJoin:error in reading the file");
					System.exit(1);
				}
			}

			// still under the progress of comparing the current left with the whole right table
			while (endOfRightStream == false) {
				try {
					if (rightCursor == 0 && leftCursor == 0) {
						rightInputPage = (Batch) in.readObject();
					}
					/** iterate through to find join-able pairs **/
					for (int i = 0; i < block.size(); i ++) {
						for (int j = 0; j < rightInputPage.size(); j ++) {
							Tuple leftTuple = block.get(i);
							Tuple rightTuple = rightInputPage.get(j);
							if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex)) {
								Tuple outTuple = leftTuple.joinWith(rightTuple);
								Debug.PPrint(outTuple);
								outputPage.add(outTuple);
								/** conditions of left and right cursors when the output buffer page is full **/
								if (outputPage.isFull()) {
									if (i == block.size() - 1 && j == rightInputPage.size() - 1) {
										leftCursor = 0;
										rightCursor = 0;
									} else if (i != block.size() - 1 && j == rightInputPage.size() - 1) {
										leftCursor = i + 1;
										rightCursor = 0;
									} else if (i == block.size() - 1 && j != rightInputPage.size() - 1) {
										leftCursor = i;
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
				} catch (EOFException e) {
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
		File f = new File(rightFileName);
		f.delete();
		return true;
	}
}
