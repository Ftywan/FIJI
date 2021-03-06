package qp.operators;

import java.util.ArrayList;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;


/**
 * This class is used to define a distinct operator.
 */
public class Distinct extends Operator {
    private final ArrayList<Attribute> projectedlist;
    ArrayList<Integer> projectedIndices = new ArrayList<Integer>();
    private Operator base;
    private Sort sortedFile;
    private int batchSize;
	private int numOfBuffer;
    private boolean eos = false;
    private Batch in = null;
    // batch index is the index for tuple on each batch.
    private int batchIndex = 0;
    private Tuple lastTuple = null; // To keep track of lastTuple


    /**
     * Constructor for Distinct operator
     */
    public Distinct(Operator base, ArrayList<Attribute> projected) {
        super(OpType.DISTINCT);
        this.base = base;
        this.projectedlist  = projected;
        numOfBuffer = BufferManager.getNumBuffer();
    }

    /**
     * open the base(relation to be sorted)
     * perform the sorting on the relation
     * and output the sorted file for duplicate elimination.
     */
    public boolean open() {
        batchSize  = Batch.getPageSize() / schema.getTupleSize();
        sortedFile = new Sort(base, projectedlist, numOfBuffer);

        Schema baseSchema = base.getSchema();
        for (int i = 0; i < projectedlist.size(); ++i) {
            Attribute attr = projectedlist.get(i);

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            projectedIndices.add(index);
        }
        return sortedFile.open(); // the file is now sorted and ready to use
    }

    /**
     * Sorting based approach is used to implement Distinct.
     * Distinct is done by pairwise comparision on a sorted relation.
     * @return
     */

    public Batch next() {
        if (eos) {
            close();
            return null;
        } else if(in == null) {//when there is nothing in batch, you open the next batch
            in = sortedFile.next();
        }
        // add in the first tuple into the out batch because it is used as
        // seed for duplication elimination.
        Batch out = new Batch(batchSize);
        Tuple currentTuple = in.get(batchIndex);
        out.add(currentTuple);
        batchIndex++;
        // Main duplicate elimination process. we keep track of the last tuple and always look for the next tuple if there is any.
        // Since the relation is sorted, we can safely output the current tuple as a distinct one if it is different from the previous tuple.
        while (!out.isFull()) {
            if (batchIndex >= in.size()) {//batchIndex exceeds inbatch size. we reach the end of the schema.
                eos = true;
                break;
            }

            lastTuple = currentTuple;
            currentTuple = in.get(batchIndex);
            int compareResult = Tuple.compareTuples(currentTuple, lastTuple, projectedIndices, projectedIndices);
            if (compareResult != 0) {
                out.add(currentTuple);
            }
            batchIndex++;

            if (batchIndex == batchSize) {//when one batch is read completely
                in = sortedFile.next();
                if (in == null) {
                    eos = true;
                    break;
                }
                batchIndex = 0;
            }
        }
        return out;
    }

    @Override
    public boolean close() {
        return sortedFile.close();
    }

    public Operator getBase() {
        return base;
    }
    public void setBase(Operator base) {
        this.base = base;
    }
    public void setNumOfBuffer(int numOfBuffer) {
        this.numOfBuffer = numOfBuffer;
    }

    public Object clone() {
        Operator newBase = (Operator) base.clone();
        ArrayList<Attribute> newProjectList = new ArrayList<>();
        for (int i = 0; i < projectedlist.size(); i++) {
            Attribute attribute = (Attribute) projectedlist.get(i).clone();
            newProjectList.add(attribute);
        }
        Distinct newDistinct = new Distinct(newBase, newProjectList);
        Schema newSchema = newBase.getSchema();
        newDistinct.setSchema(newSchema);
        return newDistinct;
    }

}