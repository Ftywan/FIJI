package qp.operators;

import java.util.ArrayList;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;

/** TO DO:
 *  Update cost of OrderBy Operator!!!
 */
/** Order by extendds Operator
 *  Make use of Sort
 *  In open(): Sort is called to create sortedFile
 *  In next(): Call Sort.next() to get next batch of sorted files
 *  In close(): close the sorted file
 */

 //TODO: Add in those exception catch! 看起来好像没有什么error要catch????

public class OrderBy extends Operator {
    
    Operator base;
    ArrayList<Attribute> orderedlist; // Set of attributes that are to be ordered

    // Corresponding index of the attributes in above list
    // Use it to get the attributes from schema
    ArrayList<Integer> asIndices = new ArrayList<>();

    // How many tuples each page can have
    int batchSize;

    // How many buffers are available for performing this operation
    // Get the numBuff for External Sort
    int numBuff;
    Sort sortedFile;
    boolean eos = false;
    Batch inBatch = null;
    int inIndex = 0;

    public OrderBy(Operator base, ArrayList<Attribute> orderedlist) {
        super(OpType.ORDERBY);
        this.base = base;
        this.orderedlist = orderedlist;
        numBuff = BufferManager.getNumBuffer();
    }
    
    public boolean open() {
        batchSize = Batch.getPageSize() / schema.getTupleSize();
        for (int i = 0; i < orderedlist.size(); i++) {
            Attribute attribute = (Attribute) orderedlist.get(i);
            asIndices.add(schema.indexOf(attribute));
        }
        sortedFile = new Sort(base, orderedlist, numBuff);
        sortedFile.open();
        return true;
    }

    public Batch next() {
        if (eos) {
            close();
            return null;
        } else if (inBatch == null) {
            // Get a batch from sortedFile
            inBatch = sortedFile.next();
        }

        //Adding inBatch to outBatch until

        Batch outBatch = new Batch(batchSize);
        while (!outBatch.isFull()) {
            if (inBatch == null || inBatch.size() <= inIndex) {
                eos = true;
                break;
            }

            outBatch.add(inBatch.get(inIndex));
            inIndex++;

            if (inIndex == batchSize) {
                inBatch = sortedFile.next();
                inIndex = 0;
            }
        }
        return outBatch;
    }

    /**
     * Close the opened sort class in Sort
     */
    public boolean close() {
        return sortedFile.close();
    }

    public void setNumOfBuffer(int numBuff) {
        this.numBuff = numBuff;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }


    public Object clone() {
        Operator newBase = (Operator) base.clone();
        ArrayList<Attribute> newOrderByList = new ArrayList<>();
        for (int i = 0; i < orderedlist.size(); i++) {
            Attribute attribute = (Attribute) ((Attribute) orderedlist.get(i)).clone();
            newOrderByList.add(attribute);
        }

        OrderBy newOrderBy = new OrderBy(newBase, newOrderByList);
        Schema newSchema = newBase.getSchema();
        newOrderBy.setSchema(newSchema);
        return newOrderBy;
    }
}