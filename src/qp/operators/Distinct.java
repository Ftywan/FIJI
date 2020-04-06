package qp.operators;

import java.util.ArrayList;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;


// TODO: error when small number of buffers

public class Distinct extends Operator {
    private final ArrayList<Attribute> projectedlist;
    ArrayList<Integer> projectedIndices = new ArrayList<Integer>();
    private Operator base;
    private SortOriginal sortedFile;
    private int batchSize;;
    private int numOfBuffer;
    private boolean eos = false;
    private Batch in = null;
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
        //System.out.print("Distinct: ");
        //System.out.println(projectedlist);
        sortedFile = new SortOriginal(base, projectedlist, numOfBuffer);

        Schema baseSchema = base.getSchema();
        for (int i = 0; i < projectedlist.size(); ++i) {
            Attribute attr = projectedlist.get(i);

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            projectedIndices.add(index);
        }
        //System.out.print("Distinct: What are projectedIndices");
        //System.out.println(projectedIndices);
        return sortedFile.open(); // the file is now sorted and ready to use
    }

    public Batch next() {
        if (eos) {
            close();
            return null;
        } else if(in == null) {//where there is not in batch
            in = sortedFile.next();
        }
        // add in the first tuple into the out batch because it is used as
        // seed for duplication elimination.
        Batch out = new Batch(batchSize);
        Tuple currentTuple = in.get(batchIndex);
        //System.out.println("Distinct: adding to outBatch");
        //Debug.PPrint(in);
        out.add(currentTuple);
        batchIndex++;

        while (!out.isFull()) {
            if (batchIndex >= in.size()) {//batchIndex exceeds inbatch size. we reach the end of the schema
                eos = true;
                break;
            }

            lastTuple = currentTuple;
            currentTuple = in.get(batchIndex);
            int compareResult = Tuple.compareTuples(currentTuple, lastTuple, projectedIndices, projectedIndices);
            if (compareResult != 0) {
                // System.out.println("Distinct: adding to outBatch");
                // Debug.PPrint(lastTuple);
                // Debug.PPrint(currentTuple);
                out.add(currentTuple);
                // System.out.println("Distinct: current outbatch is");
                // Debug.PPrint(out);
                // System.out.println();
            }
            batchIndex++;

            if (batchIndex == batchSize) {//when one batch is read completely
                in = sortedFile.next();
                //Debug.PPrint(in);
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
            Attribute attribute = (Attribute) ((Attribute) projectedlist.get(i)).clone();
            newProjectList.add(attribute);
        }
        Distinct newDistinct = new Distinct(newBase, newProjectList);
        Schema newSchema = newBase.getSchema();
        newDistinct.setSchema(newSchema);
        return newDistinct;
    }

}