package qp.operators;

import java.lang.reflect.Array;
import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

public class Distinct extends Operator {
    private final ArrayList<Attribute> projected;
    private Operator base;
    private Sort sortedFile;
    private int batchSize;;
    private int numOfBuffer;
    private boolean eos = false;
    private Batch in = null;
    private int startIndex = 0;


    /**
     * Constructor for Distinct operator
     */
    public Distinct(Operator base, ArrayList<Attribute> projected) {
        super(OpType.DISTINCT);
        this.base = base;
        this.projected  = projected;


    }

    /**
     * open the base(relation to be sorted)
     * perform the sorting on the relation
     * and output the sorted file for duplicate elimination.
     */
    public boolean open() {
        batchSize  = Batch.getPageSize() / schema.getTupleSize();
        sortedFile = new Sort(base, projected, numOfBuffer);
        return sortedFile.open(); // the file is now sorted and ready to use
    }

    public Batch next() {
        if (eos) {
            close();
            return null;
        } else if(in == null) {
            in = sortedFile.next();
        }
        Debug.PPrint(in);
        // add in the first tuple into the out batch because it is used as
        // seed for duplication elimination.
        Batch out = new Batch(batchSize);
        Tuple firstTuple = in.get(startIndex);
        out.add(firstTuple);
        startIndex++;

        while (!out.isFull()) {
            if (startIndex >= in.size()) {
                eos = true;
                break;
            }

            Tuple tuple = in.get(startIndex);
            boolean flag = true;
            for (int i = 0; i < projected.size(); i++) {
                // may need casting herr, check if there is error pop out.
                int index = schema.indexOf(projected.get(i));
                Tuple lastTuple = in.get(startIndex - 1);
                if (Tuple.compareTuples(lastTuple, tuple, index) == 0) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                out.add(tuple);
            }
            startIndex++;

            if (startIndex == batchSize) {
                in = sortedFile.next();
                startIndex = 0;
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
        for (int i = 0; i < projected.size(); i++) {
            Attribute attribute = (Attribute) ((Attribute) projected.get(i)).clone();
            newProjectList.add(attribute);
        }
        Distinct newDistinct = new Distinct(newBase, newProjectList);
        Schema newSchema = newBase.getSchema();
        newDistinct.setSchema(newSchema);
        return newDistinct;
    }

}