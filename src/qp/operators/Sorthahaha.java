package qp.operators;

import java.io.*;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.Vector;

import org.w3c.dom.Attr;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleInRun;

public class Sorthahaha extends Operator {
    private final Operator base;
    private final int numOfBuffers;
    private final ArrayList<Attribute> attributeArrayList;
    private final int batchSize = Batch.getPageSize() / schema.getTupleSize();
    private ObjectInputStream sortedStream;
    private boolean eos = false;

    public Sorthahaha(Operator base, ArrayList<Attribute> attr, int numOfBuffers) {
        super(OpType.SORT);
        this.schema = base.schema;
        this.base = base;
        this.numOfBuffers = numOfBuffers;
        this.attributeArrayList = attr;

    }

    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }
        int numOfRuns = sortedRuns();
        if (mergeRuns(numOfRuns, 1) ==1) {
            return true;
        } else {
            return false;
        }

    }

    private int sortedRuns() {
        int runs = 0;
        Batch inBatch = base.next();

        while (!inBatch.isEmpty()) {
            ArrayList<Tuple> tuplesToSort = new ArrayList<>();
            int i = 0;
            // read the base batch by batch until all the available buffers have been taken up.
            while (i < numOfBuffers) {
                tuplesToSort.addAll(inBatch.getTuples());
                // stop increment of buffer counter because there is no more buffer pages
                // available to read more inBatch. we now need to perform in-memory sort and
                // write the sorted run into outStream.
                if (i != numOfBuffers - 1) {
                    inBatch = base.next();
                }
                i++;
            }
            tuplesToSort.sort(this::compareTuples);
            // the output filename.
            String fileName = outFileName(0,runs);
            // ignore the error handling for now
            try {
                ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(fileName));
                for (Tuple tuple: tuplesToSort) {
                    stream.writeObject(tuple);
                }
                stream.close();
            } catch (IOException e) {
                System.err.println("Cannot write the sorted file into disk because " + e.toString());
            }

            // to open up the
            inBatch = base.next();
            runs++;

        }
        return runs;
    }

    private int mergeRuns(int numOfRuns, int passNum) {
        if (numOfRuns <= 1) {
            // ignore error handling here
            try {
                String fileName = outFileName(passNum - 1, numOfRuns - 1);
                sortedStream = new ObjectInputStream(new FileInputStream(fileName));
            } catch (FileNotFoundException e) {
                System.out.println("Sort: cannot find the input file ");
            } catch (IOException e) {
                System.out.println("Sort: cannot output the file due to " + e.toString());
            }
            return numOfRuns;
        }

        int outputRuns = 0;
        int start = 0;
        while (start < numOfRuns) {
            int end = Math.min(start + numOfBuffers - 1, numOfRuns);
            merge(start, end,passNum,outputRuns);
            outputRuns++;
            start += numOfBuffers - 1;
        }
        return mergeRuns(outputRuns, passNum + 1);
    }
    private void merge(int start, int end, int passNum, int output) {

    }

    private int compareTuples(Tuple t1, Tuple t2) {
        int idx = 0;
        // check on each sort index, if one of them is not 0, return the ordering immediately.
        while (idx < attributeArrayList.size()) {
            int sortIndex = schema.indexOf((Attribute) attributeArrayList.get(idx));
            int res = Tuple.compareTuples(t1, t2, sortIndex);
            if (res != 0) {
                return res;
            }
            idx++;
        }
        return 0;
    }


    private String outFileName(int passNum, int runNum) {
        return "temp_" + passNum + "_" + runNum;
    }

    public Batch next() {
        if (eos) {
            close();
            return null;
        }



    }

    public boolean close() {
        super.close();
        try{
            sortedStream.close();
        } catch (IOException e) {
            System.err.println("Cannot close sortedStream " + e.toString());
        }
    }



}