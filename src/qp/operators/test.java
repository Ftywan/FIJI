package qp.operators;

import java.io.*;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleInRun;

public class Sort extends Operator {
    private final Operator base;
    private final int numOfBuffers;
    private final ArrayList<Integer> sortKeyIndex = new ArrayList<>();
    private final int batchSize;
    private ObjectInputStream sortedStream;
    private boolean eos = false;

    public Sort(Operator base, ArrayList<Attribute> attr, int numOfBuffers) {
        super(OpType.SORT);
        this.schema = base.schema;
        this.base = base;
        this.numOfBuffers = numOfBuffers;
        this.batchSize = Batch.getPageSize() / schema.getTupleSize();
        int idx = 0;
        while (idx < attr.size()) {
            Attribute attribute = (Attribute) attr.get(idx);
            sortKeyIndex.add(schema.indexOf(attribute));
        }
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
        Batch inBatch = base.next();
        int numOfRuns = 0;
        while (inBatch != null) {
            ArrayList<Tuple> tuples = new ArrayList<>();
            int i = 0;
            while (i < numOfBuffers && inBatch != null) {
                tuples.addAll(inBatch.getTuples());
                if (i != numOfBuffers - 1) {
                    inBatch = base.next();
                }
            }
            tuples.sort(this::compareTuples);
            String fileName = runFileName(0,numOfRuns);
            // ignore the error handling for now
            ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(fileName));
            for (Tuple tuple: tuples) {
                stream.writeObject(tuple);
            }
            stream.close();

            inBatch = base.next();
            numOfRuns++;

        }
        return numOfRuns;
    }

    private int mergeRuns(int numOfRuns, int passNum) {
        if (numOfRuns <= 1) {
            // ignore error handling here
            try {
                String fileName = runFileName(passNum - 1, numOfRuns - 1);
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

    private String runFileName(int passNum, int runNum) {
        return "temp_" + passNum + "_" + runNum;
    }

    public Batch next() {

    }

    public boolean close() {

    }



}