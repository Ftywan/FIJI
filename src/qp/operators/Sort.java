package qp.operators;

import java.io.*;
import java.util.UUID;
import java.util.ArrayList;
import java.util.PriorityQueue;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleInRun;

/**
 * This sort class adopts external sorting algorithm and provide sorting basis
 * for Distinct/OrderBy/SortMerge operator.
 */
public class Sort extends Operator {
    private final Operator base;
    private final int numOfBuffers;
    private final ArrayList<Attribute> attributeArrayList;
    private final int batchSize;
    private ObjectInputStream sortedStream;
    private boolean eos = false;
    // UUID is used to randomly generate string for naming of the sorted/merged files.
    private final String uuid = UUID.randomUUID().toString();
    private ArrayList<String> nameList = new ArrayList<>();

    public Sort(Operator base, ArrayList<Attribute> attr, int numOfBuffers) {
        super(OpType.SORT);
        this.schema = base.schema;
        this.base = base;
        this.numOfBuffers = numOfBuffers;
        this.attributeArrayList = attr;
        this.batchSize = Batch.getPageSize() / schema.getTupleSize();

    }

    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }

        int i = sortedRuns(); //generate sorted runs
        return merge(i, 1) == 1;
    }

    /**
     * This method load data from disk into buffer and utilise all the buffers to perform
     * an in-memory sort.
     * @return    The number of sorted run generated after the 0th pass.
     */
    private int sortedRuns() {
        Batch inBatch = base.next();
        //runs denotes the number of files generated from the 0th pass
        int runs = 0;
        while (inBatch != null) {
            ArrayList<Tuple> tuplesToSort = new ArrayList<>();
            int i = 0;
            // read the base relation batch by batch until all the available buffers have been taken up.
            while (i < numOfBuffers && inBatch != null) {
                tuplesToSort.addAll(inBatch.getTuples());
                // stop increment of buffer counter because there is no more buffer pages
                // available. we now need to perform in-memory sort and
                // write the sorted run into outStream.
                if (i != numOfBuffers - 1) {
                    inBatch = base.next();
                }
                i++;
            }
            tuplesToSort.sort(this::compareTuples);
            String fileName = outFileName(0,runs);
            nameList.add(fileName);

            try {
                ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(fileName));
                for (Tuple tuple: tuplesToSort) {
                    stream.writeObject(tuple);
                }
                stream.close();
            } catch (IOException e) {
                System.err.println("Cannot write the sorted file into disk because " + e.toString());
                System.exit(1);
            }

            inBatch = base.next();
            runs++;

        }
        return runs;
    }

    /**
     * This method merge invokes recursive call on merging process(mergeIntermediate) until one file sorted file is generated.
     * @param fileNum   number of sorted runs to be merged
     * @param pass      number of passes so far
     * @return          number of merged files generated.
     */
    private int merge(int fileNum, int pass) {

        int fileNumNow = fileNum;
        // if the number of sorted file is <= 1, no merge required. Output the file.
        if (fileNumNow <= 1) {
            String fileName = outFileName(pass - 1, 0);
            try {
                sortedStream = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException e) {
                System.err.printf("cannot write out the sorted file=%s because %s\n", fileName, e.toString());
            }
            return fileNumNow;
        }
        fileNumNow = mergeIntermediate(fileNumNow, pass);
        pass++;
        return merge(fileNumNow, pass);

    }

    /**
     * This method is equivalent of one pass of merging process.
     * It merges existing sorted runs into shorter sorted runs. After one pass of merging,
     * there are fewer but longer sorted runs.
     * @param preFileNum    number of sorted runs to be merged.
     * @param pass          number of pass so far
     * @return              number of merged files generated.
     */
    private int mergeIntermediate(int preFileNum, int pass) {
        int fileNum = 0;

        for (int start = 0; start < preFileNum; start += numOfBuffers - 1) {
            int end = Math.min(start + numOfBuffers-1, preFileNum);

            // At first we load data into memory as much as possible
            // until all the buffers are fully loaded
            Batch[] ins = new Batch[end - start];
            boolean[] endFlag = new boolean[end - start];
            ObjectInputStream[] inStreams = new ObjectInputStream[end - start];
            for (int i = start; i < end; i++) {
                String fileName = outFileName(pass - 1, i);
                ObjectInputStream inStream = null;
                try {
                    inStream = new ObjectInputStream(new FileInputStream(fileName));
                } catch (IOException e) {
                    System.out.println("File cannot be input to stream");
                    System.exit(1);
                }

                inStreams[i - start] = inStream;
                Batch in = new Batch(batchSize);
                while (!in.isFull()) {
                    try {
                        Tuple data = (Tuple) inStream.readObject();
                        in.add(data);
                    } catch (EOFException eof) {
                        break;
                    } catch (IOException e) {
                        System.out.println("Cannot load data into memory due to " + e.toString());
                        System.exit(1);
                    } catch (ClassNotFoundException e) {
                        System.out.println("Cannot load data into memory due to " + e.toString());
                        System.exit(1);
                    }
                }
                ins[i - start] = in;
            }

            //put everything from buffer into pq.
            PriorityQueue<TupleInRun> pq = new PriorityQueue<>(batchSize, (t1, t2) -> compareTuples(t1.tuple, t2.tuple));
            // name the output merged file with current pass number and file number;
            // create the corresponding outStream for continuous data flow;
            String outFileName = outFileName(pass, fileNum);
            nameList.add(outFileName);
            ObjectOutputStream outStream = null;
            try {
                outStream = new ObjectOutputStream(new FileOutputStream(outFileName));
            } catch (IOException e) {
                System.err.printf("cannot output from pq to outStream (%s) due to %s\n", outFileName, e.toString());
            }
            // put the first tuple in each buffer(in) into the pq so that we can find the
            // min of the remaining tuples each time.
            for (int idx = 0; idx < end - start; idx++) {
                Batch in = ins[idx];
                if (in == null || in.isEmpty()) {
                    endFlag[idx] = true;
                    continue;
                }
                Tuple tpl = in.get(0);
                pq.add(new TupleInRun(tpl, idx, 0));
            }
            //now there are (n-1) tuples in the qp and you can start the loop
            // to continuously output the smallest tuple.
            while (!pq.isEmpty()) {
                TupleInRun tpl = pq.poll();
                try {
                    outStream.writeObject(tpl.tuple);
                } catch (IOException e) {
                    System.err.printf("the min tuple cannot be put into outStream due to %s\n", e.toString());
                }
                // now we look for the replacement of the tuple
                // that have been popped in the above line. This is done by finding
                // where the tuples comes from. if the buffer it comes from has remaining tuple left,
                // put it into pq,
                int nextInID = tpl.runID;
                int nextIdx = tpl.tupleID + 1;
                // if the tuple it comes from is already empty, then go to its corresponding inStream and
                // load another batch of data until the buffer page is full again.
                if (nextIdx == batchSize) {
                    Batch in = new Batch(batchSize);
                    while (!in.isFull()) {
                        try {
                            Tuple data = (Tuple) inStreams[nextInID].readObject();
                            in.add(data);
                        } catch (EOFException eof) {
                            break;
                        } catch (ClassNotFoundException e) {
                            System.err.printf("cannot load the data into buffer due to %s\n", e.toString());
                        } catch (IOException e) {
                            System.err.printf("cannot load the data into buffer due to %s\n", e.toString());
                        }
                    }
                    //when the buffer is fully loaded, assign it to our buffer pool
                    ins[nextInID] = in;
                    // Resets the index for that input buffer to be 0.
                    nextIdx = 0;
                }
                // now we have alr locate the next tuple to put into pq
                Batch in = ins[nextInID];
                // This is handle the case where the last inStream is sometimes shorter
                // then all the previous stream, Hence, when the last few data is load into buffer,
                // they do not occupy the buffer fully. so size < index;
                if (in == null || in.size() <= nextIdx) {
                    endFlag[nextInID] = true;
                    continue;
                }
                Tuple nextTuple = in.get(nextIdx);
                pq.add(new TupleInRun(nextTuple, nextInID, nextIdx));

            }

            for (ObjectInputStream inStream : inStreams) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    System.err.printf("cannot close the inStream in start=%d, end = %d due to %s\n", start, end, e.toString());
                }
            }
            try {
                outStream.close();
            } catch (IOException e) {
                System.err.printf("cannot close the outStream in start=%d, end = %d due to %s\n", start, end, e.toString());
            }
            fileNum++;
        }

        return fileNum;

    }

    /**
     * This method is to provide a basis for comparison between tuples based on the attribute of interest.
     * @param t1    first tuple to be compared
     * @param t2    second tuple to be compared
     * @return      integer indicating ordering of twp tuples.
     */
    private int compareTuples(Tuple t1, Tuple t2) {
        int idx = 0;
        // check on each sort index, if one of them is not 0, return the ordering immediately.
        while (idx < attributeArrayList.size()) {
            int sortIndex = schema.indexOf(attributeArrayList.get(idx));
            int res = Tuple.compareTuples(t1, t2, sortIndex);
            if (res != 0) {
                return res;
            }
            idx++;
        }
        return 0;
    }

    /**
     * To generate random file names for all the intermediate sorted/merged runs.
     * @param passID    current pass number.
     * @param runID     current nun number.
     * @return          String representation of the files.
     */
    private String outFileName(int passID, int runID) {
        return "Sorted" + uuid + "_" + passID + "_" + runID;
    }

    @Override
    public Batch next() {
        if (eos) {
            close();
            return null;
        }

        Batch out = new Batch(batchSize);
        while (!out.isFull()) {
            try {
                Tuple data = (Tuple) sortedStream.readObject();
                out.add(data);
            } catch (ClassNotFoundException cnf) {
                System.err.printf("Cannot read from sortedrun due to %s\n", cnf.toString());
                System.exit(1);
            } catch (EOFException EOF) {
                eos = true;
                return out;
            } catch (IOException e) {
                System.err.printf("Cannot read from sortedrun due to %s\n", e.toString());
                System.exit(1);
            }
        }
        return out;
    }

    @Override
    public boolean close() {
        super.close();
        try{
            sortedStream.close();
        } catch (IOException e) {
            System.err.println("Cannot close sortedStream " + e.toString());
            return false;
        }
        // To delete all the sorted files generated.
        for (String name : nameList) {
            File f = new File(name);
            f.delete();
        }
        return true;
    }



}