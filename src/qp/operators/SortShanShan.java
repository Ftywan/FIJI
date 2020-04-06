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

public class SortShanShan extends Operator {
    private final Operator base;
    private final int numOfBuffers;
    private final ArrayList<Attribute> attributeArrayList;
    private final int batchSize;
    private ObjectInputStream sortedStream;
    private boolean eos = false;
    private int fileNum;

    public SortShanShan(Operator base, ArrayList<Attribute> attr, int numOfBuffers) {
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
        /*
        int numOfRuns = sortedRuns();
        if (mergeRuns(numOfRuns, 1) ==1) {
            return true;
        } else {
            return false;
        }*/
        int i = sortedRuns(); //generate sorted runs
        merge(i); //merge sorted runs
        return true;
    }

    private int sortedRuns() {
        // the runs here denotes the number of files generates
        int runs = 0;
        Batch inBatch = base.next();
        while (inBatch != null) {
            ArrayList<Tuple> tuplesToSort = new ArrayList<>();
            int i = 0;
            // read the base batch by batch until all the available buffers have been taken up.
            while (i < numOfBuffers && inBatch != null) {
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

    private void merge(int fileNum) {
        // one while loop represent on pass of the merging process.
        // after one pass, the merged files will be stored and fileNum will reduced
        int pass = 0;
        while (fileNum > 1) {
            try {
                fileNum = mergeIntermediate(fileNum, pass + 1);
            } catch (Exception e) {
                System.err.printf("mergeIntermediate function does not work for pass = %d and filenum = %d\n ", pass, fileNum, e.toString());
            }
            pass++;
            System.out.println(pass + " merge finished");
        }

        if (fileNum <= 1) {
            String fileName = outFileName(pass, fileNum-1);
            try {
                sortedStream = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException e) {
                System.err.printf("cannot write out the sorted file=%s because %s\n", fileName, e.toString());
            }
        }
        //catch some errors????
    }
    // take in # of runs from the previous pass, and output the # of sorted runs of current pass
    // # of pass
    private int mergeIntermediate(int preFileNum, int pass) {
        int fileNum = 0;
        for (int start = 0; start < preFileNum; start += numOfBuffers) {
            int end = Math.min(start + numOfBuffers-1, preFileNum);
            // merge sorted runs from start to end.
            // now you have your start and your end, you want to merge them now.
            // first: load the data from the corresponding file into your memory
            // second: put everything from memory into pq
            // poll them one by one in the queue until queue is empty
            // third: reload the files pages into memory again.

            // now we first load data into memory as much as possible
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
                    System.err.printf("mergeintermediate cannot find the file %s to put into memory due to %s\n", fileName, e.toString());
                }
                inStreams[i - start] = inStream;
                Batch in = new Batch(batchSize);
                while (!in.isFull()) {
                    Tuple tuple = null;
                    try {
                        tuple = (Tuple) inStream.readObject();
                    } catch (EOFException e) {
                        break;
                    } catch (ClassNotFoundException e) {
                        System.err.printf("cannot load data into buffer due to %s\n", e.toString());
                    } catch (IOException e) {
                        System.err.printf("cannot load data into buffer due to %s\n", e.toString());
                    }
                    in.add(tuple);
                }
                ins[i - start] = in;
            }
            //put everything from buffer into pq.
            PriorityQueue<TupleInRun> pq = new PriorityQueue<>(batchSize, (t1, t2) -> compareTuples(t1.tuple, t2.tuple));
            // name the output merged file with current pass number and file numeber;
            // create the corresponding outStream for continuous data flow;
            String outFileName = outFileName(pass, fileNum);
            ObjectOutputStream outStream = null;
            try {
                outStream = new ObjectOutputStream(new FileOutputStream(outFileName));
            } catch (IOException e) {
                System.err.printf("cannot output from pq to outStream (%s) due to %s\n", outFileName, e.toString());
            }
            // put the first tuple in each buffer(in) into the pq so that we can find the
            // min of the remaining tuples each time.
            int idx = 0;
            while (idx < end - start) {
                Batch in = ins[idx];
                if (in == null || in.isEmpty()) {
                    endFlag[idx] = true;
                    continue;
                }
                Tuple tpl = in.get(0);
                pq.add(new TupleInRun(tpl, idx, 0));
                idx++;
            }
            //now there are (n-1) tuples in the qp and you can start the loop
            // to continuously output the smallest tuple.
            while (!pq.isEmpty()) {
                TupleInRun tpl = pq.poll();
                try {
                    Debug.PPrint(tpl.tuple);
                    outStream.writeObject(tpl.tuple);
                } catch (IOException e) {
                    System.err.printf("the min tuple cannot be put into outStream due to %s\n", e.toString());
                }
                // now we look for the replacement of the tuple
                // that have been popped in the above line. This is done by find the
                // where the tuples comes from. if the buffer it comes from has tuple left,
                // put the next one into pq,
                int nextInID = tpl.runID;
                int nextIdx = tpl.tupleID + 1;
                // if the tuple it comes from is alr empty, then you go to its corresponding inStream and
                // input another batch of data until the buffer page  is full again.
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
                // this is too handle the case where the last inStream is sometimes shorter
                // then all the previous stream, Hence, when the last few data is load into buffer,
                // they do not occupy the buffer fully. so size < index;
                if (in == null || in.size() <= nextIdx) {
                    endFlag[nextInID] = true;
                    continue;
                }
                Tuple nextTuple = in.get(nextIdx);
                pq.add(new TupleInRun(nextTuple, nextInID, nextIdx));

            }
            fileNum++;
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
        }
        return fileNum;

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

        Batch out = new Batch(batchSize);
        while (!out.isFull()) {
            try {
                Tuple data = (Tuple) sortedStream.readObject();
                out.add(data);
            } catch (ClassNotFoundException cnf) {
                System.err.printf("Sort: class not found for reading from sortedStream due to %s\n", cnf.toString());
                System.exit(1);
            } catch (EOFException EOF) {
                // Sends the incomplete page and close in the next call.
                eos = true;
                return out;
            } catch (IOException e) {
                System.err.printf("Sort: error reading from sortedStream due to %s\n", e.toString());
                System.exit(1);
            }
        }
        return out;


    }

    public boolean close() {
        super.close();
        try{
            sortedStream.close();
        } catch (IOException e) {
            System.err.println("Cannot close sortedStream " + e.toString());
            return false;
        }
        return true;
    }



}