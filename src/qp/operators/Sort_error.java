package qp.operators;

import java.util.UUID;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.PriorityQueue;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleInRun;

public class Sort_error extends Operator {
    private final Operator base;
    private final int numOfBuffers;
    private final ArrayList<Attribute> attributeArrayList;
    private final int batchSize;
    private ObjectInputStream sortedStream;
    private boolean eos = false;
    private int fileNum;
    private final String uuid = UUID.randomUUID().toString();

    public Sort_error(Operator base, ArrayList<Attribute> attr, int numOfBuffers) {
        super(OpType.SORT);
        this.schema = base.schema;
        this.base = base;
        this.numOfBuffers = numOfBuffers;
        this.attributeArrayList = attr;
        this.batchSize = Batch.getPageSize() / schema.getTupleSize();

    }

    @Override
    public boolean open() {
        System.out.println("entering open()");
        if (!base.open()) {
            return false;
        }

        int i = sortedRuns(); //generate sorted runs
        System.out.println("in sort open(), the number of sorted file is " + i);; //merge sorted runs
        return merge(i, 1) == 1;
    }

    private int sortedRuns() {
        // the runs here denotes the number of files generates

        Batch inBatch = base.next();
        int runs = 0;
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
                System.exit(1);
            }

            // to open up the
            inBatch = base.next();
            runs++;

        }
        return runs;
    }

    private int merge(int fileNum, int pass) {
        System.out.println("entering merge");
        // one while loop represent on pass of the merging process.
        // after one pass, the merged files will be stored and fileNum will reduced

        int fileNumNow = fileNum;
        if (fileNumNow <= 1) {
            //System.out.println("Hiiiiiiiiii, is the fileNumNow is 1?" + fileNumNow);
            String fileName = outFileName(pass - 1, 0);
            try {
                sortedStream = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException e) {
                System.err.printf("cannot write out the sorted file=%s because %s\n", fileName, e.toString());
            }
            return fileNumNow;
        }
        fileNumNow = mergeIntermediate(fileNumNow, pass + 1);
        pass++;
        return merge(fileNumNow, pass);

        //catch some errors????
    }
    // take in # of runs from the previous pass, and output the # of sorted runs of current pass
    // # of pass
    private int mergeIntermediate(int preFileNum, int pass) {
        int fileNum = 0;
        System.out.println("In merge intermediate, the input filenum = " + preFileNum);
        for (int start = 0; start < preFileNum; start += numOfBuffers - 1) {
            System.out.println("i am here outer intermediate " + start);
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
                System.out.println("i am here inner intermediate " + i + " " + end );
                String fileName = outFileName(pass - 1, i);
                ObjectInputStream inStream = null;
                try {
                    inStream = new ObjectInputStream(new FileInputStream(fileName));
                } catch (IOException e) {
                    System.out.println("File cannot be input to stream");
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
            for (int idx = 0; idx < end - start; idx++) {
                //System.out.println("i am here below inner intermediate " + idx);
                Batch in = ins[idx];
                if (in == null || in.isEmpty()) {
                    endFlag[idx] = true;
                    //idx++;
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
                    //Debug.PPrint(tpl.tuple);
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


    private String outFileName(int passID, int runID) {
        return "Sorted" + uuid + "_" + passID + "_" + runID;
    }

    public Batch next() {
        if (eos) {
            close();
            return null;
        }

        Batch out = new Batch(batchSize);
        while (!out.isFull()) {
            try {
                //Debug.PPrint(sortedStream);
                Tuple data = (Tuple) sortedStream.readObject();
                out.add(data);
            } catch (ClassNotFoundException cnf) {
                System.err.printf("Cannot read from sortedrun due to %s\n", cnf.toString());
                System.exit(1);
            } catch (EOFException EOF) {
                // Sends the incomplete page and close in the next call.
                eos = true;
                return out;
            } catch (IOException e) {
                System.err.printf("Cannot read from sortedrun due to %s\n", e.toString());
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