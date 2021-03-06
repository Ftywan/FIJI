/**
 * performs randomized optimization, iterative improvement algorithm
 **/

package qp.optimizer;

import qp.operators.*;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;

import java.util.ArrayList;

public class RandomOptimizer {

    /**
     * enumeration of different ways to find the neighbor plan
     **/
    public static final int METHODCHOICE = 0;  // Selecting neighbor by changing a method for an operator
    public static final int COMMUTATIVE = 1;   // By rearranging the operators by commutative rule
    public static final int ASSOCIATIVE = 2;   // Rearranging the operators by associative rule

    /**
     * constants that needed for the SA algorithm
     */
    private static final double TEMPERATUREFACTOR = 0.1;
    private static final double TEMPERATUREREDUCTIONFACTOR = 0.95;
    private static final int EQUILIBRIUMFACTOR = 16;

    /**
     * Number of altenative methods available for a node as specified above
     **/
    public static final int NUMCHOICES = 3;

    SQLQuery sqlquery;  // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;        // Number of joins in this query plan

    /**
     * constructor
     **/
    public RandomOptimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
    }

    /**
     * After finding a choice of method for each operator
     * * prepare an execution plan by replacing the methods with
     * * corresponding join operator implementation
     **/
    public static Operator makeExecPlan(Operator node) {
        // for join operation
        if (node.getOpType() == OpType.JOIN) {
            Operator left = makeExecPlan(((Join) node).getLeft());
            Operator right = makeExecPlan(((Join) node).getRight());
            int joinType = ((Join) node).getJoinType();
            int numbuff = BufferManager.getBuffersPerJoin();
            switch (joinType) {
                case JoinType.NESTEDJOIN:
                    System.out.println("Page Nested Join");
                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    return nj;

                case JoinType.BLOCKNESTED:
                    System.out.println("Block Nested Join");
                    BlockNestedJoin bj = new BlockNestedJoin((Join) node);
                    bj.setLeft(left);
                    bj.setRight(right);
                    bj.setNumBuff(numbuff);
                    return bj;
                
//                case JoinType.HASHJOIN:
//                    System.out.println("Hash Join");
//                    HashJoin hj = new HashJoin((Join) node);
//
//                    hj.setLeft(left);
//                    hj.setRight(right);
//                    hj.setNumBuff(numbuff);
//                    return hj;
                case JoinType.SORTMERGE:
                    System.out.println("SortMerge Join");
                    SortMergeJoin sj = new SortMergeJoin((Join) node);
                    sj.setLeft(left);
                    sj.setRight(right);
                    sj.setNumBuff(numbuff);
                    return sj;

                default:
                    return node;
            }
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select) node).getBase());
            ((Select) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project) node).getBase());
            ((Project) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.DISTINCT) {
            Operator base = makeExecPlan(((Distinct) node).getBase());
            ((Distinct) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.ORDERBY) {
            Operator base = makeExecPlan(((OrderBy) node).getBase());
            ((OrderBy) node).setBase(base);
            return node;
        } else {
            return node;
        }
    }

    /**
     * Randomly selects a neighbour
     **/
    protected Operator getNeighbor(Operator root) {
        // Randomly select a node to be altered to get the neighbour
        int nodeNum = RandNumb.randInt(0, numJoin - 1);
        // Randomly select type of alteration: Change Method/Associative/Commutative
        int changeType = RandNumb.randInt(0, NUMCHOICES - 1);
        Operator neighbor = null;
        switch (changeType) {
            case METHODCHOICE:   // Select a neighbour by changing the method type
                neighbor = neighborMeth(root, nodeNum);
                break;
            case COMMUTATIVE:
                neighbor = neighborCommut(root, nodeNum);
                break;
            case ASSOCIATIVE:
                neighbor = neighborAssoc(root, nodeNum);
                break;
        }
        return neighbor;
    }

    /**
     * Implementation of the 2 Phase Optimization with Iterative Improvement and the Simulated Annealing Algorithm
     */
    public Operator getOptimizedPlan() {
        Operator phaseOne = runIIOptimization();
        Operator phaseTwo = runSAOptimization(phaseOne);

        return phaseTwo;
    }

    /**
     * Implementation of Iterative Improvement Algorithm for Randomized optimization of Query Plan
     **/
    public Operator runIIOptimization() {
        System.out.println("Starting Iterative Improvement Phase");
        /** get an initial plan for the given sql query **/
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();
        long MINCOST = Long.MAX_VALUE;
        Operator finalPlan = null;

        /** NUMITER is number of times random restart
         * as we are using 2PO, we will iterate 10 times in this phase
         **/
        int NUMITER = 10;

        /** Randomly restart the gradient descent until
         *  the maximum specified number of random restarts (NUMITER)
         *  has satisfied
         **/
        for (int j = 0; j < NUMITER; ++j) {
            Operator initPlan = rip.prepareInitialPlan();
            modifySchema(initPlan);
            System.out.println("-----------initial Plan-------------");
            Debug.PPrint(initPlan);
            PlanCost pc = new PlanCost();
            long initCost = pc.getCost(initPlan);
            System.out.println(initCost);

            boolean flag = true;
            long minNeighborCost = initCost;   //just initialization purpose;
            Operator minNeighbor = initPlan;  //just initialization purpose;

            // looking for the local minimum
            if (numJoin != 0) {
                while (flag) {  // flag = false when local minimum is reached
                    System.out.println("---------------while--------");
                    Operator initPlanCopy = (Operator) initPlan.clone();
                    minNeighbor = getNeighbor(initPlanCopy);

                    // initialize a neighbor cost with a random neighbor
                    System.out.println("--------------------------neighbor---------------");
                    Debug.PPrint(minNeighbor);
                    pc = new PlanCost();
                    minNeighborCost = pc.getCost(minNeighbor);
                    System.out.println("  " + minNeighborCost);

                    /** In this loop we consider from the
                     ** possible neighbors (randomly selected)
                     ** and take the minimum among for next step
                     **/
                    for (int i = 1; i < 2 * numJoin; ++i) {
                        initPlanCopy = (Operator) initPlan.clone();
                        Operator neighbor = getNeighbor(initPlanCopy);
                        System.out.println("------------------neighbor--------------");
                        Debug.PPrint(neighbor);
                        pc = new PlanCost();
                        long neighborCost = 0;
                        try {
                            neighborCost = pc.getCost(neighbor);
                        } catch (Exception e) {
                            System.out.println("fatal error.");
                            System.exit(0);
                        }
                        System.out.println(neighborCost);

                        if (neighborCost < minNeighborCost) {
                            minNeighbor = neighbor;
                            minNeighborCost = neighborCost;
                        }
                    }
                    if (minNeighborCost < initCost) {
                        initPlan = minNeighbor;
                        initCost = minNeighborCost;
                    } else {
                        minNeighbor = initPlan;
                        minNeighborCost = initCost;
                        flag = false;  // local minimum reached
                    }
                }
                System.out.println("------------------local minimum--------------");
                Debug.PPrint(minNeighbor);
                System.out.println(" " + minNeighborCost);
            }
            if (minNeighborCost < MINCOST) {
                MINCOST = minNeighborCost;
                finalPlan = minNeighbor;
            }
        }
        System.out.println("\n\n");
        System.out.println("----------------Iterative Improvement Final Plan----------------");
        Debug.PPrint(finalPlan);
        System.out.println("  " + MINCOST);
        System.out.println("\n");
        return finalPlan;
    }

    /**
     * Implementation of Simulated Annealing Algorithm for Randomized optimization of Query Plan
     **/
    public Operator runSAOptimization(Operator initialPlan) {
        System.out.println("Starting Simulated Annealing Phase");
        Operator currentPlan = initialPlan;
        Operator minCostPlan = currentPlan;
        PlanCost initialCost = new PlanCost();
        long minCostValue = initialCost.getCost(currentPlan);
        double temperature = TEMPERATUREFACTOR * minCostValue;
        int stableTime = 0;
        int equilibrium = EQUILIBRIUMFACTOR * numJoin;

        while (temperature >= 1 && stableTime <= 4) {
            Operator startPlan = minCostPlan;
            for (int i = 0; i < equilibrium; i ++) {
                Operator neighborPlan = getNeighbor((Operator) currentPlan.clone());
                System.out.println("\n---------------SA Neighbor---------------");
                Debug.PPrint(neighborPlan);
                PlanCost neighborCost = new PlanCost();
                PlanCost currentCost = new PlanCost();
                long neighborCostValue = neighborCost.getCost(neighborPlan);
                System.out.println("   "+ neighborCostValue);
                System.out.println("\n");
                long deltaCost = neighborCostValue - currentCost.getCost(currentPlan);

                if (deltaCost <= 0) {
                    currentPlan = neighborPlan;
                }
                if (deltaCost > 0) {
                    double probability = Math.exp(-(deltaCost / temperature));
                    if (Math.random() < probability) {
                        currentPlan = neighborPlan;
                    }
                }
                currentCost = new PlanCost();
                long currentCostValue = currentCost.getCost(currentPlan);
                if (currentCostValue < minCostValue) {
                    minCostPlan = neighborPlan;
                    minCostValue = currentCostValue;
                }
            }
            Operator endPlan = minCostPlan;
            temperature *= TEMPERATUREREDUCTIONFACTOR;
            if (startPlan.hashCode() == endPlan.hashCode()) {
                stableTime ++;
            } else {
                stableTime = 0;
            }
        }
        System.out.println("----------------Simulated Annealing Final Plan----------------\n");
        Debug.PPrint(minCostPlan);
        System.out.println("\n");
        return minCostPlan;
    }

    /**
     * Selects a random method choice for join with number joinNum
     * *  e.g., Nested loop join, Sort-Merge Join, Hash Join etc..,
     * * returns the modified plan
     **/
    protected Operator neighborMeth(Operator root, int joinNum) {
        System.out.println("------------------neighbor by method change----------------");
        int numJMeth = JoinType.numJoinTypes();
        if (numJMeth > 1) {
            /** find the node that is to be altered **/
            Join node = (Join) findNodeAt(root, joinNum);
            int prevJoinMeth = node.getJoinType();
            int joinMeth = RandNumb.randInt(0, numJMeth - 1);
            while (joinMeth == prevJoinMeth) {
                joinMeth = RandNumb.randInt(0, numJMeth - 1);
            }
            node.setJoinType(joinMeth);
        }
        return root;
    }

    /**
     * Applies join Commutativity for the join numbered with joinNum
     * *  e.g.,  A X B  is changed as B X A
     * * returns the modifies plan
     **/
    protected Operator neighborCommut(Operator root, int joinNum) {
        System.out.println("------------------neighbor by commutative---------------");
        /** find the node to be altered**/
        Join node = (Join) findNodeAt(root, joinNum);
        Operator left = node.getLeft();
        Operator right = node.getRight();
        node.setLeft(right);
        node.setRight(left);
        node.getCondition().flip();
        modifySchema(root);
        return root;
    }

    /**
     * Applies join Associativity for the join numbered with joinNum
     * *  e.g., (A X B) X C is changed to A X (B X C)
     * *  returns the modifies plan
     **/
    protected Operator neighborAssoc(Operator root, int joinNum) {
        /** find the node to be altered**/
        Join op = (Join) findNodeAt(root, joinNum);
        Operator left = op.getLeft();
        Operator right = op.getRight();

        if (left.getOpType() == OpType.JOIN && right.getOpType() != OpType.JOIN) {
            transformLefttoRight(op, (Join) left);
        } else if (left.getOpType() != OpType.JOIN && right.getOpType() == OpType.JOIN) {
            transformRighttoLeft(op, (Join) right);
        } else if (left.getOpType() == OpType.JOIN && right.getOpType() == OpType.JOIN) {
            if (RandNumb.flipCoin())
                transformLefttoRight(op, (Join) left);
            else
                transformRighttoLeft(op, (Join) right);
        } else {
            // The join is just A X B,  therefore Association rule is not applicable
        }

        /** modify the schema before returning the root **/
        modifySchema(root);
        return root;
    }

    /**
     * This is given plan (A X B) X C
     **/
    protected void transformLefttoRight(Join op, Join left) {
        System.out.println("------------------Left to Right neighbor--------------");
        Operator right = op.getRight();
        Operator leftleft = left.getLeft();
        Operator leftright = left.getRight();
        Attribute leftAttr = op.getCondition().getLhs();
        Join temp;

        if (leftright.getSchema().contains(leftAttr)) {
            System.out.println("----------------CASE 1-----------------");
            /** CASE 1 :  ( A X a1b1 B) X b4c4  C     =  A X a1b1 (B X b4c4 C)
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftright, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftleft);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            op.setRight(temp);
            op.setCondition(left.getCondition());

        } else {
            System.out.println("--------------------CASE 2---------------");
            /**CASE 2:   ( A X a1b1 B) X a4c4  C     =  B X b1a1 (A X a4c4 C)
             ** a1b1,  a4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftleft, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftright);
            op.setRight(temp);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            Condition newcond = left.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }

    protected void transformRighttoLeft(Join op, Join right) {
        System.out.println("------------------Right to Left Neighbor------------------");
        Operator left = op.getLeft();
        Operator rightleft = right.getLeft();
        Operator rightright = right.getRight();
        Attribute rightAttr = (Attribute) op.getCondition().getRhs();
        Join temp;

        if (rightleft.getSchema().contains(rightAttr)) {
            System.out.println("----------------------CASE 3-----------------------");
            /** CASE 3 :  A X a1b1 (B X b4c4  C)     =  (A X a1b1 B ) X b4c4 C
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(left, rightleft, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightright);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            op.setCondition(right.getCondition());
        } else {
            System.out.println("-----------------------------CASE 4-----------------");
            /** CASE 4 :  A X a1c1 (B X b4c4  C)     =  (A X a1c1 C ) X c4b4 B
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(left, rightright, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightleft);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            Condition newcond = right.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }

    /**
     * This method traverses through the query plan and
     * * returns the node mentioned by joinNum
     **/
    protected Operator findNodeAt(Operator node, int joinNum) {
        if (node.getOpType() == OpType.JOIN) {
            if (((Join) node).getNodeIndex() == joinNum) {
                return node;
            } else {
                Operator temp;
                temp = findNodeAt(((Join) node).getLeft(), joinNum);
                if (temp == null)
                    temp = findNodeAt(((Join) node).getRight(), joinNum);
                return temp;
            }
        } else if (node.getOpType() == OpType.SCAN) {
            return null;
        } else if (node.getOpType() == OpType.SELECT) {
            // if sort/project/select operator
            return findNodeAt(((Select) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.PROJECT) {
            return findNodeAt(((Project) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.DISTINCT) {
            return findNodeAt(((Distinct) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.ORDERBY) {
            return findNodeAt(((OrderBy) node).getBase(), joinNum);
        } else {
            return null;
        }
    }

    /**
     * Modifies the schema of operators which are modified due to selecting an alternative neighbor plan
     **/
    private void modifySchema(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            Operator left = ((Join) node).getLeft();
            Operator right = ((Join) node).getRight();
            modifySchema(left);
            modifySchema(right);
            node.setSchema(left.getSchema().joinWith(right.getSchema()));
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = ((Select) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = ((Project) node).getBase();
            modifySchema(base);
            ArrayList attrlist = ((Project) node).getProjAttr();
            node.setSchema(base.getSchema().subSchema(attrlist));
        } else if (node.getOpType() == OpType.DISTINCT) {
            Operator base = ((Distinct) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.ORDERBY) {
            Operator base = ((OrderBy) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        }
    }
}
