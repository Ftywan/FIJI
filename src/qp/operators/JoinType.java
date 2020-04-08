/**
 * Enumeration of join algorithm types
 * Change this class depending on actual algorithms
 * you have implemented in your query processor
 **/

package qp.operators;

public class JoinType {

<<<<<<< Updated upstream
    public static final int NESTEDJOIN = 2;
    public static final int BLOCKNESTED = 1;
    public static final int SORTMERGE = 0;
=======
    public static final int NESTEDJOIN = 1;
    public static final int BLOCKNESTED = 0;
    public static final int SORTMERGE = 2;
>>>>>>> Stashed changes
    public static final int HASHJOIN = 3;

    public static int numJoinTypes() {
        return 3;
    }
}
