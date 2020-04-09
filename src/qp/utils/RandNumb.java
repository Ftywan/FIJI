/**
 * functions to get some random numbers
 * useful in random optimizer
 **/

package qp.utils;

import java.lang.Math;

public class RandNumb {

    /** Get a random number between a and b **/
    public static int randInt(int a, int b) {
        return ((int) (Math.floor(Math.random() * (b - a + 1)) + a));
    }

    /** Coin flip **/
    public static boolean flipCoin() {
	    return Math.random() < 0.5;
    }

}
