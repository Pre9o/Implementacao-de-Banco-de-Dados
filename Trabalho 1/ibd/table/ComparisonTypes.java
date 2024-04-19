/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

/**
 *
 * @author Sergio
 */
public class ComparisonTypes {

    public static final int EQUAL = 1;
    public static final int DIFF = 2;
    public static final int GREATER_THAN = 3;
    public static final int GREATER_EQUAL_THAN = 4;
    public static final int LOWER_THAN = 5;
    public static final int LOWER_EQUAL_THAN = 6;

    /**
     *
     * @param type the comparison type
     * @return the string that described the comparison type
     */
    public static String getComparisonType(int type) {
        return switch (type) {
            case EQUAL -> "EQUAL";
            case DIFF -> "DIFF";
            case GREATER_THAN -> "GREATER_THAN";
            case GREATER_EQUAL_THAN -> "GREATER_EQUAL_THAN";
            case LOWER_THAN -> "LOWER_THAN";
            case LOWER_EQUAL_THAN -> "LOWER_EQUAL_THAN";
            default -> "---";
        };
    }
    
    /**
     *
     * @param type the comparison type
     * @return the string that described the operation
     */
    public static String getComparisonOperation(int type) {
        return switch (type) {
            case EQUAL -> "=";
            case DIFF -> "<>";
            case GREATER_THAN -> ">";
            case GREATER_EQUAL_THAN -> ">=";
            case LOWER_THAN -> "<";
            case LOWER_EQUAL_THAN -> "<=";
            default -> "---";
        };
    }
    
    
     /**
     * Compares two values according to a comparison type
     * @param value1 the first operand
     * @param value2 the second operand
     * @param comparisonType the comparison type
     * @return true if the comparable object match according to the comparison type
     */
    public static boolean match(Comparable value1, Comparable value2, int comparisonType) {

        int resp = value1.compareTo(value2);
        if (resp == 0 && (comparisonType == ComparisonTypes.EQUAL
                || comparisonType == ComparisonTypes.LOWER_EQUAL_THAN
                || comparisonType == ComparisonTypes.GREATER_EQUAL_THAN)) {
            return true;
        } else if (resp < 0 && (comparisonType == ComparisonTypes.LOWER_THAN
                || comparisonType == ComparisonTypes.LOWER_EQUAL_THAN)) {
            return true;
        } else if (resp > 0 && (comparisonType == ComparisonTypes.GREATER_THAN
                || comparisonType == ComparisonTypes.GREATER_EQUAL_THAN)) {
            return true;
        } else if (resp != 0 && comparisonType == ComparisonTypes.DIFF) {
            return true;
        } else {
            return false;
        }
    }

}
