/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop;

import ibd.query.Operation;

/**
 * Defines the template of a binary operation. A binary operation accesses data
 * that comes from two operations (from the left and from the right) to perform
 * data transformation. The data transformation is usually based on comparisons between tuples 
 * @author Sergio
 */
public abstract class BinaryOperation extends Operation {

    /**
     * the left side operation
     */
    protected Operation leftOperation;

    /**
     * the right side operation
     */
    protected Operation rightOperation;

    /**
     *
     * @param leftOperation the left side operation
     * @param rightOperation the right side operation
     * @throws Exception
     */
    public BinaryOperation(Operation leftOperation, Operation rightOperation) throws Exception {
        super();
        setLeftOperation(leftOperation);
        setRightOperation(rightOperation);
    }
    
    public boolean useLeftSideLookups(){
        return false;
    }
    
    @Override
    public void prepare() throws Exception {
        super.prepare();
        //the preparation is propagated to the left and right operations
        getLeftOperation().prepare();
        getRightOperation().prepare();

    }
    
    @Override
    public final void setProcessedOperations() {
        super.setProcessedOperations();
        getLeftOperation().setProcessedOperations();
        getRightOperation().setProcessedOperations();
    }

    /**
     * sets the left side operation
     *
     * @param op
     */
    public final void setLeftOperation(Operation op) {
        leftOperation = op;
        op.setParentOperation(this);
    }

    /**
     * sets the right side operation
     *
     * @param op
     */
    public final void setRightOperation(Operation op) {
        rightOperation = op;
        op.setParentOperation(this);
    }

    /**
     * gets the left side operation
     *
     * @return
     */
    public Operation getLeftOperation() {
        return leftOperation;
    }

    /**
     * gets the right side operation
     *
     * @return
     */
    public Operation getRightOperation() {
        return rightOperation;
    }

    @Override
    public void close() throws Exception {
        leftOperation.close();
        rightOperation.close();
    }


}
