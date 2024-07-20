/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.transaction;

import ibd.table.Table;
import ibd.transaction.concurrency.rafaelcarneiropregardier.RafaelCarneiroPregardierConcurrencyManager;
import ibd.transaction.instruction.SingleReadInstruction;
import ibd.transaction.instruction.SingleUpdateInstruction;
import ibd.transaction.concurrency.ConcurrencyManager;
import ibd.table.Utils;
import static ibd.transaction.SimulatedIterations.getValue;
import ibd.transaction.concurrency.LockBasedConcurrencyManager;

/**
 *
 * @author pccli
 */
public class Main2 {



    //classic deadlock
    public void test1(ConcurrencyManager manager) throws Exception {
        Table table1 = Utils.createTable("C:\\Users\\naoca\\Documents\\GitHub\\Implementacao-de-Banco-de-Dados\\Trabalho 4\\ibd", "t3", Table.DEFULT_PAGE_SIZE, 1000, false, 1, 50);
        Transaction t1 = new Transaction();
        t1.addInstruction(new SingleReadInstruction(table1, getValue("A")));
        t1.addInstruction(new SingleUpdateInstruction(table1, getValue("B"), "bla"));

        Transaction t2 = new Transaction();
        t2.addInstruction(new SingleReadInstruction(table1, getValue("B")));
        t2.addInstruction(new SingleUpdateInstruction(table1, getValue("A"), "bla"));

        SimulatedIterations simulation = new SimulatedIterations();
        simulation.addTransaction(t1);
        simulation.addTransaction(t2);
        simulation.run(100, false, manager);
    }

    //classic deadlock 2
    public void test2(ConcurrencyManager manager) throws Exception {
        Table table1 = Utils.createTable("C:\\Users\\naoca\\Documents\\GitHub\\Implementacao-de-Banco-de-Dados\\Trabalho 4\\ibd", "t3", Table.DEFULT_PAGE_SIZE, 1000, false, 1, 50);
        Transaction t1 = new Transaction();

        t1.addInstruction(new SingleUpdateInstruction(table1, getValue("B"), "bla"));
        t1.addInstruction(new SingleReadInstruction(table1, getValue("A")));

        Transaction t2 = new Transaction();
        t2.addInstruction(new SingleUpdateInstruction(table1, getValue("A"), "bla"));
        t2.addInstruction(new SingleReadInstruction(table1, getValue("B")));

        SimulatedIterations simulation = new SimulatedIterations();
        simulation.addTransaction(t1);
        simulation.addTransaction(t2);
        simulation.run(100, false, manager);
    }

    //deadlock exercise of lesson 22 - Controle de Concorrência - Deadlock
    public void test3(ConcurrencyManager manager) throws Exception {
        Table table1 = Utils.createTable("C:\\Users\\naoca\\Documents\\GitHub\\Implementacao-de-Banco-de-Dados\\Trabalho 4\\ibd", "t4", Table.DEFULT_PAGE_SIZE, 1000, false, 1, 50);
        Transaction t1 = new Transaction();
        t1.addInstruction(new SingleReadInstruction(table1, getValue("B")));
        t1.addInstruction(new SingleUpdateInstruction(table1, getValue("A"), "bla"));
        t1.addInstruction(new SingleUpdateInstruction(table1, getValue("C"), "bla"));

        Transaction t2 = new Transaction();
        t2.addInstruction(new SingleReadInstruction(table1, getValue("A")));
        t2.addInstruction(new SingleReadInstruction(table1, getValue("B")));
        t2.addInstruction(new SingleReadInstruction(table1, getValue("C")));

        Transaction t3 = new Transaction();
        t3.addInstruction(new SingleUpdateInstruction(table1, getValue("C"), "bla"));
        t3.addInstruction(new SingleReadInstruction(table1, getValue("A")));
        t3.addInstruction(new SingleUpdateInstruction(table1, getValue("B"), "bla"));

        SimulatedIterations simulation = new SimulatedIterations();
        simulation.addTransaction(t1);
        simulation.addTransaction(t2);
        simulation.addTransaction(t3);
        simulation.run(100, false, manager);

    }

    //extension of test3
    public void test4(ConcurrencyManager manager) throws Exception {
        Table table1 = Utils.createTable("C:\\Users\\naoca\\Documents\\GitHub\\Implementacao-de-Banco-de-Dados\\Trabalho 4\\ibd", "t5", Table.DEFULT_PAGE_SIZE, 1000, false, 1, 50);
        Transaction t1 = new Transaction();
        t1.addInstruction(new SingleReadInstruction(table1, getValue("B")));
        t1.addInstruction(new SingleUpdateInstruction(table1, getValue("A"), "bla"));
        t1.addInstruction(new SingleUpdateInstruction(table1, getValue("C"), "bla"));

        Transaction t2 = new Transaction();
        t2.addInstruction(new SingleReadInstruction(table1, getValue("A")));
        t2.addInstruction(new SingleReadInstruction(table1, getValue("B")));
        t2.addInstruction(new SingleReadInstruction(table1, getValue("C")));

        Transaction t3 = new Transaction();
        t3.addInstruction(new SingleUpdateInstruction(table1, getValue("C"), "bla"));
        t3.addInstruction(new SingleReadInstruction(table1, getValue("A")));
        t3.addInstruction(new SingleUpdateInstruction(table1, getValue("B"), "bla"));

        Transaction t4 = new Transaction();
        t4.addInstruction(new SingleUpdateInstruction(table1, getValue("B"), "bla"));
        t4.addInstruction(new SingleReadInstruction(table1, getValue("F")));

        SimulatedIterations simulation = new SimulatedIterations();
        simulation.addTransaction(t1);
        simulation.addTransaction(t2);
        simulation.addTransaction(t3);
        simulation.addTransaction(t4);
        simulation.run(100, false, manager);
    }

    //a more complex case
    public void test5(ConcurrencyManager manager) throws Exception {
        Table table1 = Utils.createTable("C:\\Users\\naoca\\Documents\\GitHub\\Implementacao-de-Banco-de-Dados\\Trabalho 4\\ibd", "t6", Table.DEFULT_PAGE_SIZE, 1000, false, 1, 50);
        Transaction t1 = new Transaction();
        t1.addInstruction(new SingleReadInstruction(table1, getValue("A")));
        t1.addInstruction(new SingleUpdateInstruction(table1, getValue("C"), "bla"));
        t1.addInstruction(new SingleReadInstruction(table1, getValue("D")));

        Transaction t2 = new Transaction();
        t2.addInstruction(new SingleUpdateInstruction(table1, getValue("A"), "ds"));
        t2.addInstruction(new SingleUpdateInstruction(table1, getValue("C"), "ds"));

        Transaction t3 = new Transaction();
        t3.addInstruction(new SingleReadInstruction(table1, getValue("C")));
        t3.addInstruction(new SingleUpdateInstruction(table1, getValue("B"), "dd"));
        t3.addInstruction(new SingleUpdateInstruction(table1, getValue("A"), "bla"));

        Transaction t4 = new Transaction();
        t4.addInstruction(new SingleReadInstruction(table1, getValue("F")));
        t4.addInstruction(new SingleUpdateInstruction(table1, getValue("E"), "ss"));

        Transaction t5 = new Transaction();
        t5.addInstruction(new SingleUpdateInstruction(table1, getValue("C"), "dsds"));
        t5.addInstruction(new SingleReadInstruction(table1, getValue("B")));
        t5.addInstruction(new SingleUpdateInstruction(table1, getValue("A"), "bla"));

        SimulatedIterations simulation = new SimulatedIterations();
        simulation.addTransaction(t1);
        simulation.addTransaction(t2);
        simulation.addTransaction(t3);
        simulation.addTransaction(t4);
        simulation.addTransaction(t5);
        simulation.run(100, false, manager);

    }



    ConcurrencyManager manager;
    public ConcurrencyManager getManager(boolean recreateManager) throws Exception{
        if (recreateManager)
            return new RafaelCarneiroPregardierConcurrencyManager();
        if (manager==null){
            manager = new RafaelCarneiroPregardierConcurrencyManager();
        }
        return manager;

    }

    public static void main(String[] args) {
        Main2 m = new Main2();
        try {
            boolean recreateManager = false;

            System.out.println("TESTE 1");
            m.test1(m.getManager(recreateManager));

            System.out.println("TESTE 2");
            m.test2(m.getManager(recreateManager));

            System.out.println("TESTE 3");
            m.test3(m.getManager(recreateManager));

            System.out.println("TESTE 4");
            m.test4(m.getManager(recreateManager));

            System.out.println("TESTE 5");
            m.test5(m.getManager(recreateManager));


        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}