/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop.join;

import ibd.table.Params;
import ibd.query.Operation;
import ibd.query.TreePrinter;
import ibd.query.TuplesPrinter;
import ibd.query.sourceop.FullTableScan;
import ibd.table.DataFaker;
import ibd.table.Directory;
import ibd.table.Table;
import ibd.table.prototype.BasicDataRow;
import java.util.logging.Level;
import java.util.logging.Logger;
import ibd.table.prototype.Prototype;
import ibd.table.prototype.column.IntegerColumn;
import ibd.table.prototype.column.StringColumn;

/**
 *
 * @author Sergio
 */
public class Main {

    Table funcTable = null;
    Table phoneTable = null;
    Table addressTable = null;
    Table socialInfoTable = null;

    private Table createFuncTable(String folder, String name, int pageSize, boolean override) throws Exception {
        Prototype prototype = new Prototype();
        prototype.addColumn(new IntegerColumn("id", true));
        prototype.addColumn(new StringColumn("name", (short)20));
        return Directory.getTable(folder, name, prototype, 0, pageSize, override);
    }

    private Table createPhoneTable(String folder, String name, int pageSize, boolean override) throws Exception {
        Prototype prototype = new Prototype();
        prototype.addColumn(new IntegerColumn("id", true));
        prototype.addColumn(new StringColumn("phone", (short)20));
        return Directory.getTable(folder, name, prototype, 0, pageSize, override);
    }

    private Table createAddressTable(String folder, String name, int pageSize, boolean override) throws Exception {
        Prototype prototype = new Prototype();
        prototype.addColumn(new IntegerColumn("id", true));
        prototype.addColumn(new StringColumn("address", (short)20));
        return Directory.getTable(folder, name, prototype, 0, pageSize, override);
    }

    private Table createSocialInfoTable(String folder, String name, int pageSize, boolean override) throws Exception {
        Prototype prototype = new Prototype();
        prototype.addColumn(new IntegerColumn("id", true));
        prototype.addColumn(new StringColumn("socialInfo", (short)20));
        return Directory.getTable(folder, name, prototype, 0, pageSize, override);
    }

    private void addFunc(Table funcTable, int id, String name) throws Exception {
        BasicDataRow row = new BasicDataRow();
        row.setInt("id", id);
        row.setString("name", name);
        funcTable.addRecord(row);
    }

    private void addPhone(Table phoneTable, int id, String phone) throws Exception {
        BasicDataRow row = new BasicDataRow();
        row.setInt("id", id);
        row.setString("phone", phone);
        phoneTable.addRecord(row);
    }

    private void addAddress(Table addressTable, int id, String address) throws Exception {
        BasicDataRow row = new BasicDataRow();
        row.setInt("id", id);
        row.setString("address", address);
        addressTable.addRecord(row);
    }

    private void addSocialInfo(Table socialInfoTable, int id, String socialInfo) throws Exception {
        BasicDataRow row = new BasicDataRow();
        row.setInt("id", id);
        row.setString("socialInfo", socialInfo);
        socialInfoTable.addRecord(row);
    }

    public void prepareTables(String folder, int pageSize, int size, boolean override) throws Exception {

        funcTable = createFuncTable(folder, "func", pageSize, override);
        phoneTable = createPhoneTable(folder, "phone", pageSize, override);
        addressTable = createAddressTable(folder, "address", pageSize, override);
        socialInfoTable = createSocialInfoTable(folder, "socialInfo", pageSize, override);

        if (override) {
            String names[] = DataFaker.generateNames(size, size);

            for (int i = 0; i < size; i++) {
                //String text = name + "(" + array1[i] + ")";
                String text = names[i];
                //text = Utils.pad(text, 40);
                addFunc(funcTable, i, text);
                addPhone(phoneTable, i, text + "'s phone");
                addAddress(addressTable, i, text + "'s address");
                addSocialInfo(socialInfoTable, i, "@" + text);

                //table.addRecord(array1[i], String.valueOf(array1[i]));
                //table.addRecord(array1[i], "0");
            }
            funcTable.flushDB();
            addressTable.flushDB();
            phoneTable.flushDB();
            socialInfoTable.flushDB();
        }
    }

    public Operation createQuery() throws Exception {

        Operation funcScan = new FullTableScan("t1", funcTable);
        //PKFilter scan1 = new PKFilter(scan, "t1", ComparisonTypes.LOWER_THAN, 2L);
        Operation addressScan = new FullTableScan("t2", addressTable);
        Operation phoneScan = new FullTableScan("t3", phoneTable);
        Operation socialInfoScan = new FullTableScan("t4", socialInfoTable);

        JoinPredicate terms = new JoinPredicate();
        terms.addTerm("id", "id");


        Operation join1 = new NestedLoopJoin(funcScan, phoneScan, terms);
        Operation join2 = new NestedLoopJoin(join1, addressScan, terms);
        Operation join = new NestedLoopJoin(join2, socialInfoScan, terms);

        return join;

    }

    public static void main(String[] args) {
        try {
            Main m = new Main();

            boolean override = false;
            m.prepareTables("C:\\Users\\naoca\\Documents\\GitHub\\Implementacao-de-Banco-de-Dados\\Trabalho 2\\ibd", 4096, 200, override);
            Operation query = m.createQuery();
            
            System.out.println("Tree:");
            TreePrinter treePrinter= new TreePrinter();
            treePrinter.printTree(query);
            System.out.println("");
            
            Params.BLOCKS_LOADED = 0;
            
            TuplesPrinter printer = new TuplesPrinter();
            printer.execQueryAndPrint1(query, -1);

            System.out.println("blocks loaded " + Params.BLOCKS_LOADED);

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
