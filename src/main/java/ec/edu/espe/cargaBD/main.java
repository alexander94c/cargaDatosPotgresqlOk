/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ec.edu.espe.cargaBD;

/**
 *
 * @author AC
 */
public class main {

    public static final leeEscribeTxt letC = new leeEscribeTxt();
    public static final leeEscribeTxt letC2 = new leeEscribeTxt();
    public static final leeEscribeTxt letC3 = new leeEscribeTxt();
    public static final leeEscribeTxt letC4 = new leeEscribeTxt();
    public static final leeEscribeTxt letC5 = new leeEscribeTxt();
    public static final leeEscribeTxt letC6 = new leeEscribeTxt();
    public static final leeEscribeTxt letC7 = new leeEscribeTxt();
    public static final leeEscribeTxt letC8 = new leeEscribeTxt();
    public static final leeEscribeTxt letC9 = new leeEscribeTxt();
    public static final leeEscribeTxt letC10 = new leeEscribeTxt();
    
    

    public static void main(String[] args) throws Exception {

        leeEscribeTxt let=new leeEscribeTxt();
        mysqlMongo mymongo = new mysqlMongo();
        MongoARedis mr = new MongoARedis();

        long start = System.currentTimeMillis();

        /*
        letC.setLine(0);
        letC.setIns(100000);
        Thread let1 = new Thread(letC);
        let1.start();

        letC2.setLine(100000);
        letC2.setIns(100000);
        Thread let2 = new Thread(letC2);
        let2.start();

        letC3.setLine(200000);
        letC3.setIns(100000);
        Thread let3 = new Thread(letC3);
        let3.start();

        letC4.setLine(300000);
        letC4.setIns(100000);
        Thread let4 = new Thread(letC4);
        let4.start();
        
        letC5.setLine(400000);
        letC5.setIns(100000);
        Thread let5 = new Thread(letC5);
        let5.start();
        
        letC6.setLine(500000);
        letC6.setIns(100000);
        Thread let6 = new Thread(letC6);
        let6.start();
        
        letC7.setLine(600000);
        letC7.setIns(100000);
        Thread let7 = new Thread(letC7);
        let7.start();
        
        letC8.setLine(700000);
        letC8.setIns(100000);
        Thread let8 = new Thread(letC8);
        let8.start();
        
        letC9.setLine(800000);
        letC9.setIns(100000);
        Thread let9 = new Thread(letC9);
        let9.start();
        
        letC10.setLine(900000);
        letC10.setIns(100000);
        Thread let10 = new Thread(letC10);
        let10.start();
        */

//let.leerDatos();
        //letC.desconectar();
        let.conectar();
        mymongo.conectarMysqlMongo();
        mr.EscribirDatos();
        long end = System.currentTimeMillis();
        System.out.println("Final Time:" + (end - start));
    }

}
