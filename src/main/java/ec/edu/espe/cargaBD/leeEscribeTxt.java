/*
 */
package ec.edu.espe.cargaBD;

import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author AC
 */
public class leeEscribeTxt extends Thread {            

    private String rutaArchivo;
    private String linea;

    String[] parts;
//String part1 ;
    String part2;
    String part3;
    String part4;
    String part5;
    String part6;
    String part7;
    String servidor;
    String usuarioDB;
    String passwordDB;

    protected Connection conn = null;
    protected Statement stmt = null;
    //private Object DriveManager;
    protected PreparedStatement pst1 = null;

    public void conectar() throws Exception {

        servidor = "jdbc:postgresql://localhost:5432/taller1";
        usuarioDB = "postgres";
        passwordDB = "sql123";

        try {
            Class.forName("org.postgresql.Driver");
            Connection con = DriverManager.getConnection(servidor, usuarioDB, passwordDB);
            Statement stat = con.createStatement();
            //Lectura del archivo
            this.rutaArchivo = "C://tmp//data.txt";
            FileReader fr = new FileReader(rutaArchivo);
            BufferedReader entradaArchivo = new BufferedReader(fr);

            linea = entradaArchivo.readLine();
            System.out.println(linea);

            while (linea != null) {
                parts = new String[7];
                parts = linea.split(",");
                /*part1 = ; //cedula
                part2 = parts[1]; //apellidos
                part3 = parts[2]; //nombres
                part4 = parts[3]; //fechaNacimiento
                part5 = parts[4]; //provinciaNacimiento
                part6 = parts[5]; //genero
                part7 = parts[6]; //estadoCivil*/

 /*
                System.out.println(linea);
                System.out.println(part1);
                System.out.println(part2);
                System.out.println(part3);
                System.out.println(part4);
                System.out.println(part5);
                System.out.println(part6);
                System.out.println(part7);
                
                System.out.println("Inicio sentencia SQL");*/
                String sqlQuery = "INSERT INTO persona(\"cedula\",\"apellidos\",\"nombres\",\"fechanacimiento\",\"codigoprovincia\",\"genero\",\"estadocivil\") VALUES (?,?,?,?,?,?,?)";

                pst1 = con.prepareStatement(sqlQuery);

                pst1.setString(1, parts[0]);
                pst1.setString(2, parts[1]);
                pst1.setString(3, parts[2]);
                pst1.setString(4, parts[3]);
                pst1.setString(5, parts[4]);
                pst1.setString(6, parts[5]);
                pst1.setString(7, parts[6]);
                pst1.executeUpdate();
                linea = entradaArchivo.readLine();

            }
        } catch (SQLException SQLExeption) {
            System.out.println(SQLExeption.getMessage());
        }
    }

    public void desconectar() throws Exception {

        if (this.conn != null) {
            try {
                this.conn.close();
            } catch (SQLException SQLE) {
                System.out.println(SQLE.getMessage());
            }
        }
    }

}
