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

/**
 *
 * @author AC
 */
public class leeEscribeTxt extends Thread {

    private String rutaArchivo;
    private String linea;

    String[] parts;
    String part1;
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

    public void conectar() throws Exception {

        servidor = "jdbc:mysql://localhost:3306/pruebamongo";
        usuarioDB = "root";
        passwordDB = "";

        //this.conn = DriverManager.getConnection(servidor, usuarioDB, passwordDB);
        //System.out.println("CONEXION BD EXITOSA!!");
        //this.stmt = conn.createStatement();

        Class.forName("com.mysql.jdbc.Driver");
        conn = (Connection) DriverManager.getConnection(servidor, usuarioDB, passwordDB);

    }

    public void leerDatos() throws UnsupportedEncodingException, IOException, SQLException {

        long start = System.currentTimeMillis();

        System.out.println("Funcion Leer Datos");

        try {

            //this.rutaArchivo = "C://tmp//tres.txt";
            this.rutaArchivo = "C://tmp//registroCivil1000000.txt";

            FileReader fr = new FileReader(rutaArchivo);
            BufferedReader entradaArchivo = new BufferedReader(fr);
            linea = entradaArchivo.readLine();

            while (linea != null) {
                parts = linea.split(",");
                part1 = parts[0]; //cedula
                part2 = parts[1]; //apellidos
                part3 = parts[2]; //nombres
                part4 = parts[3]; //fechaNacimiento
                part5 = parts[4]; //provinciaNacimiento
                part6 = parts[5]; //genero
                part7 = parts[6]; //estadoCivil

                PreparedStatement st = conn.prepareStatement("INSERT INTO persona(cedula,apellidos,nombres,fechaNacimiento,codigoProvincia,genero,estadoCivil) VALUES (?,?,?,?,?,?,?)");

                st.setString(1, part1);
                st.setString(2, part2);
                st.setString(3, part3);
                st.setString(4, part4);
                st.setString(5, part5);
                st.setString(6, part6);
                st.setString(7, part7);

                st.executeUpdate();

                System.out.println(linea);
                System.out.println(part1);
                System.out.println(part2);
                System.out.println(part3);
                System.out.println(part4);
                System.out.println(part5);
                System.out.println(part6);
                System.out.println(part7);

                part1 = "";
                part2 = "";
                part4 = "";
                part5 = "";
                part6 = "";
                part7 = "";

                linea = entradaArchivo.readLine();
                if (linea == null) {
                    conn.close();
                }
            }
            System.out.println("Datos ingresados correctamente !!");
        } catch (IOException ex) {
            System.out.println("Error en la apertura del archivo " + ex.toString());
        }

        long end = System.currentTimeMillis();

        System.out.println("Final Time:" + (end - start));
    }

    public void desconectar() throws Exception {

        if (this.conn != null) {
            try {
                this.conn.close();
            } catch (SQLException SQLE) {
                System.out.println(SQLE);
            }
        }
    }

}
