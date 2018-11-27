/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ec.edu.espe.cargaBD;

import com.mongodb.MongoClient;
import ec.edu.espe.cargaMysql.modelo.Persona;
import ec.edu.espe.cargaMysql.modelo.Usuario;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

/**
 *
 * @author Alejandro Torres
 */
public class mysqlMongo {

    private static Connection conn;
    private static final String driver = "com.mysql.jdbc.Driver";
    private static final String user = "root";
    private static final String password = "";
    private static final String url = "jdbc:mysql://localhost:3306/pruebamongo";

    String sql = "Select * from persona";
    Statement st;
    Date fechaDate = null;
    Usuario usu = null;
    Persona per = new Persona();
    
    public void conectarMysqlMongo(){
        try {
            Class.forName(driver);
            conn = (Connection) DriverManager.getConnection(url, user, password);
            //System.out.println("Hola Taller Mongo!");
            //System.out.println("Conectandose a mongo...");
            Morphia morphia = new Morphia();
            morphia.mapPackage("ec.edu.espe.arquitectura.taller.mongo.modelo");
            Datastore ds = morphia.createDatastore(new MongoClient(), "mysqlMongo");
            ds.ensureIndexes();

            System.out.println("Conexion establecida a Mongo");
            
            if (conn != null) {
                System.out.println("Conexion establecida a MySQL");
                st = conn.createStatement();
                long start = System.currentTimeMillis();
                System.out.println("Tiempo inicial: "+start);

                ResultSet rs = st.executeQuery(sql);
                while (rs.next()) {
                    //System.out.println(rs.getString(1));
                    per.setCedula(rs.getString(1));
                    per.setApellidos(rs.getString(2));
                    per.setNombres(rs.getString(3));
                    per.setFechaNacimiento(rs.getString(4));
                    per.setCodigoProvincia(Integer.parseInt(rs.getString(5)));
                    per.setGenero(rs.getString(6));
                    per.setEstadoCivil(rs.getString(7));
                    
                    usu = guardarMongo(per.getCedula(), per.getApellidos(), per.getNombres(), per.getFechaNacimiento(), per.getCodigoProvincia(), per.getGenero(), per.getEstadoCivil());
                    ds.save(usu);
                }
                long end = System.currentTimeMillis();
                System.out.println("Tiempo final: " +end);
                System.out.println("El tiempo que se demoro es: " + (end - start) * 0.001);
            }
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("Error al conectar " + e);
        }
    }
    
    private static Usuario guardarMongo(String cedula, String apellidos, String nombres, String fechaNacimiento, int codigoProvincia, String genero, String estadoCivil) {
        Usuario usuario = new Usuario();
        usuario.setCedula(cedula);
        usuario.setApellidos(apellidos);
        usuario.setNombres(nombres);
        usuario.setFechaNacimiento(fechaNacimiento);
        usuario.setCodProvincia(codigoProvincia);
        usuario.setGenero(genero);
        usuario.seteCivil(estadoCivil);        
        //System.out.println("Usuario Creado");
        return usuario;
    }
}
