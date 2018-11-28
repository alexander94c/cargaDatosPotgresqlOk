/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ec.edu.espe.cargaBD;

import com.mongodb.MongoClient;
import ec.edu.espe.cargaMysql.modelo.Persona;
import ec.edu.espe.cargaMysql.modelo.PersonaM;
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
    private static final String driver = "org.postgresql.Driver";
    private static final String user = "postgres";
    private static final String password = "sql123";
    private static final String url = "jdbc:postgresql://localhost:5432/taller1";

    String sql = "Select * from persona";
    Statement st;
    Date fechaDate = null;
    PersonaM usu = null;
    Persona per = new Persona();

    public void conectarMysqlMongo() {
        try {
            Class.forName(driver);
            conn = (Connection) DriverManager.getConnection(url, user, password);
            //System.out.println("Hola Taller Mongo!");
            //System.out.println("Conectandose a mongo...");
            Morphia morphia = new Morphia();
            morphia.mapPackage("ec.edu.espe.arquitectura.taller.mongo.modelo");
            Datastore ds = morphia.createDatastore(new MongoClient(), "mysqlMongo");
            ds.ensureIndexes();

            if (conn != null) {
                st = conn.createStatement();

                ResultSet rs = st.executeQuery(sql);
                while (rs.next()) {
                    per.setCedula(rs.getString(1));
                    per.setApellidos(rs.getString(2));
                    per.setNombres(rs.getString(3));
                    per.setFechaNacimiento(rs.getString(4));
                    per.setCodigoProvincia(rs.getString(5));
                    per.setGenero(rs.getString(6));
                    per.setEstadoCivil(rs.getString(7));

                    PersonaM usuario = new PersonaM();
                    usuario.setCedula(per.getCedula());
                    usuario.setApellidos(per.getApellidos());
                    usuario.setNombres(per.getNombres());
                    usuario.setFechaNacimiento(per.getFechaNacimiento());
                    usuario.setCodProvincia(per.getCodigoProvincia());
                    usuario.setGenero(per.getGenero());
                    usuario.seteCivil(per.getEstadoCivil());
                    ds.save(usuario);
                }

            }
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("Error al conectar " + e.getMessage());
        }
    }
}
