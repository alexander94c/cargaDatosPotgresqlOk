/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ec.edu.espe.cargaBD;

import com.mongodb.MongoClient;
import ec.edu.espe.cargaMysql.modelo.PersonaM;
import java.util.List;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import redis.clients.jedis.Jedis;

/**
 *
 * @author User
 */
public class MongoARedis {

    public void EscribirDatos() {
        Jedis jedis = new Jedis("localhost", 6379);
        Morphia morphia = new Morphia();
        morphia.mapPackage("ec.edu.espe.arquitectura.taller.mongo.modelo");
        Datastore ds = morphia.createDatastore(new MongoClient(), "mysqlMongo");
        


        /*
        Query<Usuario> qry=ds.createQuery(Usuario.class)
                .field("codigoUsuario").equal("fmacurio").asList();
        List<Usuario> usuarios=new ArrayList<Usuario>();
         */
        //List<Usuario> usuarios=ds.createQuery(Usuario.class).field("codigoUsuario").equal("fmacurio").asList();
        List<PersonaM> usuarios = ds.createQuery(PersonaM.class).asList();
        //System.out.println(usuarios.get(0).getClave());
        for (PersonaM user : usuarios) {

            jedis.hset("persona", user.getCedula(),
                    "" + user.getApellidos()
                    + "," + user.getNombres()
                    + "," + user.getFechaNacimiento()
                    + "," + user.getCodProvincia()
                    + "," + user.getGenero()
                    + "," + user.geteCivil()
            );
        }

        /*

        jedis.hset("usuarios", "1725", "t1");
        jedis.hset("usuarios", "1726", "t2");
        jedis.hset("usuarios", "1725", "t3");
        jedis.hset("usuarios", "1727", "t4");
        System.out.println(jedis.hgetAll("usuarios"));*/
    }

}
