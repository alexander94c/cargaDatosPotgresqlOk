/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ec.edu.espe.cargaMysql.modelo;

import com.oracle.webservices.internal.api.message.PropertySet.Property;
import java.util.Date;
import java.util.Objects;
import org.mongodb.morphia.annotations.Entity;

/**
 *
 * @author Alejandro Torres
 */

@Entity(noClassnameStored = true, value = "Usuario")

public class Usuario {
    @Property("cedula")
    private String cedula;
    
    @Property("apellidos")    
    private String apellidos;
    
    @Property("nombres")    
    private String nombres;
    
    @Property("fechaNacimiento")    
    private String fechaNacimiento;
    
    @Property("codigoProvincia")    
    private int codProvincia;
    
    @Property("genero")    
    private String genero;
    
    @Property("estadoCivil")    
    private String eCivil;
    
    public String getCedula() {
        return cedula;
    }

    public void setCedula(String cedula) {
        this.cedula = cedula;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + Objects.hashCode(this.cedula);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Usuario other = (Usuario) obj;
        if (!Objects.equals(this.cedula, other.cedula)) {
            return false;
        }
        return true;
    }

    public String getApellidos() {
        return apellidos;
    }

    public void setApellidos(String apellidos) {
        this.apellidos = apellidos;
    }

    public String getNombres() {
        return nombres;
    }

    public void setNombres(String nombres) {
        this.nombres = nombres;
    }

    
    
    public String getFechaNacimiento() {
        return fechaNacimiento;
    }

    public void setFechaNacimiento(String fechaNacimiento) {
        this.fechaNacimiento = fechaNacimiento;
    }

    public int getCodProvincia() {
        return codProvincia;
    }

    public void setCodProvincia(int codProvincia) {
        this.codProvincia = codProvincia;
    }

    public String getGenero() {
        return genero;
    }

    public void setGenero(String genero) {
        this.genero = genero;
    }

    public String geteCivil() {
        return eCivil;
    }

    public void seteCivil(String eCivil) {
        this.eCivil = eCivil;
    }

    @Override
    public String toString() {
        return "Usuario{" + "cedula=" + cedula + ", apellidos=" + apellidos + ", nombres=" + nombres + ", fechaNacimiento=" + fechaNacimiento + ", codProvincia=" + codProvincia + ", genero=" + genero + ", eCivil=" + eCivil + '}';
    }
}
