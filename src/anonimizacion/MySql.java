/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package anonimizacion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

/**
 *
 * @author richard
 */

public class MySql {
    
    private static Connection Conexion;
    private String usuario;
    private String contrasenia;
    private String base;
    
    //conexión con la bbdd
    public void MySQLConnection(String user, String pass, String db_name) throws Exception {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Conexion = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + db_name, user, pass);
            JOptionPane.showMessageDialog(null, "It has started the connection to the server successfully");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void MySQLConnection() throws Exception {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Conexion = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + base, usuario, contrasenia);
            JOptionPane.showMessageDialog(null, "It has started the connection to the server successfully");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    
  
    //cerrar la conexión
     public void closeConnection() {
        try {
            Conexion.close();
            JOptionPane.showMessageDialog(null, "It has completed the connection to the server");
        } catch (SQLException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
   
     //crea una bbdd
     public void createDB(String name) throws Exception {
        try {
            String Query = "CREATE DATABASE " + name;
            Statement st = Conexion.createStatement();
            st.executeUpdate(Query);
            closeConnection();
            MySQLConnection("root", "", name);
            JOptionPane.showMessageDialog(null, "Se ha creado la base de datos " + name + " de forma exitosa");
        } catch (SQLException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
     
     //crea una tabla 
      public void createTable(String name) {
        try {
            String Query = "CREATE TABLE " + name + ""
                    + "(DNI VARCHAR(15),Edad int,CP VARCHAR(10), Diagnostico VARCHAR(1000))";

            Statement st = Conexion.createStatement();
            st.executeUpdate(Query);
            JOptionPane.showMessageDialog(null, "Se ha creado la tabla " + name + " de forma exitosa");
        } catch (SQLException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
      
      //inserta datos en la tabla
      public void insertData(String table_name,String DNI, int ID, int edad, String diag) {
        try {
            String Query = "INSERT INTO " + table_name + " VALUES("
                    + "\"" + DNI + "\""
                    + ID + ","
                    + edad + ","
                    + "\"" + diag + "\")";
            Statement st = Conexion.createStatement();
            st.executeUpdate(Query);
            JOptionPane.showMessageDialog(null, "Datos almacenados de forma exitosa");
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en el almacenamiento de datos");
        }
    }
    
       //select *
      public void getValues(String table_name) {
        try {
            String Query = "SELECT * FROM " + table_name;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                System.out.println("ID: " + resultSet.getInt("ID") + " "
                        + "Edad: " + resultSet.getInt("Edad") + " "
                        + "Diagnóstico: " + resultSet.getString("Diagnostico"));
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en la adquisición de datos");
        }
    }
      
      
      
    public void getK(String table_name,String campos) {
        try {
            
            
            String Query = "SELECT MIN(CUENTA) FROM  (select count(*) as cuenta from " + table_name+" GROUP BY "+ campos+" ) tabla";
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                
                int val =  ((Number) resultSet.getObject(1)).intValue();
                JOptionPane.showMessageDialog(null, "El valor de K es:" + val);
               
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en la adquisición de datos");
        }
    }
    
    
    
    //genera tablas de la forma ID,EDAD,CP,GENERO con gente aleatoria
     public void generateTable(String table,int num,int min, int max, int CP,String sex) {
        
         int a=28000;
         String[] cp = new String[CP];
         for (int i=0;i<CP;i++)
         {
             cp[i]=Integer.toString(a+i);
         }
          
         Random r = new Random();
         String[] sentencias = new String[num];
         String genre;
         if(sex.equals("man"))
             genre="man";
         else genre="woman";
         
         for(int i =0; i<num;i++)
         {
             int aux=max-min;
             int n=r.nextInt(aux);
             int age=min+n;
             String code=cp[n%cp.length];
             
             if(sex.equals("") && (n % 2==0))
                 genre="man";
             if(sex.equals("") && (n % 2==1))
                 genre="woman";
             sentencias[i]="INSERT INTO "+ table + " VALUES("
                    + i + ","
                    + age + ","
                    + "\"" + code + "\""+ ","
                    + "\"" + genre + "\")" ;
  
         }
         
         //crear tabla y ejecutar sentencias
         
         
          try {
            String Query = "DROP TABLE " + table ;

            Statement st = Conexion.createStatement();
            st.executeUpdate(Query);
           // JOptionPane.showMessageDialog(null, "Se borrado la tabla " + table + " de forma exitosa");
        } catch (SQLException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        }
         
  
        try {
            String Query = "CREATE TABLE " + table + ""
                    + "(ID VARCHAR(15),AGE int,CP VARCHAR(10), SEX VARCHAR(100))";

            Statement st = Conexion.createStatement();
            st.executeUpdate(Query);
           // JOptionPane.showMessageDialog(null, "Se ha creado la tabla " + table + " de forma exitosa");
        } catch (SQLException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        for(int i=0;i<sentencias.length;i++)
        {
            
            try {
            
            Statement st = Conexion.createStatement();
            st.executeUpdate(sentencias[i]);
           // JOptionPane.showMessageDialog(null, "Datos almacenados de forma exitosa");
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en el almacenamiento de datos");
        }
            
        }
        
        
    }
      
      public void generateTables(int numTables,String table,int num,int min, int max, int CP,String sex) {
        
         for(int i =0;i<numTables;i++)
         {
             String name=table+i;
             generateTable(name,num,min,max,CP,sex);
         }
        
        
    }
      
      
      
      
      //borra por DNI 
      public void deleteRecord(String table_name, String DNI) {
        try {
            String Query = "DELETE FROM " + table_name + " WHERE DNI = " + DNI ;
            Statement st = Conexion.createStatement();
            st.executeUpdate(Query);

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            JOptionPane.showMessageDialog(null, "Error borrando el registro especificado");
        }
    }
      
      public void setUser(String u){ 
          usuario=u;
      }
       public void setPass(String p){ 
          contrasenia=p;
      }
        public void setBBDD(String b){ 
          base=b;
      }
       
}
