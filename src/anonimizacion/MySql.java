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
      
      
      
    public int getK(String table_name,String campos,boolean mensaje) {
        try {
            
            
            String Query = "SELECT MIN(CUENTA) FROM  (select count(*) as cuenta from " + table_name+" GROUP BY "+ campos+" ) tabla";
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                
                int val =  ((Number) resultSet.getObject(1)).intValue();
                if(mensaje)
                     JOptionPane.showMessageDialog(null, "El valor de K es:" + val);
                return val;
               
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en la adquisición de datos");
        }
        return 0;
    }
    
    
    
    public void getKs(int num,String table_name,String campos) {
        
        int max=0;
        int min=0;
        int media=0;
        int[] ks= new int[num];//se guardan los K
        
        for(int i=0;i<num;i++)
        {
            String nombre=table_name+i;//nombre de las tablas
            int k = getK(nombre,campos,false);
            media=media+k;
            ks[i]=k;
            
        }
        media=media/num;
        //hallar max
        max=ks[0];
        min=ks[0];
        int maxTable=0;
        int minTable=0;
         for(int i=1;i<num;i++)
        {
            if(ks[i]>=max)
            {
                max=ks[i];
                maxTable=i;
            }
             if(ks[i]<=min)
            {
                min=ks[i];
                minTable=i;
            }
            
        }
         //Propiedad del sistema para salto de línea:
         String n = System.getProperty("line.separator");

         JOptionPane.showMessageDialog(null, 
                 "The maximun value of K is: " + max+" in the table "+table_name+maxTable+n+
                 "The minimun value of K is: " + min+" in the table "+table_name+minTable+n+
                 "The average of K is: " + media,"Information",JOptionPane.INFORMATION_MESSAGE);
        
    }
    
    
     //genera tablas de la forma ID,EDAD,CP,GENERO poblacion Española
     public void generateTableSpain(String table,int num, int CP,String sex) {
        
         
         //Informacion demografica
         double total=46439864;
         double p0 = 2256137/total;//0-4
         double p1 = 2484228/total;//5-9
         double p2 = 2307748/total;//10/14
         double p3 = 2152888/total;//15-19
         double p4 = 2318277/total;//20/24
         double p5 = 2637741/total;//25-29
         double p6 = 3267325/total;//30-34
         double p7 = 3948602/total;//35-39
         double p8 = 3888532/total;//40-44
         double p9 = 3690385/total;//45-49
         double p10 = 3409097/total;//50-54
         double p11 = 2978760/total;//55-59
         double p12 = 2508107/total;//60-64
         double p13 = 2357956/total;//65-69
         double p14 = 1949490/total;//70-74
         double p15 = 1553295/total;//75-79
         double p16 = 1425513/total;//80-84
         double p17 = 854988/total;//85-89
         double p18 = 356755/total;//90-94
         double p19 = 94038/total;//95+
         
         int[] t=new int[20];
         t[0]=(int) (p0*num);
         t[1]=(int) (p1*num);
         t[2]=(int) (p2*num);
         t[3]=(int) (p3*num);
         t[4]=(int) (p4*num);
         t[5]=(int) (p5*num);
         t[6]=(int) (p6*num);
         t[7]=(int) (p7*num);
         t[8]=(int) (p8*num);
         t[9]=(int) (p9*num);
         t[10]=(int) (p10*num);
         t[11]=(int) (p11*num);
         t[12]=(int) (p12*num);
         t[13]=(int) (p13*num);
         t[14]=(int) (p14*num);
         t[15]=(int) (p15*num);
         t[16]=(int) (p16*num);
         t[17]=(int) (p17*num);
         t[18]=(int) (p18*num);
         t[19]=(int) (p19*num);
         
         int resto=num -(t[0]+t[1]+t[2]+t[3]+t[4]+t[5]+t[6]+t[7]+t[8]+t[9]+t[10]+t[11]+t[12]+t[13]+t[14]+t[15]+t[16]+t[17]+t[18]+t[19]);
         //lo añadimos por ejermplo a un grupo.
         t[19]=t[19]+resto;
         
         //hombres mujeres
         double hombres=0.4914048628566182;//49%
         double mujeres =0.5085951371433818;//51%
         
         
         int a=28000;
         String[] cp = new String[CP];
         for (int i=0;i<CP;i++)
         {
             cp[i]=Integer.toString(a+i);
         }
         
         
         Random r = new Random();
         String genre;
         if(sex.equals("man"))
             genre="man";
         else genre="woman";
         
        String[] sentencias = new String[num];
        
        int[] edades=new int[num] ;
        int k=0;
        int min=0;
         for(int i=0;i<20;i++){
             for(int j=0;j<t[i];j++)
             {
                 int n=r.nextInt(5);
                 edades[k]=min+n;
                 k++;
                 
                 
             }
             min=min+5;
         }
         
         
         
          for(int i =0; i<num;i++)
         {
            int aux=r.nextInt(100);
            String code=cp[aux%cp.length];
             
             if(sex.equals("") && (aux >=0)&& (aux<=48))//49%
                 genre="man";
             if(sex.equals("") && (aux >=49)&& (aux<=99))//51%
                 genre="woman";
             sentencias[i]="INSERT INTO "+ table + " VALUES("
                    + i + ","
                    + edades[i] + ","
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
          //  Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
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
     public void generateTablesSpain(int numTables,String table,int num,int CP,String sex) {
        
         for(int i =0;i<numTables;i++)
         {
             String name=table+i;
             generateTableSpain(name,num,CP,sex);
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
          //  Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
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
