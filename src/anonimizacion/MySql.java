/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package anonimizacion;


import org.chocosolver.solver.ResolutionPolicy;
import org.chocosolver.solver.Solver;
import org.chocosolver.solver.constraints.IntConstraintFactory;
import org.chocosolver.solver.constraints.LogicalConstraintFactory;
import org.chocosolver.solver.trace.Chatterbox;
import org.chocosolver.solver.variables.IntVar;
import org.chocosolver.solver.variables.Variable;
import org.chocosolver.solver.variables.VariableFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

/**
 *
 * @author richard
 */

final class MyResult {
    private final int[][] first;
    private final int[] second;

    public MyResult(int[][] first, int[] second) {
        this.first = first;
        this.second = second;
    }

    public int[][] getFirst() {
        return first;
    }

    public int[] getSecond() {
        return second;
    }
}






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
            JOptionPane.showMessageDialog(null, "Connection completed!!");
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
                     JOptionPane.showMessageDialog(null, "The K value is: " + val);
                return val;
               
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data!");
        }
        return 0;
    }
    
 
    public void getKs(int num,String table_name,String campos) {
        
        int max=0;
        int min=0;
        float media=0;
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
    
    
    
    MyResult getKMin(int Q, int[] qf, int R, int[] rc, int[] v)
    {

        int[][] total=null;
        
        int max1=0;
        for(int i=1; i<Q+1;i++)
            if(qf[i]>max1)
                max1=qf[i];//frec maxima
        

        int max2=0;
        for(int i=1; i<R+1;i++)
            if(rc[i]>max2)
                max2=rc[i];//recuso maximo
        
        int max=0;
        if (max1<max2)
                max=max1;
        else max=max2; //maximo de los maximos
        //COTA MINIMA DE LOS MAXIMOS
       
        int p=0;
        for(int i=0; i<Q+1;i++)
           p=p+qf[i]; //poblacion total
     
        v= new int[max+1];//vector de anonimicidad TAMAÑO MIN DE LOS MAX
        for(int i=0; i<max+1;i++)
           v[i]=0;
        int suma=0;
        for(int i=0; i<max+1;i++)
           suma=suma+v[i]*i;
           
        IntVar[] vchoco=null;   
        IntVar k;
        int l=1; //nivel
        IntVar[] a;
        IntVar[] cuenta;
        int kvalor=0;
        while (suma != p){
            
            Solver solver = new Solver("Minimaze K");
            a = new IntVar[Q * R];//matriz plana
            for (int i = 0; i < Q; i++) {
                for (int j = 0; j < R; j++) {
                    a[i * R + j] = VariableFactory.enumerated("a" + i + "_" + j, 0, max, solver);
                }
             }
            
            //restricciones
            IntVar[] fila = null;
            IntVar[] columna = null;
            //C1
             for (int i = 0; i < Q; i++) {
                 fila = new IntVar[R];
                for (int j = 0; j < R; j++) {
                    
                    fila[j]=a[i*R+j];
                }
                
                IntVar sum=VariableFactory.enumerated(qf[i+1]+"", qf[i+1], qf[i+1], solver);//TRUCO?
                solver.post(IntConstraintFactory.sum(fila,sum));
             }
             //C2
             for (int i = 0; i < R; i++) {
                 columna = new IntVar[Q];
                for (int j = 0; j < Q; j++) {
                    
                    columna[j]=a[i+j*R];
                }
                IntVar sum1=VariableFactory.enumerated(rc[i+1]+"", rc[i+1], rc[i+1], solver);
                solver.post(IntConstraintFactory.sum(columna,"<=",sum1));
             }
            
             //C3
             if(l!=1){
                vchoco= VariableFactory.enumeratedArray("vchoco" ,max+1, 0, max, solver);
                for (int i=1;i<=l;i++)
                 {
                     solver.post(IntConstraintFactory.arithm(vchoco[i], "=", v[i])); // se supone q los ceros no se consideran
                 }
                
            }
  
             for(int i =1;i<l;i++)
                solver.post(IntConstraintFactory.count(i,a,vchoco[i]));

             
             //C4
             k=VariableFactory.enumerated("k", 0, max, solver);
             solver.post(IntConstraintFactory.count(l,a,k));
            //  solver.post(IntConstraintFactory.arithm(vchoco[l], "=", k));
             //minimizar k
             solver.findOptimalSolution(ResolutionPolicy.MINIMIZE, k);
             
               if(solver.findSolution()){
            do{
            Chatterbox.printStatistics(solver);
            Chatterbox.printSolutions(solver);
            kvalor =k.getValue();
           
            total=new int[Q][R];
               
               for (int i = 0; i < Q; i++) {
                for (int j = 0; j < R; j++) {
                   total[i][j] = a[i * R + j].getValue();
                     }
                 }
            
            
            //OBTENER K Y AÑADIRLO A V.
            }while(solver.nextSolution());
            }
             
         
             v[l]=kvalor;
             l++;
             suma=0;
             for(int i=0; i<max+1;i++)
                   suma=suma+v[i]*i;
            // break;
        }
        
        return new  MyResult(total,v);
    
    }
    
     public void assignAppointmentsComplete(int numTables,String nameTable,String campos){
           for(int i =0;i<numTables;i++)
         {
             String name=nameTable+i+"_resource";
             String name1=nameTable+i;
             assignAppointmentComplete(name1,name,campos);
         }
     }
    
    
     public void assignAppointmentComplete(String nameTable,String nameResource,String campos){
//PARTE INTELIGENTE         
         //Q=Qnumero de cuasi id diferentes.
         int Q =0;
          try {
            //SELECT cp,SEX,COUNT(*) FROM `t0` GROUP BY cp,SEX si se quiere saber el cuasi
            String Query = "SELECT count(CUENTA) FROM  (select count(*) as cuenta from " + nameTable+" GROUP BY "+ campos+" ) tabla";
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);
            while (resultSet.next()) {         
                int val =  ((Number) resultSet.getObject(1)).intValue();
                Q=val;           
            }
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data!");
        }
         
          //qf=Array [1,Q] frecuencia de cada cuasi ,el k sería el menor de todos estos
          
          int[] qf=new int[Q+1];
          int i=1;
          int j=0;
          try {

            String Query = "SELECT COUNT(*) FROM " + nameTable+" GROUP BY "+ campos  ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                j =  ((Number) resultSet.getObject(1)).intValue();    
                qf[i]=j;
                i++;
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data.");
        }
          
          //R=nº recursos (filas de  recursos).
         int R=0;
         try {

            String Query = "SELECT COUNT(*) FROM " + nameResource ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                R =  ((Number) resultSet.getObject(1)).intValue();     
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data.");
        }
          //RC CAPACIDAD DE CADA RECURSO.[1,R]
          int[] rc= new int[R+1];
          i=1;
          j=0;
          try {
            String Query = "SELECT * FROM " + nameResource;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);
            

            while (resultSet.next()) {
               j= resultSet.getInt("CAPACITY");
               rc[i]=j;
              i++;
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data.");
        }   
      //creamos matriz de ejemplo o la que seria solucion e implementamos la actualizacion
          //en su tabla correspondiente
          
          int [][] matriz = new int[Q][R];
          int [][] matriz1 = new int[Q][R];
          int[] v=null;
          MyResult r1=getKMin(Q,qf,R,rc,v);
          
          matriz=r1.getFirst();
          v=r1.getSecond();
          
          for(int xx=0;xx<Q;xx++)
              for(int yy=0;yy<R;yy++)
                  matriz1[xx][yy]=matriz[xx][yy];
              
          
          
          
      
        
          //leer la tabla de las frecuencias de cada cuasi con el cuasi valor incluido.
          campos = campos.replace("\n","");
          String[] atributos=campos.split(",");
          int numAtributos=atributos.length;
          String[][] cuasis= new String[Q+1][numAtributos+1];
          String consulta=campos+",";
          
          
          
          int k=1;
          try {

            String Query = "SELECT "+consulta+"COUNT(*) FROM " + nameTable+" GROUP BY "+ campos  ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                int z=0;
                for(int x=0;x<numAtributos;x++)
                {
                    cuasis[k][z]=resultSet.getString(atributos[x]);
                    z++;
                }
                cuasis[k][z]=resultSet.getString("COUNT(*)");
                k++;
                
            }
            
            /*
                Cp  sex count
            0                   
            1   2800 M  3
            2   2801 W  4
            3   2802 M  1
            
            */
            
            
            
            

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data.");
        }
          
          int total=0;
          for (int x=0; x < matriz.length; x++) {
             for (int y=0; y < matriz[x].length; y++) {
                total=total+ matriz[x][y];    
                }
            }  
          //habra que adaptar segun los indices, en este caso la matriz empieza en 0,0 y cusais en 1
          String[] sentens=new String[total];
  
          String campo="";
         
          for(int u=0;u<total;u++)
          {
              
              
              for (int x=0; x < matriz.length; x++) {
                 for (int y=0; y < matriz[x].length; y++) {
                    while(matriz[x][y]!=0)           
                    {
                        int recurso =y+1;
                        
                        sentens[u]="UPDATE "+ nameTable + " SET REC_INTELLIGENT=" + recurso + " WHERE ";
                        for (int a=0;a<numAtributos;a++)
                        {
                            sentens[u]=sentens[u]+ atributos[a]+"= "+"\""+cuasis[x+1][a]+"\""+" AND ";
                        }  
                        sentens[u] = sentens[u].substring(0, sentens[u].length() - 5);
                        sentens[u]=sentens[u]+" AND REC_INTELLIGENT= -1 LIMIT 1";
                        u++;
                        matriz[x][y]--;
                        
                    }
                }
            }  
  
          }
          
           for(int l=0;l<sentens.length;l++)
        {
            
            try {
            
            Statement st = Conexion.createStatement();
            st.executeUpdate(sentens[l]);
           // JOptionPane.showMessageDialog(null, "Datos almacenados de forma exitosa");
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en el almacenamiento de datos");
        }
          
        }
//FIN PARTE INTELIGENTE
           
           
//PARTE ALEATORIA
           
           
            //leer tabla de recursos y guardarlo en memoria
         int[][]citas;
         int x=0;
         
         try {

            String Query = "SELECT COUNT(*) FROM " + nameResource ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                x =  ((Number) resultSet.getObject(1)).intValue();     
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en la adquisición de datos");
        }
         citas =new int[x][2];
         int m=0;
         int p;
         int n=0;
         
         try {
            String Query = "SELECT * FROM " + nameResource;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);
            

            while (resultSet.next()) {
               m= resultSet.getInt("RESOURCE");
               p=m-1;
               n= resultSet.getInt("CAPACITY");
               citas[p][0]=m;
               citas[p][1]=n;
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en la adquisición de datos");
        }
         
         //algoritmo
         int numPersons=0;
          try {

            String Query = "SELECT COUNT(*) FROM " + nameTable ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                numPersons =  ((Number) resultSet.getObject(1)).intValue();     
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en la adquisición de datos");
        }
         
          String[] sentencias = new String[numPersons];
         Random r = new Random();
         
         
         for(int l =0; l<numPersons;l++)
         {
             int aux=r.nextInt(m);// m+1
             
             while( citas[aux][1]==0)
              aux=(aux +1 ) % (m);
             
             citas[aux][1]--;
             
             
             sentencias[l]="UPDATE "+ nameTable + " SET REC_RAND="
                    + citas[aux][0] + " WHERE ID=" + l;
                    
  
         }
        for(int l=0;l<sentencias.length;l++)
        {
            
            try {
            
            Statement st = Conexion.createStatement();
            st.executeUpdate(sentencias[l]);
           // JOptionPane.showMessageDialog(null, "Datos almacenados de forma exitosa");
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en el almacenamiento de datos");
        }
            
        }
        
     //hallar a y v de aleatorio
        //Q Y R lo tenemos ya
        String[] sentensSelect=new String[total];
        
        int contador=0;
        String campos1="";
        String campos2="";
        int[][] aRandom= new int[Q][R];
        
        for(int xx=0;xx<Q;xx++){
            
           campos1="";
            for (int a=0;a<numAtributos;a++)
                 campos1=campos1+atributos[a]+"= "+"\""+cuasis[xx+1][a]+"\""+" AND ";
            campos1=campos1+" REC_RAND = ";
            campos2=campos1;
            for(int yy=0;yy<R;yy++){
                int aux=yy+1;
                campos1=campos2+aux;
                String querySelect= " SELECT COUNT(*) FROM " + nameTable + " WHERE "+ campos1 + " ";
             
            try{
              Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(querySelect);
            while (resultSet.next()) {         
                    int val =  ((Number) resultSet.getObject(1)).intValue();
                    aRandom[xx][yy]=val;           
                }
            } catch (SQLException ex) {
                JOptionPane.showMessageDialog(null, "Error getting data!");
                }
            }
        }
        
        //hallar V
        int maximoA=aRandom[0][0];
         for(int xx=0;xx<Q;xx++)      
            for(int yy=0;yy<R;yy++)
              if(maximoA<aRandom[xx][yy])
                maximoA=aRandom[xx][yy];
         
        int[] vRandom=new int[maximoA+1];    
        for(int ww=1; ww<=maximoA;ww++)
            vRandom[ww]=0;
        
            for(int xx=0;xx<Q;xx++){
             for(int yy=0;yy<R;yy++){
                 vRandom[aRandom[xx][yy]]++;
             }  
            }
        vRandom[0]=0;
            
        //crear tabla y guardar!
            
        String nombreTablaV=nameTable+"Vector";
          try {
            String Query = "DROP TABLE " + nombreTablaV ;

            Statement st = Conexion.createStatement();
            st.executeUpdate(Query);
           // JOptionPane.showMessageDialog(null, "Se borrado la tabla " + table + " de forma exitosa");
        } catch (SQLException ex) {
          //  Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        }
         
  
        try {
            String Query = "CREATE TABLE " + nombreTablaV + ""
                    + "(VRANDOM VARCHAR(1000),VINTELLIGENT VARCHAR(1000))";

            Statement st = Conexion.createStatement();
            st.executeUpdate(Query);
           // JOptionPane.showMessageDialog(null, "Se ha creado la tabla " + table + " de forma exitosa");
        } catch (SQLException ex) {
            Logger.getLogger(MySql.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
        
         try {
            String sent="INSERT INTO "+ nombreTablaV + " VALUES("
                    + "\"" + Arrays.toString(vRandom) + "\""+ ","
                    + "\"" + Arrays.toString(v) + "\"" + ")" ;
            Statement st = Conexion.createStatement();
            st.executeUpdate(sent);
           // JOptionPane.showMessageDialog(null, "Datos almacenados de forma exitosa");
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en el almacenamiento de datos");
        }
            
        
        
        
         
           
//FIN PARTE ALEATORIA
         
     }
    
    
    
    //NO FUNCIONA POR LA PAREJA MATRIX
     public void assignAppointmentInt(String nameTable,String nameResource,String campos) {
        //Q=Qnumero de cuasi id diferentes.
         int Q =0;
          try {
            
            //SELECT cp,SEX,COUNT(*) FROM `t0` GROUP BY cp,SEX si se quiere saber el cuasi
            String Query = "SELECT count(CUENTA) FROM  (select count(*) as cuenta from " + nameTable+" GROUP BY "+ campos+" ) tabla";
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                
                int val =  ((Number) resultSet.getObject(1)).intValue();
                Q=val;
               
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data!");
        }
          
          //qf=Array [1,Q] frecuencia de cada cuasi ,el k sería el menor de todos estos
          
          int[] qf=new int[Q+1];
          int i=1;
          int j=0;
          try {

            String Query = "SELECT COUNT(*) FROM " + nameTable+" GROUP BY "+ campos  ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                j =  ((Number) resultSet.getObject(1)).intValue();    
                qf[i]=j;
                i++;
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data.");
        }
          
          //R=nº recursos (filas de  recursos).
         int R=0;
         try {

            String Query = "SELECT COUNT(*) FROM " + nameResource ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                R =  ((Number) resultSet.getObject(1)).intValue();     
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data.");
        }
          //RC CAPACIDAD DE CADA RECURSO.[1,R]
          int[] rc= new int[R+1];
          i=1;
          j=0;
          try {
            String Query = "SELECT * FROM " + nameResource;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);
            

            while (resultSet.next()) {
               j= resultSet.getInt("CAPACITY");
               rc[i]=j;
              i++;
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data.");
        }
         
          
          
          
      //creamos matriz de ejemplo o la que seria solucion e implementamos la actualizacion
          //en su tabla correspondiente
          
          int [][] matriz = new int[Q][R];
          int[] v=null;
          //matriz=getKMin(Q,qf,R,rc,v);
          
          
      
        
          //leer la tabla de las frecuencias de cada cuasi con el cuasi valor incluido.
          campos = campos.replace("\n","");
          String[] atributos=campos.split(",");
          int numAtributos=atributos.length;
          String[][] cuasis= new String[Q+1][numAtributos+1];
          String consulta=campos+",";
          
          
          
          int k=1;
          try {

            String Query = "SELECT "+consulta+"COUNT(*) FROM " + nameTable+" GROUP BY "+ campos  ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                int z=0;
                for(int x=0;x<numAtributos;x++)
                {
                    cuasis[k][z]=resultSet.getString(atributos[x]);
                    z++;
                }
                cuasis[k][z]=resultSet.getString("COUNT(*)");
                k++;
                
            }
            
            /*
                Cp  sex count
            0                   
            1   2800 M  3
            2   2801 W  4
            3   2802 M  1
            
            */
            
            
            
            

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error getting data.");
        }
          
          int total=0;
          for (int x=0; x < matriz.length; x++) {
             for (int y=0; y < matriz[x].length; y++) {
                total=total+ matriz[x][y];
                 
                 
                 
                }
            }  
          //habra que adaptar segun los indices, en este caso la matriz empieza en 0,0 y cusais en 1
          String[] sentens=new String[total];
  
          String campo="";
         
          for(int u=0;u<total;u++)
          {
              
              
              for (int x=0; x < matriz.length; x++) {
                 for (int y=0; y < matriz[x].length; y++) {
                    while(matriz[x][y]!=0)           
                    {
                        int recurso =y+1;
                        
                        sentens[u]="UPDATE "+ nameTable + " SET REC_INTELLIGENT=" + recurso + " WHERE ";
                        for (int a=0;a<numAtributos;a++)
                        {
                            sentens[u]=sentens[u]+ atributos[a]+"= "+"\""+cuasis[x+1][a]+"\""+" AND ";
                        }  
                        sentens[u] = sentens[u].substring(0, sentens[u].length() - 5);
                        sentens[u]=sentens[u]+" AND REC_INTELLIGENT= -1 LIMIT 1";
                        u++;
                        matriz[x][y]--;
                        
                    }
                    
                    
                    //se puede mejorar poniendo justo el limit adecuado
                 
                }
            }  
  
          }
          
           for(int l=0;l<sentens.length;l++)
        {
            
            try {
            
            Statement st = Conexion.createStatement();
            st.executeUpdate(sentens[l]);
           // JOptionPane.showMessageDialog(null, "Datos almacenados de forma exitosa");
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en el almacenamiento de datos");
        }
          
        }
          
          
          
          
          
          
          
      
        
    }
    
    
      public void assignAppointmentsInt(int numTables,String table,int numResource,String nameResource,String campos) {
        
         for(int i =0;i<numTables;i++)
         {
             String name=table+i+"_resource";
             String name1=table+i;
             assignAppointmentInt(name1,name,campos);
         }
        
        
        
    }
    
    
     public void assignAppointment(String nameTable,String nameResource,String campos) {
        //leer tabla de recursos y guardarlo en memoria
         int[][]citas;
         int i=0;
         int j=0;
         
         try {

            String Query = "SELECT COUNT(*) FROM " + nameResource ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                i =  ((Number) resultSet.getObject(1)).intValue();     
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en la adquisición de datos");
        }
         citas =new int[i][2];
         int m=0;
         int p;
         int n=0;
         
         try {
            String Query = "SELECT * FROM " + nameResource;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);
            

            while (resultSet.next()) {
               m= resultSet.getInt("RESOURCE");
               p=m-1;
               n= resultSet.getInt("CAPACITY");
               citas[p][0]=m;
               citas[p][1]=n;
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en la adquisición de datos");
        }
         
         //algoritmo
         int numPersons=0;
          try {

            String Query = "SELECT COUNT(*) FROM " + nameTable ;
            Statement st = Conexion.createStatement();
            java.sql.ResultSet resultSet;
            resultSet = st.executeQuery(Query);

            while (resultSet.next()) {
                numPersons =  ((Number) resultSet.getObject(1)).intValue();     
            }

        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en la adquisición de datos");
        }
         
          String[] sentencias = new String[numPersons];
         Random r = new Random();
         
         
         for(int l =0; l<numPersons;l++)
         {
             int aux=r.nextInt(m);// m+1
             
             while( citas[aux][1]==0)
              aux=(aux +1 ) % (m);
             
             citas[aux][1]--;
             
             
             sentencias[l]="UPDATE "+ nameTable + " SET REC_RAND="
                    + citas[aux][0] + " WHERE ID=" + l;
                    
  
         }
         
         
        
        for(int l=0;l<sentencias.length;l++)
        {
            
            try {
            
            Statement st = Conexion.createStatement();
            st.executeUpdate(sentencias[l]);
           // JOptionPane.showMessageDialog(null, "Datos almacenados de forma exitosa");
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error en el almacenamiento de datos");
        }
            
        }
       
          
         
        
    }
    
    
      public void assignAppointments(int numTables,String table,int numResource,String nameResource, String campos) {
        
         for(int i =0;i<numTables;i++)
         {
             String name=table+i+"_resource";
             String name1=table+i;
             assignAppointment(name1,name,campos);
         }
        
        
        
    }
    public void generateResource(String table,int numResource,int numPersons, int min, int max) {
        
        
        int[][] recursos=new int[numResource][2];
        Random n=new Random();
        
        for(int i=0;i<numResource;i++)
            recursos[i][0]=i;
        
        for(int i=0;i<numResource;i++)
        {
            int aux=n.nextInt(max-min+1);
            recursos[i][1]=min+aux;
            
        }
        int contador=0;
        
        for(int i=0;i<numResource;i++)
        {
            contador=contador+recursos[i][1];
            
        }
        int j=0;
        while(contador < numPersons)
        {
            recursos[j][1]++;
            contador++;
            j=(j+1)%numResource;
        }
        
         String[] sentencias = new String[numResource];
         
         
         for(int i =0; i<numResource;i++)
         {
             int aux=recursos[i][1];
             int h=i+1;
             
             sentencias[i]="INSERT INTO "+ table + " VALUES("
                    + h + ","
                    + aux + ")" ;
  
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
                    + "(RESOURCE int,CAPACITY int)";

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
    public void generateResources(int numTables,String table,int numResource,int numPersons, int min, int max) {
        
         for(int i =0;i<numTables;i++)
         {
             String name=table+i+"_resource";
             generateResource(name,numResource,numPersons,min,max);
         }
        
        
        
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
         t[0]=t[0]+resto;
         
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
                    + "\"" + genre + "\"" + ","
                    + "-1" + ","
                    + "-1" + ")" ;
  
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
                    + "(ID VARCHAR(15),AGE int,CP VARCHAR(10), SEX VARCHAR(100),REC_RAND int, REC_INTELLIGENT int)";

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
             n=r.nextInt(aux);
             if(sex.equals("") && (n % 2==0))
                 genre="man";
             if(sex.equals("") && (n % 2==1))
                 genre="woman";
             sentencias[i]="INSERT INTO "+ table + " VALUES("
                    + i + ","
                    + age + ","
                    + "\"" + code + "\""+ ","
                    + "\"" + genre + "\"" + ","
                    + "-1" + ","
                    + "-1" + ")" ;
  
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
                    + "(ID VARCHAR(15),AGE int,CP VARCHAR(10), SEX VARCHAR(100),REC_RAND int, REC_INTELLIGENT int)";

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
