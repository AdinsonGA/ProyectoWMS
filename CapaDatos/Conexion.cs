using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace CapaDatos
{
    public static class Conexion
    {

        public static bool FLPRUEBA = false;
 
        //************** Datos conexion SQL
        public static string conexion
        {
            get
            {
                if (FLPRUEBA)
                    return ConfigurationManager.ConnectionStrings["SQL_Test"].ConnectionString;
                else
                    return ConfigurationManager.ConnectionStrings["SQL"].ConnectionString;
            }
        }


        //************** Datos conexion con dbf
        //private static string rutaDbf = ConfigurationManager.ConnectionStrings["rutaDbf"].ConnectionString;
        //private static string rutaDbf = ConfigurationManager.AppSettings["rutaDbf"];
        //private static string sConn = "Provider = vfpoledb.1;Data Source=" + System.IO.Path.GetDirectoryName(rutaDbf) + ";Collating Sequence=general";

        public static string Conn1
        {
            get
            {
                if (FLPRUEBA)
                    return ConfigurationManager.ConnectionStrings["Dbf1_Test"].ConnectionString;
                else
                    return ConfigurationManager.ConnectionStrings["Dbf1"].ConnectionString;
            }
        }

        public static string Conn2
        {
            get
            {
                if (FLPRUEBA)
                    return ConfigurationManager.ConnectionStrings["Dbf2_Test"].ConnectionString;
                else
                    return ConfigurationManager.ConnectionStrings["Dbf2"].ConnectionString;
            }
        }

        //public static string Conn2 = ConfigurationManager.ConnectionStrings["Dbf2"].ConnectionString;

        //************** Datos conexion INTRANET
        public static string conexion_Postgre
        {
            get { return "Server= '172.28.7.20'; Port= '5432'; User= 'admin'; Password = 'batanet'; Database = 'scomercial'; "; }
      
        }


        public static bool Mapea_red()
        {
            NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf1"], "dmendoza", "Bata2013*");
            NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf2"], "dmendoza", "Bata2013*");
            //return true;


            return true;

        }

        public static DataTable Obt_dbf(string sql, int retail_noretail)
        {
            DataTable dt = null;
            string conex = "";

            try
            {
                if (retail_noretail == 1)
                {
                    conex = Conn2;
                    NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf2"], "SERVICIOS", "servicios123");
                }
                else
                {
                    conex = Conn1;
                    NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf1"], "SERVICIOS", "servicios123");
                }


                //File.WriteAllText(@"C:\WMS\LOG\LOGXXX.txt", conex);


                using (OleDbConnection dbConn = new OleDbConnection(conex))
                {

                    //FALTA EVALUAR new System.Data.OleDb.OleDbCommand("set enginebehavior 80", dbConn).ExecuteNonQuery();

                    dbConn.Open();

                    using (OleDbCommand cmd = dbConn.CreateCommand())
                    {
                        cmd.CommandText = "set enginebehavior 70";
                        cmd.ExecuteNonQuery();
                    }

                    // Obtenemos datos del DBF
                    System.Data.OleDb.OleDbCommand com = new System.Data.OleDb.OleDbCommand(sql, dbConn);
                    com.CommandTimeout = 0;
                    System.Data.OleDb.OleDbDataAdapter ada = new System.Data.OleDb.OleDbDataAdapter(com);
                    dt = new DataTable();
                    ada.Fill(dt);

                    if (dbConn != null)
                        if (dbConn.State == ConnectionState.Open) dbConn.Close();
                }
            }
            catch (Exception ex)
            {
                //if (dbConn != null)
                //    if (dbConn.State == ConnectionState.Open) dbConn.Close();

                // cquinto: message ? deberia grabar en el Log y continuar
                //ex.Message.ToString();
                throw ex;
                //dt = null;
            }

            //if (dbConn != null)
            //    if (dbConn.State == ConnectionState.Open) dbConn.Close();

            //OleDbConnection cnDBF = new OleDbConnection(conex);
            //cnDBF.Open();
            //OleDbCommand comando = new OleDbCommand(sql, cnDBF);
            //OleDbDataAdapter adaptador = new OleDbDataAdapter(comando);
            //DataTable tabla = new DataTable();
            //adaptador.Fill(tabla);

            // return datatable;
            return dt;
        }

        public static DataTable Obt_SQL(string sqlquery, ref string msgerror, string cabecera_detalle, Int32 dias)
        {
            //string sqlquery = "[USP_Inserta_Error_Interface]";

            DataTable dt = new DataTable();
            msgerror = "";

            try
            {
                using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                {
                    if (cn.State == 0) cn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlquery, cn))
                    {
                        cmd.CommandTimeout = 0;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@cabecera_detalle", cabecera_detalle);
                        cmd.Parameters.AddWithValue("@dias", dias);
                        //cmd.ExecuteNonQuery();
                        using (var da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                            if (cn != null)
                                if (cn.State == ConnectionState.Open) cn.Close();
                        }
                    }
                }
            }


            catch (Exception ex)
            {
                // omitido
                //LogHandle.Graba_Log("DEVOL", "ERROR CONSULTAR DATA SQL: "+ex.Message); // OJO POR MIENTRAS
                msgerror = "ERROR CONSULTAR DATA SQL: " + ex.Message;
            }

            return dt;
        }
    }
}
