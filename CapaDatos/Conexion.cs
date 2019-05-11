using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
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
        public static string conexion_Posgre
        {
            get { return "Server= '172.28.7.20'; Port= '5432'; User= 'admin'; Password = 'batanet'; Database = 'scomercial'; "; }
        }


        public static bool Mapea_red()
        {
            NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf1"], "cquinto", "Spiderman100*");
            NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf2"], "cquinto", "Spiderman100*");
            return true;
        }

        public static DataTable Obt_dbf(string sql, string retail_noretail)
        {

            DataTable dt = null;


            string conex = "";

            if (retail_noretail == "5")
            { conex = Conn2; }
            else
            { conex = Conn1; }

            using (OleDbConnection dbConn = new OleDbConnection(conex))
            {
                dbConn.Open();

                try
                {
                    // FALTA EVALUAR new System.Data.OleDb.OleDbCommand("set enginebehavior 80", dbConn).ExecuteNonQuery();

                    using (OleDbCommand cmd = dbConn.CreateCommand())
                    {
                        cmd.CommandText = "set enginebehavior 70";
                        cmd.ExecuteNonQuery();
                    }

                    //-- Obtenemos datos del DBF
                    System.Data.OleDb.OleDbCommand com = new System.Data.OleDb.OleDbCommand(sql, dbConn);
                    System.Data.OleDb.OleDbDataAdapter ada = new System.Data.OleDb.OleDbDataAdapter(com);
                    dt = new DataTable();
                    ada.Fill(dt);

                }
                catch
                {
                    // omitido
                }
            }

            return dt;

        }


        public static DataTable Obt_SQL(string sqlquery, ref string msgerror, string cabecera_detalle)
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
                        //cmd.ExecuteNonQuery();
                        using (var da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
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
