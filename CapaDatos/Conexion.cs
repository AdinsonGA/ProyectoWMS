using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;

namespace CapaDatos
{
    public static class Conexion
    {

        //************** Datos conexion SQL
        public static string conexion = ConfigurationManager.ConnectionStrings["Test"].ConnectionString;

        //************** Datos conexion con dbf
        //private static string rutaDbf = ConfigurationManager.ConnectionStrings["rutaDbf"].ConnectionString;
        //private static string rutaDbf = ConfigurationManager.AppSettings["rutaDbf"];
        //private static string sConn = "Provider = vfpoledb.1;Data Source=" + System.IO.Path.GetDirectoryName(rutaDbf) + ";Collating Sequence=general";

        public static string Conn1 = ConfigurationManager.ConnectionStrings["Dbf1"].ConnectionString;
        public static string Conn2 = ConfigurationManager.ConnectionStrings["Dbf2"].ConnectionString;

        public  static void prueba()
            {

            }

        public static DataTable Obt_dbf(string sql)
        {

            DataTable dt = null;

            using (System.Data.OleDb.OleDbConnection dbConn = new System.Data.OleDb.OleDbConnection(Conn1))
            {
                dbConn.Open();

                try
                {

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
    }
}
