using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
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


        public static bool Mapea_red()
        {
            NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf1"], "cquinto", "Spiderman100*");
            return true;
        }

        public static DataTable Obt_dbf(string sql)
        {

            DataTable dt = null;

            using (OleDbConnection dbConn = new OleDbConnection(Conn1))
            {
                dbConn.Open();

                try
                {
                    // FALTA EVALUAR new System.Data.OleDb.OleDbCommand("set enginebehavior 80", dbConn).ExecuteNonQuery();

                    using (OleDbCommand cmd = dbConn.CreateCommand())
                    {
                        cmd.CommandText = "set enginebehavior 80";
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
    }
}
