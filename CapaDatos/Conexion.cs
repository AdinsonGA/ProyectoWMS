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

        public static string Serv_Postgress = ConfigurationManager.AppSettings["Serv_Postgress"];
        public static string DB_Postgress = ConfigurationManager.AppSettings["DB_Postgress"];
        public static string User_Postgress = ConfigurationManager.AppSettings["User_Postgress"];
        public static string Pass_postgress = ConfigurationManager.AppSettings["Pass_postgress"];
        public static string Puert_Postgress = ConfigurationManager.AppSettings["Puert_Postgress"];

        //************** Datos conexion SQL
        public static string conexion
        {
            get
            {
                if (ConfigurationManager.AppSettings["FlagServProd"].ToString() == "1")

                    return ConfigurationManager.ConnectionStrings["SQL"].ConnectionString;
                else
                    return ConfigurationManager.ConnectionStrings["SQL_Test"].ConnectionString;
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
                if (ConfigurationManager.AppSettings["FlagConexDBFs"].ToString() == "1")

                    return ConfigurationManager.ConnectionStrings["Dbf1"].ConnectionString;

                else
                    return ConfigurationManager.ConnectionStrings["Dbf1_Test"].ConnectionString;
            }
        }

        public static string Conn2
        {
            get
            {
                if (ConfigurationManager.AppSettings["FlagConexDBFs"].ToString() == "1")

                    return ConfigurationManager.ConnectionStrings["Dbf2"].ConnectionString;

                else
                    return ConfigurationManager.ConnectionStrings["Dbf2_Test"].ConnectionString;
            }
        }

        //public static string Conn2 = ConfigurationManager.ConnectionStrings["Dbf2"].ConnectionString;

        //************** Datos conexion INTRANET
        public static string conexion_Postgre
        {
            //get { return "Server= '172.28.7.20'; Port= '5432'; User= 'admin_wms'; Password = '**intapp--'; Database = 'scomercial'; "; }
            get { return "Server='" + Serv_Postgress + "';Port='" + Puert_Postgress + "';User= '" + User_Postgress + "';Password ='" + Pass_postgress + "';Database ='" + DB_Postgress + "';"; }
        }


        public static bool Mapea_red()
        {
            NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf1"], "dmendoza", "Bata2013*");
            NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf2"], "dmendoza", "Bata2013*");
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
                    NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf2"], ConfigurationManager.AppSettings["pathDbf2_user"], ConfigurationManager.AppSettings["pathDbf2_pass"]);
                }
                else
                {
                    conex = Conn1;
                    NetworkShare.ConnectToShare(ConfigurationManager.AppSettings["pathDbf1"], ConfigurationManager.AppSettings["pathDbf1_user"], ConfigurationManager.AppSettings["pathDbf1_pass"]);
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
                msgerror = "ERROR CONSULTAR SQL: " + ex.Message;

            }

            return dt;
        }

        public static DataTable Obt_maestros(string sqlquery, Int32 dias) //Obtiene los maestros de Ecommerce y Aquarella
        {
            DataTable dt = new DataTable();
            //msgerror = "";

            try
            {
                using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                {
                    if (cn.State == 0) cn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlquery, cn))
                    {
                        cmd.CommandTimeout = 0;
                        cmd.CommandType = CommandType.StoredProcedure;
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
                //msgerror = "ERROR CONSULTAR SQL: " + ex.Message;

            }

            return dt;
        }


    }
}
