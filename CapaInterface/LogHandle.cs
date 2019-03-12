using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using CapaDatos;

namespace CapaInterface
{
    public static class LogHandle
    {

        static string fileLogError = DatosGenerales.rutaMain + "LogErrores.txt";


        /************** Graba_Log
        * Metodo que graba un log de errores en un archivo de texto y en el sql
        **************/
        public static void Graba_Log(string error_msg)
        {
            using (StreamWriter writer = new StreamWriter(fileLogError, true))
            {
                //writer.WriteLine("*********** ERROR ");
                writer.WriteLine("Fecha: " + DateTime.Now.ToString() + " " + error_msg);

            }

            string sqlquery = "[USP_Inserta_Error_Interface]";
            try
            {
                using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                {
                    if (cn.State == 0) cn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlquery, cn))
                    {
                        cmd.CommandTimeout = 0;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@ERR_INTERFACE", "PRESC");
                        cmd.Parameters.AddWithValue("@ERR_DESCRIP", error_msg);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return;
        }

    }
}
