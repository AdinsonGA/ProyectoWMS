using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WinSCP;
using CapaDatos;
using System.Data.OleDb;
using System.Text.RegularExpressions;
namespace CapaInterface
{
    public class Crear_Carpetas
    {
        public static string WMS = Path.Combine(ConfigurationManager.AppSettings["WMS"].ToString());
        public static string WORK = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["WORK"].ToString());
        public static string RECYCLER = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["RECYCLER"].ToString());
        public static string BACKUP = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["BACKUP"].ToString());
        public static string LOG = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["LOG"].ToString());

        //**ALMACEN 50001**//
        public static string C50001 = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["C50001"].ToString() + "");
        public static string C50001_input = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["C50001_input"].ToString() + "");
        public static string C50001_output = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["C50001_output"].ToString() + "");

        //**ALMACEN 50003**//
        public static string C50003 = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["C50003"].ToString() + "");
        public static string C50003_input = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["C50003_input"].ToString() + "");
        public static string C50003_output = Path.Combine(DatosGenerales.rutaMain, @"" + ConfigurationManager.AppSettings["C50003_output"].ToString() + "");

        public void ArchivaInterface(string CodFlag)
        {
            if (CodFlag == "WMS") // Verificando la creacion de la carpeta matriz WMS
            {
                if (!Directory.Exists(WMS))
                {
                    Directory.CreateDirectory(WMS);
                }

                if (!Directory.Exists(WORK))
                {
                    Directory.CreateDirectory(WORK);
                }

                if (!Directory.Exists(BACKUP))
                {
                    Directory.CreateDirectory(BACKUP);
                }

                if (!Directory.Exists(LOG))
                {
                    Directory.CreateDirectory(LOG);
                }
            }
            else
            {
                if (CodFlag == "50001")
                {
                    //verificando la creacion de carpetas  50001,input,output
                    if (!Directory.Exists(C50001))
                    {
                        Directory.CreateDirectory(C50001);
                    }

                    if (!Directory.Exists(C50001_input))
                    {
                        Directory.CreateDirectory(C50001_input);
                    }

                    if (!Directory.Exists(C50001_output))
                    {
                        Directory.CreateDirectory(C50001_output);
                    }
                }

                if (CodFlag == "50003")
                {
                    //verificando la creacion de carpetas  50003,input,output

                    if (!Directory.Exists(C50003))
                    {
                        Directory.CreateDirectory(C50003);
                    }

                    if (!Directory.Exists(C50003_input))
                    {
                        Directory.CreateDirectory(C50003_input);
                    }

                    if (!Directory.Exists(C50003_output))
                    {
                        Directory.CreateDirectory(C50003_output);
                    }
                }
                if (CodFlag == "RECYCLER")
                {
                    if (!Directory.Exists(RECYCLER))
                    {
                        Directory.CreateDirectory(RECYCLER);
                    }


                }

            }

        }
    }
}
