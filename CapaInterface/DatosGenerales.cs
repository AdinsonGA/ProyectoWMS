
using CapaDatos;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;


namespace CapaInterface
{
    public class CDxAlm
    {
        public string codalm = "";
        public string CD = "";
    }

    public class DatosGenerales
    {

        //************** Ruta principal
        //public static string rutaMain = ConfigurationManager.AppSettings["rutaMain"];

        public static string CodRetail = "5";
        public static string CodNoRetail = "6";

        //public static string UrlFtp;
        //public static string UserFtp;
        //public static string PassFtp;
        //public static string PuertoFtp;

        //public static string pathDbf1 = ConfigurationManager.AppSettings["pathDbf1"];
        //public static string pathDbf2 = ConfigurationManager.AppSettings["pathDbf2"];

        public static string UrlFtp
        {
            get
            {
                if (ConfigurationManager.AppSettings["FlagServProd"].ToString() == "1")

                    /*CREDENCIALES DE SERVIDOR DE PRODUCCION*/
                    return ConfigurationManager.AppSettings["UrlFtp"];
                else
                    /*CREDENCIALES DE SERVIDOR DE TEST*/
                    return ConfigurationManager.AppSettings["UrlFtp_test"];

            }
        }
        public static string UserFtp
        {
            get
            {
                if (ConfigurationManager.AppSettings["FlagServProd"].ToString() == "1")

                    /*CREDENCIALES USUARIO DE PRODUCCION*/
                    return ConfigurationManager.AppSettings["UserFtp"];
                else
                    /*CREDENCIALES USUARIO DE TEST*/
                    return ConfigurationManager.AppSettings["UserFtp_test"];
            }
        }
        public static string PassFtp
        {
            get
            {
                if (ConfigurationManager.AppSettings["FlagServProd"].ToString() == "1")

                    /*CREDENCIALES DE PASSWORD DE PRODUCCION*/
                    return ConfigurationManager.AppSettings["PassFtp"];
                else
                    /*CREDENCIALES DE PASSWORD DE TEST*/
                    return ConfigurationManager.AppSettings["PassFtp_test"];

            }
        }
        public static string PuertoFtp
        {
            get
            {
                if (ConfigurationManager.AppSettings["FlagServProd"].ToString() == "1")

                    /*CREDENCIALES DE PUERTO DE PRODUCCION*/
                    return ConfigurationManager.AppSettings["PuertoFtp"];
                else
                    /*CREDENCIALES DE PUERTO DE TEST*/
                    return ConfigurationManager.AppSettings["PuertoFtp_test"];

            }
        }
        

        //public static string codalm = "50001";
        public static string codcia = "730";

        public static List<CDxAlm> listCDxAlm;

        //public static string rutaMain = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DatosGenerales)).CodeBase);

        public static void Llena_CDxAlm()
        {
            //try
            //{

            string sql = "SELECT tab_ctab as codalm, left(tab_cpar6,3) as cd FROM TABGEN WHERE tab_tipo='206' AND !EMPTY(tab_ctab)";
            DataTable dt_tabgen = null;
            int NoRetail = 2;  // Para escoger la ruta del dbf tabgen

            dt_tabgen = Conexion.Obt_dbf(sql, NoRetail);

            listCDxAlm = dt_tabgen.AsEnumerable().Select(m => new CDxAlm()
            {
                codalm = m.Field<string>("codalm"),
                CD = m.Field<string>("CD"),
            }).ToList();

            dt_tabgen = null;

            //}
            //catch (Exception ex)
            //{

            //    //LogUtil.Graba_Log(winterface, winterface + " ERROR: " + ex.ToString(), true, "");
            //    throw ex;
            //}

        }

        public static string Obt_CDxAlm(string codalm)
        {
            //Llena_CDxAlm();  // cquinto: esto no estaba aqui
            var resu = listCDxAlm.Where(i => i.codalm.Trim() == codalm.Trim()).FirstOrDefault();
            if (resu == null)
                return " ";

            if (resu.CD.Trim() == "204")  // Tabgen.dbf  tabla 206
                return "50003";
            else
                return "50001";
        }

#if DEBUG //Ruta matriz para generar compilado en modo de pruebas

        //public static string work = @"" + ConfigurationManager.AppSettings["WORK"].ToString() + "";
        //public static string rutaMain = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + @"\" + work;
        public static string rutaMain = @"" + ConfigurationManager.AppSettings["WMS"].ToString() + ""; //+ work;

#else  //Ruta matriz para generar compilado en modo de produccion(Release)

        //public static string work = @"" + ConfigurationManager.AppSettings["WORK"].ToString() + "";
        //public static string rutaMain = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + @"\" + work;
        public static string rutaMain = @"" + ConfigurationManager.AppSettings["WMS"].ToString() + ""; //+ work;
#endif

    }
}
