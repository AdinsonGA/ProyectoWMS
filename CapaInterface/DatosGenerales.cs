
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

    public static class DatosGenerales
    {

        //************** Ruta principal
        //public static string rutaMain = ConfigurationManager.AppSettings["rutaMain"];

        public static string CodRetail = "5";
        public static string CodNoRetail = "6";

        //public static string pathDbf1 = ConfigurationManager.AppSettings["pathDbf1"];
        //public static string pathDbf2 = ConfigurationManager.AppSettings["pathDbf2"];

        public static string UrlFtp = ConfigurationManager.AppSettings["UrlFtp"];
        public static string UserFtp = ConfigurationManager.AppSettings["UserFtp"];
        public static string PassFtp = ConfigurationManager.AppSettings["PassFtp"];

        //public static string codalm = "50001";
        public static string codcia = "730";

        public static List<CDxAlm> listCDxAlm;

        //public static string rutaMain = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DatosGenerales)).CodeBase);

        public static void Llena_CDxAlm()
        {
            try
            {

                string sql = "SELECT tab_ctab as codalm, left(tab_cpar6,3) as cd FROM TABGEN WHERE tab_tipo='206' AND !EMPTY(tab_ctab)";
                DataTable dt_tabgen = null;
                dt_tabgen = Conexion.Obt_dbf(sql, DatosGenerales.CodNoRetail);

                listCDxAlm = dt_tabgen.AsEnumerable().Select(m => new CDxAlm()
                {
                    codalm = m.Field<string>("codalm"),
                    CD = m.Field<string>("CD"),
                }).ToList();

                dt_tabgen = null;
            }
            catch (Exception ex)
            {

                //LogUtil.Graba_Log(winterface, winterface + " ERROR: " + ex.ToString(), true, "");
                throw ex;
            }
           
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
