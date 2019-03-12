
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;


namespace CapaInterface
{


    public static class DatosGenerales
    {

        //************** Ruta principal
        //public static string rutaMain = ConfigurationManager.AppSettings["rutaMain"];

        public static string CodRetail = "5";
        public static string CodNoRetail = "6";

        public static string UrlFtp = ConfigurationManager.AppSettings["UrlFtp"];
        public static string UserFtp = ConfigurationManager.AppSettings["UserFtp"];
        public static string PassFtp = ConfigurationManager.AppSettings["PassFtp"];

        public static string codalm = "50001";
        public static string codcia = "PE02";

        //public static string rutaMain = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DatosGenerales)).CodeBase);

#if DEBUG
        public static string rutaMain = @"c:\pruebaservicio\";
#else
    public static string rutaMain = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DatosGenerales)).CodeBase);
#endif

    }
}
