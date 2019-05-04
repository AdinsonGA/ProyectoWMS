using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
//using System.Text;
using System.Threading.Tasks;
using System.Timers;
using CapaInterface;
using CapaDatos;
using System.Configuration;

//using WinSCP;
//using System.Data.SqlClient;
//using System.Configuration;


namespace CapaServicio
{
    public partial class ServicioInterfaceWMS : ServiceBase
    {

        // Timer para Prescripciones
        Timer tmservicio = null;
        Int32 wenproceso = 0;

        // Timer para O/C  (J Cochon)
        Timer tmservicioOC = null;
        Int32 wenprocesoOC = 0;

        // Timer para ASN O/C (J Cochon)
        Timer tmservicioASN = null;
        Int32 wenprocesoASN = 0;

        // Timer para Leer TXT del FTP (J Cochon)
        Timer tmservicio_Leer_FTP = null;
        Int32 wenproceso_Leer_FTP = 0;

        // Timer para ASN Devoluciones Tiendas
        Timer tmservicio_ASN_Devol = null;
        Int32 wenproceso_ASN_Devol = 0;

        public ServicioInterfaceWMS()
        {
            InitializeComponent();

            // Prescripciones
            tmservicio = new Timer(30000);
            tmservicio.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed);

            // O/C            
            tmservicioOC = new Timer(15000);
            tmservicioOC.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_OC);

            // ASN Purchase
            tmservicioASN = new Timer(15000);
            tmservicioASN.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_ASN_Purchase);

            // Leer archivos del Ftp
            tmservicio_Leer_FTP = new Timer(40000);
            tmservicio_Leer_FTP.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_Leer_FTP);

            // ASN Devoluciones de Tiendas
            tmservicio_ASN_Devol = new Timer(30000);
            tmservicio_ASN_Devol.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_ASN_Devol);
        }


        //****************************************************************************
        void tmpServicio_Elapsed(object sender, ElapsedEventArgs e)
        {

            if (ConfigurationManager.AppSettings["processPrescrip"] == "1")
            {
                // verificar si el servicio se esta ejecutando
                if (wenproceso == 0)
                {
                    wenproceso = 1;
                    Prescripcion oprescrip = new Prescripcion();
                    oprescrip.Genera_Interface_Prescripcion();
                    wenproceso = 0;
                }
            }
        }


        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_OC(object sender, ElapsedEventArgs e)
        {
            if (ConfigurationManager.AppSettings["processPurchase"] == "1")
            {
                // verificar si el servicio se esta ejecutando
                if (wenprocesoOC == 0)
                {
                    wenprocesoOC = 1;
                    Purchase opurchase = new Purchase();
                    opurchase.Genera_Interface_Purchase();
                    wenprocesoOC = 0;
                }
            }

        }


        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_ASN_Purchase(object sender, ElapsedEventArgs e)
        {
            if (ConfigurationManager.AppSettings["processPurchase"] == "X")
            {
                // verificar si el servicio se esta ejecutando
                if (wenprocesoASN == 0)
                {
                    wenprocesoASN = 1;
                    Asn_Purchase oasn_purchase = new Asn_Purchase();
                    oasn_purchase.Genera_Interface_Purchase();
                    wenprocesoASN = 0;
                }
            }

        }


        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_Leer_FTP(object sender, ElapsedEventArgs e)
        {
            if (ConfigurationManager.AppSettings["processLeerFTP"] == "1")
            {
                // verificar si el servicio se esta ejecutando
                if (wenproceso_Leer_FTP == 0)
                {
                    wenproceso_Leer_FTP = 1;
                    Leer_Ftp oLeer_Ftp = new Leer_Ftp();
                    oLeer_Ftp.Genera_Interface_Lectura();
                    wenproceso_Leer_FTP = 0;
                }
            }

        }


        //****************************************************************************
        void tmpServicio_Elapsed_ASN_Devol(object sender, ElapsedEventArgs e)
        {

            if (ConfigurationManager.AppSettings["process_ASN_Devol"] == "1")
            {
                // verificar si el servicio se esta ejecutando
                if (wenproceso_ASN_Devol == 0)
                {
                    wenproceso_ASN_Devol = 1;
                    ASN_Devolucion odev = new ASN_Devolucion();
                    odev.Genera_Interface_ASN_Devolucion();
                    wenproceso_ASN_Devol = 0;
                }
            }
        }

        protected override void OnStart(string[] args)
        {
            Conexion.Mapea_red();
            DatosGenerales.Llena_CDxAlm();

            tmservicio.Start();
            tmservicioOC.Start(); // Inicia Servicio de Enviar al FTP - Purchase Order (Cabecera y Detalle)
            tmservicioASN.Start();
            tmservicio_Leer_FTP.Start();
            tmservicio_ASN_Devol.Start();
        }

        protected override void OnStop()
        {
            tmservicio.Stop();
            tmservicioOC.Stop();
            tmservicioASN.Stop();
            tmservicio_Leer_FTP.Stop();
            tmservicio_ASN_Devol.Stop();
        }


        internal void TestStartupAndStop()
        {
            string[] arg = new string[] { }; ;
            this.OnStart(arg);
            Console.ReadLine();
            this.OnStop();
        }
    }
}
