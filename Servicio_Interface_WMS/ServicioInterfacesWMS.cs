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
        Timer tmservicio_Prescripciones = null;
        Int32 wenproceso_Prescripciones = 0;

        // Timer para O/C  (J Cochon)
        Timer tmservicio_Purchase = null;
        Int32 wenproceso_Purchase = 0;

        // Timer para ASN O/C (J Cochon)
        Timer tmservicio_ASN_Purchase = null;
        Int32 wenproceso_ASN_Purchase = 0;

        // Timer para Leer TXT del FTP (J Cochon)
        Timer tmservicio_Leer_FTP = null;
        Int32 wenproceso_Leer_FTP = 0;

        // Timer para ASN Devoluciones Tiendas
        Timer tmservicio_ASN_Devol = null;
        Int32 wenproceso_ASN_Devol = 0;

        //timer para order HDR,order DTL --pedido carrito
        Timer tmservicio_HDR_DTL = null;
        Int32 wenproceso_HDR_DTL = 0;

        //timer para order HDR,order DTL --pedido catalogo
        Timer tmservicio_HDR_DTL_catalogo = null;
        Int32 wenproceso_HDR_DTL_catalogo = 0;

        // timer para actualizar lista con tabla tabgen.dbf
        Timer tmactulista = null;


        public ServicioInterfaceWMS()
        {
            InitializeComponent();

            // Timer para actualizar lista de TABGEN.DBF
            tmactulista = new Timer(1000); // initial start after 1 second
            tmactulista.Elapsed += new ElapsedEventHandler(tmactulista_Elapsed);

            // Prescripciones
            tmservicio_Prescripciones = new Timer(10000); //30000
            tmservicio_Prescripciones.Elapsed += new ElapsedEventHandler(tmpServicio_Prescripciones_Elapsed);

            // Purchase            
            tmservicio_Purchase = new Timer(30000);
            tmservicio_Purchase.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_Purchase);

            // ASN Purchase
            tmservicio_ASN_Purchase = new Timer(30000);
            tmservicio_ASN_Purchase.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_ASN_Purchase);

            // Leer archivos del Ftp
            tmservicio_Leer_FTP = new Timer(30000);
            tmservicio_Leer_FTP.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_Leer_FTP);

            // ASN Devoluciones de Tiendas
            tmservicio_ASN_Devol = new Timer(600000);//600000
            tmservicio_ASN_Devol.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_ASN_Devol);

            // order HDR, DTL carrito
            tmservicio_HDR_DTL = new Timer(30000);
            tmservicio_HDR_DTL.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_HDR_DTL);

            // order HDR, DTL catalogo
            tmservicio_HDR_DTL_catalogo = new Timer(30000);
            tmservicio_HDR_DTL_catalogo.Elapsed += new ElapsedEventHandler(tmpServicio_Elapsed_HDR_DTL_catalogo);

        }

        //****************************************************************************

        void tmactulista_Elapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                Timer timer = (Timer)sender;
                timer.Interval = 60 * 60 * 1000;      // Change the interval to whatever (1 hora)
                DatosGenerales.Llena_CDxAlm();
                LogUtil.Graba_Log("SERVICIO", ConfigurationManager.AppSettings["M011"].ToString(), false, "");
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log("SERVICIO", " ERROR: " + ex.ToString(), true, "");
                //throw ex;
            }

        }

        void tmpServicio_Prescripciones_Elapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                if (ConfigurationManager.AppSettings["processPrescrip"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_Prescripciones == 0)
                    {
                        wenproceso_Prescripciones = 1;
                        Prescripcion oprescrip = new Prescripcion();
                        oprescrip.Genera_Interface_Prescripcion();
                        wenproceso_Prescripciones = 0;
                    }
                }
            }
            catch (Exception)
            {
                wenproceso_Prescripciones = 0;

            }

        }

        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_Purchase(object sender, ElapsedEventArgs e)
        {
            try
            {
                if (ConfigurationManager.AppSettings["processPurchase"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_Purchase == 0)
                    {
                        wenproceso_Purchase = 1;
                        Purchase opurchase = new Purchase();
                        opurchase.Genera_Interface_Purchase();
                        wenproceso_Purchase = 0;
                    }
                }
            }
            catch (Exception)
            {

                wenproceso_Purchase = 0;
            }

        }

        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_ASN_Purchase(object sender, ElapsedEventArgs e)
        {
            try
            {
                if (ConfigurationManager.AppSettings["process_ASN_Purchase"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_ASN_Purchase == 0)
                    {
                        wenproceso_ASN_Purchase = 1;
                        Asn_Purchase oasn_purchase = new Asn_Purchase();
                        //oasn_purchase.Genera_Interface_Purchase();
                        oasn_purchase.Genera_Interface_Asn_Purchase();
                        wenproceso_ASN_Purchase = 0;
                    }
                }
            }
            catch (Exception)
            {

                wenproceso_ASN_Purchase = 0;
            }


        }

        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_Leer_FTP(object sender, ElapsedEventArgs e)
        {
            try
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
            catch (Exception)
            {

                wenproceso_Leer_FTP = 0;
            }


        }

        //**************************************************************************** Por JC
        void tmpServicio_Elapsed_HDR_DTL(object sender, ElapsedEventArgs e)  //carrito de compras
        {
            try
            {
                if (ConfigurationManager.AppSettings["process_HDR_HDL"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_HDR_DTL == 0)
                    {
                        wenproceso_HDR_DTL = 1;
                        Pedidos_Carrito ohdr_dtl = new Pedidos_Carrito();
                        ohdr_dtl.Genera_Interface_OrdDesp();
                        wenproceso_HDR_DTL = 0;
                    }
                }
            }
            catch (Exception)
            {

                wenproceso_HDR_DTL = 0;
            }



        }

        //****************************************************************************
        void tmpServicio_Elapsed_ASN_Devol(object sender, ElapsedEventArgs e)
        {
            try
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
            catch (Exception)
            {

                wenproceso_ASN_Devol = 0;
            }

        }


        //****************************************************************************
        void tmpServicio_Elapsed_HDR_DTL_catalogo(object sender, ElapsedEventArgs e)
        {
            try
            {
                if (ConfigurationManager.AppSettings["process_HDR_HDL_catalogo"] == "1")
                {
                    // verificar si el servicio se esta ejecutando
                    if (wenproceso_HDR_DTL_catalogo == 0)
                    {
                        wenproceso_HDR_DTL_catalogo = 1;
                        Pedidos_Catalogo ohdr_dtl_catalogo = new Pedidos_Catalogo();
                        ohdr_dtl_catalogo.Genera_Interface_Catalogo_Pedido();
                        wenproceso_HDR_DTL_catalogo = 0;
                    }
                }
            }
            catch (Exception)
            {

                wenproceso_HDR_DTL_catalogo = 0;
            }


        }

        protected override void OnStart(string[] args)
        {
            //Conexion.Mapea_red();
            //DatosGenerales.Llena_CDxAlm();

            tmactulista.Start();
            tmservicio_Prescripciones.Start();
            tmservicio_Purchase.Start();
            tmservicio_ASN_Purchase.Start();
            tmservicio_Leer_FTP.Start();
            tmservicio_ASN_Devol.Start();
            ///*interface hdr/dtl*/
            tmservicio_HDR_DTL_catalogo.Start();
            tmservicio_HDR_DTL.Start();


        }

        protected override void OnStop()
        {
            tmactulista.Stop();
            tmservicio_Prescripciones.Stop();
            tmservicio_Purchase.Stop();
            tmservicio_ASN_Purchase.Stop();
            tmservicio_Leer_FTP.Stop();
            tmservicio_ASN_Devol.Stop();

            ///*interface hdr/dtl*/
            tmservicio_HDR_DTL_catalogo.Stop();
            tmservicio_HDR_DTL.Stop();


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
