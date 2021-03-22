using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using WinSCP;
using CapaDatos;
using System.Data;
using System.IO;
using Npgsql;
using System.Net;
using System.Collections;
using System.Data.SqlClient;
using System.Configuration;

namespace CapaInterface
{
    public class Resultado_Leer_Ftp
    {
        public bool Exito { get; set; }
        public string MensajeError { get; set; }
        public DataSet DatosDevueltos { get; set; }
    }

    public class Leer_Ftp
    {

        //************** Envio de Prescripciones       
        string wcodcia = DatosGenerales.codcia;
        string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";
        string Interface = "LEER";
        string[] Arr_Txt_Obl;
        string[] Arr_Txt_Obs;

        string[] Arr_Txt_Svh;
        string[] Arr_Txt_Svd;
        string[] Arr_Txt_Iht;


        // Sesion para Loguearce al FTP
        private SessionOptions sessionOptions = new SessionOptions
        {
            Protocol = Protocol.Sftp,
            HostName = DatosGenerales.UrlFtp, //"172.24.20.183"
            UserName = DatosGenerales.UserFtp, //"retailc"
            Password = DatosGenerales.PassFtp, //"1wiAwNRa"
            PortNumber = 22,
            GiveUpSecurityAndAcceptAnySshHostKey = true
        };


        public void Genera_Interface_Lectura()
        {
            //verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
            Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
            objCreaCarpeta.ArchivaInterface("WMS");

            bool exito = false;
            string wtipo = "";
            try
            {
                LogUtil.Graba_Log(Interface, "******* INICIO DE LECTURA TXT *******", false, "");

                if (Lee_Descarga_Archivo())// Descarga los archivos del FTP a la Carpeta WORK (Local)
                {
                    string wruta_50001 = "\\50001\\";
                    string wruta_50003 = "\\50003\\";


                    for (int xi = 1; xi <= 3; xi++)
                    {
                            if (xi == 1) wtipo = "OBL";
                            if (xi == 2) wtipo = "SVH";
                            if (xi == 3) wtipo = "IHT";

                        if (Graba_Sql(wtipo)) // Llenamos los Arreglos segun los Tipos
                        {
                            //return;
                            if (Archiva_TXT(Arr_Txt_Obl, Arr_Txt_Obs, Arr_Txt_Svh, Arr_Txt_Svd, Arr_Txt_Iht, wruta_50001, wtipo) && Archiva_TXT(Arr_Txt_Obl, Arr_Txt_Obs, Arr_Txt_Svh, Arr_Txt_Svd, Arr_Txt_Iht, wruta_50003, wtipo))
                            {
                                if (Borra_FTP(Arr_Txt_Obl, Arr_Txt_Obs, Arr_Txt_Svh, Arr_Txt_Svd, Arr_Txt_Iht, wruta_50001, wtipo) && Borra_FTP(Arr_Txt_Obl, Arr_Txt_Obs, Arr_Txt_Svh, Arr_Txt_Svd, Arr_Txt_Iht, wruta_50003, wtipo))
                                {
                                    exito = true;
                                }
                            }
                        }
                    }//for



                    if (exito)
                    {
                        //LogUtil.Graba_Log(Interface, "LECTURA DE ARCHIVOS DE TEXTO OK", false, "");
                    }
                    else
                    {
                        //LogUtil.Graba_Log(Interface, "NO EXISTE INFORMACION PARA LEER", false, "");
                    }

                }
            }


            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "ERROR: " + ex.Message, true, "");
            }
            finally
            {
                LogUtil.Graba_Log(Interface, "******* FIN DE LECTURA DE TXT *********", false, "");
                LogUtil.Graba_Log(Interface, " ", false, "");

            }
        }
        //****************************************************************************

        public bool Lee_Descarga_Archivo()
        {
            LogUtil.Graba_Log(Interface, "   Inicio Lee_Descarga_Archivo", false, "");
            bool exito = false;


            using (Session session = new Session())
            {

                // Connect
                session.Open(sessionOptions);


                // Upload files
                TransferOptions transferOptions = new TransferOptions();
                transferOptions.FilePermissions = null; // This is default
                transferOptions.PreserveTimestamp = false;
                transferOptions.TransferMode = TransferMode.Binary;
                transferOptions.FileMask = "*.*|success/";
                //transferOptions.FileMask = "*.tmp|success/";

                TransferOperationResult transferResult1;
                TransferOperationResult transferResult2;

                // Lee y copia los archivos al disco local, carpeta Temporal
                transferResult1 = session.GetFiles("/Peru/730/50001/output/", Crear_Carpetas.WORK, false, transferOptions);
                transferResult2 = session.GetFiles("/Peru/730/50003/output/", Crear_Carpetas.WORK, false, transferOptions);

                // Throw on any error
                transferResult1.Check();
                transferResult2.Check();

                exito = (transferResult1.IsSuccess && transferResult2.IsSuccess);
                LogUtil.Graba_Log(Interface, "   Finaliza Lee_Descarga_Archivo", false, "");
            }

            return exito;
        }

        private bool Graba_Sql(string wtipo)
        {
            bool exito = false;
            string error = "";
            string ArchiOBL = "", ArchiOBS = "", ArchiSVH = "", ArchiSVD = "", ArchiIHT = "";

            string sqlquery = "USP_INSERTAR_TEMP_OBL_OBS_SVH_SVD"; // Copia
            try
            {

                string carpetatemporal = Crear_Carpetas.WORK;

                //var allFiles = Directory.GetFiles(@"C:\Path\", "");
                //var filesToExclude = Directory.GetFiles(@"C:\Path\", "*.txt");
                //var wantedFiles = allFiles.Except(filesToExclude);



                Arr_Txt_Obl = Directory.GetFiles(@carpetatemporal, "OBL*.*");//cabecera



                //Aplicando el Filtro, que no incluya los *.tmp en el arreglo
                Arr_Txt_Obs = Directory.GetFiles(@carpetatemporal, "OBS*.*");//detalle

                //var allFiles = Directory.GetFiles(@carpetatemporal, "OBS*.*");
                //var filesToExclude = Directory.GetFiles(@carpetatemporal, "*.tmp");
                //var Arr_Txt_Obs = allFiles.Except(filesToExclude);





                Arr_Txt_Svh = Directory.GetFiles(@carpetatemporal, "SVH*.*");//cabecera
                Arr_Txt_Svd = Directory.GetFiles(@carpetatemporal, "SVD*.*");//detalle

                Arr_Txt_Iht = Directory.GetFiles(@carpetatemporal, "IHT*.*");

                DataTable dt_OBL = new DataTable();
                DataTable dt_OBS = new DataTable();

                //--------- Creamos tabla OBL ----------//
                dt_OBL.Columns.Add("hdr_group_nbr", typeof(string));
                dt_OBL.Columns.Add("facility_code", typeof(string));
                dt_OBL.Columns.Add("company_code", typeof(string));
                dt_OBL.Columns.Add("action_code", typeof(string));
                dt_OBL.Columns.Add("load_type", typeof(string));
                dt_OBL.Columns.Add("load_manifest_nbr", typeof(string));
                dt_OBL.Columns.Add("trailer_nbr", typeof(string));
                dt_OBL.Columns.Add("total_nbr_of_oblpns", typeof(int));
                dt_OBL.Columns.Add("total_weight", typeof(Decimal));
                dt_OBL.Columns.Add("total_volume", typeof(Decimal));
                dt_OBL.Columns.Add("total_shipping_charge", typeof(Decimal));
                dt_OBL.Columns.Add("ship_date", typeof(DateTime));
                dt_OBL.Columns.Add("ship_date_time", typeof(DateTime));


                //--------- Creamos tabla OBS ----------//
                dt_OBS.Columns.Add("hdr_group_nbr", typeof(string));                //0
                dt_OBS.Columns.Add("facility_code", typeof(string));                //1
                dt_OBS.Columns.Add("company_code", typeof(string));                 //2
                dt_OBS.Columns.Add("load_manifest_nbr", typeof(string));            //4
                dt_OBS.Columns.Add("line_nbr", typeof(int));                        //5
                dt_OBS.Columns.Add("seq_nbr", typeof(int));                         //6
                dt_OBS.Columns.Add("stop_nbr_of_oblpns", typeof(int));              //9
                dt_OBS.Columns.Add("stop_weight", typeof(Decimal));                 //10
                dt_OBS.Columns.Add("stop_volume", typeof(Decimal));                 //11
                dt_OBS.Columns.Add("shipto_facility_code", typeof(string));         //13

                dt_OBS.Columns.Add("dest_facility_code", typeof(string));           //25
                dt_OBS.Columns.Add("cust_email", typeof(string));                   //35    nuevo 20/03/2021
                dt_OBS.Columns.Add("cust_contact", typeof(string));                 //36    nuevo 20/03/2021

                dt_OBS.Columns.Add("order_nbr", typeof(string));                    //38
                dt_OBS.Columns.Add("ord_date", typeof(DateTime));                   //39
                dt_OBS.Columns.Add("req_ship_date", typeof(DateTime));              //41
                dt_OBS.Columns.Add("customer_po_nbr", typeof(string));              //45
                dt_OBS.Columns.Add("dest_dept_nbr", typeof(string));
                dt_OBS.Columns.Add("order_hdr_cust_field_1", typeof(string));
                dt_OBS.Columns.Add("order_hdr_cust_field_2", typeof(string));
                dt_OBS.Columns.Add("order_hdr_cust_field_3", typeof(string));
                dt_OBS.Columns.Add("order_hdr_cust_field_5", typeof(string));//Observacion
                dt_OBS.Columns.Add("order_seq_nbr", typeof(int));

                dt_OBS.Columns.Add("ob_lpn_nbr", typeof(string));
                dt_OBS.Columns.Add("item_alternate_code", typeof(string));
                dt_OBS.Columns.Add("item_part_a", typeof(string));
                dt_OBS.Columns.Add("item_part_b", typeof(string));
                dt_OBS.Columns.Add("item_part_c", typeof(string));
                dt_OBS.Columns.Add("item_part_d", typeof(string));
                dt_OBS.Columns.Add("pre_pack_code", typeof(string));
                dt_OBS.Columns.Add("pre_pack_ratio", typeof(Decimal));
                dt_OBS.Columns.Add("pre_pack_ratio_seq", typeof(int));
                dt_OBS.Columns.Add("hazmat", typeof(Boolean));

                dt_OBS.Columns.Add("shipped_uom", typeof(string));
                dt_OBS.Columns.Add("shipped_qty", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_weight", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_volume", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_shipping_charge", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_type", typeof(string));
                dt_OBS.Columns.Add("order_hdr_cust_number_1", typeof(int));
                dt_OBS.Columns.Add("order_hdr_cust_number_2", typeof(int));
                dt_OBS.Columns.Add("order_hdr_cust_number_3", typeof(int));
                dt_OBS.Columns.Add("order_hdr_cust_number_4", typeof(int));

                
                dt_OBS.Columns.Add("invn_attr_f", typeof(string));
                dt_OBS.Columns.Add("order_type", typeof(string));

                dt_OBS.Columns.Add("ob_lpn_length", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_width", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_height", typeof(decimal));
                dt_OBS.Columns.Add("order_hdr_cust_short_text_1", typeof(string));
                dt_OBS.Columns.Add("order_hdr_cust_short_text_5", typeof(string));//Nombre del CLiente original
                dt_OBS.Columns.Add("order_dtl_cust_short_text_1", typeof(string));// Numero de Orden de Compra

                dt_OBS.Columns.Add("erp_fulfillment_line_ref", typeof(int));
                dt_OBS.Columns.Add("sales_order_line_ref", typeof(string));
                dt_OBS.Columns.Add("sales_order_schedule_ref", typeof(string));
                dt_OBS.Columns.Add("tms_order_hdr_ref", typeof(string));
                dt_OBS.Columns.Add("tms_order_dtl_ref", typeof(string));


                DataTable dt_SVH = new DataTable();
                DataTable dt_SVD = new DataTable();

                //--------- Creamos tabla SVH ----------//
                dt_SVH.Columns.Add("hdr_group_nbr", typeof(string));
                dt_SVH.Columns.Add("shipment_nbr", typeof(string));
                dt_SVH.Columns.Add("facility_code", typeof(string));
                dt_SVH.Columns.Add("company_code", typeof(string));
                dt_SVH.Columns.Add("trailer_nbr", typeof(string));
                dt_SVH.Columns.Add("shipment_type", typeof(string));
                dt_SVH.Columns.Add("load_nbr", typeof(string));
                dt_SVH.Columns.Add("manifest_nbr", typeof(string)); // (Numero de Guia del Proveedor) Ultimo 12-08-2019 a pedido de Julito
                dt_SVH.Columns.Add("origin_info", typeof(string));
                dt_SVH.Columns.Add("orig_shipped_units", typeof(decimal));
                dt_SVH.Columns.Add("shipped_date", typeof(DateTime));
                dt_SVH.Columns.Add("shipment_hdr_cust_field_1", typeof(string));
                dt_SVH.Columns.Add("shipment_hdr_cust_field_3", typeof(string)); // Ruc del Asociado
                dt_SVH.Columns.Add("shipment_hdr_cust_field_4", typeof(string)); // Numero de Nota de Debito
                dt_SVH.Columns.Add("shipment_hdr_cust_field_5", typeof(string));
                dt_SVH.Columns.Add("verification_date", typeof(DateTime));

                //--------- Creamos tabla SVD ----------//
                dt_SVD.Columns.Add("hdr_group_nbr", typeof(string));
                dt_SVD.Columns.Add("shipment_nbr", typeof(string));
                dt_SVD.Columns.Add("facility_code", typeof(string));
                dt_SVD.Columns.Add("company_code", typeof(string));
                dt_SVD.Columns.Add("seq_nbr", typeof(int));
                dt_SVD.Columns.Add("lpn_nbr", typeof(string));
                dt_SVD.Columns.Add("lpn_weight", typeof(decimal));
                dt_SVD.Columns.Add("lpn_volume", typeof(decimal));
                dt_SVD.Columns.Add("item_alternate_code", typeof(string));
                dt_SVD.Columns.Add("item_part_a", typeof(string));

                dt_SVD.Columns.Add("item_part_b", typeof(string));
                dt_SVD.Columns.Add("item_part_c", typeof(string));
                dt_SVD.Columns.Add("item_part_d", typeof(string));
                dt_SVD.Columns.Add("pre_pack_code", typeof(string));
                dt_SVD.Columns.Add("pre_pack_ratio", typeof(decimal));
                dt_SVD.Columns.Add("pre_pack_ratio_seq", typeof(decimal));
                dt_SVD.Columns.Add("pre_pack_total_units", typeof(decimal));
                dt_SVD.Columns.Add("invn_attr_a", typeof(string));
                dt_SVD.Columns.Add("invn_attr_b", typeof(string));
                dt_SVD.Columns.Add("invn_attr_c", typeof(string)); //Codigo de Descuento

                dt_SVD.Columns.Add("invn_attr_d", typeof(string)); //Codigo de Resultado
                dt_SVD.Columns.Add("shipped_qty", typeof(decimal));
                dt_SVD.Columns.Add("putaway_type", typeof(string));                 // Ultimos
                dt_SVD.Columns.Add("shipment_dtl_cust_field_1", typeof(string));    // Ultimos
                dt_SVD.Columns.Add("shipment_dtl_cust_field_2", typeof(string));    // Ultimos
                dt_SVD.Columns.Add("shipment_dtl_cust_field_3", typeof(string));    // Codigo de Proveedor
                dt_SVD.Columns.Add("priority_date", typeof(DateTime));
                dt_SVD.Columns.Add("po_nbr", typeof(string));
                dt_SVD.Columns.Add("pallet_nbr", typeof(string));
                dt_SVD.Columns.Add("received_qty", typeof(decimal));

                dt_SVD.Columns.Add("po_seq_nbr", typeof(int));



                DataTable dt_IHT = new DataTable();
                //--------- Creamos tabla IHT ----------//
                dt_IHT.Columns.Add("group_nbr", typeof(int));
                dt_IHT.Columns.Add("seq_nbr", typeof(int));
                dt_IHT.Columns.Add("facility_code", typeof(string));
                dt_IHT.Columns.Add("company_code", typeof(string));
                dt_IHT.Columns.Add("activity_code", typeof(int));
                dt_IHT.Columns.Add("reason_code", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("lock_code", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("lpn_nbr", typeof(string));
                dt_IHT.Columns.Add("location", typeof(string)); // 23-11-2020
                dt_IHT.Columns.Add("item_code", typeof(string));
                dt_IHT.Columns.Add("item_alternate_code", typeof(string));
                dt_IHT.Columns.Add("item_part_a", typeof(string));
                dt_IHT.Columns.Add("item_part_b", typeof(string));
                dt_IHT.Columns.Add("item_part_c", typeof(string));
                dt_IHT.Columns.Add("item_part_d", typeof(string));
                dt_IHT.Columns.Add("item_description", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("shipment_nbr", typeof(string));
                dt_IHT.Columns.Add("po_nbr", typeof(string));
                dt_IHT.Columns.Add("po_line_nbr", typeof(int));
                dt_IHT.Columns.Add("order_seq_nbr", typeof(int));
                dt_IHT.Columns.Add("orig_qty", typeof(decimal)); // Ultimo
                dt_IHT.Columns.Add("adj_qty", typeof(decimal));
                dt_IHT.Columns.Add("lpns_shipped", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("units_shipped", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("lpns_received", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("units_received", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("ref_code_1", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("ref_value_1", typeof(string));
                dt_IHT.Columns.Add("ref_value_2", typeof(string));
                dt_IHT.Columns.Add("ref_value_3", typeof(string));
                dt_IHT.Columns.Add("create_date", typeof(DateTime));
                dt_IHT.Columns.Add("shipment_line_nbr", typeof(int));
                dt_IHT.Columns.Add("work_order_seq_nbr", typeof(int));
                dt_IHT.Columns.Add("screen_name", typeof(string));
                dt_IHT.Columns.Add("module_name", typeof(string));
                dt_IHT.Columns.Add("order_type", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("shipment_type", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("po_type", typeof(string)); // Nuevo
                dt_IHT.Columns.Add("billing_location_type", typeof(string)); // Nuevo


                // Clonamos Data Table
                var dtAux_OBS = dt_OBS.Clone();
                var dtAux_SVD = dt_SVD.Clone();
                var dtAux_IHT = dt_IHT.Clone();

                if (wtipo == "OBL")
                {
                    if ((Arr_Txt_Obl.Length == 0) && (Arr_Txt_Obs.Length == 0)) // Si no hay informacion... retorna
                    {
                        return false;
                    }
                    else
                    {

                    }//If


                    LogUtil.Graba_Log(Interface, "   Inicia Graba_Sql, tipo: " + wtipo, false, "");

                    if (Arr_Txt_Obl.Length > 0) //CABECERA
                    {
                        for (Int32 i = 0; i < Arr_Txt_Obl.Length; ++i)
                        {
                            try
                            {
                                ArchiOBL = Arr_Txt_Obl[i].ToString();

                                string fichero = Crear_Carpetas.WORK + Path.GetFileName(Arr_Txt_Obl[i]);
                                string[] lineas = File.ReadAllLines(fichero);

                                LogUtil.Graba_Log(Interface, "      Inicia temporal OBL, tipo: " + wtipo, false, "");
                                foreach (string lin in lineas)
                                {
                                    string[] campos = lin.Split('|');

                                    string whdr_group_nbr = campos[0].ToString();

                                    string wfacility_code = campos[1].ToString();
                                    string wcompany_code = campos[2].ToString();
                                    string waction_code = campos[3].ToString();
                                    string wload_type = campos[4].ToString();
                                    string wload_manifest_nbr = campos[5].ToString();
                                    string wtrailer_nbr = campos[6].ToString();

                                    string wtotal_nbr_of_oblpns = campos[14].ToString();
                                    string wtotal_weight = campos[15].ToString();
                                    string wtotal_volume = campos[16].ToString();
                                    string wtotal_shipping_charge = campos[17].ToString();

                                    DateTime wship_date = DateTime.ParseExact(campos[18].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                    DateTime wship_date_time = DateTime.ParseExact(campos[22].ToString(), "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture);

                                    dt_OBL.Rows.Add(whdr_group_nbr, wfacility_code, wcompany_code, waction_code, wload_type, wload_manifest_nbr, wtrailer_nbr,
                                    wtotal_nbr_of_oblpns, wtotal_weight, wtotal_volume, wtotal_shipping_charge, wship_date, wship_date_time);
                                }//for
                                LogUtil.Graba_Log(Interface, "      Finaliza temporal OBL, tipo: " + wtipo, false, "");
                            }//try
                            catch (Exception ex)
                            {
                                LogUtil.Graba_Log(Interface, "Error en Graba_Sql " + ex.Message.ToString(), true, Path.GetFileName(ArchiOBL));
                                Crear_Carpetas objCrearCarpetas = new Crear_Carpetas();
                                objCrearCarpetas.ArchivaInterface("RECYCLER_LEER");

                                if (File.Exists(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiOBL))) File.Delete(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiOBL));
                                File.Move(ArchiOBL, Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiOBL)); // si el archivo esta con errores se mueve a la carpeta de reciclaje de archivos 
                            }//catch
                        }//for
                    }//if


                    // Valida Archivo por Archivo
                    //----------------------------//

                    if (Arr_Txt_Obs.Length > 0) //DETALLE
                    {
                        LogUtil.Graba_Log(Interface, "      Inicia temporal OBS, tipo: " + wtipo, false, "");
                        for (Int32 i = 0; i < Arr_Txt_Obs.Length; ++i)
                        {
                            dtAux_OBS.Clear();// Blanqueamnos DT por cada Archivo
                            //dtAux_OBS = null;

                            bool Flg_Obs_Ok = true;

                            ArchiOBS = Arr_Txt_Obs[i].ToString();
                            string fichero2 = Crear_Carpetas.WORK + Path.GetFileName(Arr_Txt_Obs[i]);
                            string[] lineas2 = File.ReadAllLines(fichero2);

                            foreach (string lin in lineas2)
                            {
                                try
                                {
                                    string[] campos = lin.Split('|');
                                    string witem_alternate_code = campos[61].ToString();
                                    string witem_part_a = campos[62].ToString();

                                    if (witem_alternate_code.Contains("PADRE02") || (witem_part_a.Contains("PADRE02")))
                                    {
                                        LogUtil.Graba_Log(Interface, "Error: Registro Padre ", true, Path.GetFileName(ArchiOBS.ToString()));
                                    }
                                    else
                                    {
                                        string whdr_group_nbr = campos[0].ToString();
                                        string wfacility_code = campos[1].ToString();
                                        string wcompany_code = campos[2].ToString();
                                        string wload_manifest_nbr = campos[4].ToString();
                                        string wline_nbr = campos[5].ToString();
                                        string wseq_nbr = campos[6].ToString();

                                        string wstop_nbr_of_oblpns = campos[9].ToString();
                                        string wstop_weight = campos[10].ToString();
                                        string wstop_volume = campos[11].ToString();
                                        string wshipto_facility_code = campos[13].ToString();

                                        string wdest_facility_code = campos[25].ToString();
                                        string wcust_email = campos[35].ToString();
                                        string wcust_contact = campos[36].ToString();

                                        string worder_nbr = campos[38].ToString();
                                        string worder_type = campos[161].ToString();


                                        DateTime word_date = DateTime.ParseExact(campos[39].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                        DateTime wreq_ship_date = DateTime.ParseExact(campos[39].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);

                                        if (worder_type == "6C")
                                        {
                                            worder_nbr = "V" + worder_nbr;
                                            if (string.IsNullOrEmpty(campos[41].ToString()))
                                            {
                                                wreq_ship_date = word_date;
                                            }
                                            else
                                            {
                                                wreq_ship_date = DateTime.ParseExact(campos[41].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                            }
                                        }

                                        string wcustomer_po_nbr = campos[45].ToString();
                                        string wdest_dept_nbr = campos[48].ToString();
                                        string worder_hdr_cust_field_1 = campos[49].ToString();

                                        string s = campos[50].ToString();
                                        int end = s.IndexOf(" ");
                                        end = end == -1 ? s.Length : end;
                                        string worder_hdr_cust_field_2 = s.Substring(0, end);

                                        string worder_hdr_cust_field_3 = campos[51].ToString();
                                        string worder_hdr_cust_field_5 = campos[53].ToString();


                                        string worder_seq_nbr = campos[54].ToString();
                                        string wob_lpn_nbr = campos[60].ToString();
                                        string witem_part_b = campos[63].ToString();
                                        string witem_part_c = campos[64].ToString();
                                        string witem_part_d = campos[65].ToString();
                                        string wpre_pack_code = campos[68].ToString();

                                        string wpre_pack_ratio = campos[69].ToString();
                                        string wpre_pack_ratio_seq = campos[70].ToString();
                                        string whazmat = campos[75].ToString();

                                        string wshipped_uom = campos[76].ToString();
                                        string wshipped_qty = campos[77].ToString();
                                        string wob_lpn_weight = campos[88].ToString();
                                        string wob_lpn_volume = campos[89].ToString();

                                        string wob_lpn_shipping_charge = campos[90].ToString();
                                        string wob_lpn_type = campos[91].ToString();

                                        string worder_hdr_cust_number_1 = campos[102].ToString();
                                        string worder_hdr_cust_number_2 = campos[103].ToString();
                                        string worder_hdr_cust_number_3 = campos[104].ToString();
                                        string worder_hdr_cust_number_4 = campos[105].ToString();

                                        string worder_hdr_cust_short_text_1 = campos[112].ToString();
                                        string worder_hdr_cust_short_text_5 = campos[116].ToString();//Cliente Original

                                        string worder_dtl_cust_short_text_1 = campos[142].ToString();// Numero de Orden de Compra

                                        string winvn_attr_f = campos[159].ToString();
//                                        string worder_type = campos[161].ToString();
                                        string wob_lpn_length = campos[165].ToString();
                                        string wob_lpn_width = campos[166].ToString();
                                        string wob_lpn_height = campos[167].ToString();

                                        int werp_fulfillment_line_ref = 0;


                                        if (campos[181].ToString() != "")
                                        {
                                            werp_fulfillment_line_ref = Convert.ToInt32(campos[181]);
                                        }

                                        string wsales_order_line_ref = campos[182].ToString();
                                        string wsales_order_schedule_ref = campos[183].ToString();
                                        string wtms_order_hdr_ref = campos[184].ToString();
                                        string wtms_order_dtl_ref = campos[185].ToString();


                                        dtAux_OBS.Rows.Add(whdr_group_nbr, wfacility_code, wcompany_code, wload_manifest_nbr, wline_nbr, wseq_nbr, wstop_nbr_of_oblpns,
                                            wstop_weight, wstop_volume, wshipto_facility_code, wdest_facility_code, wcust_email, wcust_contact, worder_nbr, word_date, wreq_ship_date, wcustomer_po_nbr,
                                            wdest_dept_nbr, worder_hdr_cust_field_1, worder_hdr_cust_field_2, worder_hdr_cust_field_3, worder_hdr_cust_field_5,
                                            worder_seq_nbr, wob_lpn_nbr, witem_alternate_code, witem_part_a, witem_part_b, witem_part_c, witem_part_d,
                                            wpre_pack_code, wpre_pack_ratio, wpre_pack_ratio_seq, whazmat, wshipped_uom, wshipped_qty, wob_lpn_weight,
                                            wob_lpn_volume, wob_lpn_shipping_charge, wob_lpn_type, worder_hdr_cust_number_1, worder_hdr_cust_number_2,
                                            worder_hdr_cust_number_3, worder_hdr_cust_number_4, winvn_attr_f, worder_type, wob_lpn_length, wob_lpn_width, wob_lpn_height,
                                            worder_hdr_cust_short_text_1, worder_hdr_cust_short_text_5, worder_dtl_cust_short_text_1, werp_fulfillment_line_ref, wsales_order_line_ref, wsales_order_schedule_ref, wtms_order_hdr_ref, wtms_order_dtl_ref);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    LogUtil.Graba_Log(Interface, "Error en Graba_Sql " + ex.Message.ToString(), true, Path.GetFileName(ArchiOBS));
                                    Crear_Carpetas objCrearCarpetas = new Crear_Carpetas();
                                    objCrearCarpetas.ArchivaInterface("RECYCLER_LEER");

                                    if (File.Exists(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiOBS))) File.Delete(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiOBS));
                                    File.Move(ArchiOBS, Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiOBS.ToString())); // si el archivo esta con errores se mueve a la carpeta de reciclaje de archivos

                                    Flg_Obs_Ok = false;

                                    //Cambiamos nombre del elemento del arreglo
                                    Arr_Txt_Obs[i] = "XXX";
                                }//catch
                            }//foreach

                            // Añade los registros Correctos de cada Archivo
                            if (Flg_Obs_Ok == true)
                            {
                                foreach (DataRow datRow in dtAux_OBS.Rows)
                                {
                                    dt_OBS.ImportRow(datRow);
                                }
                            }
                        }//for del arreglo
                        LogUtil.Graba_Log(Interface, "      Finaliza temporal OBS, tipo: " + wtipo, false, "");
                    }//if
                }//If wtipo = 'OBL'


                if (wtipo == "SVH")
                {
                    if ((Arr_Txt_Svh.Length == 0) && (Arr_Txt_Svd.Length == 0))// Si no hay informacion... retorna
                    {
                        return false;
                    }
                    else
                    {
                    }//If


                    LogUtil.Graba_Log(Interface, "   Inicia Graba_Sql, tipo: " + wtipo, false, "");
                    if (Arr_Txt_Svh.Length > 0) //cabecera
                    {
                        LogUtil.Graba_Log(Interface, "      Inicia temporal SVH, tipo: " + wtipo, false, "");
                        for (Int32 i = 0; i < Arr_Txt_Svh.Length; ++i)
                        {
                            try
                            {
                                ArchiSVH = Arr_Txt_Svh[i].ToString();

                                string fichero = Crear_Carpetas.WORK + Path.GetFileName(Arr_Txt_Svh[i]);
                                string[] lineas = File.ReadAllLines(fichero);

                                foreach (string lin in lineas)
                                {
                                    string[] campos = lin.Split('|');

                                    string whdr_group_nbr = campos[0].ToString();
                                    string wshipment_nbr = campos[1].ToString();
                                    string wfacility_code = campos[2].ToString();
                                    string wcompany_code = campos[3].ToString();
                                    string wtrailer_nbr = campos[4].ToString();

                                    string wshipment_type = campos[6].ToString();
                                    string worigin_info = campos[11].ToString();

                                    string wload_nbr = campos[7].ToString();
                                    string wmanifest_nbr = campos[8].ToString();
                                    string worig_shipped_units = campos[13].ToString();
                                    DateTime wshipped_date = DateTime.ParseExact(campos[14].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);

                                    string wshipment_hdr_cust_field_1 = campos[16].ToString();
                                    string wshipment_hdr_cust_field_3 = campos[18].ToString();
                                    string wshipment_hdr_cust_field_4 = campos[19].ToString();
                                    string wshipment_hdr_cust_field_5 = campos[20].ToString();

                                    DateTime wverification_date = DateTime.ParseExact(campos[21].ToString(), "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture);

                                    dt_SVH.Rows.Add(whdr_group_nbr, wshipment_nbr, wfacility_code, wcompany_code, wtrailer_nbr, wshipment_type, worigin_info, wload_nbr,
                                    wmanifest_nbr, worig_shipped_units, wshipped_date, wshipment_hdr_cust_field_1, wshipment_hdr_cust_field_3, wshipment_hdr_cust_field_4, wshipment_hdr_cust_field_5, wverification_date);
                                }//for
                            }//try
                            catch (Exception ex)
                            {
                                LogUtil.Graba_Log(Interface, "Error en Graba_Sql " + ex.Message.ToString(), true, Path.GetFileName(ArchiSVH.ToString()));
                                Crear_Carpetas objCrearCarpetas = new Crear_Carpetas();
                                objCrearCarpetas.ArchivaInterface("RECYCLER_LEER");

                                if (File.Exists(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiSVH))) File.Delete(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiSVH));
                                File.Move(ArchiSVH.ToString(), Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiSVH.ToString())); // si el archivo esta con errores se mueve a la carpeta de reciclaje de archivos
                            }//catch
                        }//for
                        LogUtil.Graba_Log(Interface, "      Finaliza temporal SVH, tipo: " + wtipo, false, "");
                    }//if



                    if (Arr_Txt_Svd.Length > 0) //detalle
                    {
                        LogUtil.Graba_Log(Interface, "      Inicia temporal SVD, tipo: " + wtipo, false, "");
                        for (Int32 i = 0; i < Arr_Txt_Svd.Length; ++i)
                        {
                            dtAux_SVD.Clear();// Blanqueamnos DT por cada Archivo
                            bool Flg_Svd_Ok = true;
                            ArchiSVD = Arr_Txt_Svd[i].ToString();
                            string fichero = Crear_Carpetas.WORK + Path.GetFileName(Arr_Txt_Svd[i]);
                            string[] lineas = File.ReadAllLines(fichero);

                            foreach (string lin in lineas)
                            {
                                try
                                {
                                    string[] campos = lin.Split('|');
                                    string whdr_group_nbr = campos[0].ToString();
                                    string wshipment_nbr = campos[1].ToString();
                                    string wfacility_code = campos[2].ToString();
                                    string wcompany_code = campos[3].ToString();
                                    string wseq_nbr = campos[4].ToString();
                                    string wlpn_nbr = campos[5].ToString();
                                    string wlpn_weight = campos[6].ToString();
                                    string wlpn_volume = campos[7].ToString();
                                    string witem_alternate_code = campos[8].ToString();

                                    string witem_part_a = campos[9].ToString();
                                    string witem_part_b = campos[10].ToString();
                                    string witem_part_c = campos[11].ToString();
                                    string witem_part_d = campos[12].ToString();

                                    string wpre_pack_code = campos[15].ToString();
                                    string wpre_pack_ratio = campos[16].ToString();
                                    string wpre_pack_ratio_seq = campos[17].ToString();
                                    string wpre_pack_total_units = campos[18].ToString();
                                    string winvn_attr_a = campos[19].ToString();
                                    string winvn_attr_b = campos[20].ToString(); // Número de Factura
                                    string winvn_attr_c = campos[21].ToString(); 


                                    string wshipped_qty = campos[22].ToString();

                                    string wputaway_type = campos[26].ToString();
                                    string wshipment_dtl_cust_field_1 = campos[31].ToString();
                                    string wshipment_dtl_cust_field_2 = campos[32].ToString();
                                    string wshipment_dtl_cust_field_3 = campos[33].ToString();

                                    DateTime wpriority_date = Convert.ToDateTime("1900/01/01");


                                    if (campos[23].ToString() != "")
                                    {
                                        wpriority_date = DateTime.ParseExact(campos[23].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                    }


                                    string wpo_nbr = campos[24].ToString();
                                    string wpallet_nbr = campos[25].ToString();

                                    string wreceived_qty = campos[27].ToString();

                                    int wpo_seq_nbr = 0;
                                    if (campos[37].ToString() != "")
                                    {
                                        wpo_seq_nbr = Convert.ToInt32(campos[37]);
                                    }
                                    string winvn_attr_d = campos[40].ToString();


                                    dtAux_SVD.Rows.Add(whdr_group_nbr, wshipment_nbr, wfacility_code, wcompany_code, wseq_nbr, wlpn_nbr, wlpn_weight, wlpn_volume,
                                        witem_alternate_code, witem_part_a, witem_part_b, witem_part_c, witem_part_d, wpre_pack_code, wpre_pack_ratio,
                                        wpre_pack_ratio_seq, wpre_pack_total_units, winvn_attr_a, winvn_attr_b, winvn_attr_c, winvn_attr_d, wshipped_qty, wputaway_type, wshipment_dtl_cust_field_1,
                                        wshipment_dtl_cust_field_2, wshipment_dtl_cust_field_3, wpriority_date, wpo_nbr, wpallet_nbr, wreceived_qty, wpo_seq_nbr);
                                }
                                catch (Exception ex)
                                {
                                    LogUtil.Graba_Log(Interface, "Error en Graba_Sql " + ex.Message.ToString(), true, Path.GetFileName(ArchiSVD));
                                    Crear_Carpetas objCrearCarpetas = new Crear_Carpetas();
                                    objCrearCarpetas.ArchivaInterface("RECYCLER_LEER");

                                    if (File.Exists(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiSVD))) File.Delete(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiSVD));
                                    File.Move(ArchiSVD, Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiSVD)); // si el archivo esta con errores se mueve a la carpeta de reciclaje de archivos

                                    Flg_Svd_Ok = false;

                                    //Cambiamos nombre del elemento del arreglo
                                    Arr_Txt_Svd[i] = "XXX";
                                }//try
                            }//foreach

                            // Añade los registros Correctos de cada Archivo
                            if (Flg_Svd_Ok == true)
                            {
                                foreach (DataRow datRow in dtAux_SVD.Rows)
                                {
                                    dt_SVD.ImportRow(datRow);
                                }
                            }
                        }//for del arreglo
                        LogUtil.Graba_Log(Interface, "      Finaliza temporal SVD, tipo: " + wtipo, false, "");
                    }//if
                }//if wtipo = 'SVH'







                if (wtipo == "IHT")
                {
                    if ((Arr_Txt_Iht.Length == 0)) // Si no hay informacion... retorna
                    {
                        return false;
                    }
                    else
                    {
                    }//If


                    LogUtil.Graba_Log(Interface, "   Inicia Graba_Sql, tipo: " + wtipo, false, "");
                    if (Arr_Txt_Iht.Length > 0)
                    {
                        LogUtil.Graba_Log(Interface, "      Inicia temporal IHT, tipo: " + wtipo, false, "");
                        for (Int32 i = 0; i < Arr_Txt_Iht.Length; ++i)
                        {
                            dtAux_IHT.Clear();// Blanqueamnos DT por cada Archivo
                            bool Flg_Iht_Ok = true;
                            ArchiIHT = Arr_Txt_Iht[i].ToString();
                            string fichero = Crear_Carpetas.WORK + Path.GetFileName(Arr_Txt_Iht[i]);
                            string[] lineas = File.ReadAllLines(fichero);

                            foreach (string lin in lineas)
                            {
                                try
                                {
                                    string[] campos = lin.Split('|');

                                    string wgroup_nbr = campos[0].ToString();
                                    string wseq_nbr = campos[1].ToString();
                                    string wfacility_code = campos[2].ToString();
                                    string wcompany_code = campos[3].ToString();
                                    string wactivity_code = campos[4].ToString();

                                    string wreason_code = campos[5].ToString();
                                    string wlock_code = campos[6].ToString();

                                    string wlpn_nbr = campos[7].ToString();
                                    string wlocation = campos[8].ToString();//23-11-2020
                                    string witem_code = campos[9].ToString();
                                    string witem_alternate_code = campos[10].ToString();

                                    string witem_part_a = campos[11].ToString();
                                    string witem_part_b = campos[12].ToString();
                                    string witem_part_c = campos[13].ToString();
                                    string witem_part_d = campos[14].ToString();

                                    string witem_description = campos[17].ToString();

                                    string wshipment_nbr = campos[18].ToString();
                                    string wpo_nbr = campos[20].ToString();

                                    string wpo_line_nbr = campos[21].ToString();
                                    string worder_seq_nbr = campos[24].ToString();

                                    string wadj_qty, worig_qty = "";


                                    if (campos[26].ToString() == "")
                                    {
                                        worig_qty = "0";
                                    }
                                    else
                                    {
                                        worig_qty = campos[26].ToString();
                                    }


                                    if (campos[27].ToString() == "")
                                    {
                                        wadj_qty = "0";
                                    }
                                    else
                                    {
                                        wadj_qty = campos[27].ToString();
                                    }

                                    string wlpns_shipped = campos[28].ToString();
                                    string wunits_shipped = campos[29].ToString();
                                    string wlpns_received = campos[30].ToString();
                                    string wunits_received = campos[31].ToString();
                                    string wref_code_1 = campos[32].ToString();

                                    string wref_value_1 = campos[33].ToString();
                                    string wref_value_2 = campos[35].ToString();
                                    string wref_value_3 = campos[37].ToString();

                                    DateTime wcreate_date = DateTime.ParseExact(campos[42].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);

                                    string wshipment_line_nbr = campos[46].ToString();
                                    string wwork_order_seq_nbr = campos[53].ToString();
                                    string wscreen_name = campos[54].ToString();
                                    string wmodule_name = campos[55].ToString();

                                    string worder_type = campos[70].ToString();
                                    string wshipment_type = campos[71].ToString();
                                    string wpo_type = campos[72].ToString();
                                    string wbilling_location_type = campos[73].ToString();

                                    dtAux_IHT.Rows.Add(wgroup_nbr, wseq_nbr, wfacility_code, wcompany_code, wactivity_code, wreason_code, wlock_code,
                                        wlpn_nbr, wlocation, witem_code, witem_alternate_code, witem_part_a, witem_part_b, witem_part_c, witem_part_d, witem_description,
                                        wshipment_nbr, wpo_nbr, wpo_line_nbr, worder_seq_nbr, worig_qty, wadj_qty, wlpns_shipped, wunits_shipped, wlpns_received,
                                        wunits_received, wref_code_1, wref_value_1, wref_value_2, wref_value_3, wcreate_date, wshipment_line_nbr,
                                        wwork_order_seq_nbr, wscreen_name, wmodule_name, worder_type, wshipment_type, wpo_type, wbilling_location_type);
                                }//try
                                catch (Exception ex)
                                {
                                    LogUtil.Graba_Log(Interface, "Error en Graba_Sql " + ex.Message.ToString(), true, Path.GetFileName(ArchiIHT));
                                    Crear_Carpetas objCrearCarpetas = new Crear_Carpetas();
                                    objCrearCarpetas.ArchivaInterface("RECYCLER_LEER");

                                    if (File.Exists(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiIHT))) File.Delete(Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiIHT));
                                    File.Move(ArchiIHT, Crear_Carpetas.RECYCLER_LEER + Path.GetFileName(ArchiIHT)); // si el archivo esta con errores se mueve a la carpeta de reciclaje de archivos

                                    Flg_Iht_Ok = false;

                                    //Cambiamos nombre del elemento del arreglo
                                    Arr_Txt_Iht[i] = "XXX";

                                }//catch
                            }//foreach
                            // Añade los registros Correctos de cada Archivo
                            if (Flg_Iht_Ok == true)
                            {
                                foreach (DataRow datRow in dtAux_IHT.Rows)
                                {
                                    dt_IHT.ImportRow(datRow);
                                }
                            }

                        }//for
                        LogUtil.Graba_Log(Interface, "      Finaliza temporal IHT, tipo: " + wtipo, false, "");
                    }//if
                }//If wtipo = 'IHT'


                
                //--------- Insertamos a la Base de Datos -------------//
                using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                {
                    //try
                    //{
                    if (cn.State == 0) cn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlquery, cn))
                    {
                        cmd.CommandTimeout = 0;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@OBL", dt_OBL);
                        cmd.Parameters.AddWithValue("@OBS", dt_OBS);

                        cmd.Parameters.AddWithValue("@SVH", dt_SVH);
                        cmd.Parameters.AddWithValue("@SVD", dt_SVD);

                        cmd.Parameters.AddWithValue("@IHT", dt_IHT);
                        cmd.Parameters.AddWithValue("@TIPO", wtipo);

                        LogUtil.Graba_Log(Interface, "      Envia sentencia a SQL, tipo: " + wtipo, false, "");
                        cmd.ExecuteNonQuery();
                        LogUtil.Graba_Log(Interface, "      Finaliza sentencia SQL, tipo: " + wtipo, false, "");

                        exito = true;

                        String Wmsg = "";
                        if (exito == true)
                        {
                            Wmsg = "OK";
                        }
                        else
                        {
                            Wmsg = "Error";
                        }
                        LogUtil.Graba_Log(Interface, "   Finaliza Graba_Sql, exito: " + Wmsg, false, "");

                    }//using
                    if (cn != null)
                        if (cn.State == ConnectionState.Open) cn.Close();
                }
            }//try
            catch (Exception exc)
            {
                error = exc.Message;
                LogUtil.Graba_Log(Interface, "ERROR: " + error, true, "");
                exito = false;
            }
            return exito;
        }

        private bool Archiva_TXT(string[] Arr_OBL, string[] Arr_OBS, string[] Arr_SVH, string[] Arr_SVD, string[] Arr_IHT, string ruta, string wtipo)
        {
            LogUtil.Graba_Log(Interface, "   Inicia Archiva_TXT, tipo: " + wtipo, false, "");
            bool exito = false;
            string Archivo_Destino = "";
            try
            {
                string path;

                if (ruta == "\\50001\\")
                {
                    path = Path.Combine(Crear_Carpetas.C50001_output);
                }
                else
                {
                    path = Path.Combine(Crear_Carpetas.C50003_output);
                }

                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }

                if (wtipo == "OBL")
                {
                    //--------- Procesamos tabla OBL ----------//
                    if (Arr_OBL != null || Arr_OBL.Length == 0)
                    {
                        for (Int32 i = 0; i < Arr_OBL.Length; ++i)
                        {
                            String Arch_OBL = Arr_OBL[i].ToString();

                            if (File.Exists(Arch_OBL))
                            {
                                if (Path.GetFileName(Arch_OBL).Substring(9, 5) == "50001")
                                {
                                    Archivo_Destino = Crear_Carpetas.C50001_output + Path.GetFileName(Arch_OBL);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_OBL, Archivo_Destino); // Mueve al 50001 
                                }
                                else
                                {
                                    Archivo_Destino = Crear_Carpetas.C50003_output + Path.GetFileName(Arch_OBL);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_OBL, Archivo_Destino); // Mueve al 50003
                                }
                            }
                        }//for
                    }//if

                    

                    //--------- Procesamos tabla OBS ----------//
                    if (Arr_OBS != null || Arr_OBS.Length == 0)
                    {
                        for (Int32 i = 0; i < Arr_OBS.Length; ++i)
                        {
                            String Arch_OBS = Arr_OBS[i].ToString();

                            if (File.Exists(Arch_OBS))
                            {
                                if (Path.GetFileName(Arch_OBS).Substring(9, 5) == "50001")
                                {
                                    Archivo_Destino = Crear_Carpetas.C50001_output + Path.GetFileName(Arch_OBS);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_OBS, Archivo_Destino); // Mueve al 50001
                                }
                                else
                                {
                                    Archivo_Destino = Crear_Carpetas.C50003_output + Path.GetFileName(Arch_OBS);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_OBS, Archivo_Destino); // Mueve al 50003
                                }
                            }
                        }//For
                    }//if

                }//if wtipo


                if (wtipo == "SVH")
                {
                    //--------- Procesamos tabla SVH ----------//
                    if (Arr_SVH != null || Arr_SVH.Length == 0)
                    {
                        for (Int32 i = 0; i < Arr_SVH.Length; ++i)
                        {
                            String Arch_SVH = Arr_SVH[i].ToString();

                            if (File.Exists(Arch_SVH))
                            {
                                if (Path.GetFileName(Arch_SVH).Substring(9, 5) == "50001")
                                {
                                    Archivo_Destino = Crear_Carpetas.C50001_output + Path.GetFileName(Arch_SVH);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_SVH, Archivo_Destino); // Mueve a Backup 50001
                                }
                                else
                                {
                                    Archivo_Destino = Crear_Carpetas.C50003_output + Path.GetFileName(Arch_SVH);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_SVH, Archivo_Destino); // Mueve a Backup 50003
                                }
                            }
                        }//For
                    }//if


                    //--------- Procesamos tabla SVD ----------//
                    if (Arr_SVD != null || Arr_SVD.Length == 0)
                    {
                        for (Int32 i = 0; i < Arr_SVD.Length; ++i)
                        {
                            String Arch_SVD = Arr_SVD[i].ToString();

                            if (File.Exists(Arch_SVD))
                            {
                                if (Path.GetFileName(Arch_SVD).Substring(9, 5) == "50001")
                                {
                                    Archivo_Destino = Crear_Carpetas.C50001_output + Path.GetFileName(Arch_SVD);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_SVD, Archivo_Destino); // Mueve al 50001
                                }
                                else
                                {
                                    Archivo_Destino = Crear_Carpetas.C50003_output + Path.GetFileName(Arch_SVD);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_SVD, Archivo_Destino); // Mueve al 50003
                                }
                            }
                        }//For
                    }//if
                }// if wtipo


                if (wtipo == "IHT")
                {
                    //--------- Procesamos tabla IHT ----------//
                    if (Arr_IHT != null || Arr_IHT.Length == 0)
                    {
                        for (Int32 i = 0; i < Arr_IHT.Length; ++i)
                        {
                            String Arch_IHT = Arr_IHT[i].ToString();

                            if (File.Exists(Arch_IHT))
                            {
                                if (Path.GetFileName(Arch_IHT).Substring(9, 5) == "50001")
                                {
                                    Archivo_Destino = Crear_Carpetas.C50001_output + Path.GetFileName(Arch_IHT);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_IHT, Archivo_Destino); // Mueve al 50001
                                }
                                else
                                {
                                    Archivo_Destino = Crear_Carpetas.C50003_output + Path.GetFileName(Arch_IHT);
                                    if (File.Exists(Archivo_Destino)) File.Delete(Archivo_Destino);
                                    File.Move(Arch_IHT, Archivo_Destino); // Mueve al 50003
                                }
                            }
                        }//For
                    }//if
                }//if wtipo

                exito = true;

                String Wmsg = "";
                if (exito == true)
                {
                    Wmsg = "OK";
                }
                else
                {
                    Wmsg = "Error";
                }
                LogUtil.Graba_Log(Interface, "   Finaliza Archiva_TXT, exito: " + Wmsg, false, "");

                return exito;

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "Error en Archiva_TXT: Mover de Word a Backup, " + Archivo_Destino, false, "");
                exito = false;
                return exito;
            }

        }

        private bool Borra_FTP(string[] Arr_OBL, string[] Arr_OBS, string[] Arr_SVH, string[] Arr_SVD, string[] Arr_IHT, string ruta, string wtipo)
        {

            LogUtil.Graba_Log(Interface, "   Inicia Borra_FTP, tipo: " + wtipo, false, "");

            bool exito = false;
            ruta = ruta.Replace("\\", "");
            string Archivo_Ftp = "";

            try
            {
                using (Session session = new Session())
                {
                    session.Open(sessionOptions); // Connect

                    if (wtipo == "OBL")
                    {
                        //--------- Procesamos tabla OBL ----------//
                        if (Arr_OBL != null || Arr_OBL.Length == 0)
                        {
                            for (Int32 i = 0; i < Arr_OBL.Length; ++i)
                            {
                                Archivo_Ftp = Arr_OBL[i].ToString();
                                session.RemoveFiles("/Peru/730/" + ruta + "/output/" + Path.GetFileName(Archivo_Ftp)); // Borramos el archivo del FTP
                            }
                        }


                        //--------- Procesamos tabla OBS ----------//
                        if (Arr_OBS != null || Arr_OBS.Length == 0)
                        {
                            for (Int32 i = 0; i < Arr_OBS.Length; ++i)
                            {
                                Archivo_Ftp = Arr_OBS[i].ToString();
                                session.RemoveFiles("/Peru/730/" + ruta + "/output/" + Path.GetFileName(Archivo_Ftp)); // Borramos el archivo del FTP
                            }
                        }
                    }// if wtipo


                    if (wtipo == "SVH")
                    {


                        //--------- Procesamos tabla SVH ----------//
                        if (Arr_SVH != null || Arr_SVH.Length == 0)
                        {
                            for (Int32 i = 0; i < Arr_SVH.Length; ++i)
                            {
                                Archivo_Ftp = Arr_SVH[i].ToString();
                                session.RemoveFiles("/Peru/730/" + ruta + "/output/" + Path.GetFileName(Archivo_Ftp)); // Borramos el archivo del FTP
                            }
                        }


                        //--------- Procesamos tabla SVD ----------//
                        if (Arr_SVD != null || Arr_SVD.Length == 0)
                        {
                            for (Int32 i = 0; i < Arr_SVD.Length; ++i)
                            {
                                Archivo_Ftp = Arr_SVD[i].ToString();
                                session.RemoveFiles("/Peru/730/" + ruta + "/output/" + Path.GetFileName(Archivo_Ftp)); // Borramos el archivo del FTP
                            }
                        }
                    }// if wtipo


                    if (wtipo == "IHT")
                    {

                        //--------- Procesamos tabla IHT ----------//
                        if (Arr_IHT != null || Arr_IHT.Length == 0)
                        {
                            for (Int32 i = 0; i < Arr_IHT.Length; ++i)
                            {
                                Archivo_Ftp = Arr_IHT[i].ToString();
                                session.RemoveFiles("/Peru/730/" + ruta + "/output/" + Path.GetFileName(Archivo_Ftp)); // Borramos el archivo del FTP
                            }
                        }
                    }// if wtipo

                }//using
                exito = true;

                String Wmsg = "";
                if (exito == true)
                {
                    Wmsg = "OK";
                }
                else
                {
                    Wmsg = "Error";
                }
                LogUtil.Graba_Log(Interface, "   Finaliza Borra_FTP, exito: " + Wmsg, false, "");

                return exito;
            }//try
            catch
            {
                LogUtil.Graba_Log(Interface, "Error en Borra_FTP:  " + Archivo_Ftp, false, "");
                exito = false;
                return exito;
            }//catch
        }

    }
}