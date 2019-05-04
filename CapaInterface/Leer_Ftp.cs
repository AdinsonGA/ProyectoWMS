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
        string wcodalm = "50001";  //DatosGenerales.codalm;
        string wcodcia = DatosGenerales.codcia;
        string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";
        string[] file_TXT_OBL;
        string[] file_TXT_OBS;

        string[] file_TXT_SVH;
        string[] file_TXT_SVD;
        string[] file_TXT_IHT;


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
            bool exito = false;
            try
            {

                LogUtil.Graba_Log("LECTURA", "****** INICIO DE LECTURA TXT *******");

                if (Lee_Descarga_Archivo())
                {
                    if (Graba_Sql())
                    {
                        if (Archiva_TXT(file_TXT_OBL, file_TXT_OBS, file_TXT_SVH, file_TXT_SVD, file_TXT_IHT))
                        {
                            if (Borra_FTP(file_TXT_OBL, file_TXT_OBS, file_TXT_SVH, file_TXT_SVD, file_TXT_IHT))
                            {
                                exito = true;
                            }
                        }
                    }
                }

                if (exito)
                {
                    LogUtil.Graba_Log("LECTURA", "LECTURA DE ARCHIVOS DE TEXTO OK");
                }
                else
                {
                    LogUtil.Graba_Log("LECTURA", "NO SE PUDO LEER NADA");
                }


            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log("LECTURA", "ERROR: " + ex.ToString());
            }
            finally
            {
                LogUtil.Graba_Log("LECTURA", "******** FIN DE LECTURA DE TXT *********");
            }
        }
        //****************************************************************************

        public bool Lee_Descarga_Archivo()
        {

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
                TransferOperationResult transferResult;

                // Lee y copia los archivos al disco local, carpeta Temporal
                transferResult = session.GetFiles("/data/730/output/", DatosGenerales.rutaMain + @"Work\", false, transferOptions);

                // Throw on any error
                transferResult.Check();

                if (transferResult.IsSuccess == true) exito = true;
            }

            return exito;

        }

        private bool Graba_Sql()
        {
            bool exito = false;
            string error = "";
            string sqlquery = "USP_INSERTAR_TEMP_OBL_OBS_SVH_SVD";
            try
            {

                DataTable dt_OBL = new DataTable();
                DataTable dt_OBS = new DataTable();

                DataTable dt_SVH = new DataTable();
                DataTable dt_SVD = new DataTable();

                DataTable dt_IHT = new DataTable();


            string carpetatemporal = DatosGenerales.rutaMain + @"Work\";
                file_TXT_OBL = Directory.GetFiles(@carpetatemporal, "OBL*.*");
                file_TXT_OBS = Directory.GetFiles(@carpetatemporal, "OBS*.*");

                file_TXT_SVH = Directory.GetFiles(@carpetatemporal, "SVH*.*");
                file_TXT_SVD = Directory.GetFiles(@carpetatemporal, "SVD*.*");
                file_TXT_IHT = Directory.GetFiles(@carpetatemporal, "IHT*.*");


            if ((file_TXT_OBL.Length == 0) &&  (file_TXT_OBS.Length == 0) && (file_TXT_SVH.Length == 0) && (file_TXT_SVD.Length == 0) && (file_TXT_IHT.Length == 0)) // Si no hay informacion... retorna
                {
                    return false;
                }

                if (!Directory.Exists(@carpetatemporal)) Directory.CreateDirectory(@carpetatemporal);

                string path = Path.Combine(DatosGenerales.rutaMain + @"Work\");
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }



                //--------- Procesamos tabla OBL ----------//
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

                for (Int32 i = 0; i < file_TXT_OBL.Length; ++i)
                {
                    String value = file_TXT_OBL[i].ToString();

                    string fichero = DatosGenerales.rutaMain + @"Work\" + Path.GetFileName(file_TXT_OBL[i]);
                    string[] lineas = File.ReadAllLines(fichero);

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

                        dt_OBL.Rows.Add(whdr_group_nbr, wfacility_code, wcompany_code, waction_code, wload_type, wload_manifest_nbr, wtrailer_nbr, wtotal_nbr_of_oblpns, wtotal_weight, wtotal_volume, wtotal_shipping_charge, wship_date, wship_date_time);
                    }
                }//for



                //--------- Procesamos tabla OBS ----------//

                dt_OBS.Columns.Add("hdr_group_nbr", typeof(string));
                dt_OBS.Columns.Add("facility_code", typeof(string));
                dt_OBS.Columns.Add("company_code", typeof(string));
                dt_OBS.Columns.Add("load_manifest_nbr", typeof(string));
                dt_OBS.Columns.Add("line_nbr", typeof(int));
                dt_OBS.Columns.Add("seq_nbr", typeof(int));
                dt_OBS.Columns.Add("stop_nbr_of_oblpns", typeof(int));
                dt_OBS.Columns.Add("stop_weight", typeof(Decimal));
                dt_OBS.Columns.Add("stop_volume", typeof(Decimal));
                dt_OBS.Columns.Add("shipto_facility_code", typeof(Decimal));

                dt_OBS.Columns.Add("dest_facility_code", typeof(string));
                dt_OBS.Columns.Add("order_nbr", typeof(string));
                dt_OBS.Columns.Add("ord_date", typeof(DateTime));
                dt_OBS.Columns.Add("req_ship_date", typeof(DateTime));

                dt_OBS.Columns.Add("order_seq_nbr", typeof(int));
                dt_OBS.Columns.Add("ob_lpn_nbr", typeof(string));
                dt_OBS.Columns.Add("item_alternate_code", typeof(string));
                dt_OBS.Columns.Add("pre_pack_ratio", typeof(Decimal));
                dt_OBS.Columns.Add("pre_pack_ratio_seq", typeof(int));
                dt_OBS.Columns.Add("hazmat", typeof(Boolean));

                dt_OBS.Columns.Add("shipped_uom", typeof(string));
                dt_OBS.Columns.Add("shipped_qty", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_weight", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_volume", typeof(decimal));


                dt_OBS.Columns.Add("order_hdr_cust_number_1", typeof(int));
                dt_OBS.Columns.Add("order_hdr_cust_number_2", typeof(int));
                dt_OBS.Columns.Add("order_hdr_cust_number_3", typeof(int));
                dt_OBS.Columns.Add("order_hdr_cust_number_4", typeof(int));

                dt_OBS.Columns.Add("order_type", typeof(string));
                dt_OBS.Columns.Add("ob_lpn_length", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_width", typeof(decimal));
                dt_OBS.Columns.Add("ob_lpn_height", typeof(decimal));
                dt_OBS.Columns.Add("erp_fulfillment_line_ref", typeof(int));
                dt_OBS.Columns.Add("sales_order_line_ref", typeof(string));
                dt_OBS.Columns.Add("sales_order_schedule_ref", typeof(string));
                dt_OBS.Columns.Add("tms_order_hdr_ref", typeof(string));
                dt_OBS.Columns.Add("tms_order_dtl_ref", typeof(string));

                for (Int32 i = 0; i < file_TXT_OBS.Length; ++i)
                {
                    String value = file_TXT_OBS[i].ToString();

                    string fichero2 = DatosGenerales.rutaMain + @"Work\" + Path.GetFileName(file_TXT_OBS[i]);
                    string[] lineas2 = File.ReadAllLines(fichero2);

                    foreach (string lin in lineas2)
                    {
                        string[] campos = lin.Split('|');

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
                        string worder_nbr = campos[38].ToString();

                        DateTime word_date = DateTime.ParseExact(campos[39].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                        DateTime wreq_ship_date = DateTime.ParseExact(campos[41].ToString(), "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture);

                        string worder_seq_nbr = campos[54].ToString();
                        string wob_lpn_nbr = campos[60].ToString();
                        string witem_alternate_code = campos[61].ToString();
                        string wpre_pack_ratio = campos[69].ToString();
                        string wpre_pack_ratio_seq = campos[70].ToString();
                        string whazmat = campos[75].ToString();

                        string wshipped_uom = campos[76].ToString();
                        string wshipped_qty = campos[77].ToString();
                        string wob_lpn_weight = campos[88].ToString();
                        string wob_lpn_volume = campos[89].ToString();

                        string worder_hdr_cust_number_1 = campos[102].ToString();
                        string worder_hdr_cust_number_2 = campos[103].ToString();
                        string worder_hdr_cust_number_3 = campos[104].ToString();
                        string worder_hdr_cust_number_4 = campos[105].ToString();

                        string worder_type = campos[161].ToString();
                        string wob_lpn_length = campos[165].ToString();
                        string wob_lpn_width = campos[166].ToString();
                        string wob_lpn_height = campos[167].ToString();
                        string werp_fulfillment_line_ref = campos[181].ToString();
                        string wsales_order_line_ref = campos[182].ToString();
                        string wsales_order_schedule_ref = campos[183].ToString();
                        string wtms_order_hdr_ref = campos[184].ToString();
                        string wtms_order_dtl_ref = campos[185].ToString();

                        dt_OBS.Rows.Add(whdr_group_nbr, wfacility_code, wcompany_code, wload_manifest_nbr, wline_nbr, wseq_nbr, wstop_nbr_of_oblpns, wstop_weight, wstop_volume, wshipto_facility_code, wdest_facility_code, worder_nbr, word_date, wreq_ship_date, worder_seq_nbr, wob_lpn_nbr, witem_alternate_code, wpre_pack_ratio, wpre_pack_ratio_seq, whazmat, wshipped_uom, wshipped_qty, wob_lpn_weight, wob_lpn_volume, worder_hdr_cust_number_1, worder_hdr_cust_number_2, worder_hdr_cust_number_3, worder_hdr_cust_number_4, worder_type, wob_lpn_length, wob_lpn_width, wob_lpn_height, werp_fulfillment_line_ref, wsales_order_line_ref, wsales_order_schedule_ref, wtms_order_hdr_ref, wtms_order_dtl_ref);
                    }
                }//for


                //--------- Procesamos tabla SVH ----------//

                dt_SVH.Columns.Add("hdr_group_nbr", typeof(string));
                dt_SVH.Columns.Add("shipment_nbr", typeof(string));
                dt_SVH.Columns.Add("facility_code", typeof(string));
                dt_SVH.Columns.Add("company_code", typeof(string));
                dt_SVH.Columns.Add("trailer_nbr", typeof(string));
                dt_SVH.Columns.Add("load_nbr", typeof(string));
                dt_SVH.Columns.Add("orig_shipped_units", typeof(decimal));
                dt_SVH.Columns.Add("shipped_date", typeof(DateTime));
                dt_SVH.Columns.Add("verification_date", typeof(DateTime));

                for (Int32 i = 0; i < file_TXT_SVH.Length; ++i)
                {
                    String value = file_TXT_SVH[i].ToString();

                    string fichero = DatosGenerales.rutaMain + @"Work\" + Path.GetFileName(file_TXT_SVH[i]);
                    string[] lineas = File.ReadAllLines(fichero);

                    foreach (string lin in lineas)
                    {
                        string[] campos = lin.Split('|');

                        string whdr_group_nbr = campos[0].ToString();
                        string wshipment_nbr = campos[1].ToString();
                        string wfacility_code = campos[2].ToString();
                        string wcompany_code = campos[3].ToString();
                        string wtrailer_nbr = campos[4].ToString();
                        string wload_nbr = campos[7].ToString();
                        string worig_shipped_units = campos[13].ToString();

                        DateTime wshipped_date = DateTime.ParseExact(campos[14].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                        DateTime wverification_date = DateTime.ParseExact(campos[21].ToString(), "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture);

                        dt_SVH.Rows.Add(whdr_group_nbr, wshipment_nbr, wfacility_code, wcompany_code, wtrailer_nbr, wload_nbr, worig_shipped_units, wshipped_date, wverification_date);
                    }
                }//for




                //--------- Procesamos tabla SVD ----------//
                dt_SVD.Columns.Add("hdr_group_nbr", typeof(string));
                dt_SVD.Columns.Add("seq_nbr", typeof(int));
                dt_SVD.Columns.Add("lpn_nbr", typeof(string));
                dt_SVD.Columns.Add("lpn_weight", typeof(decimal));
                dt_SVD.Columns.Add("lpn_volume", typeof(decimal));
                dt_SVD.Columns.Add("item_alternate_code", typeof(string));
                dt_SVD.Columns.Add("invn_attr_a", typeof(string));
                dt_SVD.Columns.Add("shipped_qty", typeof(decimal));
                dt_SVD.Columns.Add("received_qty", typeof(decimal));

                for (Int32 i = 0; i < file_TXT_SVD.Length; ++i)
                {
                    String value = file_TXT_SVD[i].ToString();

                    string fichero = DatosGenerales.rutaMain + @"Work\" + Path.GetFileName(file_TXT_SVD[i]);
                    string[] lineas = File.ReadAllLines(fichero);

                    foreach (string lin in lineas)
                    {
                        string[] campos = lin.Split('|');

                        string whdr_group_nbr = campos[0].ToString();
                        string wseq_nbr = campos[4].ToString();
                        string wlpn_nbr = campos[5].ToString();
                        string wlpn_weight = campos[6].ToString();
                        string wlpn_volume = campos[7].ToString();
                        string witem_alternate_code = campos[8].ToString();
                        string winvn_attr_a = campos[19].ToString();
                        string wshipped_qty = campos[22].ToString();
                        string wreceived_qty = campos[27].ToString();

                        dt_SVD.Rows.Add(whdr_group_nbr, wseq_nbr, wlpn_nbr, wlpn_weight, wlpn_volume, witem_alternate_code, winvn_attr_a, wshipped_qty, wreceived_qty);
                    }
                }//for
                


                //--------- Procesamos tabla IHT ----------//
                dt_IHT.Columns.Add("group_nbr", typeof(int));
                dt_IHT.Columns.Add("seq_nbr", typeof(int));
                dt_IHT.Columns.Add("facility_code", typeof(string));
                dt_IHT.Columns.Add("company_code", typeof(string));
                dt_IHT.Columns.Add("activity_code", typeof(int));

                dt_IHT.Columns.Add("lpn_nbr", typeof(string));
                dt_IHT.Columns.Add("item_code", typeof(string));
                dt_IHT.Columns.Add("item_alternate_code", typeof(string));
                dt_IHT.Columns.Add("shipment_nbr", typeof(string));
                dt_IHT.Columns.Add("po_nbr", typeof(string));

                dt_IHT.Columns.Add("po_line_nbr", typeof(int));
                dt_IHT.Columns.Add("order_seq_nbr", typeof(int));

                dt_IHT.Columns.Add("adj_qty", typeof(decimal));


                dt_IHT.Columns.Add("ref_value_1", typeof(string));
                dt_IHT.Columns.Add("ref_value_2", typeof(string));
                dt_IHT.Columns.Add("ref_value_3", typeof(string));

                dt_IHT.Columns.Add("create_date", typeof(DateTime));

                dt_IHT.Columns.Add("shipment_line_nbr", typeof(int));
                dt_IHT.Columns.Add("work_order_seq_nbr", typeof(int));
                dt_IHT.Columns.Add("screen_name", typeof(string));
                dt_IHT.Columns.Add("module_name", typeof(string));

                for (Int32 i = 0; i < file_TXT_IHT.Length; ++i)
                {
                String value = file_TXT_IHT[i].ToString();

                string fichero = DatosGenerales.rutaMain + @"Work\" + Path.GetFileName(file_TXT_IHT[i]);
                string[] lineas = File.ReadAllLines(fichero);

                    foreach (string lin in lineas)
                    {
                    string[] campos = lin.Split('|');

                        string wgroup_nbr = campos[0].ToString();
                        string wseq_nbr = campos[1].ToString();
                        string wfacility_code = campos[2].ToString();
                        string wcompany_code = campos[3].ToString();
                        string wactivity_code = campos[4].ToString();
                        string wlpn_nbr = campos[7].ToString();
                        string witem_code = campos[9].ToString();
                        string witem_alternate_code = campos[10].ToString();

                        string wshipment_nbr = campos[18].ToString();
                        string wpo_nbr = campos[20].ToString();

                        string wpo_line_nbr = campos[21].ToString();
                        string worder_seq_nbr = campos[24].ToString();

                        string wadj_qty = "";

                        if (campos[27].ToString() == "")
                        {
                            wadj_qty = "0";
                        }
                        else
                        {
                            wadj_qty = campos[27].ToString();
                        }

                        string wref_value_1 = campos[33].ToString();
                        string wref_value_2 = campos[35].ToString();
                        string wref_value_3 = campos[37].ToString();

                        DateTime wcreate_date = DateTime.ParseExact(campos[42].Substring(0, 8).ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);

                        string wshipment_line_nbr = campos[46].ToString();
                        string wwork_order_seq_nbr = campos[53].ToString();
                        string wscreen_name = campos[54].ToString();
                        string wmodule_name = campos[55].ToString();

                        dt_IHT.Rows.Add(wgroup_nbr, wseq_nbr, wfacility_code, wcompany_code, wactivity_code, wlpn_nbr, witem_code, witem_alternate_code, wshipment_nbr, wpo_nbr, wpo_line_nbr, worder_seq_nbr, wadj_qty, wref_value_1, wref_value_2, wref_value_3, wcreate_date, wshipment_line_nbr, wwork_order_seq_nbr, wscreen_name, wmodule_name);
                    }
                }//for



            //--------- Insertamos a la Base de Datos -------------//

            using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                {
                    try
                    {
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
                            cmd.ExecuteNonQuery();
                        }
                    }
                    catch (Exception exc)
                    {
                        error = exc.Message;
                    }

                    if (cn != null)
                        if (cn.State == ConnectionState.Open) cn.Close();
                }
            }
            catch (Exception exc)
            {
                error = exc.Message;
                exito = false;
            }

            exito = true;
            return exito;
        }

        private bool Archiva_TXT(string[] Arr_OBL, string[] Arr_OBS, string[] Arr_SVH, string[] Arr_SVD, string[] Arr_IHT)
        {
            bool exito = false;

            string path = Path.Combine(DatosGenerales.rutaMain, @"BACKUP\");
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }


            //--------- Procesamos tabla OBL ----------//
            for (Int32 i = 0; i < Arr_OBL.Length; ++i)
                {
                    String Arch_OBL = Arr_OBL[i].ToString();

                    if (File.Exists(Arch_OBL))
                    {
                        if (File.Exists(path + Path.GetFileName(Arch_OBL))) File.Delete(path + Path.GetFileName(Arch_OBL));
                        File.Move(Arch_OBL, path + Path.GetFileName(Arch_OBL)); // Try to move
                    }
                }//For

                //--------- Procesamos tabla OBS ----------//
                for (Int32 i = 0; i < Arr_OBS.Length; ++i)
                {
                    String Arch_OBS = Arr_OBS[i].ToString();

                    if (File.Exists(Arch_OBS))
                    {
                        if (File.Exists(path + Path.GetFileName(Arch_OBS))) File.Delete(path + Path.GetFileName(Arch_OBS));
                        File.Move(Arch_OBS, path + Path.GetFileName(Arch_OBS)); // Try to move
                    }
                }//For


                //--------- Procesamos tabla SVH ----------//
                for (Int32 i = 0; i < Arr_SVH.Length; ++i)
                {
                    String Arch_SVH = Arr_SVH[i].ToString();

                    if (File.Exists(Arch_SVH))
                    {
                        if (File.Exists(path + Path.GetFileName(Arch_SVH))) File.Delete(path + Path.GetFileName(Arch_SVH));
                        File.Move(Arch_SVH, path + Path.GetFileName(Arch_SVH)); // Try to move
                    }
                }//For


                //--------- Procesamos tabla SVD ----------//
                for (Int32 i = 0; i < Arr_SVD.Length; ++i)
                {
                    String Arch_SVD = Arr_SVD[i].ToString();

                    if (File.Exists(Arch_SVD))
                    {
                        if (File.Exists(path + Path.GetFileName(Arch_SVD))) File.Delete(path + Path.GetFileName(Arch_SVD));
                        File.Move(Arch_SVD, path + Path.GetFileName(Arch_SVD)); // Try to move
                    }
                }//For


                //--------- Procesamos tabla IHT ----------//
                for (Int32 i = 0; i < Arr_IHT.Length; ++i)
                {
                    String Arch_IHT = Arr_IHT[i].ToString();

                    if (File.Exists(Arch_IHT))
                    {
                        if (File.Exists(path + Path.GetFileName(Arch_IHT))) File.Delete(path + Path.GetFileName(Arch_IHT));
                        File.Move(Arch_IHT, path + Path.GetFileName(Arch_IHT)); // Try to move
                    }
                }//For

                exito = true;
            return exito;
        }

        private bool Borra_FTP(string[] Arr_OBL, string[] Arr_OBS, string[] Arr_SVH, string[] Arr_SVD, string[] Arr_IHT)
        {
            bool exito = false;

            using (Session session = new Session())
            {
                session.Open(sessionOptions); // Connect
                //--------- Procesamos tabla OBL ----------//
                for (Int32 i = 0; i < Arr_OBL.Length; ++i)
                {
                    String Arch_OBL = Arr_OBL[i].ToString();
                    session.RemoveFiles("/data/730/output/" + Path.GetFileName(Arch_OBL)); // Borramos el archivo del FPT
                }//For



                //--------- Procesamos tabla OBS ----------//
                for (Int32 i = 0; i < Arr_OBS.Length; ++i)
                {
                    String Arch_OBS = Arr_OBS[i].ToString();
                    session.RemoveFiles("/data/730/output/" + Path.GetFileName(Arch_OBS)); // Borramos el archivo del FPT
                }//For


                //--------- Procesamos tabla SVH ----------//
                for (Int32 i = 0; i < Arr_SVH.Length; ++i)
                {
                    String Arch_SVH = Arr_SVH[i].ToString();
                    session.RemoveFiles("/data/730/output/" + Path.GetFileName(Arch_SVH)); // Borramos el archivo del FPT
                }//For


                //--------- Procesamos tabla SVD ----------//
                for (Int32 i = 0; i < Arr_SVD.Length; ++i)
                {
                    String Arch_SVD = Arr_SVD[i].ToString();
                    session.RemoveFiles("/data/730/output/" + Path.GetFileName(Arch_SVD)); // Borramos el archivo del FPT
                    //}
                }//For


                //--------- Procesamos tabla IHT ----------//
                for (Int32 i = 0; i < Arr_IHT.Length; ++i)
                {
                    String Arch_IHT = Arr_IHT[i].ToString();
                    session.RemoveFiles("/data/730/output/" + Path.GetFileName(Arch_IHT)); // Borramos el archivo del FPT
                }//For
            }//using
                exito = true;
            return exito;
        }

    }
}