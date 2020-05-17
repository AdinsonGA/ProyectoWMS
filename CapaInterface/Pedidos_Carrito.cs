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

//using System.Data.SqlTypes;

namespace CapaInterface
{
    public class Pedidos_Carrito
    {
        //************** Variables       
        //string wcodalm = DatosGenerales.codalm;
        string wcodcia = DatosGenerales.codcia;
        string wcd = "50001";
        string wmae = "MAE_AQ_EC";
        //string waction = "CREATE";
        string winterface = "ORD_CAR";
        string winMaestros = "MAE_CAR";
        //int wdiasatras = 7;

        //************** Datatables globales para guardar 
        DataTable dt_cab = null;
        DataTable dt_det = null;
        DataTable dt_maestros = null;

        //************** Files de texto
        //string nomfiltxt1 = $"ORH{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        //string nomfiltxt2 = $"ORD{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        string fileTXTc = "";
        string fileTXTd = "";
        string fileMaestrosTXT = "";


        //public void Genera_Interface_Carrito_Maestro()
        public bool Genera_Interface_Carrito_Maestro()
        {
            bool exito = false;
            string wcd = "";

            try
            {
                //verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface(wmae);

                LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M001"].ToString(), false, ""); //INICIO DE PROCESO

                //Genera y envia interfaces de maestros de Ecommerce a WMS
                if (Obtiene_Maestro_Carrito())
                {
                    if (Genera_File_Maestro_CarritoTXT())
                    {
                        if (EnviaMaestros_FTP())
                        {
                            if (Actualiza_Flag_Maestro_Carrito_Data())
                            {
                                exito = true;
                                Archiva_MaestrosTXT();
                            }
                        }
                    }
                }
                if (exito)
                {
                    LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M002"].ToString(), false, ""); //MSJ SE PROCESO LA DATA OK
                }
                else
                {
                    LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M003"].ToString(), false, ""); //MSJ NO SE REALIZO NINGUN PROCESO
                }

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winMaestros, winMaestros + " : Error: " + ex.ToString(), true, fileMaestrosTXT); //ERROR AL PROCESAR

            }
            finally
            {
                LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M004"].ToString(), false, ""); //MSJ FIN DE PROCESO DE DATA
            }
            return exito;

        }


        public void Genera_Interface_Carrito_Pedido()
        {
            bool exito = false;
            string wcd = "";

            try
            {
                //verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface("WMS");

                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M001"].ToString(), false, ""); //INICIO DE PROCESO

                //Genera y envia interfaces de ordenes de pedido de Ecommerce a WMS

                if (Obtiene_Data())
                {
                    if (Genera_FileTXT())
                    {
                        if (Envia_FTP())
                        {
                            if (Actualiza_Flag_Data())
                            {
                                exito = true;
                            }
                            Archiva_TXT();
                        }
                    }
                }


                if (exito)
                {
                    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M002"].ToString(), false, ""); //MSJ SE PROCESO LA DATA OK
                }
                else
                {
                    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M003"].ToString(), false, ""); //MSJ NO SE REALIZO NINGUN PROCESO
                }
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, winterface + " ERROR: " + ex.ToString(), true, fileTXTc + "/" + fileTXTd); //ERROR AL PROCESAR
            }
            finally
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M004"].ToString(), false, ""); //MSJ FIN DE PROCESO DE DATA
            }
        }

        /************** Genera_FileTXT
        * Metodo que genera la interface como archivo de texto para el WMS
        ***************/
        private bool Genera_FileTXT()
        {
            bool exito = false;
            string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

            fileTXTc = Path.Combine(Crear_Carpetas.WORK, "ORH_CAR_" + fechor);
            fileTXTd = Path.Combine(Crear_Carpetas.WORK, "ORD_CAR_" + fechor);

            // Eliminar archivos ORH_CAR, ORD_CAR.TXT
            try
            {
                if (File.Exists(fileTXTc)) File.Delete(fileTXTc);
                if (File.Exists(fileTXTd)) File.Delete(fileTXTd);
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, winterface + " ERROR: " + ex.ToString(), true, "");
            }


            if (dt_cab == null || dt_cab.Rows.Count == 0)
            { return false; }

            string delimited = "|";
            string zcd = "";
            var str = new StringBuilder();



            foreach (DataRow datarow in dt_cab.Rows)
            {
                if (datarow["cod_intranet"].ToString() != "") //obviamos los clientes que no tienen codigo de intranet hasta que lo tenga
                {
                    str.Append(datarow["liq_id"].ToString() + delimited);           // Numero de pedido
                    str.Append(datarow["codAlmacen"].ToString() + delimited);           // codigo de almacen de chorrillos
                    str.Append(datarow["codEmpresa"].ToString() + delimited);           // codigo de la empresa
                    str.Append(datarow["order_nbr"].ToString() + delimited);           //
                    str.Append(datarow["ordertype"].ToString() + delimited);           //
                    str.Append(datarow["Liq_Fecha_c"].ToString() + delimited);           //s
                    str.Append(datarow["exp_date"].ToString() + delimited);           //
                    str.Append(datarow["Liq_Fecha_e"].ToString() + delimited);           //
                    str.Append(datarow["cod_intranet"].ToString() + delimited);           //
                    str.Append(datarow["cust_name"].ToString() + delimited);           //
                    str.Append(datarow["cust_addr"].ToString() + delimited);           //
                    str.Append(datarow["cust_addr2"].ToString() + delimited);           //
                    str.Append(datarow["cust_addr3"].ToString() + delimited);           //
                    str.Append(datarow["ref_nbr"].ToString() + delimited);           //
                    str.Append(datarow["action_code"].ToString() + delimited);           //
                    str.Append(datarow["route_nbr"].ToString() + delimited);           //
                    str.Append(datarow["cust_city"].ToString() + delimited);           //
                    str.Append(datarow["cust_state"].ToString() + delimited);           //
                    str.Append(datarow["cust_zip"].ToString() + delimited);           //
                    str.Append(datarow["cust_country"].ToString() + delimited);           //
                    str.Append(datarow["cust_phone_nbr"].ToString() + delimited);           //
                    str.Append(datarow["cust_email"].ToString() + delimited);           //
                    str.Append(datarow["cust_contact"].ToString() + delimited);           //
                    str.Append(datarow["cust_nbr"].ToString() + delimited);           //
                    str.Append(datarow["cod_intranet"].ToString() + delimited);           //
                    str.Append(datarow["shipto_name"].ToString() + delimited);           //
                    str.Append(datarow["shipto_addr"].ToString() + delimited);           //
                    str.Append(datarow["shipto_addr2"].ToString() + delimited);           //
                    str.Append(datarow["shipto_addr3"].ToString() + delimited);           //
                    str.Append(datarow["shipto_city"].ToString() + delimited);           //
                    str.Append(datarow["shipto_state"].ToString() + delimited);           //
                    str.Append(datarow["shipto_zip"].ToString() + delimited);           //
                    str.Append(datarow["shipto_country"].ToString() + delimited);           //
                    str.Append(datarow["shipto_phone_nbr"].ToString() + delimited);           //
                    str.Append(datarow["shipto_email"].ToString() + delimited);           //
                    str.Append(datarow["shipto_contact"].ToString() + delimited);           //
                    str.Append(datarow["dest_company_code"].ToString() + delimited);           //
                    str.Append(datarow["priority"].ToString() + delimited);           //
                    str.Append(datarow["ship_via_code"].ToString() + delimited);           //
                    str.Append(datarow["carrier_account_nbr"].ToString() + delimited);           //
                    str.Append(datarow["payment_method"].ToString() + delimited);           //
                    str.Append(datarow["host_allocation_nbr"].ToString() + delimited);           //
                    str.Append(datarow["customer_po_nbr"].ToString() + delimited);           //
                    str.Append(datarow["sales_order_nbr"].ToString() + delimited);           //
                    str.Append(datarow["sales_channel"].ToString() + delimited);           //
                    str.Append(datarow["dest_dept_nbr"].ToString() + delimited);           //
                    str.Append(datarow["start_ship_date"].ToString() + delimited);           //
                    str.Append(datarow["Liq_Fecha_e2"].ToString() + delimited);           //
                    str.Append(datarow["spl_instr"].ToString() + delimited);           //
                    str.Append(datarow["vas_group_code"].ToString() + delimited);           //
                    str.Append(datarow["currency_code"].ToString() + delimited);           //
                    str.Append(datarow["stage_location_barcode"].ToString() + delimited);           //
                    str.Append(datarow["cust_field_1"].ToString() + delimited);           //
                    str.Append(datarow["liq_almac"].ToString() + delimited);           //
                    str.Append(datarow["liq_canal"].ToString() + delimited);           //
                    str.Append(datarow["bas_documento"].ToString() + delimited);           //
                    str.Append("\r\n");
                }
            }


            if (dt_cab.Rows.Count > 0 && str.Length == 0)
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M009"].ToString(), false, ""); // no genera interface pero informa en el archivo log que a pesar de que viene data no existe codigo de intranet 
                return false;
            }

            File.AppendAllText(fileTXTc, str.ToString());

            //// DETALLE NORETAIL

            //int correlativo = 0;
            //string keyitem = null;
            //char cero = '0';
            //string grupo = "";

            str = new StringBuilder();

            //grupo = dt_det.Rows[0]["od_nord"].ToString();

            //1.0

            //foreach (DataRow datarow in dt_det.Rows)
            //{
            //    if (datarow["cod_intranet"].ToString() != "") //obviamos los clientes que no tienen codigo de intranet hasta que lo tenga
            //    {
            //        str.Append(datarow["hdr_group_nbr"].ToString() + delimited);           // 
            //        str.Append(datarow["codAlmacen"].ToString() + delimited);           // 
            //        str.Append(datarow["codEmpresa"].ToString() + delimited);           // 
            //        str.Append(datarow["order_nbr"].ToString() + delimited);           //
            //        str.Append(datarow["Liq_Det_Items"].ToString() + delimited);           //
            //        str.Append(datarow["item_alternate_code"].ToString() + delimited);           //
            //        str.Append(datarow["item_part_a"].ToString() + delimited);           //
            //        str.Append(datarow["item_part_b"].ToString() + delimited);           //
            //        str.Append(datarow["item_part_c"].ToString() + delimited);           //
            //        str.Append(datarow["item_part_d"].ToString() + delimited);           //
            //        str.Append(datarow["item_part_e"].ToString() + delimited);           //
            //        str.Append(datarow["item_part_f"].ToString() + delimited);           //
            //        str.Append(datarow["pre_pack_code"].ToString() + delimited);           //
            //        str.Append(datarow["pre_pack_ratio"].ToString() + delimited);           //
            //        str.Append(datarow["pre_pack_ratio_seq"].ToString() + delimited);           //
            //        str.Append(datarow["pre_pack_total_unit"].ToString() + delimited);           //
            //        str.Append(datarow["Liq_Det_Cantidad"].ToString() + delimited);           //
            //        str.Append(datarow["req_cntr_nbr"].ToString() + delimited);           //
            //        str.Append(datarow["action_code"].ToString() + delimited);           //
            //        str.Append(datarow["batch_nbr"].ToString() + delimited);           //
            //        str.Append(datarow["invn_attr_a"].ToString() + delimited);           //
            //        str.Append(datarow["invn_attr_b"].ToString() + delimited);           //
            //        str.Append(datarow["invn_attr_c"].ToString() + delimited);           //
            //        str.Append("\r\n");
            //    }
            //}


            //2.0

            foreach (DataRow datarowCab in dt_cab.Rows)
            {
                DataView dtDetail;
                DataTable dt_det_Filtro; //tabla filtrado por codigo

                string codCab = datarowCab["liq_id"].ToString();
                dt_det.DefaultView.RowFilter = "hdr_group_nbr = '" + codCab + "'";
                dtDetail = dt_det.DefaultView;

                dt_det_Filtro = dtDetail.ToTable();

                if (dt_det_Filtro.Rows.Count > 0)
                {
                    foreach (DataRow datarow in dt_det_Filtro.Rows)
                    {
                        str.Append(datarow["hdr_group_nbr"].ToString() + delimited);           // 
                        str.Append(datarow["codAlmacen"].ToString() + delimited);           // 
                        str.Append(datarow["codEmpresa"].ToString() + delimited);           // 
                        str.Append(datarow["order_nbr"].ToString() + delimited);           //
                        str.Append(datarow["Liq_Det_Items"].ToString() + delimited);           //
                        str.Append(datarow["item_alternate_code"].ToString() + delimited);           //
                        str.Append(datarow["item_part_a"].ToString() + delimited);           //
                        str.Append(datarow["item_part_b"].ToString() + delimited);           //
                        str.Append(datarow["item_part_c"].ToString() + delimited);           //
                        str.Append(datarow["item_part_d"].ToString() + delimited);           //
                        str.Append(datarow["item_part_e"].ToString() + delimited);           //
                        str.Append(datarow["item_part_f"].ToString() + delimited);           //
                        str.Append(datarow["pre_pack_code"].ToString() + delimited);           //
                        str.Append(datarow["pre_pack_ratio"].ToString() + delimited);           //
                        str.Append(datarow["pre_pack_ratio_seq"].ToString() + delimited);           //
                        str.Append(datarow["pre_pack_total_unit"].ToString() + delimited);           //
                        str.Append(datarow["Liq_Det_Cantidad"].ToString() + delimited);           //
                        str.Append(datarow["req_cntr_nbr"].ToString() + delimited);           //
                        str.Append(datarow["action_code"].ToString() + delimited);           //
                        str.Append(datarow["batch_nbr"].ToString() + delimited);           //
                        str.Append(datarow["invn_attr_a"].ToString() + delimited);           //
                        str.Append(datarow["invn_attr_b"].ToString() + delimited);           //
                        str.Append(datarow["invn_attr_c"].ToString() + delimited);           //
                        str.Append("\r\n");

                    }
                }
            }

            File.AppendAllText(fileTXTd, str.ToString());

            //using (StreamWriter filtxt = new StreamWriter(fileTXTd, true, System.Text.Encoding.Default))
            //{
            //    filtxt.WriteLine(str.ToString());
            //}

            exito = (File.Exists(fileTXTc) && File.Exists(fileTXTd));
            //exito = (File.Exists(fileTXTc));

            if (exito)
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M012"] + " : " + Path.GetFileName(fileTXTc) + "  " + Path.GetFileName(fileTXTd), false, ""); // MSJ SE GENERO LOS ARCHIVOS OK
            }
            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M013"] + " : " + Path.GetFileName(fileTXTc) + "  " + Path.GetFileName(fileTXTd), false, ""); //MSJ NO SE GENERO LOS ARCHIVOS
            }

            return exito;

        }

        private bool Obtiene_Data()
        {
            Int32 Dias = Convert.ToInt32(ConfigurationManager.AppSettings["dias"]);

            bool exito = false;

            dt_cab = null;
            dt_det = null;
            string msgerror = "";

            // CABECERA
            string sql = "[USP_WMS_Obt_Pedidos_Carrito]";
            //string sql = "select * from BDPOS.dbo.FVDESPC where DESC_FECHA>=GETDATE()-7";
            dt_cab = Conexion.Obt_SQL(sql, ref msgerror, "C", Dias);

            if (msgerror != "")
            {
                LogUtil.Graba_Log(winterface, msgerror, true, "");
                return false;
            }

            // DETALLE
            if (dt_cab != null && dt_cab.Rows.Count > 0)
            {
                //sql = "SELECT dgud_gudis,dgud_artic,dgud_calid,dgud_costo,dgud_codpp,dgud_cpack,dgud_touni,dgud_med00,dgud_med01,dgud_med02,dgud_med03,dgud_med04,dgud_med05,dgud_med06,dgud_med07,dgud_med08,dgud_med09,dgud_med10,dgud_med11 FROM SCCCGUD INNER JOIN SCDDGUD ON CGUD_GUDIS=DGUD_GUDIS WHERE CGUD_FEMIS>=DATE()-" + wdiasatras.ToString() + " AND EMPTY(FLAG_WMS) ORDER BY cgud_gudis ";
                dt_det = Conexion.Obt_SQL(sql, ref msgerror, "D", Dias);

                if (msgerror != "")
                {
                    LogUtil.Graba_Log(winterface, msgerror, true, "");
                    return false;
                }
            }

            if (dt_cab != null && dt_cab.Rows.Count > 0)
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M008"].ToString(), false, ""); //MSJ CONSULTA OK
                exito = true;
            }
            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M010"].ToString(), false, ""); // MSJ NO HAY DATOS PARA PROCESAR
                return exito;
            }
            return exito;
        }

        /************** Envia_FTP
        * Metodo que envia el archivo de texto al FTP
        ***************/
        private bool Envia_FTP()
        {
            bool exito1 = false;
            bool exito2 = false;

            exito1 = FTPUtil.Send_FTP_WMS(fileTXTc, fileTXTc, wcd, winterface);
            exito2 = FTPUtil.Send_FTP_WMS(fileTXTd, fileTXTd, wcd, winterface);

            if (exito1 && exito2)
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M006"].ToString(), false, ""); // MSJ SE ENVIO AL FTP OK
            }
            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M007"].ToString(), true, fileTXTc); // MSJ NO SE ENVIO AL FTP
            }

            return (exito1 && exito2);
        }

        private void Archiva_TXT()
        {
            try
            {
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface(wcd);

                if (File.Exists(fileTXTc))
                {
                    if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc));
                    File.Move(fileTXTc, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc)); // Try to move

                }

                if (File.Exists(fileTXTd))
                {
                    if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd));
                    File.Move(fileTXTd, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd)); // Try to move
                }

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, winterface + " ERROR: " + ex.ToString(), true, "");
            }
        }

        /************** Actualiza_Flag
        * Metodo que actualiza el flag de envio de las Devoluciones (para que no lo vuelva a enviar)
        ***************/
        private bool Actualiza_Flag_Data()
        {

            bool exito = false;
            string cade = "";

            if (dt_cab != null && dt_cab.Rows.Count > 0)
            {

                foreach (DataRow fila in dt_cab.Rows)
                {
                    cade += "'" + Convert.ToString(fila["liq_id"]).Trim().Replace("E", "") + "',";
                }

                cade = cade.TrimEnd(',');

                string sql_upd = "UPDATE BD_ECOMMERCE.dbo.LIQUIDACION SET FLAG_WMS=1 WHERE ltrim(rtrim(liq_id)) IN (" + cade + ")"; //

                try
                {
                    using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                    {
                        if (cn.State == 0) cn.Open();
                        using (SqlCommand cmd = new SqlCommand(sql_upd, cn))
                        {
                            cmd.CommandText = sql_upd;
                            cmd.CommandTimeout = 0;
                            cmd.ExecuteNonQuery();
                            exito = true;

                            if (cn != null)
                                if (cn.State == ConnectionState.Open) cn.Close();

                            LogUtil.Graba_Log(winterface, winterface + " : Se actualizó : " + Convert.ToString(dt_cab.Rows.Count) + " ORDEN(ES) - [ECOMMERCE]", false, "");
                        }

                    }

                }
                catch (Exception ex)
                {
                    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M005"].ToString() + ex.ToString(), true, fileTXTc);  //MSJ ERROR AL ACTUALIZAR DATA

                }

            }
            return exito;
        }

        /************** Obtiene Maestros para ecommerce *****************/

        private bool Obtiene_Maestro_Carrito()
        {
            Int32 Dias = Convert.ToInt32(ConfigurationManager.AppSettings["dias"]);

            bool exito = false;

            dt_maestros = null;
            string msgerror = "";


            string sql = "USP_WMS_Obt_Maestro_Carrito";
            dt_maestros = Conexion.Obt_maestros(sql, Dias);


            if (dt_maestros != null && dt_maestros.Rows.Count > 0)
            {
                LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M008"].ToString(), false, ""); //MSJ CONSULTA OK
                exito = true;
            }
            else
            {
                LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M010"].ToString(), false, ""); // MSJ NO HAY DATOS PARA PROCESAR
                return exito;
            }
            return exito;

        }

        private bool Genera_File_Maestro_CarritoTXT()
        {
            bool exito = false;
            string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";
            string NomCli, DirCli, CiuCli, EmailCli = "";

            fileMaestrosTXT = Path.Combine(Crear_Carpetas.WORK_MAE_AQ_EC, "STR_CAR" + fechor);

            try
            {
                if (File.Exists(fileMaestrosTXT)) File.Delete(fileMaestrosTXT);
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winMaestros, winMaestros + " ERROR: " + ex.ToString(), true, "");
            }


            if (dt_maestros == null || dt_maestros.Rows.Count == 0)
            { return false; }

            string delimited = "|";
            string zcd = "";
            var str = new StringBuilder();

            try
            {
                foreach (DataRow datarow in dt_maestros.Rows)
                {

                    if (datarow["NOM_CLI"].ToString() != "") //Valida si existe nombre (dato obligatorio para WMS)
                    {
                        str.Append(datarow["COD_CLI"].ToString() + delimited);
                        str.Append(datarow["COD_INT"].ToString() + delimited);

                        NomCli = ValidaCaracteres.Numeros_Letras(datarow["NOM_CLI"].ToString());

                        if (NomCli == "")
                        {
                            LogUtil.Graba_Log(winMaestros, "ERROR al enviar maestro : El Código Intranet " + datarow["COD_INT"].ToString() + " tiene nombre con caracter incorrecto", true, "");
                            return false;
                        }
                        else
                        {
                            str.Append(NomCli + delimited);
                        }


                        if (datarow["DIR_CLI"].ToString() != "")
                        {
                            DirCli = ValidaCaracteres.Numeros_Letras(datarow["DIR_CLI"].ToString());
                            str.Append(DirCli + delimited);
                        }
                        else
                        {
                            str.Append(datarow["DIR_CLI"].ToString() + delimited);
                        }

                        str.Append(datarow["VAC1"].ToString() + delimited);
                        str.Append(datarow["VAC2"].ToString() + delimited);
                        str.Append(datarow["VAC3"].ToString() + delimited);

                        if (datarow["CIU_CLI"].ToString() != "")
                        {
                            CiuCli = ValidaCaracteres.Numeros_Letras(datarow["CIU_CLI"].ToString());
                            str.Append(CiuCli + delimited);
                        }
                        else
                        {
                            str.Append(datarow["CIU_CLI"].ToString() + delimited);
                        }

                        str.Append(datarow["VAC4"].ToString() + delimited);
                        str.Append(datarow["ZIP"].ToString() + delimited);
                        str.Append(datarow["PAI_CLI"].ToString() + delimited);
                        str.Append(datarow["VAC5"].ToString() + delimited);

                        if (datarow["EMA_CLI"].ToString() != null)
                        {
                            EmailCli = ValidaCaracteres.Email(datarow["EMA_CLI"].ToString());
                            str.Append(EmailCli + delimited);
                        }
                        else
                        {
                            str.Append(datarow["EMA_CLI"].ToString() + delimited);
                        }

                        str.Append(datarow["VAC6"].ToString() + delimited);
                        str.Append(datarow["COMANDO"].ToString() + delimited);
                        str.Append(datarow["ESP_CL"].ToString() + delimited);
                        str.Append(datarow["VAC7"].ToString() + delimited);
                        str.Append(datarow["VAC8"].ToString() + delimited);
                        str.Append(datarow["VAC9"].ToString() + delimited);
                        str.Append(datarow["VAC10"].ToString() + delimited);
                        str.Append(datarow["VAC11"].ToString() + delimited);
                        str.Append(datarow["VAC12"].ToString() + delimited);
                        str.Append(datarow["VAC13"].ToString() + delimited);
                        str.Append("\r\n");

                    }
                    else
                    {
                        LogUtil.Graba_Log(winMaestros, "ERROR al enviar maestro : El Código de Intranet " + datarow["COD_INT"].ToString() + " No tiene nombre asignado", true, "");
                        return false;
                    }
                }
            }

            catch (Exception ex)
            {

                LogUtil.Graba_Log(winMaestros, "Error :" + ex.Message.ToString(), true, "");
            }

            File.AppendAllText(fileMaestrosTXT, str.ToString());

            exito = (File.Exists(fileMaestrosTXT));

            if (exito)
            {
                LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M012"] + " : " + Path.GetFileName(fileMaestrosTXT), false, ""); // MSJ SE GENERO MAESTROS OK
            }
            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M013"] + " : " + Path.GetFileName(fileMaestrosTXT), false, ""); //MSJ NO SE GENERO LOS ARCHIVOS DE MAESTROS
            }

            return exito;

        }

        private bool EnviaMaestros_FTP()
        {
            bool exito1 = false;

            exito1 = FTPUtil.Send_FTP_WMS(fileMaestrosTXT, fileMaestrosTXT, "50003", winMaestros);


            if (exito1)
            {
                LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M006"].ToString(), false, ""); // MSJ SE ENVIO AL FTP OK
            }
            else
            {
                LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M007"].ToString(), true, fileTXTc); // MSJ NO SE ENVIO AL FTP
            }

            return (exito1);
        }

        private bool Actualiza_Flag_Maestro_Carrito_Data()
        {

            bool exito = false;
            string cade = "";

            if (dt_maestros != null && dt_maestros.Rows.Count > 0)
            {

                foreach (DataRow fila in dt_maestros.Rows)
                {
                    cade += "'" + Convert.ToString(fila["COD_INT"]).Trim() + "',"; ;
                }
                cade = cade.TrimEnd(',');

                string sql_upd = "UPDATE BD_ECOMMERCE..Basico_Dato SET Flag_WMS=1 WHERE ltrim(rtrim(Cod_Intranet)) IN (" + cade + ")"; //

                try
                {
                    using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                    {
                        if (cn.State == 0) cn.Open();
                        using (SqlCommand cmd = new SqlCommand(sql_upd, cn))
                        {
                            cmd.CommandText = sql_upd;
                            cmd.CommandTimeout = 0;
                            cmd.ExecuteNonQuery();
                            exito = true;

                            if (cn != null)
                                if (cn.State == ConnectionState.Open) cn.Close();
                            LogUtil.Graba_Log(winMaestros, winMaestros + " : Se actualizó : " + Convert.ToString(dt_maestros.Rows.Count) + " CLIENTES - [ECOMMERCE]", false, "");
                        }

                    }

                }
                catch (Exception ex)
                {
                    LogUtil.Graba_Log(winMaestros, winMaestros + ConfigurationManager.AppSettings["M005"].ToString() + ex.ToString(), true, fileMaestrosTXT);  //MSJ ERROR AL ACTUALIZAR DATA

                }

            }
            return exito;
        }

        private void Archiva_MaestrosTXT()
        {
            try
            {
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface(wmae);

                if (File.Exists(fileMaestrosTXT))
                {
                    if (File.Exists(Crear_Carpetas.BACKUP_MAE_AQ_EC + Path.GetFileName(fileMaestrosTXT))) File.Delete(Crear_Carpetas.BACKUP_MAE_AQ_EC + Path.GetFileName(fileMaestrosTXT));
                    File.Move(fileMaestrosTXT, Crear_Carpetas.BACKUP_MAE_AQ_EC + Path.GetFileName(fileMaestrosTXT)); // Try to move

                }

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, winterface + " ERROR: " + ex.ToString(), true, "");
            }
        }


    }
}
