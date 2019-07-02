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
    public class Pedidos_Catalogo
    {

        //************** Variables       
        //string wcodalm = DatosGenerales.codalm;
        string wcodcia = DatosGenerales.codcia;
        string wcd = "50001";
        //string waction = "CREATE";
        string winterface = "ORDECAT";
        //int wdiasatras = 7;

        //************** Datatables globales para guardar 
        DataTable dt_cab = null;
        DataTable dt_det = null;

        //************** Files de texto
        //string nomfiltxt1 = $"ORH{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        //string nomfiltxt2 = $"ORD{DateTime.Now:yyyyMMdd}_{DateTime.Now:hhmmss}.TXT";
        string fileTXTc = "";
        string fileTXTd = "";

        public void Genera_Interface_Catalogo_Pedido()
        {
            bool exito = false;
            string wcd = "";

            try
            {
                //verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface("WMS");

                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M001"].ToString(), false, "");

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
                    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M002"].ToString(), false, "");
                }
                else
                {
                    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M003"].ToString(), false, "");
                }

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(winterface, winterface + " : Error: " + ex.ToString(), true, fileTXTc + fileTXTc + "/" + fileTXTd);
            }
            finally
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M004"].ToString(), false, "");
            }
        }

        private bool Obtiene_Data()
        {
            Int32 Dias = Convert.ToInt32(ConfigurationManager.AppSettings["dias"]);

            bool exito = false;

            dt_cab = null;
            dt_det = null;
            string msgerror = "";

            // CABECERA
            string sql = "[USP_WMS_Obt_Pedido_Catalogo]";
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
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M008"].ToString(), false, "");
                exito = true;
            }

            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M010"].ToString(), false, "");
                return exito;
            }

            return exito;
        }

        private bool Genera_FileTXT()
        {

            string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

            fileTXTc = Path.Combine(Crear_Carpetas.WORK,  "ORH_CAT_" + fechor);
            fileTXTd = Path.Combine(Crear_Carpetas.WORK, "ORD_CAT_" + fechor);

            // Eliminar archivos ORH_CAT, ORD_CAT.TXT
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
            bool exito = false;
            string zcd = "";
            var str = new StringBuilder();

            foreach (DataRow datarow in dt_cab.Rows)
            {
                if (datarow["cod_intranet"].ToString() != "")
                {
                    str.Append(datarow["liq_id"].ToString() + delimited);           // Numero de pedido
                    str.Append(datarow["codAlmacen"].ToString() + delimited);           // codigo de almacen de chorrillos
                    str.Append(datarow["codEmpresa"].ToString() + delimited);           // codigo de la empresa
                    str.Append(datarow["order_nbr"].ToString() + delimited);           //
                    str.Append(datarow["ordertype"].ToString() + delimited);           //
                    str.Append(datarow["Liq_Fecha_c"].ToString() + delimited);           //
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
                LogUtil.Graba_Log(winterface, ConfigurationManager.AppSettings["M009"].ToString(), false, ""); // no genera interface pero informa en el archivo log que a pesar de que viene data no existe codigo de intranet 
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

            foreach (DataRow datarow in dt_det.Rows)
            {

                if (datarow["cod_intranet"].ToString() != "")
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
                    str.Append(datarow["voucher_exp_date"].ToString() + delimited);           //
                    str.Append("\r\n");

                }
            }

            File.AppendAllText(fileTXTd, str.ToString());

            //using (StreamWriter filtxt = new StreamWriter(fileTXTd, true, System.Text.Encoding.Default))
            //{
            //    filtxt.WriteLine(str.ToString());
            //}

            exito = (File.Exists(fileTXTc) && File.Exists(fileTXTd));
            //exito = (File.Exists(fileTXTc));

            return exito;
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


        private bool Envia_FTP()
        {
            bool exito1 = false;
            bool exito2 = false;


            exito1 = FTPUtil.Send_FTP_WMS(fileTXTc, fileTXTc, wcd);
            exito2 = FTPUtil.Send_FTP_WMS(fileTXTd, fileTXTd, wcd);

            if (exito1 && exito2)
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M006"].ToString(), false, "");
            }
            else
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M007"].ToString(), true, fileTXTc);
            }

            return (exito1 && exito2);
        }


        /************** Actualiza_Flag
     * Metodo que actualiza el flag de envio de las Devoluciones (para que no lo vuelva a enviar)
     ***************/
        private bool Actualiza_Flag_Data()
        {

            bool exito = false;
            string cade = "";
            //var listaCade = new List<string>();

            //DataTable dtaux = null;

            //dtaux = dt_cabe; 


            // OJO FALTA EVALUAR new System.Data.OleDb.OleDbCommand("set enginebehavior 80", dbConn).ExecuteNonQuery();

            if (dt_cab != null && dt_cab.Rows.Count > 0)
            {

                foreach (DataRow fila in dt_cab.Rows)
                {
                    cade += "'" + Convert.ToString(fila["liq_id"]).Trim() + "',";

                    // DIVIDIMOS LA CADENA PQ SALE ERROR EN EL VFP (STATEMENT TOO LONG)
                    //if (cade.Length > 900)
                    //{
                    //    cade = cade.TrimEnd(',');
                    //    listaCade.Add(cade);
                    //    cade = "";
                    //}
                }

                cade = cade.TrimEnd(',');
                //listaCade.Add(cade);


                // RETAIL
                //string conex = "";

                //conex = Conexion.Conn2;                 

                //using (OleDbConnection dbConn = new OleDbConnection(conex))
                //{
                //    dbConn.Open();                    

                //    string sql_upd = "";

                //foreach (var caden in listaCade)

                string sql_upd = "UPDATE BdAquarella.dbo.LIQUIDACION SET FLAG_WMS=1 WHERE ltrim(rtrim(liq_id)) IN (" + cade + ")";

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

                            //System.Data.OleDb.OleDbCommand com_upd = new System.Data.OleDb.OleDbCommand(sql_upd, dbConn);
                            //com_upd.ExecuteNonQuery();
                            //int count = caden.Count(f => f == ',');
                            LogUtil.Graba_Log(winterface, winterface + " : Se actualizó : " + Convert.ToString(dt_cab.Rows.Count) + " documentos", false, "");
                        }

                    }

                }
                catch (Exception ex)
                {
                    LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M005"].ToString() + ex.ToString(), true, fileTXTc);
                }

            }

            return exito;

        }


    }
}
