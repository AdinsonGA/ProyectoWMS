using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;
using CapaDatos;
using System.IO;

namespace CapaInterface
{
    public class ASN_Catalogo
    {
        string wcodcia = DatosGenerales.codcia;
        string wcd = "50001";
        string winterface = "DEV_CAT";

        DataTable dt_cab = null;
        DataTable dt_det = null;

        string fileTXTc = "";
        string fileTXTd = "";


        public void GeneraInterfaceNC_catalogo()
        {
            bool exito = false;

            try
            {
                //verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface("WMS");

                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M001"].ToString(), false, ""); //INICIO DE PROCESO

                if (ObtieneData())
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

                LogUtil.Graba_Log(winterface, winterface + " : Error: " + ex.ToString(), true, fileTXTc + "/" + fileTXTd); //ERROR AL PROCESAR
            }
            finally
            {
                LogUtil.Graba_Log(winterface, winterface + ConfigurationManager.AppSettings["M004"].ToString(), false, ""); //MSJ FIN DE PROCESO DE DATA
            }
        }

        private bool ObtieneData()
        {
            Int32 Dias = Convert.ToInt32(ConfigurationManager.AppSettings["dias"]);

            bool exito = false;

            dt_cab = null;
            dt_det = null;
            string msgerror = "";

            // CABECERA
            string sql = "USP_WMS_Obt_Nota_Credito_Catalogo"; // FALTA
            dt_cab = Conexion.Obt_SQL(sql, ref msgerror, "C", Dias);

            if (msgerror != "")
            {
                LogUtil.Graba_Log(winterface, msgerror, true, "");
                return false;
            }

            // DETALLE
            if (dt_cab != null && dt_cab.Rows.Count > 0)
            {
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

        private bool Genera_FileTXT()
        {
            bool exito = false;
            string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

            fileTXTc = Path.Combine(Crear_Carpetas.WORK, "ISH_DEV_CAT_" + fechor);
            fileTXTd = Path.Combine(Crear_Carpetas.WORK, "ISL_DEV_CAT_" + fechor);

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
                str.Append(datarow["hdr_group_nbr"].ToString() + delimited);           // Numero de NC
                str.Append(datarow["hdr_group_nbr"].ToString() + delimited);           // 
                str.Append(datarow["facility_code"].ToString() + delimited);           // 
                str.Append(datarow["company_code"].ToString() + delimited);           //
                str.Append(datarow["trailer_nbr"].ToString() + delimited);           //
                str.Append(datarow["action_code"].ToString() + delimited);           //
                str.Append(datarow["ref_nbr"].ToString() + delimited);           //
                str.Append(datarow["shipment_type"].ToString() + delimited);           //
                str.Append(datarow["load_nbr"].ToString() + delimited);           //
                str.Append(datarow["manifest_nbr"].ToString() + delimited);           //
                str.Append(datarow["trailer_type"].ToString() + delimited);           //
                str.Append(datarow["vendor_info"].ToString() + delimited);           //
                str.Append(datarow["origin_info"].ToString() + delimited);           //
                str.Append(datarow["origin_code"].ToString() + delimited);           //
                str.Append(datarow["orig_shipped_units"].ToString() + delimited);           //
                str.Append(datarow["lock_code"].ToString() + delimited);           //
                str.Append(datarow["shipped_date"].ToString() + delimited);           //
                str.Append(datarow["motivo"].ToString() + delimited);           //
                str.Append(datarow["ind_nc"].ToString() + delimited);           //
                str.Append(datarow["Not_Id"].ToString() + delimited);           //
                str.Append("\r\n");

            }

            File.AppendAllText(fileTXTc, str.ToString());

            //// DETALLE 

            str = new StringBuilder();

            foreach (DataRow datarow in dt_det.Rows)
            {
                str.Append(datarow["hdr_group_nbr"].ToString() + delimited);           // 
                str.Append(datarow["shipment_nbr"].ToString() + delimited);           // 
                str.Append(datarow["facility_code"].ToString() + delimited);           //
                str.Append(datarow["company_code"].ToString() + delimited);           // 
                str.Append(datarow["seq_nbr"].ToString() + delimited);           //
                str.Append(datarow["action_code"].ToString() + delimited);           //
                str.Append(datarow["lpn_nbr"].ToString() + delimited);           //
                str.Append(datarow["lpn_weight"].ToString() + delimited);           //
                str.Append(datarow["lpn_volume"].ToString() + delimited);           //
                str.Append(datarow["item_alternate_code"].ToString() + delimited);           //
                str.Append(datarow["item_part_a"].ToString() + delimited);           //
                str.Append(datarow["item_part_b"].ToString() + delimited);           //
                str.Append(datarow["item_part_c"].ToString() + delimited);           //
                str.Append(datarow["item_part_d"].ToString() + delimited);           //
                str.Append(datarow["item_part_e"].ToString() + delimited);           //
                str.Append(datarow["item_part_f"].ToString() + delimited);           //
                str.Append(datarow["pre_pack_code"].ToString() + delimited);           //
                str.Append(datarow["pre_pack_ratio"].ToString() + delimited);           //
                str.Append(datarow["pre_pack_total_units"].ToString() + delimited);           //
                str.Append(datarow["invn_attr_a"].ToString() + delimited);           //
                str.Append(datarow["invn_attr_b"].ToString() + delimited);           //
                str.Append(datarow["invn_attr_c"].ToString() + delimited);           //
                str.Append(datarow["shipped_qty"].ToString() + delimited);           //
                str.Append(datarow["priority_date"].ToString() + delimited);           //
                str.Append(datarow["po_nbr"].ToString() + delimited);           //
                str.Append(datarow["pallet_nbr"].ToString() + delimited);           //
                str.Append(datarow["putaway_type"].ToString() + delimited);           //
                str.Append(datarow["expiry_date_"].ToString() + delimited);           //
                str.Append(datarow["batch_nbr"].ToString() + delimited);           //
                str.Append(datarow["recv_xdock_facility_code"].ToString() + delimited);           //
                str.Append(datarow["cust_field_1"].ToString() + delimited);           //
                str.Append("\r\n");
            }
            File.AppendAllText(fileTXTd, str.ToString());

            exito = (File.Exists(fileTXTc) && File.Exists(fileTXTd));

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

        private bool Actualiza_Flag_Data()
        {
            bool exito = false;
            string cade = "";

            if (dt_cab != null && dt_cab.Rows.Count > 0)
            {

                foreach (DataRow fila in dt_cab.Rows)
                {
                    cade += "'" + Convert.ToString(fila["Not_Id"]).Trim() + "',";
                }

                cade = cade.TrimEnd(',');

                string sql_upd = "UPDATE BDAquarella..Nota_Credito SET FLAG_WMS=1 WHERE ltrim(rtrim(Not_Id)) IN (" + cade + ")";

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

                            LogUtil.Graba_Log(winterface, winterface + " : Se actualizó : " + Convert.ToString(dt_cab.Rows.Count) + " NOTAS DE CREDITO - [AQUARELLA]", false, "");
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
    }
}

