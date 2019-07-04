using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using WinSCP;
using CapaDatos;
using System.Data;
using System.IO;
using Npgsql;
using System.Configuration;


namespace CapaInterface
{
    public class Resultado_Asn_Purchase
    {
        public bool Exito { get; set; }
        public string MensajeError { get; set; }
        public DataSet DatosDevueltos { get; set; }
    }

    public class Asn_Purchase
    {
        //************** Envio de Prescripciones       
        string wcodcia = DatosGenerales.codcia;
        DataSet ds = null;
        DataSet dsc = null;
        string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";

        string fileTXTc_A_50001 = "";
        string fileTXTd_A_50001 = "";

        string fileTXTc_A_50003 = "";
        string fileTXTd_A_50003 = "";

        string fileTXTc_Cho = "";
        string fileTXTc_Lur = "";

        string fileTXTd_Cho = ""; //CD Chorrillos
        string fileTXTd_Lur = ""; //Cd Lurin
        string Interface = "ASN_PURCHASE";

        public void Genera_Interface_Asn_Purchase()
        {

            //verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
            Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
            objCreaCarpeta.ArchivaInterface("WMS");

            bool exito = false;
            string wcd = "";
            //************** File de texto

            fileTXTc_A_50001 = Path.Combine(Crear_Carpetas.WORK, "ISH_A_" + fechor);
            fileTXTd_A_50001 = Path.Combine(Crear_Carpetas.WORK, "ISL_A_" + fechor);

            fileTXTc_A_50003 = Path.Combine(Crear_Carpetas.WORK, "ISH_A_" + fechor);
            fileTXTd_A_50003 = Path.Combine(Crear_Carpetas.WORK, "ISL_A_" + fechor);


            fileTXTc_Cho = Path.Combine(Crear_Carpetas.WORK, "ISH_50001_C_" + fechor);
            fileTXTd_Cho = Path.Combine(Crear_Carpetas.WORK, "ISL_50001_C_" + fechor);

            fileTXTc_Lur = Path.Combine(Crear_Carpetas.WORK, "ISH_50003_L_" + fechor);
            fileTXTd_Lur = Path.Combine(Crear_Carpetas.WORK, "ISL_50003_L_" + fechor);


            try
            {

                LogUtil.Graba_Log(Interface, "****** INICIO PROCESO ASN PURCHASE *******", false, "");
                //if (Obtiene_Asn_Purchase())
                //{
                //    if (Envia_FTP())
                //    {
                //        if (Actualiza_Flag_Asn_Purchase())
                //        {
                //            exito = true;
                //        }
                //    }
                //}

                //Archiva_TXT();

                if (Obtiene_Asn_Purchase())
                {
                    for (int xi = 1; xi <= 2; xi++)
                    {
                        if (xi == 1)
                            wcd = "50001";
                        else
                            wcd = "50003";

                        if (Envia_FTP(wcd))
                        {
                            if (Actualiza_Flag_Asn_Purchase())
                            {
                                exito = true;
                            }

                            Archiva_TXT(wcd);
                        }

                    }
                }

                if (exito)
                {
                    LogUtil.Graba_Log(Interface, "PROCESO ASN PURCHASE OK", false, "");
                }
                else
                {
                    LogUtil.Graba_Log(Interface, "NO EXISTE INFORMACION A PROCESAR", false, "");
                }


            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "ERROR: " + ex.ToString(), true, fileTXTc_Cho);
            }
            finally
            {
                LogUtil.Graba_Log(Interface, "******** FIN PROCESO ASN PURCHASE *********", false, "");
            }
        }
        //****************************************************************************



        public bool Obtiene_Asn_Purchase()
        {
            bool exito = false;

            ////-----------------------
            ////----- DETALLE ---------
            ////-----------------------

            Procesa_Data_Det("A"); // Ambos (BA = Bata)
            Procesa_Data_Det("C"); // Solo Chorrillos
            Procesa_Data_Det("L"); // Solo Lurin

            ////-----------------------
            ////----- CABECERA---------
            ////-----------------------

            Procesa_Data_Cab("A"); // Ambos (BA = Bata)
            Procesa_Data_Cab("C"); // Solo Chorrillos
            Procesa_Data_Cab("L"); // Solo Lurin

            { exito = true; }

            return exito;
        }

        /************** Envia_FTP
        * Metodo que envia el archivo de texto al FTP
        ***************/
        private bool Envia_FTP(string wcd)
        {
            bool exito1, exito2, exito3, exito4, exito5, exito6, exito7, exito8 = false;
            int cont = 0;

            if (wcd == "50001")
            {
                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Cho)) && (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_A_50001))))
                {
                    exito1 = Send_FTP_WMS(fileTXTc_Cho, Crear_Carpetas.C50001_input, "50001");
                    exito2 = Send_FTP_WMS(fileTXTd_Cho, Crear_Carpetas.C50001_input, "50001");
                    exito3 = Send_FTP_WMS(fileTXTc_A_50001, Crear_Carpetas.C50001_input, "50001");
                    exito4 = Send_FTP_WMS(fileTXTd_A_50001, Crear_Carpetas.C50001_input, "50001");
                    cont = cont + 1;

                }
                else if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Cho)) || (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_A_50001))))
                {
                    if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Cho)))
                    {
                        exito1 = Send_FTP_WMS(fileTXTc_Cho, Crear_Carpetas.C50001_input, "50001");
                        exito2 = Send_FTP_WMS(fileTXTd_Cho, Crear_Carpetas.C50001_input, "50001");
                        cont = cont + 1;
                    }
                    else
                    {
                        exito3 = Send_FTP_WMS(fileTXTc_A_50001, Crear_Carpetas.C50001_input, "50001");
                        exito4 = Send_FTP_WMS(fileTXTd_A_50001, Crear_Carpetas.C50001_input, "50001");
                        cont = cont + 1;
                    }
                }


            }
            else
            {

                if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Lur)) && (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_A_50003))))
                {
                    exito5 = Send_FTP_WMS(fileTXTc_Lur, Crear_Carpetas.C50003_input, "50003");
                    exito6 = Send_FTP_WMS(fileTXTd_Lur, Crear_Carpetas.C50003_input, "50003");
                    exito7 = Send_FTP_WMS(fileTXTc_A_50003, Crear_Carpetas.C50003_input, "50003");
                    exito8 = Send_FTP_WMS(fileTXTd_A_50003, Crear_Carpetas.C50003_input, "50003");
                    cont = cont + 1;

                }
                else if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Lur)) || (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_A_50003))))
                {
                    if (File.Exists(Crear_Carpetas.WORK + Path.GetFileName(fileTXTc_Lur)))
                    {
                        exito5 = Send_FTP_WMS(fileTXTc_Lur, Crear_Carpetas.C50003_input, "50003");
                        exito6 = Send_FTP_WMS(fileTXTd_Lur, Crear_Carpetas.C50003_input, "50003");
                        cont = cont + 1;
                    }
                    else
                    {
                        exito7 = Send_FTP_WMS(fileTXTc_A_50003, Crear_Carpetas.C50003_input, "50003");
                        exito8 = Send_FTP_WMS(fileTXTd_A_50003, Crear_Carpetas.C50003_input, "50003");
                        cont = cont + 1;
                    }

                }
                
            }

            if (cont >= 1 && wcd == "50001")
            {
                LogUtil.Graba_Log(Interface, "SE ENVIO A FTP EL ASN PURCHASE PARA ALMACEN 50001", false, "");
                return true;

            }
            else if (cont >= 1 && wcd == "50003")
            {
                LogUtil.Graba_Log(Interface, "SE ENVIO A FTP EL ASN PURCHASE PARA ALMACEN 50003", false, "");
                return true;
            }
            else return false;
       
        }

        /************** Actualiza_Flag_Purchase
        * Metodo que actualiza el flag de envio de las prescripciones (para que no lo vuelva a enviar)
        ***************/
        private bool Actualiza_Flag_Asn_Purchase()
        {
            bool exito = false;

            using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Postgre))
            {
                try
                {
                    if (cn.State == 0) cn.Open();
                    // Grabando al Postgres CABECERA
                    for (
                        int ix = 0; ix < dsc.Tables[0].Rows.Count; ix++)
                    {
                        string wnro_ocompra = "";
                        string wsql = "";

                        wnro_ocompra = dsc.Tables[0].Rows[ix][1].ToString(); // Elige el campo

                        wsql = "UPDATE TOCOMPRAPLX SET FLG_TXWMS = '1' WHERE NRO_OCOMPRA = '" + wnro_ocompra + "'";
                        using (NpgsqlCommand cmd = new NpgsqlCommand(wsql, cn))
                        {
                            cmd.CommandTimeout = 0;
                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch
                {
                    if (cn != null)
                        if (cn.State == ConnectionState.Open) cn.Close();
                }
            }

            exito = true;
            return exito;
        }

        private void Archiva_TXT(string CodAlmacen)
        {
            try
            {

                Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
                objCreaCarpeta.ArchivaInterface(CodAlmacen);


                if (File.Exists(fileTXTc_Cho)) //CHORRILLOS
                {
                    if (CodAlmacen == "50001") //cabecera
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Cho))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Cho));
                        File.Move(fileTXTc_Cho, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Cho)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Cho))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Cho));
                        File.Move(fileTXTc_Cho, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Cho)); // Try to move
                    }
                }

                if (File.Exists(fileTXTd_Cho))//detalle
                {
                    if (CodAlmacen == "50001")
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho));
                        File.Move(fileTXTd_Cho, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Cho)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Cho))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Cho));
                        File.Move(fileTXTd_Cho, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Cho)); // Try to move
                    }

                }

                if (File.Exists(fileTXTc_Lur)) //LURIN
                {
                    if (CodAlmacen == "50001") //cabecera
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Lur))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Lur));
                        File.Move(fileTXTc_Lur, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_Lur)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Lur))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Lur));
                        File.Move(fileTXTc_Lur, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_Lur)); // Try to move
                    }
                }

                if (File.Exists(fileTXTd_Lur))//detalle
                {
                    if (CodAlmacen == "50001")
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Lur))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Lur));
                        File.Move(fileTXTd_Lur, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_Lur)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur));
                        File.Move(fileTXTd_Lur, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_Lur)); // Try to move
                    }

                }

                if (File.Exists(fileTXTc_A_50001)) //A_50001
                {
                    if (CodAlmacen == "50001") //cabecera
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_A_50001))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_A_50001));
                        File.Move(fileTXTc_A_50001, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_A_50001)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_A_50001))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_A_50001));
                        File.Move(fileTXTc_A_50001, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_A_50001)); // Try to move
                    }
                }

                if (File.Exists(fileTXTd_A_50001))//detalle
                {
                    if (CodAlmacen == "50001")
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_A_50001))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_A_50001));
                        File.Move(fileTXTd_A_50001, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_A_50001)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_A_50001))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_A_50001));
                        File.Move(fileTXTd_A_50001, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_A_50001)); // Try to move
                    }

                }

                if (File.Exists(fileTXTc_A_50003)) //A_50003
                {
                    if (CodAlmacen == "50001") //cabecera
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_A_50003))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_A_50003));
                        File.Move(fileTXTc_A_50003, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTc_A_50003)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_A_50001))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_A_50001));
                        File.Move(fileTXTc_A_50003, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTc_A_50003)); // Try to move
                    }
                }

                if (File.Exists(fileTXTd_A_50003))//detalle
                {
                    if (CodAlmacen == "50001")
                    {
                        if (File.Exists(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_A_50003))) File.Delete(Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_A_50003));
                        File.Move(fileTXTd_A_50003, Crear_Carpetas.C50001_input + Path.GetFileName(fileTXTd_A_50003)); // Try to move
                    }
                    else
                    {
                        if (File.Exists(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_A_50003))) File.Delete(Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_A_50003));
                        File.Move(fileTXTd_A_50003, Crear_Carpetas.C50003_input + Path.GetFileName(fileTXTd_A_50003)); // Try to move
                    }

                }

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "ASN PURCHASE" + " ERROR: " + ex.ToString(), true, "");
            }
        }

        private bool Send_FTP_WMS(string file_origen, string file_destino, string modo)
        {
            bool exito = false;

            try
            {
                // Setup session options
                SessionOptions sessionOptions = new SessionOptions
                {
                    Protocol = Protocol.Sftp,
                    HostName = DatosGenerales.UrlFtp, //"172.24.20.183"
                    UserName = DatosGenerales.UserFtp, //"retailc"
                    Password = DatosGenerales.PassFtp, //"1wiAwNRa"
                    PortNumber = 22,
                    GiveUpSecurityAndAcceptAnySshHostKey = true
                };

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

                    if (modo == "50001")
                    {
                        transferResult = session.PutFiles(file_origen, "/data/730/50001/input/" + Path.GetFileName(file_destino), false, transferOptions);
                    }
                    else
                    {
                        transferResult = session.PutFiles(file_origen, "/data/730/50003/input/" + Path.GetFileName(file_destino), false, transferOptions);
                    }

                    // Throw on any error
                    transferResult.Check();
                    exito = transferResult.IsSuccess;

                }
            }

            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "ASN PURCHASE" + " ERROR: " + ex.ToString(), true, "");
            }

            return exito;
        }
        private string Right(string param, int length)
        {
            //start at the index based on the lenght of the sting minus
            //the specified lenght and assign it a variable
            string result = param.Substring(param.Length - length, length);
            //return the result of the operation
            return result;
        }

        private DataTable Dt_pivot(DataTable dt)
        {
            DataTable tmp = dt;
            DataTable dt_tabla = new DataTable();
            try
            {

                if (tmp.Rows.Count > 0 && tmp.Columns.Count > 1)
                {
                    int index = 1;
                    DataRow dtRow;

                    dt_tabla.Columns.Add("llave01", typeof(string));
                    dt_tabla.Columns.Add("llave02", typeof(string));
                    dt_tabla.Columns.Add("cd", typeof(string));
                    dt_tabla.Columns.Add("empresa", typeof(string));
                    dt_tabla.Columns.Add("dord_secue", typeof(string));
                    dt_tabla.Columns.Add("tipo", typeof(string));
                    dt_tabla.Columns.Add("item", typeof(string));
                    dt_tabla.Columns.Add("cod_secci", typeof(string));
                    dt_tabla.Columns.Add("cod_cpack", typeof(string));
                    dt_tabla.Columns.Add("cantidad", typeof(string));
                    dt_tabla.Columns.Add("val_precio", typeof(string));
                    dt_tabla.Columns.Add("cod_cadenad", typeof(string));
                    dt_tabla.Columns.Add("cod_almac", typeof(string));

                    for (int i = 10; i < tmp.Columns.Count - 1; i++)
                    {
                        DataColumn dc = tmp.Columns[i];
                        string clname = dc.ColumnName;
                        for (int y = 0; y < tmp.Rows.Count; y++)
                        {
                            dtRow = dt_tabla.NewRow();
                            dtRow[0] = tmp.Rows[y][0].ToString();
                            dtRow[1] = tmp.Rows[y][1].ToString();
                            dtRow[2] = tmp.Rows[y][2].ToString();
                            dtRow[3] = tmp.Rows[y][3].ToString();
                            dtRow[4] = tmp.Rows[y][4].ToString();
                            dtRow[5] = tmp.Rows[y][5].ToString();

                            string wcantidad;
                            int valor;


                            if (tmp.Rows[y][i] == null)
                            {
                                valor = 0;
                            }
                            else
                            {
                                valor = Convert.ToInt32(tmp.Rows[y][i]);
                            }

                            if (valor == 0 && valor == 0.0000)
                            {
                                wcantidad = "000";
                            }
                            else
                            {
                                wcantidad = Convert.ToString(tmp.Rows[y][i]);
                            }

                            dtRow[6] = tmp.Rows[y][6].ToString() + Right(clname, 2);
                            dtRow[7] = tmp.Rows[y][7].ToString();
                            dtRow[8] = tmp.Rows[y][8].ToString();
                            dtRow[9] = wcantidad;
                            dtRow[10] = tmp.Rows[y][10].ToString();
                            dtRow[11] = tmp.Rows[y][11].ToString();
                            dtRow[12] = tmp.Rows[y][12].ToString();

                            dt_tabla.Rows.Add(dtRow);
                            index++;
                        }//for
                    }//for
                }//if
            }
            catch (Exception)
            {
                LogUtil.Graba_Log(Interface, "Error en Detallar Tallas De Columnas a Filas", false, "");
            }

            return dt_tabla;
        }//private


        private DataTable Procesa_Data_Det(String Modo)
        {
            DataTable dt_tabla = new DataTable();
            DataTable dt_tocompradx = null;
            ds = new DataSet();

            string sql_tocompradx, wfiltro = "", wCD = "";
            string codcia = "730";


            if (Modo == "A")            // Ambos
            {
                wfiltro = " LENGTH(CA.DES_CDS) > 5 ";
                wCD = "50001";

            }
            if (Modo == "C")            // Es Chorillos
            {
                wfiltro = " CA.DES_CDS = '50001' ";
                wCD = "50001";
            }
            if (Modo == "L")            // Lurin
            {
                wfiltro = " CA.DES_CDS = '50003' ";
                wCD = "50003";
            }

            try
            {
                sql_tocompradx = "SELECT C.NRO_PROFORM AS LLAVE01 " +
                                          ", C.NRO_OCOMPRA AS LLAVE02 " +
                                          ", " + wCD + " AS CD, '" +
                                          codcia + "' AS EMPRESA " +
                                          ", 'CREATE' AS ACCION " +
                                          ", 'NAC' AS TIPO " +
                                          ", D.COD_PRODUCTO || D.COD_CALID AS ITEM " +
                                          ", C.COD_SECCI " +
                                          ", D.COD_CPACK " +
                                          ", D.CAN_MED00 AS CANTIDAD " +
                                          ", D.CAN_MED00 AS CAN_MED01 " +
                                          ", D.CAN_MED01 AS CAN_MED02 " +
                                          ", D.CAN_MED02 AS CAN_MED03 " +
                                          ", D.CAN_MED03 AS CAN_MED04 " +
                                          ", D.CAN_MED04 AS CAN_MED05 " +
                                          ", D.CAN_MED05 AS CAN_MED06 " +
                                          ", D.CAN_MED06 AS CAN_MED07 " +
                                          ", D.CAN_MED07 AS CAN_MED08 " +
                                          ", D.CAN_MED08 AS CAN_MED09 " +
                                          ", D.CAN_MED09 AS CAN_MED10 " +
                                          ", D.CAN_MED10 AS CAN_MED11 " +
                                          ", D.CAN_MED11 AS CAN_MED12" +
                                          ", C.COD_CADENA " +
                                          "FROM TOCOMPRAPLX_ASN D " +
                                          "LEFT JOIN TOCOMPRACX C ON C.NRO_OCOMPRA = D.NRO_OCOMPRA " +
                                          "INNER JOIN TCADENA CA ON D.COD_CADENAD = CA.COD_CADENA " +
                                          "WHERE C.COD_SECCI NOT IN ('M','V') " +
                                          "AND D.FLG_TXWMS != '1' " +
                                          "AND " + wfiltro + "AND C.FEC_EMISION > CURRENT_DATE - " + ConfigurationManager.AppSettings["dias"].ToString() +
                                          "AND TRIM(C.NRO_OCOMPRA) != '' ";


                //"AND TC.FEC_EMISION > CURRENT_DATE -10 AND TC.FEC_EMISION <= CURRENT_DATE ORDER BY TD.NRO_PROFORM ";

                using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Postgre))
                {
                    /*selecccionando el archivo TOCOMPRADX */
                    using (NpgsqlCommand cmd = new NpgsqlCommand(sql_tocompradx, cn))
                    {
                        cmd.CommandTimeout = 0;
                        using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd))
                        {
                            dt_tocompradx = new DataTable();
                            da.Fill(dt_tocompradx);
                        }
                        dt_tocompradx.TableName = "TOCOMPRADX";
                    }

                }

                if ((dt_tocompradx != null && dt_tocompradx.Rows.Count > 0))
                {
                    ds.Tables.Add(dt_tocompradx);

                    DataTable dtdet = ds.Tables[0];
                    dtdet = Dt_pivot(dtdet);

                    dtdet.DefaultView.Sort = "llave02";
                    dtdet = dtdet.DefaultView.ToTable();


                    int wcorrelativo = 0;
                    string worden = "";
                    StringBuilder str = new StringBuilder();

                    foreach (DataRow fila in dtdet.Rows)
                    {
                        string valor;
                        string cad_envio = "";
                        string moneda = "";


                        valor = fila["cod_cpack"].ToString();
                        moneda = ds.Tables[0].Rows[0][13].ToString(); // Asignamos Moneda


                        if (fila["item"].ToString().Length < 10)
                        {
                            string val = fila["item"].ToString();
                        }

                        // Inicializamos el correlativo
                        if (fila["llave01"].ToString() != worden)
                        {
                            wcorrelativo = 0;
                            worden = fila["llave01"].ToString();
                        }


                        if (valor == "00001")// No es Pre-Pack
                        {

                            if (fila["cantidad"].ToString() != "000")
                            {

                                wcorrelativo = wcorrelativo + 1;
                                string numero_formateado = wcorrelativo.ToString("000");

                                cad_envio = fila["llave01"].ToString() +
                                                    "|" + fila["llave02"].ToString() +
                                                    "|" + fila["cd"].ToString() +
                                                    "|" + fila["empresa"].ToString() +
                                                    "|" + numero_formateado +
                                                    "|" + fila["tipo"].ToString() +
                                                    "|" + fila["item"].ToString().Substring(0, 10) + fila["cod_secci"].ToString() +
                                                    "|" + fila["item"].ToString().Substring(0, 7) +
                                                    "|" + fila["item"].ToString().Substring(7, 1) +
                                                    "|" + fila["item"].ToString().Substring(8, 2) +
                                                    "|" + fila["cod_secci"].ToString() +
                                                    "|" + "|" + "|" + "|" + "|" + "|" +
                                                    fila["cantidad"].ToString() +
                                                    "|" + fila["val_precio"].ToString() +
                                                    "|" + "|" + "|" + "|" +
                                                    "0" + "|" +
                                                    moneda +
                                                    "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" +
                                                    fila["cod_cadenad"].ToString() + fila["cod_almac"].ToString() + "|";

                                str.Append(cad_envio);
                                str.Append("\r\n");

                            }

                        }
                        else // Es Pre-Pack
                        {
                            if (fila["cantidad"].ToString() != "000")
                            {
                                wcorrelativo = wcorrelativo + 1;
                                string numero_formateado = wcorrelativo.ToString("000");


                                cad_envio = fila["llave01"].ToString() +
                                                    "|" + fila["llave02"].ToString() +
                                                    "|" + fila["cd"].ToString() +
                                                    "|" + fila["empresa"].ToString() +
                                                    "|" + numero_formateado +
                                                    "|" + fila["tipo"].ToString() +
                                                    "|" + fila["item"].ToString().Substring(0, 8) + fila["cod_cpack"].ToString() + fila["cod_secci"].ToString() +
                                                    "|" + fila["item"].ToString().Substring(0, 7) +
                                                    "|" + fila["item"].ToString().Substring(7, 1) +
                                                    "|" + fila["item"].ToString().Substring(8, 2) +
                                                    "|" + fila["cod_secci"].ToString() +
                                                    "|" + "|" + "|" + "|" + "|" + "|" +
                                                    fila["cantidad"].ToString() +
                                                    "|" + fila["val_precio"].ToString() +
                                                    "|" + "|" + "|" + "|" +
                                                    "0" + "|" +
                                                    moneda +
                                                    "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" + "|" +
                                                    fila["cod_cadenad"].ToString() + fila["cod_almac"].ToString() + "|";

                                str.Append(cad_envio);
                                str.Append("\r\n");
                            }
                        }

                    }// End For

                    /*  Generando Archivos de Texto */
                    if (Modo == "A")            // Ambos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTd_A_50001, str2);

                        // Reemplazando Archivo de Texto 
                        string textoC = File.ReadAllText(fileTXTd_A_50001);
                        textoC = textoC.Replace("|50001|730|", "|50003|730|");
                        File.WriteAllText(fileTXTd_A_50003, textoC); // 50003
                    }
                    if (Modo == "C")            // Solo Chorillos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTd_Cho, str2);
                    }
                    if (Modo == "L")            // Solo Lurin
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTd_Lur, str2);
                    }

                }//if Si tiene registros


            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, ex.Message, true, "Procesa_Data");
            }
            return dt_tabla;
        }

        private DataTable Procesa_Data_Cab(String Modo)
        {
            DataTable dt_tabla = new DataTable();
            DataTable dt_tocompracx = null;
            //dsc = new DataSet();

            string wfiltro = "", wCD = "";
            string codcia = "730";
            StringBuilder str = new StringBuilder();


            if (Modo == "A")            // Ambos
            {
                wfiltro = " LENGTH(CA.DES_CDS) > 5 ";
                wCD = "50001";

            }

            if (Modo == "C")            // Es Chorillos
            {
                wfiltro = " CA.DES_CDS = '50001' ";
                wCD = "50001";
            }
            if (Modo == "L")            // Lurin
            {
                wfiltro = " CA.DES_CDS = '50003' ";
                wCD = "50003";
            }

            try
            {

                string sql_tocompracx = "";

                //-----------------------
                //------ CABECERA -------
                //-----------------------


                sql_tocompracx = "SELECT DISTINCT C.NRO_PROFORM AS LLAVE01 " +
                                ", C.NRO_OCOMPRA " +
                                ", " + wCD + " AS CD " +
                                ", " + codcia + " AS EMPRESA " +
                                ", '' AS TRAILER_NBR " +
                                ", 'CREATE' AS ACCION " +
                                ", '' AS REF_NBR " +
                                ", 'NAC' AS TIPO " +
                                ", '' AS LOAD_NBR " +
                                ", '' AS MANIFEST_NBR " +
                                ", '' AS TRAILER_TYPE " +
                                ", '' AS VENDOR_INFO " +
                                ", '' AS ORIGIN_INFO " +
                                ", '' AS ORIGIN_CODE " +
                                ", '' AS ORIGIN_SHIPPED_UNITS " +
                                ", '' AS LOCK_CODE " +
                                ", TO_CHAR(C.FEC_ENTINI, 'YYYYMMDD') AS FECHA " +
                                ", COD_MONEDA " +
                                "FROM TOCOMPRAPLX D " +
                                "LEFT JOIN TOCOMPRACX C ON C.NRO_OCOMPRA = D.NRO_OCOMPRA " +
                                "INNER JOIN TCADENA CA ON D.COD_CADENAD = CA.COD_CADENA " +
                                "WHERE C.COD_SECCI NOT IN('M','V') " +
                                "AND C.NRO_OCOMPRA != '' " +
                                "AND " + wfiltro + 
                                "AND C.FEC_EMISION > CURRENT_DATE - " + ConfigurationManager.AppSettings["dias"].ToString() +
                                "AND D.FLG_TXWMS != '1' ";

                //"AND C.FLG_TXWMS != '1' "
                //"AND C.FEC_EMISION > CURRENT_DATE - 10 AND C.FEC_EMISION <= CURRENT_DATE ";

                using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Postgre))
                {
                    /*selecccionando el archivo TOCOMPRACX */
                    using (NpgsqlCommand cmd = new NpgsqlCommand(sql_tocompracx, cn))
                    {
                        cmd.CommandTimeout = 0;
                        using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd))
                        {
                            dt_tocompracx = new DataTable();
                            da.Fill(dt_tocompracx);
                        }
                        dt_tocompracx.TableName = "TOCOMPRACX";
                    }
                }

                if ((dt_tocompracx != null && dt_tocompracx.Rows.Count > 0))
                {

                    dsc = new DataSet();
                    dsc.Tables.Add(dt_tocompracx);

                    string delimited = "|";
                    var strCab = new StringBuilder();

                    for (int ix = 0; ix < dsc.Tables[0].Rows.Count; ix++)
                    {
                        for (int j = 0; j < dsc.Tables[0].Columns.Count - 1; j++)
                        {
                            strCab.Append(dsc.Tables[0].Rows[ix][j].ToString() + delimited);    // Numero de orden de despacho
                        }
                        strCab.Append("\r\n");
                    }


                    /*  Generando Archivos de Texto */
                    if (Modo == "A")            // Ambos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTc_A_50001, strCab.ToString());

                        // Reemplazando Archivo de Texto 
                        string textoC = File.ReadAllText(fileTXTc_A_50001);
                        textoC = textoC.Replace("|50001|730|", "|50003|730|");
                        File.WriteAllText(fileTXTc_A_50003, textoC); // 50003
                    }
                    if (Modo == "C")            // Solo Chorillos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTc_Cho, strCab.ToString());
                    }
                    if (Modo == "L")            // Solo Lurin
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTc_Lur, strCab.ToString());
                    }

                }// if

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, ex.Message, true, "Procesa_Data");
            }
            return dt_tabla;

        }// Procesa_Data_Cab

    }
}