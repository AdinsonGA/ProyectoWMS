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
//**

namespace CapaInterface
{
    public class Resultado_Purchase
    {
        public bool Exito { get; set; }
        public string MensajeError { get; set; }
        public DataSet DatosDevueltos { get; set; }
    }

    public class Purchase
    {

        //************** Envio de Prescripciones       
        //string wcodalm = DatosGenerales.codalm;
        string wcodcia = DatosGenerales.codcia;
        DataSet ds_det = null;
        DataSet ds_cab = null;

        string Interface = "PURCHASE";

        string fechor = DateTime.Now.ToString("yyyyMMddHHmmss") + ".TXT";
        //string fileTXTc = "";

        string fileTXTc_A_50001 = "";
        string fileTXTd_A_50001 = "";

        string fileTXTc_A_50003 = "";
        string fileTXTd_A_50003 = "";

        string fileTXTc_Cho = "";
        string fileTXTc_Lur = "";

        string fileTXTd_Cho = ""; //CD Chorrillos
        string fileTXTd_Lur = ""; //Cd Lurin



        public void Genera_Interface_Purchase()
        {
            //verifica si existe la carpeta WMS antes de empezar a crear los archivo , si no existe lo crea
            Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
            objCreaCarpeta.ArchivaInterface("WMS");

            string wcd = "";
            bool exito = false;
            //************** File de texto

            fileTXTc_A_50001 = Path.Combine(Crear_Carpetas.WORK, "POH_A_" + fechor);
            fileTXTd_A_50001 = Path.Combine(Crear_Carpetas.WORK, "POD_A_" + fechor);

            fileTXTc_A_50003 = Path.Combine(Crear_Carpetas.WORK, "POH_A_" + fechor);
            fileTXTd_A_50003 = Path.Combine(Crear_Carpetas.WORK, "POD_A_" + fechor);


            fileTXTc_Cho = Path.Combine(Crear_Carpetas.WORK, "POH_50001_" + fechor);
            fileTXTc_Lur = Path.Combine(Crear_Carpetas.WORK, "POH_50003_" + fechor);

            fileTXTd_Cho = Path.Combine(Crear_Carpetas.WORK, "POD_50001_" + fechor);
            fileTXTd_Lur = Path.Combine(Crear_Carpetas.WORK, "POD_50003_" + fechor);

            try
            {

                LogUtil.Graba_Log(Interface, "****** INICIO PROCESO PURCHASE *******", false, "");


                if (Obtiene_Purchase())
                {
                    for (int xi = 1; xi <= 2; xi++)
                    {
                        if (xi == 1)
                            wcd = "50001";
                        else
                            wcd = "50003";

                        if (Envia_FTP(wcd))
                        {
                            if (Actualiza_Flag_Purchase())
                            {
                                exito = true;
                            }
                            Archiva_TXT(wcd);
                        }
                    }

                }


                if (exito)
                {
                    LogUtil.Graba_Log(Interface, "PROCESO PURCHASE OK", false, ""); 
                }
                else
                {
                    LogUtil.Graba_Log(Interface, "NO EXISTE DATA PARA PROCESAR", false, ""); 
                }

            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "ERROR: " + ex.ToString(), true, "");
            }
            finally
            {
                LogUtil.Graba_Log(Interface, "******** FIN PROCESO PURCHASE *********", false, "");
            }
        }
        //****************************************************************************



        /************** Actualiza_Flag_Purchase
        * Metodo que actualiza el flag de envio de las prescripciones (para que no lo vuelva a enviar)
        ***************/
        private bool Actualiza_Flag_Purchase()
        {
            bool exito = false;

            using (NpgsqlConnection cn = new NpgsqlConnection(Conexion.conexion_Postgre))
            {
                try
                {
                    if (cn.State == 0) cn.Open();
                    // Actualiza flagCABECERA - Postgres
                    for (int ix = 0; ix < ds_cab.Tables[0].Rows.Count; ix++)
                    {
                        string wnro_ocompra = "";
                        string wsql = "";

                        wnro_ocompra = ds_cab.Tables[0].Rows[ix][1].ToString();

                        wsql = "UPDATE TOCOMPRACX SET FLG_TXWMS = '1' WHERE NRO_OCOMPRA = '" + wnro_ocompra + "'";
                        using (NpgsqlCommand cmd = new NpgsqlCommand(wsql, cn))
                        {
                            cmd.CommandTimeout = 0;
                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogUtil.Graba_Log(Interface, "ERROR: " + ex.ToString(), true, "");
                    return false;
                }
            }

            exito = true;
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


        public bool Obtiene_Purchase()
        {
            bool exito = false;

            //-----------------------
            //------ CABECERA -------
            //-----------------------
            Procesa_Data_Cab("A"); // Ambos (BA = Bata)
            Procesa_Data_Cab("C"); // Solo Chorrillos
            Procesa_Data_Cab("L"); // Solo Lurin


            //-----------------------
            //----- DETALLE ---------
            //-----------------------
            Procesa_Data_Det("A"); // Ambos (BA = Bata)
            Procesa_Data_Det("C"); // Solo Chorrillos
            Procesa_Data_Det("L"); // Solo Lurin
            { exito = true; }

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
            catch
            {
                // omitido
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
                LogUtil.Graba_Log(Interface, "ERROR AL SUBIR PURCHASE AL FTP: " + ex.Message, true, "Send_FTP_WMS");
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

                    for (int i = 13; i < tmp.Columns.Count - 1; i++)
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
                LogUtil.Graba_Log(Interface, "Error en Detallar Tallas De Columnas a Filas", true, "");
            }

            return dt_tabla;
        }//private




        private DataTable Procesa_Data_Det(string modo)
        {


            DataTable dt_tabla = new DataTable();
            DataTable dt_tocompradx = null;
            ds_det = new DataSet();

            string sql_tocompradx, wfiltro = "", wCD = "";
            string codcia = "730";


            if (modo == "A")            // Ambos
            {
                wfiltro = " LENGTH(CA.DES_CDS) > 5 ";
                wCD = "50001";

            }
            if (modo == "C")            // Es Chorillos
            {
                wfiltro = " CA.DES_CDS = '50001' ";
                wCD = "50001";
            }
            if (modo == "L")            // Lurin
            {
                wfiltro = " CA.DES_CDS = '50003' ";
                wCD = "50003";
            }


            try
            {
                sql_tocompradx = "SELECT TD.NRO_PROFORM AS LLAVE01 " +
                                          ", TD.NRO_OCOMPRA AS LLAVE02 " +
                                          ", " + wCD + " AS CD, '" +
                                          codcia + "' AS EMPRESA " +
                                          ", TD.NRO_ITEM " +
                                          ", 'CREATE' AS TIPO " +
                                          ", TD.COD_PRODUCTO || TD.COD_CALID AS ITEM " +
                                          ", TD.COD_SECCI " +
                                          ", TD.COD_CPACK " +
                                          ", TD.CAN_MED00 AS CANTIDAD " +
                                          ", TD.VAL_PRECIO " +
                                          ", TD.COD_CADENAD " +
                                          ", TD.COD_ALMAC " +
                                          ", TD.CAN_MED00  AS CAN_MED01 , TD.CAN_MED01 AS CAN_MED02, TD.CAN_MED02 AS CAN_MED03 , TD.CAN_MED03 CAN_MED04, TD.CAN_MED04  AS CAN_MED05, TD.CAN_MED05  AS CAN_MED06, TD.CAN_MED06 AS CAN_MED07 , TD.CAN_MED07 AS CAN_MED08 , TD.CAN_MED08 AS CAN_MED09 , TD.CAN_MED09 AS CAN_MED10 , TD.CAN_MED10 AS CAN_MED11, TD.CAN_MED11 AS CAN_MED12 " +
                                          ", CA.DES_CDS " +
                                          "FROM TOCOMPRADX TD " + 
                                          "INNER JOIN TOCOMPRACX TC ON TD.NRO_OCOMPRA = TC.NRO_OCOMPRA " +
                                          "INNER JOIN TCADENA CA ON TD.COD_CADENAD = CA.COD_CADENA " +
                                          "WHERE TC.COD_SECCI NOT IN ('M','V') " +
                                          "AND TC.FLG_TXWMS != '1' " +
                                          "AND " + wfiltro +
                                          "AND TC.FEC_EMISION > CURRENT_DATE - " + +Convert.ToInt32(ConfigurationManager.AppSettings["dias"]) +
                                          " AND TRIM(TC.NRO_OCOMPRA) != '' ";

                //"AND TRIM(TC.NRO_OCOMPRA) = '201901073' " +
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
                    ds_det.Tables.Add(dt_tocompradx);

                    DataTable dtdet = ds_det.Tables[0];
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
                        moneda = ds_det.Tables[0].Rows[0][13].ToString(); // Asignamos Moneda


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
                    if (modo == "A")            // Ambos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTd_A_50001, str2);

                        // Reemplazando Archivo de Texto 
                        string textoC = File.ReadAllText(fileTXTd_A_50001);
                        textoC = textoC.Replace("|50001|730|", "|50003|730|");
                        File.WriteAllText(fileTXTd_A_50003, textoC); // 50003
                    }
                    if (modo == "C")            // Solo Chorillos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTd_Cho, str2);
                    }
                    if (modo == "L")            // Solo Lurin
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTd_Lur, str2);
                    }

                }//if Si tiene registros
            }
            catch (Exception ex)
            {
                LogUtil.Graba_Log(Interface, "Error en Detallar Tallas De Columnas a Filas : " + ex.Message.ToString() , true, "Procesa_Data");
            }
            return dt_tabla;
        }

        private DataTable Procesa_Data_Cab(String modo)
        {
            DataTable dt_tabla = new DataTable();
            DataTable dt_tocompracx = null;

            string wfiltro = "", wCD = "";
            string codcia = "730";
            StringBuilder str = new StringBuilder();


            if (modo == "A")            // Ambos
            {
                wfiltro = " LENGTH(CA.DES_CDS) > 5 ";
                wCD = "50001";

            }

            if (modo == "C")            // Es Chorillos
            {
                wfiltro = " CA.DES_CDS = '50001' ";
                wCD = "50001";
            }
            if (modo == "L")            // Lurin
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

                sql_tocompracx = "SELECT DISTINCT TC.NRO_PROFORM AS LLAVE01, " +
                                " TC.NRO_OCOMPRA AS LLAVE02 " +
                                ", " + wCD + " AS CD " +
                                ", " + codcia + " AS EMPRESA " +
                                ", COD_PROVE, 'CREATE' AS ACCION, " +
                                "TO_CHAR(FEC_EMISION,'YYYYMMDD') AS FECHA " +
                                ", '' AS BLANCO01 " +
                                ", 'NAC' AS TIPO " +
                                ", TO_CHAR(FEC_EMBARQ,'YYYYMMDD') AS FECHAEMB " +
                                ", '' AS BLANCO02 " +
                                ", TO_CHAR(FEC_EMBARQ,'YYYYMMDD') AS FECHAEMB " +
                                ", TO_CHAR(FEC_EMBARQ,'YYYYMMDD') AS FECHAEMB " +
                                ", COD_MONEDA " +
                                "FROM TOCOMPRACX TC " +
                                "INNER JOIN TOCOMPRADX TD ON TC.NRO_OCOMPRA = TD.NRO_OCOMPRA " +
                                "INNER JOIN TCADENA CA ON TD.COD_CADENAD = CA.COD_CADENA " +
                                "WHERE TC.COD_SECCI NOT IN ('M','V') " +
                                "AND TC.FLG_TXWMS != '1' " +
                                "AND " + wfiltro +
                                "AND TC.FEC_EMISION > CURRENT_DATE - " + Convert.ToInt32(ConfigurationManager.AppSettings["dias"]) +
                                " AND TRIM(TC.NRO_OCOMPRA) != '' ";


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

                    ds_cab = new DataSet();
                    ds_cab.Tables.Add(dt_tocompracx);

                    string delimited = "|";
                    var strCab = new StringBuilder();

                    for (int ix = 0; ix < ds_cab.Tables[0].Rows.Count; ix++)
                    {
                        for (int j = 0; j < ds_cab.Tables[0].Columns.Count - 1; j++)
                        {
                            strCab.Append(ds_cab.Tables[0].Rows[ix][j].ToString() + delimited);    // Numero de orden de despacho
                        }
                        strCab.Append("\r\n");
                    }


                    /*  Generando Archivos de Texto */
                    if (modo == "A")            // Ambos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTc_A_50001, strCab.ToString());

                        // Reemplazando Archivo de Texto 
                        string textoC = File.ReadAllText(fileTXTc_A_50001);
                        textoC = textoC.Replace("|50001|730|", "|50003|730|");
                        File.WriteAllText(fileTXTc_A_50003, textoC); // 50003
                    }
                    if (modo == "C")            // Solo Chorillos
                    {
                        string str2 = str.ToString();
                        // Grabando registro al Archivo de Texto DETALLE
                        File.WriteAllText(fileTXTc_Cho, strCab.ToString());
                    }
                    if (modo == "L")            // Solo Lurin
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
