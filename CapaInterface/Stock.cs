using System;
using WinSCP;
using CapaDatos;
using System.Data;
using System.IO;
using System.Data.SqlClient;
using Excel;

namespace CapaInterface
{
    public class Stock
    {
        string[] file_stock_WMS;
        DataTable dt_stock = new DataTable();
        //string file_archivo;

        //Sesion para Loguearce al FTP
        private SessionOptions sessionOptions = new SessionOptions
        {
            Protocol = Protocol.Sftp,
            HostName = DatosGenerales.UrlFtp, //"172.24.20.183"
            UserName = DatosGenerales.UserFtp, //"retailc"
            Password = DatosGenerales.PassFtp, //"1wiAwNRa"
            PortNumber = Convert.ToInt32(DatosGenerales.PuertoFtp),
            GiveUpSecurityAndAcceptAnySshHostKey = true
        };

        public void LeerStock()
        {
            if (DescargarArchivo())
            {
                if (GrabarSQL())
                {
                    if (ArchivarCSV())
                    {
                        BorrarCSV_FTP();
                    }
                }
            }
        }

        public bool DescargarArchivo()
        {

            bool exito = false;

            try
            {
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

                    // copia el archivo stock de la ruta FTP  a la ruta local
                    string ruta_archi = "/Peru/730/" + "*_REPEX_*.csv*";
                    //string ruta_archi = "/data/730/" + "730_50001_Bata Peru_REPEX_8_14_2019_18_25_58_608.csv";

                    transferResult = session.GetFiles(ruta_archi, Crear_Carpetas.WORK, false, transferOptions);

                    // Throw on any error 

                    transferResult.Check();

                    if (transferResult.IsSuccess == true) exito = true;

                }

            }
            catch (Exception ex)
            {

                //ex.Message.ToString();
                LogUtil.Graba_Log("FOTOSTOCK", "Error : " + ex.Message.ToString(), true, "");
            }
            return exito;
        }

        public bool GrabarSQL()
        {

            bool exito = false;
            string error = "";
            string ArchiOBL = "";
            //string sqlquery = "USP_INSERTAR_STOCK_WMS";

            string carpetatemporal = Crear_Carpetas.WORK;

            file_stock_WMS = Directory.GetFiles(@carpetatemporal, "*_REPEX_*.csv*"); //nombre del archivo stock en la ruta FTP


            //file_archivo = file_stock_WMS[0].ToString();

            if (file_stock_WMS.Length == 0)
            {

                return false;
            }
            else
            {
                dt_stock.Columns.Add("COD_SUCURSAL", typeof(string));
                dt_stock.Columns.Add("COD_CADENA", typeof(string));
                dt_stock.Columns.Add("COD_ALMACEN", typeof(string));
                dt_stock.Columns.Add("COD_ARTICULO", typeof(string));
                dt_stock.Columns.Add("COD_ALTERNATIVO", typeof(string));
                dt_stock.Columns.Add("STOCK", typeof(Int32));
                dt_stock.Columns.Add("FEC_STOCK", typeof(string));
                //dt_stock.Columns.Add("FECHA_REG", typeof(string));

                for (int i = 0; i < file_stock_WMS.Length; i++)
                {
                    string[] lineas = File.ReadAllLines(file_stock_WMS[i]);
                    string cod_sucursal = "", cod_cadena = "", cod_almacen = "", producto = "", cod_alternativo = "", fec_stock = "";
                    Int32 stock = 0;

                    foreach (string lin in lineas)
                    {
                        string[] campos = lin.Split(',');

                        if (campos[0] != "\"Sucursal\"" && campos[1] != "\"Cod Bloq\"" && campos[2] != "\"Producto\"" && campos[3] != "\"Alternativo\"" && campos[4] != "\"UnAct\"" && campos[5] != "\"Fecha\"")
                        {
                            if (campos[0].ToString().Remove(0, 1).Replace('"', ' ') != " ")
                            {
                                cod_sucursal = campos[0].ToString().Remove(0, 1).Replace('"', ' ');// COD_SUCURSAL
                            }

                            if (campos[1].ToString().Remove(0, 1).Replace('"', ' ') != " ")
                            {
                                cod_cadena = campos[1].ToString().Remove(0, 1).Replace('"', ' ').Substring(0, 2); //COD_CADENA
                            }

                            if (campos[1].ToString().Remove(0, 1).Replace('"', ' ') != " ")
                            {
                                cod_almacen = campos[1].ToString().Remove(0, 1).Replace('"', ' ').Substring(2, 1); //COD_ALMACEN
                            }

                            if (campos[2].ToString().Remove(0, 1).Replace('"', ' ') != " ")
                            {
                                producto = campos[2].ToString().Remove(0, 1).Replace('"', ' '); //PRODUCTO
                            }

                            if (campos[3].ToString().Remove(0, 1).Replace('"', ' ') != " ")
                            {
                                cod_alternativo = campos[3].ToString().Remove(0, 1).Replace('"', ' '); //COD_ALTERNATIVO
                            }

                            stock = Convert.ToInt32(campos[4].ToString().Remove(0, 1).Replace('"', ' ')); //STOCK

                            if (campos[5].ToString().Remove(0, 1).Replace('"', ' ') != " ")
                            {
                                fec_stock = campos[5].ToString().Remove(0, 1).Replace('"', ' '); //FECHA STOCK
                            }

                            dt_stock.Rows.Add(cod_sucursal, cod_cadena, cod_almacen, producto, cod_alternativo, stock, fec_stock);
                        }
                    }
                    //lib de excel para obtener registros
                    //DataSet ds;

                    //using (var stream = new FileStream(file_archivo, FileMode.Open))
                    //{
                    //    var file = new FileInfo(file_archivo);

                    //    IExcelDataReader reader = null;

                    //    if (file.Extension == ".xls")
                    //    {
                    //        reader = ExcelReaderFactory.CreateBinaryReader(stream);

                    //    }
                    //    else if (file.Extension == ".xlsx")
                    //    {
                    //        reader = ExcelReaderFactory.CreateOpenXmlReader(stream);
                    //    }
                    //    else if (file.Extension == "csv")
                    //    {

                    //    }
                    //    ds = reader.AsDataSet();

                    /*-------------------insertamos a BD--------------------------------*/

                    using (SqlConnection cn = new SqlConnection(Conexion.conexion))
                    {

                        try
                        {
                            if (cn.State == 0) cn.Open();
                            using (SqlCommand cmd = new SqlCommand("USP_INSERTAR_STOCK_WMS", cn))
                            {
                                cmd.CommandTimeout = 0;
                                cmd.CommandType = CommandType.StoredProcedure;
                                cmd.Parameters.AddWithValue("@TAB", dt_stock);

                                cmd.ExecuteNonQuery();
                            }

                            if (cn != null)

                                if (cn.State == ConnectionState.Open) cn.Close();

                            exito = true;

                            LogUtil.Graba_Log("FOTOSTOCK", "Se actualizo : " + dt_stock.Rows.Count + " Registros", true, "");
                            dt_stock.Clear();
                        }

                        catch (Exception ex)
                        {
                            LogUtil.Graba_Log("FOTOSTOCK", "Error : " + ex.Message.ToString(), true, "");

                            exito = false;
                        }
                    }

                }

            }

            return exito;
        }
        public bool ArchivarCSV()
        {
            bool exito = false;

            //verifica si existe la carpeta stock antes de guardar el archivo csv , si no existe lo crea
            Crear_Carpetas objCreaCarpeta = new Crear_Carpetas();
            objCreaCarpeta.ArchivaInterface("FOTOSTOCK");

            try
            {

                for (int i = 0; i < file_stock_WMS.Length; i++) // recorre la ruta local (Work)
                {
                    // Elimina el archivo existente de la carpeta FOTOSTOCK
                    if (File.Exists(Crear_Carpetas.CFOTOSTOCK + Path.GetFileName(file_stock_WMS[i])))
                    {
                        File.Delete(Crear_Carpetas.CFOTOSTOCK + Path.GetFileName(file_stock_WMS[i]));
                        File.Move(file_stock_WMS[i], Crear_Carpetas.CFOTOSTOCK + Path.GetFileName(file_stock_WMS[i]));
                    }
                    else
                    {
                        File.Move(file_stock_WMS[i], Crear_Carpetas.CFOTOSTOCK + Path.GetFileName(file_stock_WMS[i]));
                    }

                }
                exito = true;
            }

            catch (Exception ex)
            {
                LogUtil.Graba_Log("FOTOSTOCK", "Error al tratar de archivar el .csv , sin embargo la actualizacion del STOCK se completo satisfactoriamente : " + ex.Message.ToString(), true, "");
                exito = true; //no se pudo archivar porque supuestamente el archivo ya existe ,pero deberia enviar true para que elimine el archivo del ftp sino seguiria enviando en la proxima lectura 

            }
            return exito;
        }

        public bool BorrarCSV_FTP()
        {
            bool exito = false;

            try
            {
                using (Session session = new Session())
                {
                    string ruta = "";
                    ruta.Replace("\\", "");
                    session.Open(sessionOptions); // Connect

                    for (int i = 0; i < file_stock_WMS.Length; i++)
                    {
                        session.RemoveFiles("/Peru/730/" + ruta + Path.GetFileName(file_stock_WMS[i]));
                    }

                    exito = true;
                }

            }
            catch (Exception ex)
            {

                LogUtil.Graba_Log("FOTOSTOCK", "Error al tratar de borrar el .csv del FTP, sin embargo la actualizacion del STOCK se completo satisfactoriamente : " + ex.Message.ToString(), true, "");
                exito = false;
            }

            return exito;


        }

    }
}





