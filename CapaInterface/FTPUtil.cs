using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using WinSCP;

namespace CapaInterface
{
    public class FTPUtil
    {

        public static bool Send_FTP_WMS(string file_origen, string file_destino, string wcd, string winterface)
        {
            bool exito = false;
            //bool exito2 = false;

            try
            {

                // Setup session options
                SessionOptions sessionOptions = new SessionOptions

                {
                    //Protocol = Protocol.Sftp,
                    //HostName = DatosGenerales.UrlFtp, //"172.24.20.183"
                    //UserName = DatosGenerales.UserFtp, //"retailc"
                    //Password = DatosGenerales.PassFtp, //"1wiAwNRa"
                    //PortNumber = Convert.ToInt32(DatosGenerales.PuertoFtp),
                    //GiveUpSecurityAndAcceptAnySshHostKey = true

                    Protocol = Protocol.Sftp,
                    HostName = DatosGenerales.UrlFtp, //"172.24.20.183"
                    UserName = DatosGenerales.UserFtp, //"retailc"
                    Password = DatosGenerales.PassFtp, //"1wiAwNRa"
                    PortNumber = Convert.ToInt32(DatosGenerales.PuertoFtp),
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
                    //TransferOperationResult transferResult2;

                    //ruta ftp principal
                    transferResult = session.PutFiles(file_origen, "/Peru/730/" + wcd + "/input/" + Path.GetFileName(file_destino), false, transferOptions);
                    transferResult.Check();
                    exito = transferResult.IsSuccess;

                    /**/
                    ///ruta bkp (solo para catalogo, carrito, prescripciones)
                    //if (winterface == "PRESC" || winterface == "ORD_CAT" || winterface == "ORD_CAR")
                    //{
                    //    transferResult2 = session.PutFiles(file_origen, "/Peru/730/" + wcd + "/bkinput/" + Path.GetFileName(file_destino), false, transferOptions);

                    //    transferResult2.Check();
                    //    exito2 = transferResult2.IsSuccess;
                  
                    //}else
                    //{
                    //    exito2 = true;

                    //}
                    //exito = (exito && exito2);
                    /**/
                }

                using (Session session2 = new Session())
                {

                    // Connect
                    session2.Open(sessionOptions);

                    // Upload files
                    TransferOptions transferOptions2 = new TransferOptions();
                    transferOptions2.FilePermissions = null; // This is default
                    transferOptions2.PreserveTimestamp = false;
                    transferOptions2.TransferMode = TransferMode.Binary;
                    TransferOperationResult transferResult2;

                    //ruta bkp (solo para catalogo,carrito,prescripciones)
                    if (winterface == "PRESC" || winterface == "ORD_CAT" || winterface == "ORD_CAR")
                    {
                        transferResult2 = session2.PutFiles(file_origen, "/Peru/730/" + wcd + "/bkinput/" + Path.GetFileName(file_destino), false, transferOptions2);

                        // Throw on any error
                        transferResult2.Check();

                        //if (transferResult.IsSuccess == true) exito = true;
                        exito = transferResult2.IsSuccess;
                    }
                }
            }

            catch (Exception ex)
            {
                //varFinal = string.Empty + "°" + string.Empty + "°" + "[ERROR] NO SE PUDO CARGAR EL DOCUMENTO " + NombreArchivo + " " + DateTime.Now + "°" + "0";
                //str.WriteLine("ERROR AL SUBIR ARCHIVO: " + fileTXTc + " " + e.Message + " " + DateTime.Now);
                //LogUtil.Graba_Log("INTERFACE WMS", "ERROR AL SUBIR FTP: " + ex.Message,true,"");
                throw ex;
            }
            return exito;
        }
    }
}
