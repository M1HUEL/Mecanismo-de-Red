package red;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ClienteTCP implements ObservadorDispatcher {

  private static final int TIEMPO_ESPERA_CONEXION = 5000;

  @Override
  public void alEnviar(String host, int puerto, byte[] datos) throws IOException {
    byte[] contenido = datos == null ? new byte[0] : datos;
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, puerto), TIEMPO_ESPERA_CONEXION);
      try (DataOutputStream salida = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
        salida.writeInt(contenido.length);
        salida.write(contenido);
        salida.flush();
      }
    }
  }
}
