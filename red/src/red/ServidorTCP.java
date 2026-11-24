package red;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ServidorTCP {

  private static final int TAMANO_MAXIMO = 10 * 1024 * 1024;

  private final List<ObservadorReceptor> observadores = new CopyOnWriteArrayList<>();
  private final Set<Socket> clientes = ConcurrentHashMap.newKeySet();
  private volatile ServerSocket servidor;
  private Thread hiloAceptacion;
  private volatile boolean activo;

  public ServidorTCP(ObservadorReceptor... observadores) {
    if (observadores == null) {
      return;
    }
    for (ObservadorReceptor observador : observadores) {
      if (observador != null) {
        this.observadores.add(observador);
      }
    }
  }

  public void agregarObservador(ObservadorReceptor observador) {
    if (observador != null) {
      this.observadores.add(observador);
    }
  }

  public void eliminarObservador(ObservadorReceptor observador) {
    this.observadores.remove(observador);
  }

  public void notificarObservadores(byte[] datos) {
    for (ObservadorReceptor observador : this.observadores) {
      observador.alRecibir(datos);
    }
  }

  public synchronized void iniciar(int puerto) {
    if (this.activo) {
      return;
    }

    try {
      this.servidor = new ServerSocket(puerto);
    } catch (IOException e) {
      this.servidor = null;
      throw new IllegalStateException("No se pudo abrir el servidor TCP en el puerto " + puerto + ": " + e.getMessage(), e);
    }

    this.activo = true;
    this.hiloAceptacion = new Thread(this::aceptarConexiones, "servidor-tcp");
    this.hiloAceptacion.setDaemon(true);
    this.hiloAceptacion.start();
  }

  public synchronized void detener() {
    this.activo = false;
    cerrarServidor();
    cerrarClientes();
    if (this.hiloAceptacion != null) {
      this.hiloAceptacion.interrupt();
      this.hiloAceptacion = null;
    }
  }

  private void aceptarConexiones() {
    while (this.activo) {
      ServerSocket actual = this.servidor;
      if (actual == null) {
        return;
      }
      try {
        Socket cliente = actual.accept();
        this.clientes.add(cliente);
        iniciarLector(cliente);
      } catch (IOException e) {
        if (!this.activo || this.servidor == null || this.servidor.isClosed()) {
          return;
        }
        System.err.println("Error al aceptar una conexión: " + e.getMessage());
      }
    }
  }

  private void iniciarLector(Socket cliente) {
    Thread lector = new Thread(() -> leerDe(cliente), "lector-tcp-" + cliente.getPort());
    lector.setDaemon(true);
    lector.start();
  }

  private void leerDe(Socket cliente) {
    try (DataInputStream entrada
      = new DataInputStream(new BufferedInputStream(cliente.getInputStream()))) {
      int longitud = entrada.readInt();
      if (longitud < 0 || longitud > TAMANO_MAXIMO) {
        throw new IOException("Tamaño de paquete no permitido: " + longitud);
      }
      byte[] datos = new byte[longitud];
      entrada.readFully(datos);
      notificarObservadores(datos);
    } catch (Exception e) {
      if (this.activo && !(e instanceof EOFException)) {
        System.err.println("Error al leer del cliente " + cliente.getPort() + ": " + e.getMessage());
      }
    } finally {
      this.clientes.remove(cliente);
      cerrarSocket(cliente);
    }
  }

  private void cerrarServidor() {
    ServerSocket actual = this.servidor;
    this.servidor = null;
    if (actual == null) {
      return;
    }
    try {
      actual.close();
    } catch (IOException e) {
      System.err.println("No se pudo cerrar el servidor TCP: " + e.getMessage());
    }
  }

  private void cerrarClientes() {
    for (Socket cliente : this.clientes) {
      cerrarSocket(cliente);
    }
    this.clientes.clear();
  }

  private void cerrarSocket(Socket socket) {
    try {
      socket.close();
    } catch (IOException e) {
      if (this.activo) {
        System.err.println("No se pudo cerrar un socket: " + e.getMessage());
      }
    }
  }

  public boolean estaActivo() {
    return this.activo;
  }

  public int obtenerPuerto() {
    ServerSocket actual = this.servidor;
    return actual == null ? -1 : actual.getLocalPort();
  }
}
