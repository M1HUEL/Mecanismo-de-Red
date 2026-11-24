package red;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import red.api.IReceptorExterno;

public class Receptor implements ObservadorReceptor {

  private final BlockingQueue<byte[]> cola = new LinkedBlockingQueue<>();
  private final List<IReceptorExterno> receptores = new CopyOnWriteArrayList<>();
  private final ServidorTCP servidor;
  private Thread hilo;
  private volatile boolean activo;

  public Receptor(IReceptorExterno... receptores) {
    agregarReceptores(receptores);
    this.servidor = new ServidorTCP(this);
  }

  private void agregarReceptores(IReceptorExterno... receptores) {
    if (receptores == null) {
      throw new IllegalArgumentException("Los receptores no pueden ser nulos");
    }
    for (IReceptorExterno receptor : receptores) {
      if (receptor == null) {
        throw new IllegalArgumentException("El receptor no puede ser nulo");
      }
      this.receptores.add(receptor);
    }
  }

  public void agregarReceptor(IReceptorExterno receptor) {
    if (receptor != null) {
      this.receptores.add(receptor);
    }
  }

  public void eliminarReceptor(IReceptorExterno receptor) {
    this.receptores.remove(receptor);
  }

  @Override
  public void alRecibir(byte[] datos) {
    try {
      this.cola.put(datos == null ? new byte[0] : datos);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public synchronized void iniciar(int puerto) {
    if (this.activo) {
      return;
    }
    this.servidor.iniciar(puerto);
    if (!this.servidor.estaActivo()) {
      return;
    }
    this.cola.clear();
    this.activo = true;
    this.hilo = new Thread(this::procesarCola, "receptor");
    this.hilo.setDaemon(true);
    this.hilo.start();
  }

  private void procesarCola() {
    while (this.activo) {
      try {
        entregar(this.cola.take());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void entregar(byte[] datos) {
    for (IReceptorExterno receptor : this.receptores) {
      try {
        receptor.alRecibir(datos);
      } catch (Exception e) {
        System.err.println("Error al procesar un mensaje: " + e.getMessage());
      }
    }
  }

  public synchronized void detener() {
    if (!this.activo) {
      return;
    }
    this.activo = false;
    this.servidor.detener();
    if (this.hilo != null) {
      this.hilo.interrupt();
      try {
        this.hilo.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      this.hilo = null;
    }
  }

  public boolean estaActivo() {
    return this.activo && this.servidor.estaActivo();
  }

  public int obtenerPuerto() {
    return this.servidor.obtenerPuerto();
  }
}
