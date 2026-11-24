package red;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import red.api.IDispatcher;

public class Dispatcher implements IDispatcher {

  private record Sobre(String host, int puerto, byte[] datos) {

  }

  private final BlockingQueue<Sobre> cola = new LinkedBlockingQueue<>();
  private final List<ObservadorDispatcher> observadores = new CopyOnWriteArrayList<>();
  private Thread hilo;
  private volatile boolean activo;

  public Dispatcher(ObservadorDispatcher... observadores) {
    agregarObservadores(observadores);
  }

  private void agregarObservadores(ObservadorDispatcher... observadores) {
    if (observadores == null) {
      return;
    }
    for (ObservadorDispatcher observador : observadores) {
      if (observador != null) {
        this.observadores.add(observador);
      }
    }
  }

  public void agregarObservador(ObservadorDispatcher observador) {
    if (observador != null) {
      this.observadores.add(observador);
    }
  }

  public void eliminarObservador(ObservadorDispatcher observador) {
    this.observadores.remove(observador);
  }

  @Override
  public synchronized void enviar(String host, int puerto, byte[] datos) {
    if (!this.activo) {
      throw new IllegalStateException("El dispatcher no está activo. Llama a iniciar() primero.");
    }
    byte[] contenido = datos == null ? new byte[0] : datos;
    try {
      this.cola.put(new Sobre(host, puerto, contenido));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public synchronized void iniciar() {
    if (this.activo) {
      return;
    }
    this.activo = true;
    this.cola.clear();
    this.hilo = new Thread(this::procesarCola, "dispatcher");
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

  private void entregar(Sobre sobre) {
    for (ObservadorDispatcher observador : this.observadores) {
      try {
        observador.alEnviar(sobre.host(), sobre.puerto(), sobre.datos());
      } catch (IOException e) {
        System.err.println("No se pudo enviar a " + sobre.host() + ":" + sobre.puerto()
          + ": " + e.getMessage());
      }
    }
  }

  public synchronized void detener() {
    if (!this.activo) {
      return;
    }
    this.activo = false;
    List<Sobre> pendientes = new ArrayList<>();
    this.cola.drainTo(pendientes);
    for (Sobre sobre : pendientes) {
      entregar(sobre);
    }
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
    return this.activo;
  }
}
