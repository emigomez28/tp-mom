# Informe TP MoM

## Decisiones de diseño

### Exchange tipo direct y cola anónima por consumidor

El exchange se declara `direct` ya que el broker entrega cada mensaje a las colas ligadas con una routing key idéntica a la del mensaje.

Cada consumidor declara su propia cola anónima (`exclusive`, `auto-delete`) dentro de `StartConsuming` y la liga a todas sus routing keys. Cada consumidor recibe su propia copia del mensaje y la cola se elimina sola al cerrarse la conexión, sin dejar recursos huérfanos en el broker.

### ACK explícito y NACK con reencolado

El consumo se registra con `auto-ack` desactivado: el mensaje se confirma sólo cuando el callback invoca `ack()`. Un mensaje no confirmado vuelve a la cola si el consumidor se cae, lo que evita pérdidas.

Por otro lado, `nack()` reencola el mensaje (`requeue = true`) para que pueda ser reintentado por otro consumidor.
