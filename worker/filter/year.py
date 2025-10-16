from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log, setup_process_logger
import os
from pkg.message.constants import MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_EOF
from multiprocessing import Process

class FilterYearNode(Worker):
    """
    Refactor clave:
    - NO guardamos wrappers con conexiones (Queue/Exchange) en self.
    - Guardamos SOLO strings (host, nombres de colas/exchanges).
    - Cada proceso hijo crea sus propias conexiones.
    """

    def __init__(self, host: str,
                 input_queue_name: str,
                 output_exchange_name: str,
                 output_exchange_queues: dict,
                 eof_exchange_name: str,
                 eof_exchange_queues: dict,
                 eof_service_queue_name: str,
                 eof_self_queue_name: str,
                 eof_final_queue: str,
                 years_set):
        self.__init_manager__()
        # Worker base puede necesitar in_middleware/host/etc: dejamos mínimo indispensable.
        # No guardes instancias con conexiones acá.
        self.host = host
        self.input_queue_name = input_queue_name
        self.output_exchange_name = output_exchange_name
        self.output_exchange_queues = output_exchange_queues
        self.eof_exchange_name = eof_exchange_name
        self.eof_exchange_queues = eof_exchange_queues
        self.eof_service_queue_name = eof_service_queue_name
        self.eof_self_queue_name = eof_self_queue_name
        self.eof_final_queue = eof_final_queue
        self.years = years_set

    def start(self):
        logging.info("Starting data consumer process")
        p_data = Process(target=self._consume_data_queue)

        logging.info("Starting EOF consumer process")
        p_eof = Process(target=self._consume_eof)
        
        logging.info(f"Starting EOF FINAL process")
        p_eof_final = Process(target=self._consume_eof_final)

        for p in (p_data, p_eof, p_eof_final):
            p.start()
        for p in (p_data, p_eof, p_eof_final):
            p.join()

    def _consume_eof_final(self):
        setup_process_logger('name=filter_amount_node', level="INFO")
        eof_final_queue = MessageMiddlewareQueue(self.host, self.eof_final_queue)
        data_output_exchange = MessageMiddlewareExchange(self.host, self.output_exchange_name, self.output_exchange_queues)
        
        def __on_eof_final_message__(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF FINAL recibido en Self EOF Queue | request_id: {message.request_id}")            
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()
                    data_output_exchange.send(message.serialize(), str(message.request_id))
                    logging.info(f"EOF FINAL enviado | request_id: {message.request_id} | type: {message.type}")
            except Exception as e:
                logging.error(f"Error al procesar el mensaje EOF FINAL: {type(e).__name__}: {e}")
        
        eof_final_queue.start_consuming(__on_eof_final_message__)

    
    def _consume_data_queue(self):
        setup_process_logger('name=filter_year_node:data', level="INFO")

        # Cada hijo crea sus propias conexiones
        data_input_queue = MessageMiddlewareQueue(self.host, self.input_queue_name)
        data_output_exchange = MessageMiddlewareExchange(self.host, self.output_exchange_name, self.output_exchange_queues)
        eof_exchange = MessageMiddlewareExchange(self.host, self.eof_exchange_name, self.eof_exchange_queues)

        def _on(message_bytes):
            
            message = None
            try:
                message = Message.deserialize(message_bytes)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF recibido en data queue | request_id: {message.request_id}")
                    # Propagar EOF a las réplicas del nodo (exchange de EOF del stage)
                    eof_exchange.send(message.serialize(), str(message.type))
                    return

                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id)

                items = message.process_message()
                if not items:
                    logging.info(f"No hay items en el mensaje | request_id: {message.request_id} | type: {message.type}")
                    return

                # Filtrado por año y reenvío
                new_chunk = ''.join(it.serialize() for it in items if it.get_year() in self.years)
                if new_chunk:
                    message.update_content(new_chunk)
                    data_output_exchange.send(message.serialize(), str(message.type))

            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
            finally:
                # Evita NameError si falló antes de construir 'message'
                if message is not None:
                    self._dec_inflight(message.request_id)

        # Mantener el loop vivo para heartbeats
        data_input_queue.start_consuming(_on)

    def _consume_eof(self):
        setup_process_logger('name=filter_year_node:eof', level=logging.getLevelName(logging.getLogger().level))

        # Conexiones locales a este proceso
        eof_self_queue = MessageMiddlewareQueue(self.host, self.eof_self_queue_name)       # recibe EOF_NOTIFY del líder/pares
        eof_service_queue = MessageMiddlewareQueue(self.host, self.eof_service_queue_name) # envía ACK al barrier/service

        def on_eof_message(message_bytes):
            try:
                msg = Message.deserialize(message_bytes)
                if msg.type != MESSAGE_TYPE_EOF:
                    return

                logging.info(f"EOF recibido en nodo | request_id: {msg.request_id} | type: {msg.type}")

                # Esperar a que se drenen los lotes en vuelo de este request_id
                self._ensure_request(msg.request_id)
                self.drained[msg.request_id].wait()

                # Enviar ACK/EOF al servicio/barrera:
                eof_service_queue.send(msg.serialize())

            except Exception as e:
                logging.error(f"Error al procesar EOF: {type(e).__name__}: {e}")

        eof_self_queue.start_consuming(on_eof_message)

    # Cierre: como cada proceso crea/cierra sus conexiones, acá no cerramos nada global.
    def close(self):
        pass


# ----------------------------
# Bootstrap
# ----------------------------
def initialize_config():
    config_params = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST'),
        "input_queue": os.getenv('INPUT_QUEUE_1'),
        "output_queue_1": os.getenv('OUTPUT_QUEUE_1'),
        "output_queue_2": os.getenv('OUTPUT_QUEUE_2'),
        "output_queue_3": os.getenv('OUTPUT_QUEUE_3'),
        "output_exchange_filter_year": os.getenv('EXCHANGE_NAME'),
        "eof_exchange_name": os.getenv('EOF_EXCHANGE_NAME'),
        "eof_self_queue": os.getenv('EOF_SELF_QUEUE'),
        "eof_queue_1": os.getenv('EOF_QUEUE_1'),
        "eof_queue_2": os.getenv('EOF_QUEUE_2'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "eof_final_queue": os.getenv('EOF_FINAL_QUEUE'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
    }

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue_1",
        "output_queue_2",
        "output_queue_3",
        "output_exchange_filter_year",
    ]
    missing = [k for k in required_keys if config_params[k] is None]
    if missing:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing)}. Aborting filter.")
    return config_params


def main():
    cfg = initialize_config()
    initialize_log(cfg["logging_level"])

    # ⚠️ Ya NO creamos conexiones acá para pasarlas a Process.
    # Pasamos SOLO strings al nodo, y que cada proceso cree su conexión.
    output_exchange_queues =  {cfg["output_queue_1"]: [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_EOF)], 
                                cfg["output_queue_2"]: [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_EOF)], 
                                cfg["output_queue_3"]: [str(MESSAGE_TYPE_TRANSACTION_ITEMS), str(MESSAGE_TYPE_EOF)]}
    eof_exchange_queues = {cfg["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                            cfg["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]}
    node = FilterYearNode(
        host=cfg["rabbitmq_host"],
        input_queue_name=cfg["input_queue"],
        output_exchange_name=cfg["output_exchange_filter_year"],
        output_exchange_queues=output_exchange_queues,
        eof_exchange_name=cfg["eof_exchange_name"],
        eof_exchange_queues=eof_exchange_queues,
        eof_service_queue_name=cfg["eof_service_queue"],
        eof_self_queue_name=cfg["eof_self_queue"],
        eof_final_queue=cfg["eof_final_queue"],
        years_set={2024, 2025},
    )
    node.start()

if __name__ == "__main__":
    main()
