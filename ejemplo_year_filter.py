# main_filter_years.py
# from middleware import MessageMiddlewareQueue
from Middleware.middleware import MessageMiddlewareQueue
from year_filter import FilterYearsNode

if __name__ == "__main__":
    in_mw  = MessageMiddlewareQueue(host="amqp://localhost", queue_name="transactions.raw")
    out_mw = MessageMiddlewareQueue(host="amqp://localhost", queue_name="transactions.y2024_2025")

    node = FilterYearsNode(
        in_mw=in_mw,
        out_mw=out_mw,
        allowed_years={2024, 2025},
        ts_field="created_at",  # <- ya viene por default, lo dejo explícito
    )
    node.start()