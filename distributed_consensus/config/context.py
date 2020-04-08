from .loader import Config
import contextvars


config: contextvars.ContextVar[Config] = contextvars.ContextVar('config')
