import logging
from unittest.mock import MagicMock

from ..consumer import prefect_consumer
from ..consumer.prefect_consumer import configure_logger, get_arg_parser


def test_arg_parser():
    "Test that CLI arguments are assigned as expected"
    args = ["arg_0", "arg_1"]
    arg_parser = get_arg_parser()
    parsed_args = arg_parser.parse_args(args)

    assert parsed_args.endstation == "arg_0"
    assert parsed_args.deployment_name == "arg_1"
    assert parsed_args.kafka_config_file == "/etc/bluesky/kafka.yml"


def test_configure_logger_without_loki_secret(monkeypatch):
    "Test that a regular StreamHandler is used when LOKI_PUSH_SECRET is not set"
    monkeypatch.delenv("LOKI_PUSH_SECRET", raising=False)
    mock_loki_handler = MagicMock()
    monkeypatch.setattr(prefect_consumer, "LokiLoggerHandler", mock_loki_handler)

    logger = logging.getLogger(prefect_consumer.__name__)
    original_handlers = list(logger.handlers)
    original_level = logger.level
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)
    try:
        configured_logger = configure_logger()
        assert configured_logger is logger
        assert configured_logger.getEffectiveLevel() == logging.INFO
        assert len(configured_logger.handlers) == 1
        assert isinstance(configured_logger.handlers[0], logging.StreamHandler)
        mock_loki_handler.assert_not_called()
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)


def test_configure_logger_with_loki_secret(monkeypatch):
    "Test that LokiLoggerHandler is used at INFO level when LOKI_PUSH_SECRET is set"
    monkeypatch.setenv("LOKI_PUSH_SECRET", "test-secret")
    fake_handler = logging.NullHandler()
    mock_loki_handler = MagicMock(return_value=fake_handler)
    monkeypatch.setattr(prefect_consumer, "LokiLoggerHandler", mock_loki_handler)

    logger = logging.getLogger(prefect_consumer.__name__)
    original_handlers = list(logger.handlers)
    original_level = logger.level
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)
    try:
        configured_logger = configure_logger()
        assert configured_logger is logger
        assert configured_logger.getEffectiveLevel() == logging.INFO
        assert configured_logger.handlers == [fake_handler]
        mock_loki_handler.assert_called_once()
        _, kwargs = mock_loki_handler.call_args
        assert kwargs["additional_headers"] == {"loki-push-secret": "test-secret"}
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
