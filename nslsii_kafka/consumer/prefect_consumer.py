import argparse
import logging
import os
import socket
from pprint import pformat

from bluesky_kafka import RemoteDispatcher
from event_model import RunRouter
from loki_logger_handler.loki_logger_handler import LokiLoggerHandler
from nslsii.kafka_utils import _read_bluesky_kafka_config_file
from prefect.deployments import run_deployment


def get_arg_parser():
    arg_parser = argparse.ArgumentParser(description="Run a Prefect 2 workflow in response to a Kafka message.")
    arg_parser.add_argument("endstation")
    arg_parser.add_argument("deployment_name")
    arg_parser.add_argument("--kafka-config-file", required=False, default="/etc/bluesky/kafka.yml")
    return arg_parser


def parse_bluesky_kafka_config_file(config_file_path):
    raw_bluesky_kafka_config = _read_bluesky_kafka_config_file(config_file_path=config_file_path)

    # convert the list of bootstrap servers into a comma-delimited string
    #   this is the format required by the confluent python api
    bootstrap_servers = ",".join(raw_bluesky_kafka_config["bootstrap_servers"])

    # extract security configuration
    #   it might be a good idea to explicitly specify consumer configuration(s)
    #   in /etc/bluesky/kafka.yml
    security_config = {
        k: raw_bluesky_kafka_config["runengine_producer_config"][k]
        for k in ("security.protocol", "sasl.mechanisms", "sasl.username", "sasl.password", "ssl.ca.location")
    }

    return bootstrap_servers, security_config


def configure_logger():
    logger = logging.getLogger(__name__)

    loki_push_secret = os.environ.get("LOKI_PUSH_SECRET")
    if loki_push_secret:
        handler = LokiLoggerHandler(
            url="https://loki-push-applications.nsls2.bnl.gov/loki/api/v1/push",
            labels={"job": "prefect_consumer", "host": socket.gethostname()},
            label_keys=["level"],
            additional_headers={
                "loki-push-secret": loki_push_secret,
            },
        )
    else:
        handler = logging.StreamHandler()

    logger.addHandler(handler)
    return logger


def message_to_workflow():
    args = get_arg_parser().parse_args()
    logger = configure_logger()

    bootstrap_servers, security_config = parse_bluesky_kafka_config_file(config_file_path=args.kafka_config_file)
    print(f"bootstrap_servers:\n{pformat(bootstrap_servers)}")
    logger.info(
        "Configured Kafka consumer for endstation=%s deployment=%s bootstrap_servers=%s",
        args.endstation,
        args.deployment_name,
        bootstrap_servers,
    )

    consumer_config = {"auto.offset.reset": "latest"}
    consumer_config.update(security_config)
    # print(f"consumer_config:\n{pformat(consumer_config)}")

    topic = f"{args.endstation}.bluesky.runengine.documents"
    document_to_workflow_dispatcher = RemoteDispatcher(
        topics=[topic],
        bootstrap_servers=bootstrap_servers,
        group_id=f"{args.endstation}-workflow-2",
        consumer_config=consumer_config,
    )

    def consumer_factory(start_doc_name, start_doc):
        print(f"start uid: {start_doc['uid']}")
        logger.info("Received start document uid=%s", start_doc["uid"], extra={"uid": start_doc["uid"]})

        def run_flow_on_stop_document(doc_name, doc):
            if doc_name == "stop":
                # kick off a Prefect 2 deployment
                print(f"stop document:\n{pformat(doc)}")
                print(f"run deployment {args.deployment_name}")
                logger.info(
                    "Received stop document for run_start=%s; triggering deployment %s",
                    doc.get("run_start"),
                    args.deployment_name,
                    extra={
                        "doc_name": doc_name,
                        "uid": doc.get("uid"),
                        "run_start": doc.get("run_start"),
                        "exit_status": doc.get("exit_status"),
                    },
                )
                try:
                    run_deployment(
                        name=args.deployment_name,
                        parameters={"stop_doc": doc},
                        timeout=0,
                    )
                except Exception:
                    logger.exception(
                        "Failed to trigger deployment %s for run_start=%s",
                        args.deployment_name,
                        doc.get("run_start"),
                    )
                    raise
            else:
                print(doc_name)
                logger.debug("Received document %s", doc_name)

        return [run_flow_on_stop_document], []

    workflow_router = RunRouter(factories=[consumer_factory])

    document_to_workflow_dispatcher.subscribe(workflow_router)

    print("start the dispatcher")
    logger.info("Starting Kafka dispatcher for topic %s", topic)
    document_to_workflow_dispatcher.start()

    print("all done")
    logger.info("Kafka dispatcher stopped")


if __name__ == "__main__":
    message_to_workflow()
