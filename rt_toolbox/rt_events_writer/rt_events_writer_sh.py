# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import argparse
import logging
import signal
import time

from rt_toolbox.config import config
from rt_toolbox.logging_configuration import (
    LoggingLevel,
    LoggingDestination,
    set_up_logging,
    configure_logging_destination,
    configure_logging_level
)
from rt_rabbitmq_wrapper.rabbitmq_utility import (
    RabbitMQError
)
from rt_rabbitmq_wrapper.exchange_types.event import (
    event_protobuf
)

from rt_rabbitmq_wrapper.exchange_types.event.event_pb2 import (
    Event as PB_Event,
    TaskStartedEvent as PB_TaskStartedEvent,
    TaskFinishedEvent as PB_TaskFinishedEvent,
    CheckpointReachedEvent as PB_CheckpointReachedEvent,
    VariableValueAssignedEvent as PB_VariableValueAssignedEvent,
    ClockPauseEvent as PB_ClockPauseEvent,
    ClockResetEvent as PB_ClockResetEvent,
    ClockResumeEvent as PB_ClockResumeEvent,
    ClockStartEvent as PB_ClockStartEvent,
    ComponentEvent as PB_ComponentEvent
)

from rt_toolbox.rt_events_writer import rabbitmq_server_connections
from rt_toolbox.utility import (
    is_valid_file_with_extension_nex,
    is_valid_file_with_extension
)


# Errors:
# -1: Output file error
# -2: RabbitMQ server setup error
def main():
    # Signal handling flags
    signal_flags = {'stop': False, 'pause': False}

    # Signal handling functions
    def sigint_handler(signum, frame):
        signal_flags['stop'] = True

    def sigtstp_handler(signum, frame):
        signal_flags['pause'] = not signal_flags['pause']  # Toggle pause state

    # Registering signal handlers
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTSTP, sigtstp_handler)

    # Argument processing
    parser = argparse.ArgumentParser(
        prog = "The Events Writer for The Runtime Reporter.",
        description = "Writes events received from the events exchange at a RabbitMQ server to a file.",
        epilog = "Example: python -m rt_toolbox.rt_events_writer.rt_events_writer_sh /path/to/file --rabbitmq_config_file=./rabbitmq_config.toml --log_file=output.log --log_level=debug --timeout=120"
    )
    parser.add_argument('dest_file', help='Path to the file to be written.')
    parser.add_argument("--rabbitmq_config_file", type=str, default='./rabbitmq_config.toml', help='Path to the TOML file containing the RabbitMQ server configuration.')
    parser.add_argument("--log_level", type=str, choices=["debug", "info", "warnings", "errors", "critical"], default="info", help="Log verbosity level.")
    parser.add_argument('--log_file', help='Path to log file.')
    parser.add_argument("--timeout", type=int, default=0, help="Timeout in seconds to wait for events after last received, from the RabbitMQ event server (0 = no timeout).")
    # Parse arguments
    args = parser.parse_args()
    # Set up the logging infrastructure
    # Configure logging level.
    match args.log_level:
        case "debug":
            logging_level = LoggingLevel.DEBUG
        case "info":
            logging_level = LoggingLevel.INFO
        case "warnings":
            logging_level = LoggingLevel.WARNING
        case "errors":
            logging_level = LoggingLevel.ERROR
        case "critical":
            logging_level = LoggingLevel.CRITICAL
        case _:
            logging_level = LoggingLevel.INFO
    # Configure logging destination.
    if args.log_file is None:
        logging_destination = LoggingDestination.CONSOLE
    else:
        valid_log_file = is_valid_file_with_extension_nex(args.log_file, "log")
        if not valid_log_file:
            logging_destination = LoggingDestination.CONSOLE
        else:
            logging_destination = LoggingDestination.FILE
    set_up_logging()
    configure_logging_destination(logging_destination, args.log_file)
    configure_logging_level(logging_level)
    # Create a logger for the RabbitMQ utility component
    logger = logging.getLogger("rt_toolbox.rt_events_writer.rt_events_writer")
    logger.info(f"Log verbosity level: {logging_level}.")
    if args.log_file is None:
        logger.info("Log destination: CONSOLE.")
    else:
        if not valid_log_file:
            logger.info("Log file error. Log destination: CONSOLE.")
        else:
            logger.info(f"Log destination: FILE ({args.log_file}).")
    # Validate and normalize the event report path
    if args.dest_file is not None:
        valid = is_valid_file_with_extension_nex(args.dest_file, 'any')
        if not valid:
            logger.error(f"Output file error.")
            exit(-1)
        dest_file = args.dest_file
    else:
        dest_file = "./output_file.txt"
    logger.info(f"Output file: {dest_file}")
    # Determine timeout
    config.timeout = args.timeout if args.timeout >= 0 else 0
    logger.info(f"Timeout for event reception from RabbitMQ server: {config.timeout} seconds.")
    # RabbitMQ infrastructure configuration
    valid = is_valid_file_with_extension(args.rabbitmq_config_file, "toml")
    if not valid:
        logger.critical(f"RabbitMQ infrastructure configuration file error.")
        exit(-1)
    logger.info(f"RabbitMQ infrastructure configuration file: {args.rabbitmq_config_file}")
    rabbitmq_server_connections.build_rabbitmq_server_connections(args.rabbitmq_config_file)
    # Start receiving events from the RabbitMQ server
    logger.info(f"Start receiving events from queue {rabbitmq_server_connections.rabbitmq_event_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
    # Open the output file and the AMQP connection
    with open(dest_file, "w") as output_file:
        # initialize last_message_time for testing timeout
        last_message_time = time.time()
        start_time_epoch = time.time()
        number_of_events = 0
        # Control variables
        poison_received = False
        stop = False
        abort = False
        while not poison_received and not stop and not abort:
            # Handle SIGINT
            if signal_flags['stop']:
                logger.info("SIGINT received. Stopping the event reception process.")
                stop = True
            # Handle SIGTSTP
            if signal_flags['pause']:
                logger.info("SIGTSTP received. Pausing the event reception process.")
                while signal_flags['pause'] and not signal_flags['stop']:
                    time.sleep(1)  # Efficiently wait for signals
                if signal_flags['stop']:
                    logger.info("SIGINT received. Stopping the event reception process.")
                    stop = True
                if not signal_flags['pause']:
                    logger.info("SIGTSTP received. Resuming the event reception process.")
            # Timeout handling for event reception
            if 0 < config.timeout < (time.time() - last_message_time):
                abort = True
            # Process event only if temination has not been decided
            if not stop and not abort:
                # Get event from RabbitMQ
                try:
                    method, properties, body = rabbitmq_server_connections.rabbitmq_event_server_connection.get_message()
                except RabbitMQError:
                    logger.critical(f"Error receiving event from queue {rabbitmq_server_connections.rabbitmq_event_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
                    exit(-2)
                if method:  # Message exists
                    # Process message
                    if properties.headers and properties.headers.get('termination'):
                        # Poison pill received
                        logger.info(f"Poison pill received from queue {rabbitmq_server_connections.rabbitmq_event_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
                        poison_received = True
                    else:
                        last_message_time = time.time()
                        # Event received
                        pb_event = PB_Event()
                        pb_event.ParseFromString(body)
                        event = event_protobuf.protobuf_to_event(pb_event)
                        # Log event reception
                        logger.debug(f"Event received: {event}.")
                        # Write event in the output file
                        output_file.write(event.serialized()+'\n')
                        output_file.flush()
                        # Only increment number_of_events is it is a valid event (rules out poisson pill)
                        number_of_events += 1
                    # ACK the message
                    try:
                        rabbitmq_server_connections.rabbitmq_event_server_connection.ack_message(method.delivery_tag)
                    except RabbitMQError:
                        logger.critical(f"Error sending ack to the exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ event server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
                        exit(-2)
        # Stop receiving messages from the RabbitMQ server
        logger.info(f"Stop receiving events from queue {rabbitmq_server_connections.rabbitmq_event_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
        # Close connection if it exists
        rabbitmq_server_connections.rabbitmq_event_server_connection.close()
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if poison_received:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received.")
        elif stop:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        elif abort:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, event reception timeout reached ({time.time()-last_message_time} secs.).")
        else:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason.")
    exit(0)


if __name__ == "__main__":
    main()
