# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import argparse
import logging
import signal
import time

from rt_toolbox.config import config
from rt_toolbox.rabbitmq_server_configs import rabbitmq_server_config, rabbitmq_exchange_config
from rt_toolbox.rabbitmq_server_connections import rabbitmq_server_connection
from rt_toolbox.logging_configuration import (
    LoggingLevel,
    LoggingDestination,
    set_up_logging,
    configure_logging_destination,
    configure_logging_level
)
from rt_rabbitmq_wrapper.rabbitmq_utility import (
    RabbitMQError,
    get_message,
    ack_message,
    connect_to_server,
    connect_to_channel_exchange,
    declare_queue
)
from rt_toolbox.utility import (
    is_valid_file_with_extension_nex
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
        prog = "The File Writer for The Runtime Reporter.",
        description="Writes a file with the messages got from a RabbitMQ server.",
        epilog = "Example: python -m rt_toolbox.rt_file_writer.rt_file_writer_sh /path/to/file --host=https://myrabbitmq.org.ar --port=5672 --user=my_user --password=my_password --exchange=events_hub --routing_key=events --log_file=output.log --log_level=event --timeout=120"
    )
    parser.add_argument('dest_file', help='Path to the file to be written.')
    parser.add_argument('--host', type=str, default='localhost', help='RabbitMQ event server host.')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ event server port.')
    parser.add_argument('--user', default='guest', help='RabbitMQ event server user.')
    parser.add_argument('--password', default='guest', help='RabbitMQ event server password.')
    parser.add_argument('--exchange', type=str, default='my_exchange', help='Name of the exchange at the RabbitMQ event server.')
    parser.add_argument('--routing_key', type=str, default='my_routing_key', help='Name of the routing key used at the RabbitMQ server.')
    parser.add_argument("--log_level", type=str, choices=["debug", "info", "warnings", "errors", "critical"], default="info", help="Log verbosity level.")
    parser.add_argument('--log_file', help='Path to log file.')
    parser.add_argument("--timeout", type=int, default=0, help="Timeout in seconds to wait for messages after last received, from the RabbitMQ event server (0 = no timeout).")
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
    logger = logging.getLogger("rt_toolbox.file_writer_sh")
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
    timeout = args.timeout if args.timeout >= 0 else 0
    logger.info(f"Timeout for the message reception from RabbitMQ server: {timeout} seconds.")
    # RabbitMQ server configuration
    rabbitmq_server_config.host = args.host
    rabbitmq_server_config.port = args.port
    rabbitmq_server_config.user = args.user
    rabbitmq_server_config.password = args.password
    # RabbitMQ exchange configuration
    rabbitmq_exchange_config.exchange = args.exchange
    rabbitmq_exchange_config.routing_key = args.routing_key
    # Other configuration
    config.timeout = timeout
    # Open the output file and the AMQP connection
    with open(dest_file, "w") as output_file:
        # Set up the connection to the RabbitMQ connection to server
        try:
            connection = connect_to_server(rabbitmq_server_config)
        except RabbitMQError:
            logger.critical(f"Error setting up the connection to the RabbitMQ server.")
            exit(-2)
        # Set up the RabbitMQ channel and exchange for log entries with the RabbitMQ server
        try:
            channel = connect_to_channel_exchange(
                rabbitmq_server_config,
                rabbitmq_exchange_config,
                connection
            )
        except RabbitMQError:
            logger.critical(f"Error setting up the channel and exchange at the RabbitMQ server.")
            exit(-2)
        # Set up the RabbitMQ queue and routing key for events with the RabbitMQ server
        try:
            queue_name = declare_queue(
                rabbitmq_server_config,
                rabbitmq_exchange_config,
                channel,
                rabbitmq_exchange_config.routing_key
            )
        except RabbitMQError:
            logger.critical(f"Error declaring and binding queue to the exchange at the RabbitMQ server.")
            exit(-2)
        # Set up connection for evens with the RabbitMQ server
        rabbitmq_server_connection.connection = connection
        rabbitmq_server_connection.channel = channel
        rabbitmq_server_connection.exchange = rabbitmq_exchange_config.exchange
        rabbitmq_server_connection.queue_name = queue_name
        # Start getting messages from the RabbitMQ server
        logger.info(f"Start getting messages from queue {queue_name} - exchange {rabbitmq_exchange_config.exchange} at RabbitMQ server at {rabbitmq_server_config.host}:{rabbitmq_server_config.port}.")
        # initialize last_message_time for testing timeout
        last_message_time = time.time()
        start_time_epoch = time.time()
        number_of_messages = 0
        # Control variables
        poison_received = False
        stop = False
        abort = False
        while not poison_received and not stop and not abort:
            # Handle SIGINT
            if signal_flags['stop']:
                logger.info("SIGINT received. Stopping the message reception process.")
                stop = True
            # Handle SIGTSTP
            if signal_flags['pause']:
                logger.info("SIGTSTP received. Pausing the message reception process.")
                while signal_flags['pause'] and not signal_flags['stop']:
                    time.sleep(1)  # Efficiently wait for signals
                if signal_flags['stop']:
                    logger.info("SIGINT received. Stopping the message reception process.")
                    stop = True
                if not signal_flags['pause']:
                    logger.info("SIGTSTP received. Resuming the message reception process.")
            # Timeout handling for message reception
            if 0 < config.timeout < (time.time() - last_message_time):
                abort = True
            # Process event only if temination has not been decided
            if not stop and not abort:
                # Get event from RabbitMQ
                try:
                    method, properties, body = get_message(rabbitmq_server_connection)
                except RabbitMQError:
                    logger.critical(f"Error getting message from RabbitMQ server.")
                    exit(-2)
                if method:  # Message exists
                    # Process message
                    if properties.headers and properties.headers.get('termination'):
                        # Poison pill received
                        logger.info(f"Poison pill received with the {rabbitmq_exchange_config.routing_key} routing_key from the RabbitMQ server.")
                        poison_received = True
                    else:
                        last_message_time = time.time()
                        # Event received
                        event = body.decode()
                        # Log event reception
                        logger.debug(f"Message received: {event}.")
                        # Write event in the output file
                        output_file.write(event+'\n')
                        output_file.flush()
                        # Only increment number_of_messages is it is a valid event (rules out poisson pill)
                        number_of_messages += 1
                    # ACK the message
                    try:
                        ack_message(rabbitmq_server_connection, method.delivery_tag)
                    except RabbitMQError:
                        logger.critical(f"Error acknowledging a message to the RabbitMQ event server.")
                        exit(-2)
        # Stop getting messages from the RabbitMQ server
        logger.info(f"Stop getting messages from the RabbitMQ server at {rabbitmq_server_config.host}:{rabbitmq_server_config.port}.")
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if poison_received:
            logger.info(f"Written messages: {number_of_messages} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received.")
        elif stop:
            logger.info(f"Written messages: {number_of_messages} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        elif abort:
            logger.info(f"Written messages: {number_of_messages} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, message reception timeout reached ({time.time()-last_message_time} secs.).")
        else:
            logger.info(f"Written messages: {number_of_messages} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason.")
        # Close connection if it exists
        if connection and connection.is_open:
            try:
                connection.close()
                logger.info(f"Connection to RabbitMQ server at {args.host}:{args.port} closed.")
            except Exception as e:
                logger.error(f"Error closing connection to RabbitMQ server at {args.host}:{args.port}: {e}.")
    exit(0)


if __name__ == "__main__":
    main()
