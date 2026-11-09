"""Shared validation and addresses for a local cluster."""
import argparse


def node_count(value):
    try:
        count = int(value)
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError("Enter a whole number from 1 to 9") from None
    if not 1 <= count < 10:
        raise argparse.ArgumentTypeError("Node count must be from 1 to 9")
    return count


def base_port(value):
    try:
        port = int(value)
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError("Port must be a whole number") from None
    if not 1024 <= port <= 65527:
        raise argparse.ArgumentTypeError("Base port must be from 1024 to 65527")
    return port


def addresses(count=5, port=50051):
    return [f"localhost:{base_port(port) + i}" for i in range(node_count(count))]
