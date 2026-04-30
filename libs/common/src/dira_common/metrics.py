from __future__ import annotations

import builtins
from typing import Any

try:
    from prometheus_client import Counter, Gauge, Histogram
except ModuleNotFoundError:
    Counter = None
    Gauge = None
    Histogram = None


class _FallbackMetric:
    def __init__(self, name: str, documentation: str, metric_type: str, labelnames: tuple[str, ...] = ()) -> None:
        self.name = name
        self.documentation = documentation
        self.metric_type = metric_type
        self.labelnames = labelnames
        self.value = 0.0
        self.samples: list[float] = []
        self.labels_called_with: tuple[tuple[Any, ...], dict[str, Any]] | None = None

    def labels(self, *args: Any, **kwargs: Any) -> "_FallbackMetric":
        self.labels_called_with = (args, kwargs)
        return self

    def inc(self, amount: float = 1.0) -> None:
        self.value += amount

    def observe(self, amount: float) -> None:
        self.samples.append(amount)

    def set(self, value: float) -> None:
        self.value = value


_METRIC_CACHE: dict[str, Any] = getattr(builtins, "_dira_prometheus_metric_cache", {})
if not hasattr(builtins, "_dira_prometheus_metric_cache"):
    builtins._dira_prometheus_metric_cache = _METRIC_CACHE


def _metric_key(name: str, metric_type: str, labelnames: tuple[str, ...]) -> str:
    return f"{metric_type}:{name}:{','.join(labelnames)}"


def _get_metric(name: str, metric_type: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Any:
    cache_key = _metric_key(name, metric_type, labelnames)
    if cache_key in _METRIC_CACHE:
        return _METRIC_CACHE[cache_key]

    if Counter is None or Histogram is None or Gauge is None:
        metric = _FallbackMetric(name, documentation, metric_type, labelnames)
    elif metric_type == "counter":
        metric = Counter(name, documentation, labelnames=labelnames)
    elif metric_type == "histogram":
        metric = Histogram(name, documentation, labelnames=labelnames)
    elif metric_type == "gauge":
        metric = Gauge(name, documentation, labelnames=labelnames)
    else:
        metric = _FallbackMetric(name, documentation, metric_type, labelnames)

    _METRIC_CACHE[cache_key] = metric
    return metric


class PrometheusRegistry:
    messages_published = _get_metric(
        "dira_messages_published_total",
        "counter",
        "Total number of messages published by DIRA components.",
        ("topic", "status"),
    )
    messages_failed = _get_metric(
        "dira_messages_failed_total",
        "counter",
        "Total number of messages that failed processing or publication.",
        ("topic", "status"),
    )
    processing_latency_seconds = _get_metric(
        "dira_processing_latency_seconds",
        "histogram",
        "Processing latency in seconds for DIRA tasks.",
        (),
    )
    kafka_messages_published_total = _get_metric(
        "kafka_messages_published_total",
        "counter",
        "Total number of Kafka messages published by DIRA components.",
        ("topic", "status"),
    )
    kafka_publish_latency_seconds = _get_metric(
        "kafka_publish_latency_seconds",
        "histogram",
        "Kafka message publish latency in seconds.",
        (),
    )
    kafka_consumer_lag = _get_metric(
        "kafka_consumer_lag",
        "gauge",
        "Current Kafka consumer lag measured in messages.",
        ("group_id", "topic"),
    )
    telecom_pings_received_total = _get_metric(
        "telecom_pings_received_total",
        "counter",
        "Total telecom pings received by the telecom connector.",
        (),
    )
    telecom_pings_filtered_residential_total = _get_metric(
        "telecom_pings_filtered_residential_total",
        "counter",
        "Total telecom pings filtered by the residential signal filter.",
        (),
    )
    telecom_pings_filtered_bbox_total = _get_metric(
        "telecom_pings_filtered_bbox_total",
        "counter",
        "Total telecom pings filtered outside the DSM bounding box.",
        (),
    )
    telecom_pings_published_total = _get_metric(
        "telecom_pings_published_total",
        "counter",
        "Total telecom pings published to Kafka.",
        (),
    )
    telecom_publish_latency_seconds = _get_metric(
        "telecom_publish_latency_seconds",
        "histogram",
        "Latency for publishing telecom messages to Kafka.",
        (),
    )
    cctv_frames_processed_total = _get_metric(
        "cctv_frames_processed_total",
        "counter",
        "Total CCTV frames processed by the video pipeline.",
        (),
    )
    cctv_vehicles_detected_total = _get_metric(
        "cctv_vehicles_detected_total",
        "counter",
        "Total vehicles detected by the CCTV pipeline.",
        (),
    )
    cctv_inference_latency_ms = _get_metric(
        "cctv_inference_latency_ms",
        "histogram",
        "Inference latency in milliseconds for CCTV frame processing.",
        (),
    )
    cctv_detections_published_total = _get_metric(
        "cctv_detections_published_total",
        "counter",
        "Total CCTV detections published to Kafka.",
        (),
    )
    cctv_stream_reconnects_total = _get_metric(
        "cctv_stream_reconnects_total",
        "counter",
        "Total CCTV stream reconnect attempts.",
        (),
    )


messages_published = PrometheusRegistry.messages_published
messages_failed = PrometheusRegistry.messages_failed
processing_latency_seconds = PrometheusRegistry.processing_latency_seconds
kafka_messages_published_total = PrometheusRegistry.kafka_messages_published_total
kafka_publish_latency_seconds = PrometheusRegistry.kafka_publish_latency_seconds
kafka_consumer_lag = PrometheusRegistry.kafka_consumer_lag
telecom_pings_received_total = PrometheusRegistry.telecom_pings_received_total
telecom_pings_filtered_residential_total = PrometheusRegistry.telecom_pings_filtered_residential_total
telecom_pings_filtered_bbox_total = PrometheusRegistry.telecom_pings_filtered_bbox_total
telecom_pings_published_total = PrometheusRegistry.telecom_pings_published_total
telecom_publish_latency_seconds = PrometheusRegistry.telecom_publish_latency_seconds
cctv_frames_processed_total = PrometheusRegistry.cctv_frames_processed_total
cctv_vehicles_detected_total = PrometheusRegistry.cctv_vehicles_detected_total
cctv_inference_latency_ms = PrometheusRegistry.cctv_inference_latency_ms
cctv_detections_published_total = PrometheusRegistry.cctv_detections_published_total
cctv_stream_reconnects_total = PrometheusRegistry.cctv_stream_reconnects_total
