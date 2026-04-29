from __future__ import annotations

from enum import Enum


class CongestionLevel(str, Enum):
    FREE_FLOW = "free_flow"
    LIGHT = "light"
    MODERATE = "moderate"
    HEAVY = "heavy"
    SEVERE = "severe"


class DataSourceType(str, Enum):
    TELECOM = "telecom"
    CCTV = "cctv"
    FLEET_GPS = "fleet_gps"
    INCIDENT = "incident"
    WEATHER = "weather"
    FUSED = "fused"


class PipelineStage(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class IncidentType(str, Enum):
    ACCIDENT = "accident"
    ROADBLOCK = "roadblock"
    CONSTRUCTION = "construction"
    FLOODING = "flooding"
    OTHER = "other"


class RoadType(str, Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    TERTIARY = "tertiary"
    RESIDENTIAL = "residential"


class WeatherCondition(str, Enum):
    CLEAR = "clear"
    RAIN = "rain"
    HEAVY_RAIN = "heavy_rain"
    FOG = "fog"


class ModelType(str, Enum):
    CONGESTION_PREDICTOR = "congestion_predictor"
    QUEUE_PROPAGATION = "queue_propagation"
    ROUTE_SCORER = "route_scorer"
    VEHICLE_DETECTOR = "vehicle_detector"
