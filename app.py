from __future__ import annotations

import csv
import io
import json
import math
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from functools import wraps
from io import BytesIO
from xml.sax.saxutils import escape

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from flask import Flask, Response, jsonify, redirect, render_template, request, session, url_for
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from connection import bootstrap_db, get_db


app = Flask(__name__)
app.secret_key = "codexmbs-clean-rebuild-secret"

DEFAULT_BUS_CAPACITY = 30
MAP_FALLBACK_COORDS = [15.4865, 120.9667]
ROUTE_STOPS = {
    "Cabiao - Cabanatuan": [
        ("Cabiao Terminal", 15.2484, 120.8542),
        ("Cabiao", 15.2861, 120.8923),
        ("San Isidro", 15.3680, 120.9431),
        ("Cabanatuan City", 15.4865, 120.9667),
    ],
    "Gapan - Cabanatuan": [
        ("Gapan City", 15.3079, 120.9460),
        ("Peñaranda", 15.3605, 120.9542),
        ("Cabanatuan City", 15.4865, 120.9667),
    ],
    "San Isidro - Cabanatuan": [
        ("San Isidro", 15.3295, 120.9392),
        ("Burgos", 15.4078, 120.9550),
        ("Cabanatuan City", 15.4865, 120.9667),
    ],
}

DISCOUNTED_PASSENGER_TYPES = {"student", "pwd", "senior"}
ROUTE_STOP_DETAILS = {
    "Cabiao - Cabanatuan": [
        {"name": "Cabiao Terminal", "lat": 15.2484, "lng": 120.8542, "minutes_from_start": 0, "landmark": "Municipal terminal"},
        {"name": "Cabiao Junction", "lat": 15.2861, "lng": 120.8923, "minutes_from_start": 10, "landmark": "Town proper boarding area"},
        {"name": "San Isidro Public Market", "lat": 15.3680, "lng": 120.9431, "minutes_from_start": 28, "landmark": "Market and jeep transfer point"},
        {"name": "Cabanatuan Central Terminal", "lat": 15.4865, "lng": 120.9667, "minutes_from_start": 55, "landmark": "Main city terminal"},
    ],
    "Gapan - Cabanatuan": [
        {"name": "Gapan Transport Hub", "lat": 15.3079, "lng": 120.9460, "minutes_from_start": 0, "landmark": "Gapan dispatch area"},
        {"name": "Penaranda Junction", "lat": 15.3605, "lng": 120.9542, "minutes_from_start": 18, "landmark": "Municipal roadside pickup"},
        {"name": "Cabanatuan Central Terminal", "lat": 15.4865, "lng": 120.9667, "minutes_from_start": 46, "landmark": "Main city terminal"},
    ],
    "San Isidro - Cabanatuan": [
        {"name": "San Isidro Public Market", "lat": 15.3295, "lng": 120.9392, "minutes_from_start": 0, "landmark": "Public market stop"},
        {"name": "Burgos", "lat": 15.4078, "lng": 120.9550, "minutes_from_start": 17, "landmark": "Barangay roadside pickup"},
        {"name": "Cabanatuan Central Terminal", "lat": 15.4865, "lng": 120.9667, "minutes_from_start": 38, "landmark": "Main city terminal"},
    ],
}
ROUTE_STOPS = {
    route_name: [(stop["name"], stop["lat"], stop["lng"]) for stop in stops]
    for route_name, stops in ROUTE_STOP_DETAILS.items()
}

FORWARD_ROUTE_NAME = "Cabiao to Cabanatuan"
REVERSE_ROUTE_NAME = "Cabanatuan to Cabiao"
CORRIDOR_TRAVEL_MINUTES = 55
CORRIDOR_DISTANCE_KM = 27.5
CORRIDOR_START = (15.2484, 120.8542)
CORRIDOR_END = (15.4865, 120.9667)
CORRIDOR_STOP_DETAILS = [
    {"name": "Cabiao Town Proper", "lat": 15.2484, "lng": 120.8542, "minutes_from_start": 0},
    {"name": "San Fernando Sur", "lat": 15.2730, "lng": 120.8780, "minutes_from_start": 4},
    {"name": "San Fernando Norte", "lat": 15.2840, "lng": 120.8890, "minutes_from_start": 6},
    {"name": "San Roque, San Isidro", "lat": 15.2970, "lng": 120.9040, "minutes_from_start": 9},
    {"name": "Sto. Cristo, San Isidro", "lat": 15.3050, "lng": 120.9150, "minutes_from_start": 11},
    {"name": "Alua, San Isidro", "lat": 15.3160, "lng": 120.9280, "minutes_from_start": 14},
    {"name": "San Isidro Town Proper", "lat": 15.3295, "lng": 120.9392, "minutes_from_start": 17},
    {"name": "Malapit, San Isidro", "lat": 15.3335, "lng": 120.9426, "minutes_from_start": 18},
    {"name": "San Isidro Border", "lat": 15.3368, "lng": 120.9450, "minutes_from_start": 19},
    {"name": "San Nicolas, Brgy. Chalmers", "lat": 15.3408, "lng": 120.9472, "minutes_from_start": 20},
    {"name": "Sto. Nino, Gapan", "lat": 15.3458, "lng": 120.9488, "minutes_from_start": 21},
    {"name": "San Leonardo Welcome", "lat": 15.3498, "lng": 120.9498, "minutes_from_start": 23},
    {"name": "Castellano, DGDLH", "lat": 15.3588, "lng": 120.9538, "minutes_from_start": 24},
    {"name": "Northview Heights", "lat": 15.3648, "lng": 120.9562, "minutes_from_start": 25},
    {"name": "Jaen Diversion", "lat": 15.3685, "lng": 120.9560, "minutes_from_start": 27},
    {"name": "NEECO II - Area 2", "lat": 15.3755, "lng": 120.9575, "minutes_from_start": 28},
    {"name": "San Leonardo Rice Mill", "lat": 15.3830, "lng": 120.9592, "minutes_from_start": 30},
    {"name": "V. Del Rosario Rice Mill", "lat": 15.3905, "lng": 120.9607, "minutes_from_start": 32},
    {"name": "Eco Energy Fuel Stop", "lat": 15.3980, "lng": 120.9620, "minutes_from_start": 34},
    {"name": "Tabuating Magnolia", "lat": 15.4060, "lng": 120.9630, "minutes_from_start": 36},
    {"name": "Fuel Star Sta. Rosa", "lat": 15.4140, "lng": 120.9640, "minutes_from_start": 38},
    {"name": "San Mariano", "lat": 15.4230, "lng": 120.9648, "minutes_from_start": 40},
    {"name": "Sta. Rosa Newstar", "lat": 15.4320, "lng": 120.9654, "minutes_from_start": 42},
    {"name": "San Gregorio", "lat": 15.4410, "lng": 120.9659, "minutes_from_start": 44},
    {"name": "NEUST Sumacab", "lat": 15.4500, "lng": 120.9662, "minutes_from_start": 47},
    {"name": "NE Pacific", "lat": 15.4600, "lng": 120.9664, "minutes_from_start": 49},
    {"name": "Lamarang", "lat": 15.4730, "lng": 120.9665, "minutes_from_start": 52},
    {"name": "Cabanatuan Terminal", "lat": 15.4865, "lng": 120.9667, "minutes_from_start": 55},
]
CORRIDOR_STOP_NAMES = [stop["name"] for stop in CORRIDOR_STOP_DETAILS]


def build_reverse_stop_details(stop_details, total_minutes):
    reversed_details = []
    for index, stop in enumerate(reversed(stop_details), start=1):
        reversed_details.append(
            {
                "name": stop["name"],
                "lat": stop["lat"],
                "lng": stop["lng"],
                "minutes_from_start": total_minutes - int(stop["minutes_from_start"]),
                "landmark": "Cabiao-Cabanatuan corridor stop",
            }
        )
    reversed_details.sort(key=lambda item: item["minutes_from_start"])
    return reversed_details


FORWARD_ROUTE_STOPS = [
    {
        "name": stop["name"],
        "lat": stop["lat"],
        "lng": stop["lng"],
        "minutes_from_start": stop["minutes_from_start"],
        "landmark": "Cabiao-Cabanatuan corridor stop",
    }
    for stop in CORRIDOR_STOP_DETAILS
]
REVERSE_ROUTE_STOPS = build_reverse_stop_details(CORRIDOR_STOP_DETAILS, CORRIDOR_TRAVEL_MINUTES)
ROUTE_STOP_DETAILS = {
    FORWARD_ROUTE_NAME: FORWARD_ROUTE_STOPS,
    REVERSE_ROUTE_NAME: REVERSE_ROUTE_STOPS,
}
ROUTE_STOPS = {
    route_name: [(stop["name"], stop["lat"], stop["lng"]) for stop in stops]
    for route_name, stops in ROUTE_STOP_DETAILS.items()
}


def now():
    return datetime.now()


def to_db_time(value: datetime | None):
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else None


def from_db_time(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def classify_capacity(occupancy, capacity=DEFAULT_BUS_CAPACITY):
    safe_capacity = max(capacity or DEFAULT_BUS_CAPACITY, 1)
    ratio = occupancy / safe_capacity

    if ratio <= 0.4:
        return "Low"
    if ratio <= 0.75:
        return "Medium"
    return "High"


def capacity_details(occupancy, capacity=DEFAULT_BUS_CAPACITY):
    safe_capacity = max(capacity or DEFAULT_BUS_CAPACITY, 1)
    percent = round((occupancy / safe_capacity) * 100)
    return {
        "limit": safe_capacity,
        "count": occupancy,
        "percent": max(0, percent),
        "label": classify_capacity(occupancy, safe_capacity),
    }


def to_non_negative_int(value, default=0):
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return default


def parse_route_coords(coords_json):
    if isinstance(coords_json, bytes):
        coords_json = coords_json.decode("utf-8")
    try:
        coords = json.loads(coords_json or "[]")
        if not isinstance(coords, list):
            return []

        normalized = []
        for pair in coords:
            if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                try:
                    normalized.append([float(pair[0]), float(pair[1])])
                except (TypeError, ValueError):
                    continue
        return normalized
    except json.JSONDecodeError:
        return []


def normalize_json_value(value):
    if isinstance(value, dict):
        return {key: normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [normalize_json_value(item) for item in value]
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, Decimal):
        return float(value)
    return value


def slugify_label(value):
    return "".join(char.lower() if char.isalnum() else "-" for char in (value or "")).strip("-")


def get_route_stop_details(route_name):
    return [dict(stop, sequence=index + 1) for index, stop in enumerate(ROUTE_STOP_DETAILS.get(route_name, []))]


def rebuild_route_stop_cache(stop_rows):
    route_stop_details = {}
    for row in stop_rows:
        route_stop_details.setdefault(row["route_name"], []).append(
            {
                "name": row["stop_name"],
                "lat": float(row["latitude"]),
                "lng": float(row["longitude"]),
                "minutes_from_start": int(row["minutes_from_start"] or 0),
                "landmark": row.get("landmark") or "Corridor stop",
            }
        )

    for route_name, stops in route_stop_details.items():
        stops.sort(key=lambda stop: stop["minutes_from_start"])

    route_stops = {
        route_name: [(stop["name"], stop["lat"], stop["lng"]) for stop in stops]
        for route_name, stops in route_stop_details.items()
    }
    return route_stop_details, route_stops


def refresh_route_stop_cache(conn):
    global ROUTE_STOP_DETAILS, ROUTE_STOPS
    stop_rows = conn.execute(
        """
        SELECT r.route_name,
               s.stop_name,
               s.latitude,
               s.longitude,
               s.landmark,
               rs.stop_sequence,
               rs.minutes_from_start
        FROM route_stops rs
        JOIN routes r ON r.id = rs.route_id
        JOIN stops s ON s.id = rs.stop_id
        WHERE s.is_active = 1
        ORDER BY r.display_order, r.route_name, rs.stop_sequence
        """
    ).fetchall()
    if not stop_rows:
        return
    route_stop_details, route_stops = rebuild_route_stop_cache(stop_rows)
    if route_stop_details:
        ROUTE_STOP_DETAILS = route_stop_details
        ROUTE_STOPS = route_stops


def stop_name_key(value):
    return " ".join((value or "").lower().split())


def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_decimal(value, default="0.00"):
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def quantize_money(value):
    return to_decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def distance_between_points_km(lat_a, lng_a, lat_b, lng_b):
    latitude_a = math.radians(float(lat_a))
    longitude_a = math.radians(float(lng_a))
    latitude_b = math.radians(float(lat_b))
    longitude_b = math.radians(float(lng_b))
    delta_latitude = latitude_b - latitude_a
    delta_longitude = longitude_b - longitude_a
    haversine = (
        math.sin(delta_latitude / 2) ** 2
        + math.cos(latitude_a) * math.cos(latitude_b) * math.sin(delta_longitude / 2) ** 2
    )
    angular_distance = 2 * math.atan2(math.sqrt(haversine), math.sqrt(1 - haversine))
    return 6371 * angular_distance


def round_peso(value):
    return to_decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def estimate_segment_distance(route_distance_km, route_duration_minutes, origin_stop, destination_stop, stop_count):
    numeric_distance_km = to_float(route_distance_km, 0.0)
    if route_duration_minutes and destination_stop["minutes_from_start"] >= origin_stop["minutes_from_start"]:
        covered_minutes = destination_stop["minutes_from_start"] - origin_stop["minutes_from_start"]
        return round(numeric_distance_km * (covered_minutes / route_duration_minutes), 1)

    index_gap = max(destination_stop["sequence"] - origin_stop["sequence"], 0)
    denominator = max(stop_count - 1, 1)
    return round(numeric_distance_km * (index_gap / denominator), 1)


def estimate_fare_table(distance_km, minimum_fare=15, discounted_fare=None):
    base_regular = max(round_peso(minimum_fare), Decimal("1"))
    numeric_distance_km = to_decimal(distance_km)
    extra_distance = max(numeric_distance_km - Decimal("4.00"), Decimal("0.00"))
    regular = max(round_peso(base_regular + (extra_distance * Decimal("1.35"))), base_regular)
    default_discounted = max(round_peso(base_regular * Decimal("0.8")), Decimal("1"))
    base_discounted = max(round_peso(discounted_fare if discounted_fare is not None else default_discounted), Decimal("1"))
    discounted_total = max(round_peso(base_discounted + (extra_distance * Decimal("1.10"))), base_discounted)
    fare_table = {"regular": float(regular)}
    for passenger_type in DISCOUNTED_PASSENGER_TYPES:
        fare_table[passenger_type] = float(discounted_total)
    return fare_table


def calculate_passenger_fare_total(passenger_type, quantity, distance_km=0, minimum_fare=15, discounted_fare=None):
    fare_table = estimate_fare_table(distance_km, minimum_fare, discounted_fare)
    unit_fare = round_peso(fare_table.get(passenger_type, fare_table["regular"]))
    return float(round_peso(unit_fare * max(int(quantity or 0), 0)))


def find_stop_index(route_stops, stop_name):
    keyed_name = stop_name_key(stop_name)
    for index, stop in enumerate(route_stops):
        if stop_name_key(stop["name"]) == keyed_name:
            return index
    return -1


def infer_bus_stop_index(bus, route_stops):
    next_stop_index = find_stop_index(route_stops, bus.get("nextStop"))
    if next_stop_index >= 0:
        return next_stop_index

    lat = bus.get("lat")
    lng = bus.get("lng")
    if lat is None or lng is None:
        return -1

    nearest_index = -1
    nearest_score = None
    for index, stop in enumerate(route_stops):
        score = distance_between_points_km(float(lat), float(lng), stop["lat"], stop["lng"])
        if nearest_score is None or score < nearest_score:
            nearest_score = score
            nearest_index = index
    return nearest_index


def estimate_bus_arrival_minutes(bus, route_stops, target_stop_name):
    target_index = find_stop_index(route_stops, target_stop_name)
    current_index = infer_bus_stop_index(bus, route_stops)
    if target_index < 0 or current_index < 0 or current_index > target_index:
        return None

    current_stop = route_stops[current_index]
    target_stop = route_stops[target_index]
    return max(target_stop["minutes_from_start"] - current_stop["minutes_from_start"], 0)


def get_trip_current_stop_details(trip, latest_gps=None, fallback_stop_name=None):
    route_stops = get_route_stop_details(trip.get("route_name"))
    if not route_stops:
        return None

    if latest_gps and latest_gps.get("latitude") is not None and latest_gps.get("longitude") is not None:
        latitude = float(latest_gps["latitude"])
        longitude = float(latest_gps["longitude"])
        nearest_stop = min(
            route_stops,
            key=lambda stop: distance_between_points_km(latitude, longitude, stop["lat"], stop["lng"]),
        )
        return nearest_stop

    fallback_index = find_stop_index(route_stops, fallback_stop_name)
    if fallback_index >= 0:
        return route_stops[fallback_index]
    return route_stops[0]


def get_trip_destination_options(trip, current_stop_name=None):
    route_stops = get_route_stop_details(trip.get("route_name"))
    current_index = find_stop_index(route_stops, current_stop_name)
    if current_index < 0:
        current_index = 0
    return route_stops[current_index + 1 :]


def estimate_trip_segment_distance(trip, origin_stop_name, destination_stop_name):
    route_stops = get_route_stop_details(trip.get("route_name"))
    origin_index = find_stop_index(route_stops, origin_stop_name)
    destination_index = find_stop_index(route_stops, destination_stop_name)
    if origin_index < 0 or destination_index < 0 or destination_index <= origin_index:
        return 0.0
    return estimate_segment_distance(
        trip.get("distance_km"),
        trip.get("expected_duration_minutes"),
        route_stops[origin_index],
        route_stops[destination_index],
        len(route_stops),
    )


def calculate_segment_fare_total(trip, passenger_type, quantity, origin_stop_name, destination_stop_name):
    segment_distance = estimate_trip_segment_distance(trip, origin_stop_name, destination_stop_name)
    return calculate_passenger_fare_total(
        passenger_type,
        quantity,
        segment_distance,
        trip.get("minimum_fare"),
        trip.get("discounted_fare"),
    )


def build_trip_destination_manifest(conn, trip, current_stop_name=None):
    route_stops = get_route_stop_details(trip.get("route_name"))
    current_index = find_stop_index(route_stops, current_stop_name)
    manifest = {}
    for row in conn.execute(
        """
        SELECT destination_stop, quantity
        FROM trip_transactions
        WHERE trip_id = ? AND event_type = 'board'
        ORDER BY recorded_at DESC, id DESC
        """,
        (trip["id"],),
    ).fetchall():
        destination_stop = row.get("destination_stop")
        if not destination_stop:
            continue
        destination_index = find_stop_index(route_stops, destination_stop)
        if current_index >= 0 and destination_index >= 0 and destination_index <= current_index:
            continue
        manifest[destination_stop] = manifest.get(destination_stop, 0) + int(row.get("quantity") or 0)
    ordered_manifest = []
    for stop in route_stops:
        count = manifest.get(stop["name"], 0)
        if count > 0:
            ordered_manifest.append({"name": stop["name"], "count": count})
    return ordered_manifest


def auto_offboard_due_passengers(conn, trip, current_stop_name=None, latitude=None, longitude=None, conductor_id=None):
    route_stops = get_route_stop_details(trip.get("route_name"))
    current_index = find_stop_index(route_stops, current_stop_name)
    if current_index < 0:
        return {"dropped": 0, "remaining": int(trip.get("occupancy") or 0)}

    destination_balances = {}
    for row in conn.execute(
        """
        SELECT event_type, destination_stop, quantity
        FROM trip_transactions
        WHERE trip_id = ?
          AND destination_stop IS NOT NULL
          AND destination_stop <> ''
        ORDER BY recorded_at ASC, id ASC
        """,
        (trip["id"],),
    ).fetchall():
        destination_stop = row.get("destination_stop")
        destination_index = find_stop_index(route_stops, destination_stop)
        if destination_index < 0:
            continue
        bucket = destination_balances.setdefault(
            destination_stop,
            {"destination_stop": destination_stop, "sequence": destination_index + 1, "boarded": 0, "dropped": 0},
        )
        quantity = int(row.get("quantity") or 0)
        if row.get("event_type") == "drop":
            bucket["dropped"] += quantity
        else:
            bucket["boarded"] += quantity

    current_outstanding = sum(max(item["boarded"] - item["dropped"], 0) for item in destination_balances.values())
    due_destinations = [
        item
        for item in destination_balances.values()
        if item["sequence"] - 1 <= current_index and item["boarded"] > item["dropped"]
    ]
    due_destinations.sort(key=lambda item: item["sequence"])
    if not due_destinations:
        return {"dropped": 0, "remaining": current_outstanding}

    recorded_at = to_db_time(now())
    total_dropped = 0
    for item in due_destinations:
        quantity = item["boarded"] - item["dropped"]
        current_outstanding = max(current_outstanding - quantity, 0)
        total_dropped += quantity
        conn.execute(
            """
            INSERT INTO trip_transactions (
                trip_id, conductor_id, event_type, passenger_type, quantity, fare_amount,
                stop_name, origin_stop, destination_stop, latitude, longitude, occupancy_after, recorded_at
            )
            VALUES (?, ?, 'drop', 'mixed', ?, NULL, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trip["id"],
                conductor_id,
                quantity,
                current_stop_name or item["destination_stop"],
                current_stop_name or item["destination_stop"],
                item["destination_stop"],
                float(latitude) if latitude is not None else None,
                float(longitude) if longitude is not None else None,
                current_outstanding,
                recorded_at,
            ),
        )

    manifest = sync_trip_occupancy_from_destinations(conn, trip, current_stop_name)
    remaining = sum(item["count"] for item in manifest)
    crowd_level = classify_capacity(remaining, trip.get("capacity"))
    conn.execute(
        """
        INSERT INTO trip_records (
            trip_id, students, pwd, senior, regular, boarded, dropped, total,
            occupancy_after, crowd_level, stop_name, latitude, longitude, recorded_at
        )
        VALUES (?, 0, 0, 0, 0, 0, ?, 0, ?, ?, ?, ?, ?, ?)
        """,
        (
            trip["id"],
            total_dropped,
            remaining,
            crowd_level,
            current_stop_name or "Route stop",
            float(latitude) if latitude is not None else None,
            float(longitude) if longitude is not None else None,
            recorded_at,
        ),
    )
    if conductor_id:
        log_event(
            conn,
            conductor_id,
            "conductor",
            "Auto Offboard",
            f"Trip #{trip['id']} automatically offboarded {total_dropped} passenger(s) at {current_stop_name}.",
        )
    return {"dropped": total_dropped, "remaining": remaining}


def sync_trip_occupancy_from_destinations(conn, trip, current_stop_name=None):
    manifest = build_trip_destination_manifest(conn, trip, current_stop_name)
    occupancy = sum(item["count"] for item in manifest)
    capacity = max(int(trip.get("capacity") or DEFAULT_BUS_CAPACITY), 1)
    peak = max(int(trip.get("peak_occupancy") or 0), occupancy)
    conn.execute(
        """
        UPDATE trips
        SET occupancy = ?, peak_occupancy = ?, average_load = ?
        WHERE id = ?
        """,
        (occupancy, peak, round((occupancy / capacity) * 100, 1), trip["id"]),
    )
    trip["occupancy"] = occupancy
    trip["peak_occupancy"] = peak
    return manifest


def get_active_service_alerts(conn):
    rows = conn.execute(
        """
        SELECT sa.id,
               sa.trip_id,
               sa.route_id,
               sa.stop_name,
               sa.title,
               sa.message,
               sa.severity,
               sa.created_at,
               r.route_name
        FROM service_alerts sa
        LEFT JOIN routes r ON r.id = sa.route_id
        WHERE sa.is_active = 1
        ORDER BY
            CASE sa.severity
                WHEN 'critical' THEN 0
                WHEN 'warning' THEN 1
                ELSE 2
            END,
            sa.created_at DESC,
            sa.id DESC
        LIMIT 8
        """
    ).fetchall()
    return [dict(row) for row in rows]


def sync_trip_service_alert(conn, trip_id, is_active):
    trip = conn.execute(
        """
        SELECT t.id, t.started_at, b.plate_number, r.id AS route_id, r.route_name, r.start_point, r.end_point
        FROM trips t
        JOIN buses b ON b.id = t.bus_id
        JOIN routes r ON r.id = t.route_id
        WHERE t.id = ?
        """,
        (trip_id,),
    ).fetchone()
    if not trip:
        return

    existing_alert = conn.execute(
        "SELECT id FROM service_alerts WHERE trip_id = ?",
        (trip_id,),
    ).fetchone()

    title = f"New trip active: {trip['plate_number']}"
    message = f"{trip['plate_number']} is now active on {trip['route_name']} from {trip['start_point']} to {trip['end_point']}."

    if is_active:
        if existing_alert:
            conn.execute(
                """
                UPDATE service_alerts
                SET route_id = ?, stop_name = ?, title = ?, message = ?, severity = 'info', is_active = 1, created_at = ?
                WHERE id = ?
                """,
                (trip["route_id"], trip["start_point"], title, message, to_db_time(now()), existing_alert["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO service_alerts (trip_id, route_id, stop_name, title, message, severity, is_active, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, 'info', 1, NULL, ?)
                """,
                (trip_id, trip["route_id"], trip["start_point"], title, message, to_db_time(now())),
            )
    elif existing_alert:
        conn.execute("UPDATE service_alerts SET is_active = 0 WHERE id = ?", (existing_alert["id"],))


def build_public_commuter_data(conn, live_data=None):
    live_data = live_data or build_live_bus_data(conn)
    active_alerts = get_active_service_alerts(conn)
    route_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT id, route_name, start_point, end_point, distance_km, expected_duration_minutes, coords_json,
                   is_published, minimum_fare, discounted_fare, display_order
            FROM routes
            WHERE is_published = 1
            ORDER BY display_order, route_name
            """
        ).fetchall()
    ]

    live_buses = [bus for bus in live_data["buses"] if bus["tripStatus"] == "active" and bus["isLiveTracked"]]
    stops_by_name = {}
    routes_payload = []

    for route in route_rows:
        stops = get_route_stop_details(route["route_name"])
        fare_guide = estimate_fare_table(route["distance_km"], route["minimum_fare"], route["discounted_fare"])
        route_live_buses = [bus for bus in live_buses if bus["direction"] == route["route_name"]]

        next_bus = None
        next_bus_minutes = None
        if stops and route_live_buses:
            for bus in route_live_buses:
                arrival_minutes = estimate_bus_arrival_minutes(bus, stops, stops[0]["name"])
                if arrival_minutes is None:
                    continue
                if next_bus_minutes is None or arrival_minutes < next_bus_minutes:
                    next_bus_minutes = arrival_minutes
                    next_bus = bus

        routes_payload.append(
            {
                "id": route["id"],
                "slug": slugify_label(route["route_name"]),
                "routeName": route["route_name"],
                "startPoint": route["start_point"],
                "endPoint": route["end_point"],
                "distanceKm": float(route["distance_km"] or 0),
                "expectedDurationMinutes": int(route["expected_duration_minutes"] or 0),
                "coords": parse_route_coords(route["coords_json"]),
                "stops": stops,
                "fareGuide": fare_guide,
                "minimumFare": to_float(route["minimum_fare"], 15.0),
                "discountedFare": to_float(route["discounted_fare"], 12.0),
                "availableBusCount": len(route_live_buses),
                "nextBus": {
                    "plateNumber": next_bus["id"],
                    "nextStop": next_bus["nextStop"],
                    "minutesToStart": next_bus_minutes,
                    "crowdLevel": next_bus["crowdLevel"],
                } if next_bus else None,
            }
        )

        for stop in stops:
            key = stop_name_key(stop["name"])
            directory_entry = stops_by_name.setdefault(
                key,
                {
                    "name": stop["name"],
                    "lat": stop["lat"],
                    "lng": stop["lng"],
                    "landmark": stop["landmark"],
                    "routes": [],
                    "nextArrivals": [],
                },
            )
            directory_entry["routes"].append(route["route_name"])

            for bus in route_live_buses:
                arrival_minutes = estimate_bus_arrival_minutes(bus, stops, stop["name"])
                if arrival_minutes is None:
                    continue
                directory_entry["nextArrivals"].append(
                    {
                        "routeName": route["route_name"],
                        "plateNumber": bus["id"],
                        "crowdLevel": bus["crowdLevel"],
                        "minutes": arrival_minutes,
                    }
                )

    stop_directory = []
    for stop in stops_by_name.values():
        next_arrivals = sorted(stop["nextArrivals"], key=lambda item: item["minutes"])[:3]
        stop_directory.append(
            {
                "name": stop["name"],
                "lat": stop["lat"],
                "lng": stop["lng"],
                "landmark": stop["landmark"],
                "routes": sorted(stop["routes"]),
                "routeCount": len(stop["routes"]),
                "nextArrivals": next_arrivals,
            }
        )
    stop_directory.sort(key=lambda item: (-item["routeCount"], item["name"]))

    return {
        "routes": routes_payload,
        "stopDirectory": stop_directory,
        "stopNames": [stop["name"] for stop in stop_directory],
        "serviceAlerts": active_alerts,
    }


def get_monitoring_mode(notes):
    note_text = notes or ""
    if "[monitoring:auto]" in note_text:
        return "auto"
    return "manual"


def set_monitoring_mode(notes, mode):
    cleaned = (notes or "").replace("[monitoring:auto]", "").replace("[monitoring:manual]", "").strip()
    prefix = f"[monitoring:{mode}]"
    return f"{prefix} {cleaned}".strip()


def derive_trip_location_label(trip, latitude, longitude):
    route_stops = ROUTE_STOPS.get(trip.get("route_name") or "", [])
    if route_stops:
        nearest_stop = min(
            route_stops,
            key=lambda stop: distance_between_points_km(latitude, longitude, stop[1], stop[2]),
        )
        if distance_between_points_km(latitude, longitude, nearest_stop[1], nearest_stop[2]) <= 3:
            return nearest_stop[0]

    coords = parse_route_coords(trip.get("coords_json"))
    if len(coords) >= 2:
        start_lat, start_lng = coords[0]
        end_lat, end_lng = coords[-1]
        if abs(latitude - start_lat) <= 0.003 and abs(longitude - start_lng) <= 0.003:
            return trip.get("start_point") or "Route start"
        if abs(latitude - end_lat) <= 0.003 and abs(longitude - end_lng) <= 0.003:
            return trip.get("end_point") or "Route end"
    return f"On route to {trip.get('end_point') or 'terminal'}"


def render_chart_image(title, labels, values, chart_type="bar", color="#D60000"):
    figure, axis = plt.subplots(figsize=(6.8, 2.8))
    axis.set_title(title, fontsize=12, fontweight="bold")

    if chart_type == "line":
        axis.plot(labels, values, color=color, linewidth=2.5, marker="o", markersize=4)
        axis.fill_between(labels, values, color=color, alpha=0.12)
    else:
        axis.bar(labels, values, color=color, alpha=0.9)

    axis.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.35)
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)
    axis.tick_params(axis="x", labelrotation=30, labelsize=8)
    axis.tick_params(axis="y", labelsize=8)
    figure.tight_layout()

    chart_buffer = BytesIO()
    figure.savefig(chart_buffer, format="png", dpi=180, bbox_inches="tight")
    plt.close(figure)
    chart_buffer.seek(0)
    return chart_buffer


def build_admin_pdf_report(overview):
    pdf_buffer = BytesIO()
    document = SimpleDocTemplate(
        pdf_buffer,
        pagesize=landscape(A4),
        rightMargin=32,
        leftMargin=32,
        topMargin=32,
        bottomMargin=32,
    )
    styles = getSampleStyleSheet()
    story = []

    title_style = styles["Heading1"]
    title_style.textColor = colors.HexColor("#D60000")
    subtitle_style = styles["Normal"]
    subtitle_style.textColor = colors.HexColor("#475569")
    table_header_dark_style = styles["BodyText"].clone("table_header_dark_style")
    table_header_dark_style.fontName = "Helvetica-Bold"
    table_header_dark_style.fontSize = 7
    table_header_dark_style.leading = 8
    table_header_dark_style.textColor = colors.white
    table_header_light_style = styles["BodyText"].clone("table_header_light_style")
    table_header_light_style.fontName = "Helvetica-Bold"
    table_header_light_style.fontSize = 7
    table_header_light_style.leading = 8
    table_header_light_style.textColor = colors.HexColor("#7f1d1d")
    table_body_style = styles["BodyText"].clone("table_body_style")
    table_body_style.fontSize = 7
    table_body_style.leading = 8

    def pdf_cell(value, style=table_body_style):
        return Paragraph(escape(str(value or "")), style)

    story.append(Paragraph("Gajoda Transportation Services", title_style))
    story.append(Paragraph("Crowd Analytics Report", styles["Heading2"]))
    story.append(Paragraph(f"Generated: {now().strftime('%B %d, %Y %I:%M %p')}", subtitle_style))
    story.append(Spacer(1, 0.18 * inch))

    summary_rows = [
        ["Passengers Today", str(overview["today_total"]), "Trips Today", str(overview["trips_today"])],
        ["Active Live Buses", str(overview["active_bus_count"]), "Average Load", f'{overview["avg_crowd"]}%'],
        ["High Crowd Trips", str(overview["high_crowd_count"]), "Peak Hour", f'{overview["peak_hour_label"]} ({overview["peak_hour_value"]})'],
    ]
    summary_table = Table(summary_rows, colWidths=[1.55 * inch, 1.0 * inch, 1.55 * inch, 2.0 * inch])
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.whitesmoke),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#0f172a")),
                ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#dbe2ea")),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
                ("PADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    story.append(summary_table)
    story.append(Spacer(1, 0.22 * inch))

    audit_summary = overview.get("audit_summary") or {}
    audit_rows = [
        ["Trips in Audit", str(audit_summary.get("trip_count", 0)), "Completed Trips", str(audit_summary.get("completed_trip_count", 0))],
        ["Total Boarded", str(audit_summary.get("total_boarded", 0)), "Total Revenue", f"PHP {audit_summary.get('total_revenue', 0):.2f}"],
        ["Avg Trip Boarded", str(audit_summary.get("average_trip_boarded", 0)), "Avg Trip Revenue", f"PHP {audit_summary.get('average_trip_revenue', 0):.2f}"],
    ]
    audit_table = Table(audit_rows, colWidths=[1.55 * inch, 1.0 * inch, 1.7 * inch, 1.85 * inch])
    audit_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#fff1f2")),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#0f172a")),
                ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#dbe2ea")),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
                ("PADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    story.append(audit_table)
    story.append(Spacer(1, 0.22 * inch))

    charts = overview["charts"]
    chart_specs = [
        ("7-Day Passenger Trend", charts["daily_labels"], charts["daily_values"], "line"),
        ("Hourly Demand", charts["hourly_labels"], charts["hourly_values"], "bar"),
        ("Route Passenger Comparison", charts["route_labels"], charts["route_values"], "bar"),
    ]
    for title, labels, values, chart_type in chart_specs:
        if not labels:
            continue
        story.append(Paragraph(title, styles["Heading3"]))
        story.append(Image(render_chart_image(title, labels, values, chart_type), width=6.7 * inch, height=2.6 * inch))
        story.append(Spacer(1, 0.15 * inch))

    story.append(Paragraph("AI Insights", styles["Heading3"]))
    for insight in overview["insights"]:
        story.append(Paragraph(f"<b>{insight['title']}</b>: {insight['body']}", styles["BodyText"]))
        story.append(Spacer(1, 0.08 * inch))

    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("Route Summary", styles["Heading3"]))
    route_table_rows = [["Route", "Trips", "Passengers", "Avg Load %"]]
    for row in overview["route_rows"]:
        route_table_rows.append([
            row["route_name"],
            str(row["trip_count"]),
            str(row["passengers"]),
            f'{row["avg_load_percent"]}%',
        ])
    route_table_rows = [
        [pdf_cell("Route", table_header_dark_style), pdf_cell("Trips", table_header_dark_style), pdf_cell("Passengers", table_header_dark_style), pdf_cell("Avg Load %", table_header_dark_style)]
    ] + [
        [pdf_cell(row["route_name"]), pdf_cell(row["trip_count"]), pdf_cell(row["passengers"]), pdf_cell(f'{row["avg_load_percent"]}%')]
        for row in overview["route_rows"]
    ]
    route_table = Table(route_table_rows, colWidths=[2.8 * inch, 0.8 * inch, 1.0 * inch, 1.0 * inch], repeatRows=1)
    route_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#D60000")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#dbe2ea")),
                ("PADDING", (0, 0), (-1, -1), 7),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("BACKGROUND", (0, 1), (-1, -1), colors.whitesmoke),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story.append(route_table)

    if overview.get("bus_report_sections"):
        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph("Bus-Specific Daily Tabulation", styles["Heading3"]))
        fleet_totals = (overview.get("report_bus_analytics") or {}).get("all") or {}
        fleet_total_rows = [[
            pdf_cell("Student", table_header_light_style),
            pdf_cell("PWD", table_header_light_style),
            pdf_cell("Senior", table_header_light_style),
            pdf_cell("Regular", table_header_light_style),
            pdf_cell("Total Pax", table_header_light_style),
            pdf_cell("Student Rev", table_header_light_style),
            pdf_cell("PWD Rev", table_header_light_style),
            pdf_cell("Senior Rev", table_header_light_style),
            pdf_cell("Regular Rev", table_header_light_style),
            pdf_cell("Total Revenue", table_header_light_style),
        ]]
        for bus in overview["bus_report_sections"]:
            fleet_total_rows.append([
                pdf_cell(bus["passengers_by_type"]["student"]),
                pdf_cell(bus["passengers_by_type"]["pwd"]),
                pdf_cell(bus["passengers_by_type"]["senior"]),
                pdf_cell(bus["passengers_by_type"]["regular"]),
                pdf_cell(bus["total_passengers"]),
                pdf_cell(f"PHP {bus['revenue_by_type']['student']:.2f}"),
                pdf_cell(f"PHP {bus['revenue_by_type']['pwd']:.2f}"),
                pdf_cell(f"PHP {bus['revenue_by_type']['senior']:.2f}"),
                pdf_cell(f"PHP {bus['revenue_by_type']['regular']:.2f}"),
                pdf_cell(f"PHP {bus['total_revenue']:.2f}"),
            ])
        fleet_total_rows.append([
            pdf_cell((fleet_totals.get("passenger_totals") or {}).get("student", 0)),
            pdf_cell((fleet_totals.get("passenger_totals") or {}).get("pwd", 0)),
            pdf_cell((fleet_totals.get("passenger_totals") or {}).get("senior", 0)),
            pdf_cell((fleet_totals.get("passenger_totals") or {}).get("regular", 0)),
            pdf_cell(fleet_totals.get("total_passengers", 0)),
            pdf_cell(f"PHP {(fleet_totals.get('revenue_totals') or {}).get('student', 0):.2f}"),
            pdf_cell(f"PHP {(fleet_totals.get('revenue_totals') or {}).get('pwd', 0):.2f}"),
            pdf_cell(f"PHP {(fleet_totals.get('revenue_totals') or {}).get('senior', 0):.2f}"),
            pdf_cell(f"PHP {(fleet_totals.get('revenue_totals') or {}).get('regular', 0):.2f}"),
            pdf_cell(f"PHP {fleet_totals.get('total_revenue', 0):.2f}"),
        ])
        fleet_total_table = Table(
            fleet_total_rows,
            colWidths=[0.5 * inch, 0.5 * inch, 0.5 * inch, 0.55 * inch, 0.6 * inch, 0.72 * inch, 0.72 * inch, 0.72 * inch, 0.76 * inch, 0.86 * inch],
            repeatRows=1,
        )
        fleet_total_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#fee2e2")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#7f1d1d")),
                    ("BACKGROUND", (0, 1), (-1, -2), colors.whitesmoke),
                    ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#fee2e2")),
                    ("TEXTCOLOR", (0, -1), (-1, -1), colors.HexColor("#7f1d1d")),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dbe2ea")),
                    ("PADDING", (0, 0), (-1, -1), 6),
                    ("FONTSIZE", (0, 0), (-1, -1), 7),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        story.append(fleet_total_table)
        story.append(Spacer(1, 0.08 * inch))
        for bus in overview["bus_report_sections"]:
            story.append(
                Paragraph(
                    f"{bus['plate_number']} | Status: {bus['status']} / {bus['trip_status']} | Route: {bus['route_name']}",
                    styles["Heading4"],
                )
            )
            bus_total_rows = [[
                pdf_cell("Student", table_header_light_style),
                pdf_cell("PWD", table_header_light_style),
                pdf_cell("Senior", table_header_light_style),
                pdf_cell("Regular", table_header_light_style),
                pdf_cell("Total Pax", table_header_light_style),
                pdf_cell("Student Rev", table_header_light_style),
                pdf_cell("PWD Rev", table_header_light_style),
                pdf_cell("Senior Rev", table_header_light_style),
                pdf_cell("Regular Rev", table_header_light_style),
                pdf_cell("Total Revenue", table_header_light_style),
            ], [
                pdf_cell(bus["passengers_by_type"]["student"]),
                pdf_cell(bus["passengers_by_type"]["pwd"]),
                pdf_cell(bus["passengers_by_type"]["senior"]),
                pdf_cell(bus["passengers_by_type"]["regular"]),
                pdf_cell(bus["total_passengers"]),
                pdf_cell(f"PHP {bus['revenue_by_type']['student']:.2f}"),
                pdf_cell(f"PHP {bus['revenue_by_type']['pwd']:.2f}"),
                pdf_cell(f"PHP {bus['revenue_by_type']['senior']:.2f}"),
                pdf_cell(f"PHP {bus['revenue_by_type']['regular']:.2f}"),
                pdf_cell(f"PHP {bus['total_revenue']:.2f}"),
            ]]
            bus_total_table = Table(
                bus_total_rows,
                colWidths=[0.5 * inch, 0.5 * inch, 0.5 * inch, 0.55 * inch, 0.6 * inch, 0.72 * inch, 0.72 * inch, 0.72 * inch, 0.76 * inch, 0.86 * inch],
                repeatRows=1,
            )
            bus_total_table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#fee2e2")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#7f1d1d")),
                        ("BACKGROUND", (0, 1), (-1, 1), colors.whitesmoke),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dbe2ea")),
                        ("PADDING", (0, 0), (-1, -1), 6),
                        ("FONTSIZE", (0, 0), (-1, -1), 7),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ]
                )
            )
            story.append(bus_total_table)
            story.append(Spacer(1, 0.08 * inch))
            if bus["rows"]:
                trip_rows = [[
                    pdf_cell("Date", table_header_dark_style),
                    pdf_cell("Trip ID", table_header_dark_style),
                    pdf_cell("Start", table_header_dark_style),
                    pdf_cell("Driver", table_header_dark_style),
                    pdf_cell("Route", table_header_dark_style),
                    pdf_cell("Student", table_header_dark_style),
                    pdf_cell("PWD", table_header_dark_style),
                    pdf_cell("Senior", table_header_dark_style),
                    pdf_cell("Regular", table_header_dark_style),
                    pdf_cell("Total Pax", table_header_dark_style),
                    pdf_cell("Student Rev", table_header_dark_style),
                    pdf_cell("PWD Rev", table_header_dark_style),
                    pdf_cell("Senior Rev", table_header_dark_style),
                    pdf_cell("Regular Rev", table_header_dark_style),
                    pdf_cell("Total Revenue", table_header_dark_style),
                ]]
                for row in bus["rows"]:
                    trip_rows.append(
                        [
                            pdf_cell(row["service_date"]),
                            pdf_cell(row["trip_id"]),
                            pdf_cell(row["started_at"] or "No start time"),
                            pdf_cell(row["driver_name"]),
                            pdf_cell(row["route_name"]),
                            pdf_cell(row["student_count"]),
                            pdf_cell(row["pwd_count"]),
                            pdf_cell(row["senior_count"]),
                            pdf_cell(row["regular_count"]),
                            pdf_cell(row["total_passengers"]),
                            pdf_cell(f"PHP {row['student_revenue']:.2f}"),
                            pdf_cell(f"PHP {row['pwd_revenue']:.2f}"),
                            pdf_cell(f"PHP {row['senior_revenue']:.2f}"),
                            pdf_cell(f"PHP {row['regular_revenue']:.2f}"),
                            pdf_cell(f"PHP {row['total_revenue']:.2f}"),
                        ]
                    )
                trip_rows.append(
                    [
                        pdf_cell("BUS TOTAL", table_header_light_style),
                        pdf_cell(""),
                        pdf_cell(""),
                        pdf_cell(""),
                        pdf_cell(""),
                        pdf_cell(bus["passengers_by_type"]["student"], table_header_light_style),
                        pdf_cell(bus["passengers_by_type"]["pwd"], table_header_light_style),
                        pdf_cell(bus["passengers_by_type"]["senior"], table_header_light_style),
                        pdf_cell(bus["passengers_by_type"]["regular"], table_header_light_style),
                        pdf_cell(bus["total_passengers"], table_header_light_style),
                        pdf_cell(f"PHP {bus['revenue_by_type']['student']:.2f}", table_header_light_style),
                        pdf_cell(f"PHP {bus['revenue_by_type']['pwd']:.2f}", table_header_light_style),
                        pdf_cell(f"PHP {bus['revenue_by_type']['senior']:.2f}", table_header_light_style),
                        pdf_cell(f"PHP {bus['revenue_by_type']['regular']:.2f}", table_header_light_style),
                        pdf_cell(f"PHP {bus['total_revenue']:.2f}", table_header_light_style),
                    ]
                )
                trip_table = Table(
                    trip_rows,
                    colWidths=[0.72 * inch, 0.52 * inch, 0.8 * inch, 0.95 * inch, 1.25 * inch, 0.46 * inch, 0.46 * inch, 0.46 * inch, 0.5 * inch, 0.56 * inch, 0.68 * inch, 0.68 * inch, 0.68 * inch, 0.72 * inch, 0.8 * inch],
                    repeatRows=1,
                )
                trip_table.setStyle(
                    TableStyle(
                        [
                            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#D60000")),
                            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dbe2ea")),
                            ("PADDING", (0, 0), (-1, -1), 6),
                            ("FONTSIZE", (0, 0), (-1, -1), 7),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("BACKGROUND", (0, 1), (-1, -1), colors.whitesmoke),
                            ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#fee2e2")),
                            ("TEXTCOLOR", (0, -1), (-1, -1), colors.HexColor("#7f1d1d")),
                        ]
                    )
                )
                story.append(trip_table)
            else:
                story.append(Paragraph("No trip data recorded for this bus yet.", styles["BodyText"]))
            story.append(Spacer(1, 0.12 * inch))

    if overview.get("attendance_rows"):
        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph("Staff Attendance and Trip Assignment", styles["Heading3"]))
        attendance_rows = [[
            pdf_cell("Staff", table_header_light_style),
            pdf_cell("Login Time", table_header_light_style),
            pdf_cell("Trip", table_header_light_style),
            pdf_cell("Trip Window", table_header_light_style),
            pdf_cell("Status", table_header_light_style),
        ]]
        for row in overview["attendance_rows"][:12]:
            trip_window = "No trip assigned"
            if row.get("trip_id"):
                trip_window = f"{row['trip_started_at'] or 'No trip start'}"
                trip_window += f" to {row['trip_ended_at']}" if row.get("trip_ended_at") else " to active trip"
            attendance_rows.append(
                [
                    pdf_cell(f"{row['full_name']} ({row['role']})"),
                    pdf_cell(row["login_time"]),
                    pdf_cell(row["trip_summary"]),
                    pdf_cell(trip_window),
                    pdf_cell(row.get("trip_status") or "attendance only"),
                ]
            )
        attendance_table = Table(attendance_rows, colWidths=[1.55 * inch, 1.15 * inch, 1.55 * inch, 1.55 * inch, 0.8 * inch], repeatRows=1)
        attendance_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#fee2e2")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#7f1d1d")),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dbe2ea")),
                    ("PADDING", (0, 0), (-1, -1), 6),
                    ("FONTSIZE", (0, 0), (-1, -1), 7),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        story.append(attendance_table)

    if overview["recent_logs"]:
        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph("Recent Logs", styles["Heading3"]))
        log_rows = [[
            pdf_cell("Time", table_header_light_style),
            pdf_cell("Role", table_header_light_style),
            pdf_cell("Action", table_header_light_style),
            pdf_cell("Description", table_header_light_style),
        ]]
        for log in overview["recent_logs"][:8]:
            log_rows.append([
                pdf_cell(log["created_at"]),
                pdf_cell(log["role"] or "system"),
                pdf_cell(log["action"]),
                pdf_cell(log["description"]),
            ])
        log_table = Table(log_rows, colWidths=[1.25 * inch, 0.7 * inch, 1.0 * inch, 3.35 * inch], repeatRows=1)
        log_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#fee2e2")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#7f1d1d")),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dbe2ea")),
                    ("PADDING", (0, 0), (-1, -1), 6),
                    ("FONTSIZE", (0, 0), (-1, -1), 7),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        story.append(log_table)

    document.build(story)
    pdf_buffer.seek(0)
    return pdf_buffer


def get_latest_trip_gps(conn, trip_id):
    row = conn.execute(
        """
        SELECT latitude, longitude, recorded_at
        FROM gps_logs
        WHERE trip_id = ?
        ORDER BY recorded_at DESC, id DESC
        LIMIT 1
        """,
        (trip_id,),
    ).fetchone()
    return dict(row) if row else None


def get_recent_trip_transactions(conn, trip_id, limit=8):
    rows = conn.execute(
        """
        SELECT recorded_at, event_type, passenger_type, quantity, stop_name, origin_stop, destination_stop, fare_amount, occupancy_after
        FROM trip_transactions
        WHERE trip_id = ?
        ORDER BY recorded_at DESC, id DESC
        LIMIT ?
        """,
        (trip_id, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_one(query, params=()):
    conn = get_db()
    row = conn.execute(query, params).fetchone()
    conn.close()
    return dict(row) if row else None


def log_event(conn, user_id, role, action, description):
    conn.execute(
        """
        INSERT INTO system_logs (user_id, role, action, description, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (user_id, role, action, description, to_db_time(now())),
    )


def require_role(role_name):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if session.get("role") != role_name:
                return redirect(url_for("login"))
            return view(*args, **kwargs)

        return wrapped

    return decorator


def get_active_trip_for_driver(conn, driver_id):
    row = conn.execute(
        """
        SELECT t.*, b.plate_number, b.capacity, b.route_color,
               r.route_name, r.start_point, r.end_point, r.coords_json, r.expected_duration_minutes
        FROM trips t
        JOIN buses b ON b.id = t.bus_id
        JOIN routes r ON r.id = t.route_id
        WHERE t.driver_id = ? AND t.status = 'active'
        ORDER BY t.id DESC
        LIMIT 1
        """,
        (driver_id,),
    ).fetchone()
    return dict(row) if row else None


def get_active_trip_for_conductor(conn, conductor_id):
    row = conn.execute(
        """
        SELECT t.*, b.plate_number, b.capacity,
               r.route_name, r.start_point, r.end_point, r.coords_json,
               r.distance_km, r.minimum_fare, r.discounted_fare
        FROM trips t
        JOIN buses b ON b.id = t.bus_id
        JOIN routes r ON r.id = t.route_id
        WHERE t.conductor_id = ? AND t.status = 'active'
        ORDER BY t.id DESC
        LIMIT 1
        """,
        (conductor_id,),
    ).fetchone()
    return dict(row) if row else None


def get_latest_trip_record(conn, trip_id):
    row = conn.execute(
        """
        SELECT *
        FROM trip_records
        WHERE trip_id = ?
        ORDER BY recorded_at DESC, id DESC
        LIMIT 1
        """,
        (trip_id,),
    ).fetchone()
    return dict(row) if row else None


def fill_missing_days(rows, key_name, days=7):
    lookup = {}
    for row in rows:
        key = row[key_name]
        if hasattr(key, "isoformat"):
            key = key.isoformat()
        else:
            key = str(key)
        lookup[key] = row["total"]
    labels = []
    values = []
    for offset in range(days - 1, -1, -1):
        day = (now() - timedelta(days=offset)).date().isoformat()
        labels.append(day[5:])
        values.append(int(lookup.get(day, 0) or 0))
    return labels, values


def build_live_bus_data(conn):
    rows = conn.execute(
        """
        SELECT b.id AS bus_id,
               b.plate_number,
               b.capacity,
               b.status AS bus_status,
               b.route_color,
               COALESCE(active_trip.id, latest_trip.id) AS trip_id,
               COALESCE(active_trip.status, latest_trip.status, 'idle') AS trip_status,
               COALESCE(active_trip.occupancy, 0) AS occupancy,
               COALESCE(active_trip.peak_occupancy, 0) AS peak_occupancy,
               COALESCE(active_trip.started_at, latest_trip.started_at) AS started_at,
               r.route_name,
               r.start_point,
               r.end_point,
               r.distance_km,
               r.expected_duration_minutes,
               r.coords_json,
               u.full_name AS driver_name,
               tr.stop_name,
               gl.latitude AS latitude,
               gl.longitude AS longitude,
               gl.recorded_at AS recorded_at
        FROM buses b
        LEFT JOIN trips active_trip ON active_trip.id = (
            SELECT id
            FROM trips
            WHERE bus_id = b.id AND status = 'active'
            ORDER BY started_at DESC, id DESC
            LIMIT 1
        )
        LEFT JOIN trips latest_trip ON latest_trip.id = (
            SELECT id
            FROM trips
            WHERE bus_id = b.id
            ORDER BY started_at DESC, id DESC
            LIMIT 1
        )
        LEFT JOIN routes r ON r.id = COALESCE(active_trip.route_id, latest_trip.route_id)
        LEFT JOIN users u ON u.id = active_trip.driver_id
        LEFT JOIN trip_records tr ON tr.id = (
            SELECT id
            FROM trip_records
            WHERE trip_id = active_trip.id
            ORDER BY recorded_at DESC, id DESC
            LIMIT 1
        )
        LEFT JOIN gps_logs gl ON gl.id = (
            SELECT id
            FROM gps_logs
            WHERE trip_id = active_trip.id
            ORDER BY recorded_at DESC, id DESC
            LIMIT 1
        )
        ORDER BY b.plate_number
        """
    ).fetchall()

    buses = []
    active_buses = 0
    for raw_row in rows:
        row = dict(raw_row)
        coords = parse_route_coords(row["coords_json"])
        gps_history_rows = []
        if row["trip_status"] == "active" and row["trip_id"]:
            gps_history_rows = conn.execute(
                """
                SELECT latitude, longitude, recorded_at
                FROM gps_logs
                WHERE trip_id = ?
                ORDER BY recorded_at ASC, id ASC
                """,
                (row["trip_id"],),
            ).fetchall()
        gps_history = []
        for gps_row in gps_history_rows:
            if gps_row["latitude"] is None or gps_row["longitude"] is None:
                continue
            gps_history.append(
                [
                    float(gps_row["latitude"]),
                    float(gps_row["longitude"]),
                ]
            )
        lat = float(row["latitude"]) if row["latitude"] is not None else None
        lng = float(row["longitude"]) if row["longitude"] is not None else None
        occupancy = int(row["occupancy"] or 0)
        capacity = int(row["capacity"] or DEFAULT_BUS_CAPACITY)
        status = row["bus_status"] or "offline"
        is_live_tracked = row["trip_status"] == "active" and lat is not None and lng is not None
        if is_live_tracked:
            active_buses += 1
        buses.append(
            {
                "tripId": int(row["trip_id"]) if row["trip_id"] else None,
                "busId": int(row["bus_id"]),
                "id": row["plate_number"],
                "lat": lat,
                "lng": lng,
                "direction": row["route_name"] or FORWARD_ROUTE_NAME,
                "start": row["start_point"] or CORRIDOR_STOP_NAMES[0],
                "end": row["end_point"] or CORRIDOR_STOP_NAMES[-1],
                "distanceKm": float(row["distance_km"] or 0),
                "expectedDurationMinutes": int(row["expected_duration_minutes"] or 0),
                "driver": row["driver_name"] or "Driver pending",
                "crowdLevel": classify_capacity(occupancy, capacity),
                "status": status,
                "tripStatus": row["trip_status"],
                "isLiveTracked": is_live_tracked,
                "nextStop": row["stop_name"] or ("Live tracking active" if row["trip_status"] == "active" else "Awaiting dispatch"),
                "eta": "Live Trip" if row["trip_status"] == "active" else "Not in service",
                "passengers": occupancy,
                "capacity": capacity,
                "routeColor": row["route_color"] or "#1d4ed8",
                "coords": coords,
                "history": gps_history,
            }
        )

    low_count = sum(1 for bus in buses if bus["crowdLevel"] == "Low")
    medium_count = sum(1 for bus in buses if bus["crowdLevel"] == "Medium")
    high_count = sum(1 for bus in buses if bus["crowdLevel"] == "High")

    average_ratio = 0
    active_bus_rows = [bus for bus in buses if bus["isLiveTracked"]]
    if active_bus_rows:
        average_ratio = sum(bus["passengers"] / max(bus["capacity"], 1) for bus in active_bus_rows) / len(active_bus_rows)

    return {
        "buses": buses,
        "active_bus_count": active_buses,
        "avg_crowd": int(round(average_ratio * 100)),
        "low_count": sum(1 for bus in active_bus_rows if bus["crowdLevel"] == "Low"),
        "medium_count": sum(1 for bus in active_bus_rows if bus["crowdLevel"] == "Medium"),
        "high_count": sum(1 for bus in active_bus_rows if bus["crowdLevel"] == "High"),
    }


def generate_ai_insights(overview):
    insights = []

    busiest_route = max(overview["route_rows"], key=lambda item: item["passengers"], default=None)
    if busiest_route:
        insights.append(
            {
                "title": f"Deploy more trips on {busiest_route['route_name']}",
                "body": f"{busiest_route['route_name']} is carrying {busiest_route['passengers']} passengers in the reporting window, which is the highest route demand in the system.",
                "tone": "hot",
            }
        )

    if overview["today_total"] > overview["yesterday_total"]:
        lift = overview["today_total"] - overview["yesterday_total"]
        insights.append(
            {
                "title": "Demand is climbing today",
                "body": f"Passenger volume is up by {lift} versus yesterday. Prepare reserve buses during the afternoon peak if that trend continues.",
                "tone": "good",
            }
        )
    else:
        drop = overview["yesterday_total"] - overview["today_total"]
        insights.append(
            {
                "title": "Use the lighter day to rebalance operations",
                "body": f"Volume is down by {drop} versus yesterday. This is a good window to shift buses into maintenance or refine scheduling without hurting availability.",
                "tone": "calm",
            }
        )

    if overview["peak_hour_value"] >= 60:
        insights.append(
            {
                "title": f"Peak pressure is centered around {overview['peak_hour_label']}",
                "body": f"Peak hour records show {overview['peak_hour_value']} passengers at {overview['peak_hour_label']}. Staff dispatch and conductor readiness should be concentrated around that time block.",
                "tone": "warn",
            }
        )

    if overview["high_crowd_count"] > 0:
        insights.append(
            {
                "title": "High-crowd trips need intervention",
                "body": f"There are currently {overview['high_crowd_count']} active buses operating in high crowd mode. Consider route staggering or short-turning one reserve unit.",
                "tone": "warn",
            }
        )

    return insights[:4]


def build_stop_analytics(conn):
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT stop_name,
                   COALESCE(SUM(CASE WHEN event_type = 'board' THEN quantity ELSE 0 END), 0) AS boarded,
                   COALESCE(SUM(CASE WHEN event_type = 'drop' THEN quantity ELSE 0 END), 0) AS dropped,
                   COUNT(*) AS transactions
            FROM trip_transactions
            WHERE stop_name IS NOT NULL AND stop_name <> ''
            GROUP BY stop_name
            ORDER BY boarded DESC, dropped DESC, stop_name
            LIMIT 8
            """
        ).fetchall()
    ]
    return rows


def get_recent_transaction_audit(conn, limit=20):
    rows = conn.execute(
        """
        SELECT tt.recorded_at,
               tt.event_type,
               tt.passenger_type,
               tt.quantity,
               tt.stop_name,
               tt.occupancy_after,
               COALESCE(tt.fare_amount, 0) AS fare_amount,
               t.id AS trip_id,
               b.plate_number,
               r.route_name,
               u.full_name AS conductor_name
        FROM trip_transactions tt
        JOIN trips t ON t.id = tt.trip_id
        JOIN buses b ON b.id = t.bus_id
        JOIN routes r ON r.id = t.route_id
        LEFT JOIN users u ON u.id = tt.conductor_id
        ORDER BY tt.recorded_at DESC, tt.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def backfill_missing_transaction_fares(conn):
    rows = conn.execute(
        """
        SELECT
            tt.id,
            tt.passenger_type,
            tt.quantity,
            r.distance_km,
            r.minimum_fare,
            r.discounted_fare
        FROM trip_transactions tt
        JOIN trips t ON t.id = tt.trip_id
        JOIN routes r ON r.id = t.route_id
        WHERE tt.event_type = 'board' AND (tt.fare_amount IS NULL OR tt.fare_amount = 0)
        """
    ).fetchall()

    updates = []
    for row in rows:
        updates.append(
            (
                calculate_passenger_fare_total(
                    row["passenger_type"],
                    row["quantity"],
                    row["distance_km"],
                    row["minimum_fare"],
                    row["discounted_fare"],
                ),
                row["id"],
            )
        )

    if updates:
        conn.executemany("UPDATE trip_transactions SET fare_amount = ? WHERE id = ?", updates)


def build_trip_audit_summary(conn, limit=50):
    rows = conn.execute(
        """
        SELECT
            t.id AS trip_id,
            t.status,
            t.started_at,
            t.ended_at,
            t.duration_minutes,
            t.occupancy,
            t.peak_occupancy,
            b.plate_number,
            b.capacity,
            r.route_name,
            driver.full_name AS driver_name,
            conductor.full_name AS conductor_name,
            COALESCE(tx.passengers_boarded, 0) AS passengers_boarded,
            COALESCE(tx.passengers_dropped, 0) AS passengers_dropped,
            COALESCE(tx.student_count, 0) AS student_count,
            COALESCE(tx.pwd_count, 0) AS pwd_count,
            COALESCE(tx.senior_count, 0) AS senior_count,
            COALESCE(tx.regular_count, 0) AS regular_count,
            COALESCE(tx.revenue, 0) AS revenue,
            COALESCE(tx.stops_served, 0) AS stops_served,
            COALESCE(rec.crowd_updates, 0) AS crowd_updates,
            rec.latest_stop
        FROM trips t
        JOIN buses b ON b.id = t.bus_id
        JOIN routes r ON r.id = t.route_id
        LEFT JOIN users driver ON driver.id = t.driver_id
        LEFT JOIN users conductor ON conductor.id = t.conductor_id
        LEFT JOIN (
            SELECT
                trip_id,
                COALESCE(SUM(CASE WHEN event_type = 'board' THEN quantity ELSE 0 END), 0) AS passengers_boarded,
                COALESCE(SUM(CASE WHEN event_type = 'drop' THEN quantity ELSE 0 END), 0) AS passengers_dropped,
                COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'student' THEN quantity ELSE 0 END), 0) AS student_count,
                COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'pwd' THEN quantity ELSE 0 END), 0) AS pwd_count,
                COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'senior' THEN quantity ELSE 0 END), 0) AS senior_count,
                COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'regular' THEN quantity ELSE 0 END), 0) AS regular_count,
                COALESCE(SUM(CASE WHEN event_type = 'board' THEN fare_amount ELSE 0 END), 0) AS revenue,
                COUNT(DISTINCT NULLIF(stop_name, 'Unknown')) AS stops_served
            FROM trip_transactions
            GROUP BY trip_id
        ) tx ON tx.trip_id = t.id
        LEFT JOIN (
            SELECT
                tr.trip_id,
                COUNT(*) AS crowd_updates,
                SUBSTRING_INDEX(
                    GROUP_CONCAT(tr.stop_name ORDER BY tr.recorded_at DESC, tr.id DESC SEPARATOR '||'),
                    '||',
                    1
                ) AS latest_stop
            FROM trip_records tr
            GROUP BY tr.trip_id
        ) rec ON rec.trip_id = t.id
        ORDER BY t.started_at DESC, t.id DESC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()

    summary_rows = []
    total_revenue = Decimal("0.00")
    total_boarded = 0
    total_completed = 0
    peak_revenue_trip = None

    for row in rows:
        trip_row = dict(row)
        trip_row["passengers_boarded"] = int(trip_row["passengers_boarded"] or 0)
        trip_row["passengers_dropped"] = int(trip_row["passengers_dropped"] or 0)
        trip_row["student_count"] = int(trip_row["student_count"] or 0)
        trip_row["pwd_count"] = int(trip_row["pwd_count"] or 0)
        trip_row["senior_count"] = int(trip_row["senior_count"] or 0)
        trip_row["regular_count"] = int(trip_row["regular_count"] or 0)
        trip_row["stops_served"] = int(trip_row["stops_served"] or 0)
        trip_row["crowd_updates"] = int(trip_row["crowd_updates"] or 0)
        trip_row["occupancy"] = int(trip_row["occupancy"] or 0)
        trip_row["peak_occupancy"] = int(trip_row["peak_occupancy"] or 0)
        trip_row["capacity"] = int(trip_row["capacity"] or DEFAULT_BUS_CAPACITY)
        trip_row["net_passengers"] = max(trip_row["passengers_boarded"] - trip_row["passengers_dropped"], 0)
        trip_row["load_percent"] = round((trip_row["peak_occupancy"] / max(trip_row["capacity"], 1)) * 100, 1)
        trip_row["revenue"] = float(trip_row["revenue"] or 0)
        trip_row["average_fare"] = round(trip_row["revenue"] / trip_row["passengers_boarded"], 2) if trip_row["passengers_boarded"] else 0
        trip_row["latest_stop"] = trip_row["latest_stop"] or "No stop recorded"
        summary_rows.append(trip_row)

        total_revenue += Decimal(str(trip_row["revenue"]))
        total_boarded += trip_row["passengers_boarded"]
        if trip_row["status"] == "completed":
            total_completed += 1
        if peak_revenue_trip is None or trip_row["revenue"] > peak_revenue_trip["revenue"]:
            peak_revenue_trip = trip_row

    trip_count = len(summary_rows)
    audit_summary = {
        "trip_count": trip_count,
        "completed_trip_count": total_completed,
        "active_trip_count": sum(1 for row in summary_rows if row["status"] == "active"),
        "total_boarded": total_boarded,
        "total_revenue": float(total_revenue),
        "average_trip_revenue": round(float(total_revenue) / trip_count, 2) if trip_count else 0,
        "average_trip_boarded": round(total_boarded / trip_count, 1) if trip_count else 0,
        "top_revenue_trip": peak_revenue_trip,
    }
    return summary_rows, audit_summary




def build_daily_bus_tabulation(conn, limit=60):
    rows = conn.execute(
        """
        SELECT
            t.id AS trip_id,
            DATE(t.started_at) AS service_date,
            t.started_at,
            t.ended_at,
            b.plate_number,
            COALESCE(driver.full_name, 'No driver assigned') AS driver_name,
            r.route_name,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' AND tt.passenger_type = 'student' THEN tt.quantity ELSE 0 END), 0) AS student_count,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' AND tt.passenger_type = 'pwd' THEN tt.quantity ELSE 0 END), 0) AS pwd_count,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' AND tt.passenger_type = 'senior' THEN tt.quantity ELSE 0 END), 0) AS senior_count,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' AND tt.passenger_type = 'regular' THEN tt.quantity ELSE 0 END), 0) AS regular_count,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' THEN tt.quantity ELSE 0 END), 0) AS total_passengers,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' AND tt.passenger_type = 'student' THEN tt.fare_amount ELSE 0 END), 0) AS student_revenue,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' AND tt.passenger_type = 'pwd' THEN tt.fare_amount ELSE 0 END), 0) AS pwd_revenue,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' AND tt.passenger_type = 'senior' THEN tt.fare_amount ELSE 0 END), 0) AS senior_revenue,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' AND tt.passenger_type = 'regular' THEN tt.fare_amount ELSE 0 END), 0) AS regular_revenue,
            COALESCE(SUM(CASE WHEN tt.event_type = 'board' THEN tt.fare_amount ELSE 0 END), 0) AS total_revenue
        FROM trips t
        JOIN buses b ON b.id = t.bus_id
        JOIN routes r ON r.id = t.route_id
        LEFT JOIN users driver ON driver.id = t.driver_id
        LEFT JOIN trip_transactions tt ON tt.trip_id = t.id
        GROUP BY t.id, DATE(t.started_at), t.started_at, t.ended_at, b.plate_number, driver.full_name, r.route_name
        ORDER BY DATE(t.started_at) DESC, b.plate_number, t.started_at DESC, t.id DESC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()

    tabulation_rows = []
    for row in rows:
        item = dict(row)
        for key in (
            "student_count",
            "pwd_count",
            "senior_count",
            "regular_count",
            "total_passengers",
        ):
            item[key] = int(item[key] or 0)
        for key in (
            "student_revenue",
            "pwd_revenue",
            "senior_revenue",
            "regular_revenue",
            "total_revenue",
        ):
            item[key] = float(item[key] or 0)
        item["trip_id"] = int(item["trip_id"])
        item["service_date"] = item["service_date"].isoformat() if hasattr(item["service_date"], "isoformat") else str(item["service_date"])
        item["started_at"] = normalize_json_value(item["started_at"])
        item["ended_at"] = normalize_json_value(item["ended_at"]) if item.get("ended_at") else None
        item["driver_name"] = item["driver_name"] or "No driver assigned"
        item["route_name"] = item["route_name"] or "No route assigned"
        tabulation_rows.append(item)
    return tabulation_rows


def compress_report_timeseries(rows, value_key):
    totals = {}
    for row in rows:
        day = str(row["service_date"])
        totals[day] = totals.get(day, 0) + row[value_key]
    ordered_days = sorted(totals.keys())
    values = [totals[day] for day in ordered_days]
    if value_key.endswith("revenue"):
        values = [round(float(value), 2) for value in values]
    else:
        values = [int(value) for value in values]
    return ordered_days, values


def build_report_bus_analytics(daily_bus_rows):
    bus_map = {}
    totals = {
        "student_count": 0,
        "pwd_count": 0,
        "senior_count": 0,
        "regular_count": 0,
        "total_passengers": 0,
        "student_revenue": 0.0,
        "pwd_revenue": 0.0,
        "senior_revenue": 0.0,
        "regular_revenue": 0.0,
        "total_revenue": 0.0,
    }

    for row in daily_bus_rows:
        plate_number = row["plate_number"]
        bus_entry = bus_map.setdefault(
            plate_number,
            {
                "plate_number": plate_number,
                "driver_name": row["driver_name"],
                "route_name": row["route_name"],
                "labels": [],
                "passenger_totals": {"student": 0, "pwd": 0, "senior": 0, "regular": 0},
                "revenue_totals": {"student": 0.0, "pwd": 0.0, "senior": 0.0, "regular": 0.0},
                "daily_passengers": [],
                "daily_revenue": [],
            },
        )
        bus_entry["driver_name"] = row["driver_name"]
        bus_entry["route_name"] = row["route_name"]

        for source_key, target_key in (
            ("student_count", "student"),
            ("pwd_count", "pwd"),
            ("senior_count", "senior"),
            ("regular_count", "regular"),
        ):
            value = int(row[source_key] or 0)
            bus_entry["passenger_totals"][target_key] += value
            totals[source_key] += value

        for source_key, target_key in (
            ("student_revenue", "student"),
            ("pwd_revenue", "pwd"),
            ("senior_revenue", "senior"),
            ("regular_revenue", "regular"),
        ):
            value = float(row[source_key] or 0)
            bus_entry["revenue_totals"][target_key] += value
            totals[source_key] += value

        totals["total_passengers"] += int(row["total_passengers"] or 0)
        totals["total_revenue"] += float(row["total_revenue"] or 0)

    for bus_entry in bus_map.values():
        bus_rows = [row for row in daily_bus_rows if row["plate_number"] == bus_entry["plate_number"]]
        labels, daily_passengers = compress_report_timeseries(bus_rows, "total_passengers")
        _, daily_revenue = compress_report_timeseries(bus_rows, "total_revenue")
        bus_entry["labels"] = labels
        bus_entry["daily_passengers"] = daily_passengers
        bus_entry["daily_revenue"] = daily_revenue

    all_labels, all_daily_passengers = compress_report_timeseries(daily_bus_rows, "total_passengers")
    _, all_daily_revenue = compress_report_timeseries(daily_bus_rows, "total_revenue")

    return {
        "bus_options": sorted(bus_map.keys()),
        "all": {
            "labels": all_labels,
            "daily_passengers": all_daily_passengers,
            "daily_revenue": all_daily_revenue,
            "passenger_totals": {
                "student": totals["student_count"],
                "pwd": totals["pwd_count"],
                "senior": totals["senior_count"],
                "regular": totals["regular_count"],
            },
            "revenue_totals": {
                "student": totals["student_revenue"],
                "pwd": totals["pwd_revenue"],
                "senior": totals["senior_revenue"],
                "regular": totals["regular_revenue"],
            },
            "total_passengers": totals["total_passengers"],
            "total_revenue": totals["total_revenue"],
        },
        "buses": bus_map,
    }


def build_bus_report_sections(fleet_rows, daily_bus_rows):
    rows_by_bus = {}
    for row in daily_bus_rows:
        rows_by_bus.setdefault(row["plate_number"], []).append(row)

    sections = []
    for fleet in fleet_rows:
        plate_number = fleet["plate_number"]
        bus_rows = rows_by_bus.get(plate_number, [])
        sections.append(
            {
                "plate_number": plate_number,
                "status": fleet["status"],
                "trip_status": fleet["trip_status"],
                "route_name": fleet.get("route_name") or "No assigned route",
                "rows": bus_rows,
                "total_passengers": sum(int(row["total_passengers"] or 0) for row in bus_rows),
                "total_revenue": round(sum(float(row["total_revenue"] or 0) for row in bus_rows), 2),
                "passengers_by_type": {
                    "student": sum(int(row["student_count"] or 0) for row in bus_rows),
                    "pwd": sum(int(row["pwd_count"] or 0) for row in bus_rows),
                    "senior": sum(int(row["senior_count"] or 0) for row in bus_rows),
                    "regular": sum(int(row["regular_count"] or 0) for row in bus_rows),
                },
                "revenue_by_type": {
                    "student": round(sum(float(row["student_revenue"] or 0) for row in bus_rows), 2),
                    "pwd": round(sum(float(row["pwd_revenue"] or 0) for row in bus_rows), 2),
                    "senior": round(sum(float(row["senior_revenue"] or 0) for row in bus_rows), 2),
                    "regular": round(sum(float(row["regular_revenue"] or 0) for row in bus_rows), 2),
                },
            }
        )
    return sections


def build_user_directory(conn):
    rows = conn.execute(
        """
        SELECT id, username, email, role, full_name, created_at
        FROM users
        ORDER BY created_at DESC, id DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def build_staff_attendance(conn, limit=25):
    rows = conn.execute(
        """
        SELECT
            s.login_time,
            u.id AS user_id,
            u.full_name,
            u.role,
            u.username,
            (
                SELECT t.id
                FROM trips t
                WHERE
                    ((u.role = 'driver' AND t.driver_id = u.id) OR (u.role = 'conductor' AND t.conductor_id = u.id))
                    AND DATE(t.started_at) = DATE(s.login_time)
                ORDER BY t.started_at DESC, t.id DESC
                LIMIT 1
            ) AS trip_id,
            (
                SELECT b.plate_number
                FROM trips t
                JOIN buses b ON b.id = t.bus_id
                WHERE
                    ((u.role = 'driver' AND t.driver_id = u.id) OR (u.role = 'conductor' AND t.conductor_id = u.id))
                    AND DATE(t.started_at) = DATE(s.login_time)
                ORDER BY t.started_at DESC, t.id DESC
                LIMIT 1
            ) AS plate_number,
            (
                SELECT r.route_name
                FROM trips t
                JOIN routes r ON r.id = t.route_id
                WHERE
                    ((u.role = 'driver' AND t.driver_id = u.id) OR (u.role = 'conductor' AND t.conductor_id = u.id))
                    AND DATE(t.started_at) = DATE(s.login_time)
                ORDER BY t.started_at DESC, t.id DESC
                LIMIT 1
            ) AS route_name,
            (
                SELECT t.started_at
                FROM trips t
                WHERE
                    ((u.role = 'driver' AND t.driver_id = u.id) OR (u.role = 'conductor' AND t.conductor_id = u.id))
                    AND DATE(t.started_at) = DATE(s.login_time)
                ORDER BY t.started_at DESC, t.id DESC
                LIMIT 1
            ) AS trip_started_at,
            (
                SELECT t.ended_at
                FROM trips t
                WHERE
                    ((u.role = 'driver' AND t.driver_id = u.id) OR (u.role = 'conductor' AND t.conductor_id = u.id))
                    AND DATE(t.started_at) = DATE(s.login_time)
                ORDER BY t.started_at DESC, t.id DESC
                LIMIT 1
            ) AS trip_ended_at,
            (
                SELECT t.status
                FROM trips t
                WHERE
                    ((u.role = 'driver' AND t.driver_id = u.id) OR (u.role = 'conductor' AND t.conductor_id = u.id))
                    AND DATE(t.started_at) = DATE(s.login_time)
                ORDER BY t.started_at DESC, t.id DESC
                LIMIT 1
            ) AS trip_status
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        ORDER BY s.login_time DESC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()

    attendance_rows = []
    for row in rows:
        attendance_row = dict(row)
        attendance_row["trip_summary"] = (
            f"{attendance_row['plate_number']} / {attendance_row['route_name']}"
            if attendance_row.get("trip_id") and attendance_row.get("plate_number") and attendance_row.get("route_name")
            else "No trip linked on login date"
        )
        attendance_rows.append(attendance_row)
    return attendance_rows


def build_admin_overview(conn):
    live_data = build_live_bus_data(conn)
    today = now().date().isoformat()
    yesterday = (now().date() - timedelta(days=1)).isoformat()

    passenger_totals_row = conn.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN event_type = 'board' AND DATE(recorded_at) = ? THEN quantity ELSE 0 END), 0) AS today_total,
            COALESCE(SUM(CASE WHEN event_type = 'board' AND DATE(recorded_at) = ? THEN quantity ELSE 0 END), 0) AS yesterday_total,
            COUNT(DISTINCT CASE WHEN event_type = 'board' AND DATE(recorded_at) = ? THEN trip_id END) AS trips_today
        FROM trip_transactions
        """,
        (today, yesterday, today),
    ).fetchone()

    records_today_row = conn.execute(
        """
        SELECT COUNT(*) AS records_today
        FROM trip_records
        WHERE DATE(recorded_at) = ?
        """,
        (today,),
    ).fetchone()

    route_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT r.route_name,
                   r.id,
                   r.is_published,
                   r.minimum_fare,
                   r.discounted_fare,
                   COUNT(DISTINCT t.id) AS trip_count,
                   COALESCE(SUM(tx.passengers), 0) AS passengers,
                   COALESCE(ROUND(AVG(t.peak_occupancy * 100.0 / b.capacity), 1), 0) AS avg_load_percent
            FROM routes r
            LEFT JOIN trips t ON t.route_id = r.id
            LEFT JOIN buses b ON b.id = t.bus_id
            LEFT JOIN (
                SELECT trip_id,
                       COALESCE(SUM(CASE WHEN event_type = 'board' THEN quantity ELSE 0 END), 0) AS passengers
                FROM trip_transactions
                GROUP BY trip_id
            ) tx ON tx.trip_id = t.id
            WHERE r.is_published = 1
            GROUP BY r.id
            ORDER BY passengers DESC, r.route_name
            """
        ).fetchall()
    ]

    live_bus_rows = []
    for bus in live_data["buses"]:
        live_bus_rows.append(
            {
                "plate_number": bus["id"],
                "route_name": bus["direction"],
                "driver": bus["driver"],
                "location": bus["nextStop"],
                "occupancy": bus["passengers"],
                "capacity": bus["capacity"],
                "crowd_level": bus["crowdLevel"],
                "lat": bus["lat"],
                "lng": bus["lng"],
            }
        )

    daily_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT DATE(recorded_at) AS day,
                   COALESCE(SUM(CASE WHEN event_type = 'board' THEN quantity ELSE 0 END), 0) AS total
            FROM trip_transactions
            WHERE DATE(recorded_at) >= DATE_SUB(%s, INTERVAL 6 DAY)
            GROUP BY DATE(recorded_at)
            ORDER BY DATE(recorded_at)
            """,
            (today,),
        ).fetchall()
    ]
    daily_labels, daily_values = fill_missing_days(daily_rows, "day", 7)

    hourly_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT CONCAT(LPAD(HOUR(recorded_at), 2, '0'), ':00') AS hour_label,
                   COALESCE(SUM(CASE WHEN event_type = 'board' THEN quantity ELSE 0 END), 0) AS total
            FROM trip_transactions
            WHERE DATE(recorded_at) = ?
            GROUP BY HOUR(recorded_at)
            ORDER BY HOUR(recorded_at)
            """,
            (today,),
        ).fetchall()
    ]
    hourly_lookup = {row["hour_label"]: row["total"] for row in hourly_rows}
    hourly_labels = [f"{str(hour).zfill(2)}:00" for hour in range(6, 22)]
    hourly_values = [int(hourly_lookup.get(label, 0) or 0) for label in hourly_labels]

    type_row = conn.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'student' THEN quantity ELSE 0 END), 0) AS students,
            COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'pwd' THEN quantity ELSE 0 END), 0) AS pwd,
            COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'senior' THEN quantity ELSE 0 END), 0) AS senior,
            COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'regular' THEN quantity ELSE 0 END), 0) AS regular
        FROM trip_transactions
        WHERE DATE(recorded_at) >= DATE_SUB(%s, INTERVAL 6 DAY)
        """,
        (today,),
    ).fetchone()

    recent_logs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT sl.created_at, sl.role, sl.action, sl.description, u.full_name
            FROM system_logs sl
            LEFT JOIN users u ON u.id = sl.user_id
            ORDER BY sl.created_at DESC, sl.id DESC
            LIMIT 10
            """
        ).fetchall()
    ]
    attendance_rows = build_staff_attendance(conn, 25)

    fleet_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT b.id, b.plate_number, b.status, b.capacity, b.route_color,
                   r.route_name,
                   COALESCE(t.status, 'idle') AS trip_status,
                   COALESCE(t.occupancy, 0) AS occupancy,
                   COALESCE(t.peak_occupancy, 0) AS peak_occupancy
            FROM buses b
            LEFT JOIN trips t ON t.id = (
                SELECT id
                FROM trips
                WHERE bus_id = b.id
                ORDER BY started_at DESC, id DESC
                LIMIT 1
            )
            LEFT JOIN routes r ON r.id = t.route_id
            ORDER BY b.plate_number
            """
        ).fetchall()
    ]
    stop_rows = build_stop_analytics(conn)
    recent_transaction_audit = get_recent_transaction_audit(conn, 20)
    trip_audit_rows, audit_summary = build_trip_audit_summary(conn, 50)
    daily_bus_rows = build_daily_bus_tabulation(conn, 60)
    report_bus_analytics = build_report_bus_analytics(daily_bus_rows)
    bus_report_sections = build_bus_report_sections(fleet_rows, daily_bus_rows)
    for fleet in fleet_rows:
        plate_number = fleet["plate_number"]
        if plate_number not in report_bus_analytics["buses"]:
            report_bus_analytics["buses"][plate_number] = {
                "plate_number": plate_number,
                "driver_name": "No driver assigned",
                "route_name": fleet.get("route_name") or "No assigned route",
                "labels": [],
                "passenger_totals": {"student": 0, "pwd": 0, "senior": 0, "regular": 0},
                "revenue_totals": {"student": 0.0, "pwd": 0.0, "senior": 0.0, "regular": 0.0},
                "daily_passengers": [],
                "daily_revenue": [],
            }
    report_bus_analytics["bus_options"] = sorted(report_bus_analytics["buses"].keys())
    service_alerts = get_active_service_alerts(conn)
    user_rows = build_user_directory(conn)

    peak_hour_label = hourly_labels[0]
    peak_hour_value = 0
    if hourly_values:
        peak_hour_value = max(hourly_values)
        peak_hour_label = hourly_labels[hourly_values.index(peak_hour_value)]

    overview = {
        "today_total": int(passenger_totals_row["today_total"] or 0),
        "yesterday_total": int(passenger_totals_row["yesterday_total"] or 0),
        "trips_today": int(passenger_totals_row["trips_today"] or 0),
        "records_today": int(records_today_row["records_today"] or 0),
        "active_bus_count": live_data["active_bus_count"],
        "avg_crowd": live_data["avg_crowd"],
        "low_count": live_data["low_count"],
        "medium_count": live_data["medium_count"],
        "high_crowd_count": live_data["high_count"],
        "route_rows": route_rows,
        "live_bus_rows": live_bus_rows,
        "fleet_rows": fleet_rows,
        "recent_logs": recent_logs,
        "attendance_rows": attendance_rows,
        "service_alerts": service_alerts,
        "stop_rows": stop_rows,
        "recent_transaction_audit": recent_transaction_audit,
        "trip_audit_rows": trip_audit_rows,
        "daily_bus_rows": daily_bus_rows,
        "bus_report_sections": bus_report_sections,
        "report_bus_analytics": report_bus_analytics,
        "audit_summary": audit_summary,
        "user_rows": user_rows,
        "peak_hour_label": peak_hour_label,
        "peak_hour_value": peak_hour_value,
        "charts": {
            "daily_labels": daily_labels,
            "daily_values": daily_values,
            "hourly_labels": hourly_labels,
            "hourly_values": hourly_values,
            "route_labels": [row["route_name"] for row in route_rows],
            "route_values": [row["passengers"] for row in route_rows],
            "stop_labels": [row["stop_name"] for row in stop_rows],
            "stop_values": [int(row["boarded"] or 0) for row in stop_rows],
            "mix_labels": ["Students", "PWD", "Senior", "Regular"],
            "mix_values": [int(type_row["students"]), int(type_row["pwd"]), int(type_row["senior"]), int(type_row["regular"])],
            "live_buses": live_data["buses"],
            "report_bus_analytics": report_bus_analytics,
        },
    }
    overview["insights"] = generate_ai_insights(overview)
    return overview


def build_admin_live_payload(conn):
    overview = build_admin_overview(conn)
    return {
        "active_bus_count": overview["active_bus_count"],
        "avg_crowd": overview["avg_crowd"],
        "live_bus_rows": overview["live_bus_rows"],
        "live_buses": overview["charts"]["live_buses"],
        "high_crowd_count": overview["high_crowd_count"],
    }


def build_driver_overview(conn, driver_id):
    driver = conn.execute(
        "SELECT id, username, full_name FROM users WHERE id = ?",
        (driver_id,),
    ).fetchone()
    active_trip = get_active_trip_for_driver(conn, driver_id)

    available_buses = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM buses
            WHERE status = 'online'
              AND id NOT IN (
                SELECT bus_id
                FROM trips
                WHERE status = 'active'
              )
            ORDER BY plate_number
            """
        ).fetchall()
    ]

    routes = [dict(row) for row in conn.execute("SELECT * FROM routes WHERE is_published = 1 ORDER BY display_order, route_name").fetchall()]

    trip_metrics = {
        "occupancy": 0,
        "capacity": DEFAULT_BUS_CAPACITY,
        "next_stop": "No active trip",
        "trip_duration": "00:00:00",
        "crowd_level": "Low",
        "updates_count": 0,
        "last_gps_at": None,
    }

    if active_trip:
        latest_record = get_latest_trip_record(conn, active_trip["id"])
        latest_gps = conn.execute(
            """
            SELECT latitude, longitude, recorded_at
            FROM gps_logs
            WHERE trip_id = ?
            ORDER BY recorded_at DESC, id DESC
            LIMIT 1
            """,
            (active_trip["id"],),
        ).fetchone()
        started_at = from_db_time(active_trip["started_at"])
        duration = now() - started_at if started_at else timedelta(0)
        total_seconds = max(int(duration.total_seconds()), 0)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        current_stop_details = get_trip_current_stop_details(
            active_trip,
            dict(latest_gps) if latest_gps else None,
            latest_record["stop_name"] if latest_record else None,
        )

        trip_metrics = {
            "occupancy": active_trip["occupancy"],
            "capacity": active_trip["capacity"],
            "next_stop": current_stop_details["name"] if current_stop_details else active_trip["end_point"],
            "trip_duration": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
            "crowd_level": classify_capacity(active_trip["occupancy"], active_trip["capacity"]),
            "updates_count": conn.execute("SELECT COUNT(*) AS total_count FROM trip_records WHERE trip_id = ?", (active_trip["id"],)).fetchone()["total_count"],
            "last_gps_at": normalize_json_value(latest_gps["recorded_at"]) if latest_gps else None,
        }

    return {
        "driver": dict(driver) if driver else None,
        "active_trip": normalize_json_value(active_trip) if active_trip else None,
        "available_buses": available_buses,
        "routes": routes,
        "trip_metrics": trip_metrics,
    }


def build_conductor_overview(conn, conductor_id):
    active_trip = get_active_trip_for_conductor(conn, conductor_id)
    available_trips = [
        dict(row)
        for row in conn.execute(
            """
            SELECT t.id, b.plate_number, r.route_name, u.full_name AS driver_name, t.occupancy
            FROM trips t
            JOIN buses b ON b.id = t.bus_id
            JOIN routes r ON r.id = t.route_id
            LEFT JOIN users u ON u.id = t.driver_id
            WHERE t.status = 'active'
              AND (t.conductor_id IS NULL OR t.conductor_id = ?)
            ORDER BY t.started_at DESC, t.id DESC
            """,
            (conductor_id,),
        ).fetchall()
    ]
    buses = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM buses
            WHERE status = 'online'
              AND id NOT IN (SELECT bus_id FROM trips WHERE status = 'active')
            ORDER BY plate_number
            """
        ).fetchall()
    ]
    routes = [dict(row) for row in conn.execute("SELECT * FROM routes WHERE is_published = 1 ORDER BY display_order, route_name").fetchall()]

    transaction_form = {
        "destination_stop": "",
        "passenger_type": "",
    }
    trip_summary = None
    latest_gps = None
    recent_transactions = []
    destination_manifest = []
    current_stop = "Waiting for location"
    destination_options = []
    fare_preview = 0.0
    today_summary = {
        "students": 0,
        "pwd": 0,
        "senior": 0,
        "regular": 0,
        "boarded": 0,
        "dropped": 0,
        "transactions": 0,
    }

    if active_trip:
        active_trip["monitoring_mode"] = get_monitoring_mode(active_trip.get("notes"))
        latest_record = get_latest_trip_record(conn, active_trip["id"])
        latest_gps = get_latest_trip_gps(conn, active_trip["id"])
        current_stop_details = get_trip_current_stop_details(
            active_trip,
            latest_gps,
            latest_record["stop_name"] if latest_record else None,
        )
        if current_stop_details:
            current_stop = current_stop_details["name"]
            auto_offboard_due_passengers(
                conn,
                active_trip,
                current_stop,
                latest_gps["latitude"] if latest_gps and latest_gps["latitude"] is not None else None,
                latest_gps["longitude"] if latest_gps and latest_gps["longitude"] is not None else None,
                conductor_id,
            )
        destination_options = []
        for option in get_trip_destination_options(active_trip, current_stop):
            destination_options.append(
                {
                    **option,
                    "fare_guide": estimate_fare_table(
                        estimate_trip_segment_distance(active_trip, current_stop, option["name"]),
                        active_trip.get("minimum_fare"),
                        active_trip.get("discounted_fare"),
                    ),
                }
            )
        destination_manifest = sync_trip_occupancy_from_destinations(conn, active_trip, current_stop)
        if latest_record:
            trip_summary = dict(latest_record)
            trip_summary["stop_name"] = current_stop
        else:
            trip_summary = {"stop_name": current_stop}

        recent_transactions = get_recent_trip_transactions(conn, active_trip["id"], 8)

    summary_row = conn.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'student' THEN quantity ELSE 0 END), 0) AS students,
            COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'pwd' THEN quantity ELSE 0 END), 0) AS pwd,
            COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'senior' THEN quantity ELSE 0 END), 0) AS senior,
            COALESCE(SUM(CASE WHEN event_type = 'board' AND passenger_type = 'regular' THEN quantity ELSE 0 END), 0) AS regular,
            COALESCE(SUM(CASE WHEN event_type = 'board' THEN quantity ELSE 0 END), 0) AS boarded,
            COALESCE(SUM(CASE WHEN event_type = 'drop' THEN quantity ELSE 0 END), 0) AS dropped,
            COUNT(*) AS transactions
        FROM trip_transactions tt
        JOIN trips t ON t.id = tt.trip_id
        WHERE t.conductor_id = ?
          AND DATE(tt.recorded_at) = DATE(?)
        """,
        (conductor_id, now().date().isoformat()),
    ).fetchone()

    if summary_row:
        today_summary = {key: int(summary_row[key] or 0) for key in today_summary}

    occupancy = capacity_details(
        active_trip["occupancy"] if active_trip else 0,
        active_trip["capacity"] if active_trip else DEFAULT_BUS_CAPACITY,
    )

    if active_trip and transaction_form["destination_stop"]:
        fare_preview = calculate_segment_fare_total(
            active_trip,
            transaction_form["passenger_type"] or "regular",
            1,
            current_stop,
            transaction_form["destination_stop"],
        )

    return {
        "active_trip": active_trip,
        "available_trips": available_trips,
        "buses": buses,
        "routes": routes,
        "transaction_form": transaction_form,
        "trip_summary": trip_summary,
        "current_stop": current_stop,
        "destination_options": destination_options,
        "destination_manifest": destination_manifest,
        "fare_preview": fare_preview,
        "today_summary": today_summary,
        "recent_transactions": recent_transactions,
        "capacity": occupancy,
        "latest_position": normalize_json_value(latest_gps) if active_trip and latest_gps else None,
        "workflow_notes": [
            {
                "title": "Ticketing + crowd merge candidate",
                "body": "The current manual counter works, but it creates extra device switching for the conductor. The next redesign should combine passenger type entry and ticketing into one capture flow.",
            },
            {
                "title": "Low-friction counting direction",
                "body": "Keep GPS and stop detection automatic from the driver trip, then reduce manual conductor actions to the fewest taps possible per boarding event.",
            },
            {
                "title": "Camera counting is future scope",
                "body": "Camera-based passenger counting can later enrich validation and analytics, but it still needs planning, device integration, and data model changes before it should affect the workflow.",
            },
        ],
    }


def get_default_routes():
    forward_coords = [[stop["lat"], stop["lng"]] for stop in FORWARD_ROUTE_STOPS]
    reverse_coords = [[stop["lat"], stop["lng"]] for stop in REVERSE_ROUTE_STOPS]
    return [
        (
            FORWARD_ROUTE_NAME,
            FORWARD_ROUTE_STOPS[0]["name"],
            FORWARD_ROUTE_STOPS[-1]["name"],
            CORRIDOR_DISTANCE_KM,
            CORRIDOR_TRAVEL_MINUTES,
            15.0,
            12.0,
            1,
            json.dumps(forward_coords),
        ),
        (
            REVERSE_ROUTE_NAME,
            REVERSE_ROUTE_STOPS[0]["name"],
            REVERSE_ROUTE_STOPS[-1]["name"],
            CORRIDOR_DISTANCE_KM,
            CORRIDOR_TRAVEL_MINUTES,
            15.0,
            12.0,
            2,
            json.dumps(reverse_coords),
        ),
    ]


def sync_default_routes(conn):
    default_routes = get_default_routes()
    default_names = [route[0] for route in default_routes]
    for route_name, start_point, end_point, distance_km, expected_duration_minutes, minimum_fare, discounted_fare, display_order, coords_json in default_routes:
        existing = conn.execute(
            "SELECT id FROM routes WHERE route_name = ?",
            (route_name,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE routes
                SET start_point = ?, end_point = ?, distance_km = ?, expected_duration_minutes = ?, minimum_fare = ?, discounted_fare = ?, display_order = ?, coords_json = ?
                WHERE id = ?
                """,
                (start_point, end_point, distance_km, expected_duration_minutes, minimum_fare, discounted_fare, display_order, coords_json, existing["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO routes (route_name, start_point, end_point, distance_km, expected_duration_minutes, minimum_fare, discounted_fare, display_order, coords_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (route_name, start_point, end_point, distance_km, expected_duration_minutes, minimum_fare, discounted_fare, display_order, coords_json),
            )
    placeholders = ",".join(["%s"] * len(default_names))
    conn.execute(
        f"UPDATE routes SET is_published = CASE WHEN route_name IN ({placeholders}) THEN 1 ELSE 0 END",
        tuple(default_names),
    )


def sync_default_stops(conn):
    for stop in CORRIDOR_STOP_DETAILS:
        existing_stop = conn.execute(
            "SELECT id FROM stops WHERE stop_name = ?",
            (stop["name"],),
        ).fetchone()
        if existing_stop:
            conn.execute(
                """
                UPDATE stops
                SET latitude = ?, longitude = ?, landmark = ?, is_active = 1
                WHERE id = ?
                """,
                (
                    stop["lat"],
                    stop["lng"],
                    stop.get("landmark") or "Cabiao-Cabanatuan corridor stop",
                    existing_stop["id"],
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO stops (stop_name, latitude, longitude, landmark, is_active)
                VALUES (?, ?, ?, ?, 1)
                """,
                (
                    stop["name"],
                    stop["lat"],
                    stop["lng"],
                    stop.get("landmark") or "Cabiao-Cabanatuan corridor stop",
                ),
            )

    route_stop_sets = {
        FORWARD_ROUTE_NAME: FORWARD_ROUTE_STOPS,
        REVERSE_ROUTE_NAME: REVERSE_ROUTE_STOPS,
    }
    for route_name, route_stops in route_stop_sets.items():
        route_row = conn.execute(
            "SELECT id FROM routes WHERE route_name = ?",
            (route_name,),
        ).fetchone()
        if not route_row:
            continue
        route_id = route_row["id"]
        conn.execute("DELETE FROM route_stops WHERE route_id = ?", (route_id,))
        for sequence, stop in enumerate(route_stops, start=1):
            stop_row = conn.execute(
                "SELECT id FROM stops WHERE stop_name = ?",
                (stop["name"],),
            ).fetchone()
            if not stop_row:
                continue
            conn.execute(
                """
                INSERT INTO route_stops (route_id, stop_id, stop_sequence, minutes_from_start)
                VALUES (?, ?, ?, ?)
                """,
                (route_id, stop_row["id"], sequence, int(stop["minutes_from_start"])),
            )


def seed_demo_data():
    conn = get_db()
    sync_default_routes(conn)
    sync_default_stops(conn)
    refresh_route_stop_cache(conn)
    conn.execute(
        """
        UPDATE users
        SET full_name = ?
        WHERE username = 'admin'
        """,
        ("Marites Mariano",),
    )
    users = [
        ("admin", "admin@example.com", "admin123", "admin", "Marites Mariano"),
        ("driver1", "driver1@example.com", "driver123", "driver", "Juan Dela Cruz"),
        ("driver2", "driver2@example.com", "driver123", "driver", "Rico Mendoza"),
        ("conductor1", "conductor1@example.com", "conduct123", "conductor", "Ana Ramos"),
    ]
    for username, email, password, role, full_name in users:
        existing_user = conn.execute(
            "SELECT id FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if not existing_user:
            conn.execute(
                """
                INSERT INTO users (username, email, password, role, full_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (username, email, password, role, full_name, to_db_time(now())),
            )

    buses = [
        ("MB-01", 32, "offline", "#0f766e", "Primary unit"),
        ("MB-02", 34, "offline", "#1d4ed8", "Secondary unit"),
        ("MB-03", 30, "offline", "#c2410c", "Reserve unit"),
        ("MB-04", 28, "offline", "#7c3aed", "Standby unit"),
    ]
    for plate_number, capacity, status, route_color, notes in buses:
        existing_bus = conn.execute(
            "SELECT id FROM buses WHERE plate_number = ?",
            (plate_number,),
        ).fetchone()
        if existing_bus:
            conn.execute(
                """
                UPDATE buses
                SET capacity = ?, route_color = ?, notes = ?
                WHERE id = ?
                """,
                (capacity, route_color, notes, existing_bus["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO buses (plate_number, capacity, status, route_color, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                (plate_number, capacity, status, route_color, notes),
            )

    seeded_trip_ids = [
        row["id"]
        for row in conn.execute(
            """
            SELECT id
            FROM trips
            WHERE notes IN ('Completed seeded trip', 'Morning live route', 'Rush interval service')
            """
        ).fetchall()
    ]
    if seeded_trip_ids:
        placeholders = ",".join(["?"] * len(seeded_trip_ids))
        conn.execute(f"DELETE FROM gps_logs WHERE trip_id IN ({placeholders})", seeded_trip_ids)
        conn.execute(f"DELETE FROM trip_records WHERE trip_id IN ({placeholders})", seeded_trip_ids)
        conn.execute(f"DELETE FROM trips WHERE id IN ({placeholders})", seeded_trip_ids)

    conn.execute(
        """
        DELETE FROM system_logs
        WHERE action = 'Seed Complete'
           OR description IN (
               'Initial analytics dataset was generated for dashboard testing.',
               'Juan Dela Cruz started MB-01 on Cabiao - Cabanatuan.',
               'Ana Ramos attached crowd analytics to MB-01.',
               'Rico Mendoza started MB-02 on Gapan - Cabanatuan.'
           )
        """
    )

    default_alerts = [
        ("Cabiao peak boarding advisory", "Board early at Cabiao Town Proper during the afternoon peak because crowding builds quickly once the trip leaves San Isidro.", "warning", FORWARD_ROUTE_NAME, "Cabiao Town Proper"),
        ("Tracker ETA notice", "Next-bus estimates depend on recent GPS updates from active trips and may pause when a unit goes offline.", "info", None, None),
    ]
    for title, message, severity, route_name, stop_name in default_alerts:
        existing_alert = conn.execute(
            "SELECT id FROM service_alerts WHERE title = ?",
            (title,),
        ).fetchone()
        if not existing_alert:
            route_row = conn.execute("SELECT id FROM routes WHERE route_name = ?", (route_name,)).fetchone() if route_name else None
            conn.execute(
                """
                INSERT INTO service_alerts (route_id, stop_name, title, message, severity, is_active, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, 1, NULL, ?)
                """,
                (route_row["id"] if route_row else None, stop_name, title, message, severity, to_db_time(now())),
            )

    conn.commit()
    backfill_missing_transaction_fares(conn)
    conn.commit()
    conn.close()


bootstrap_db()
seed_demo_data()


@app.route("/")
def landing():
    conn = get_db()
    live_data = build_live_bus_data(conn)
    commuter_data = build_public_commuter_data(conn, live_data)
    conn.close()
    commuter_payload = normalize_json_value(commuter_data)
    preview_buses = [bus for bus in live_data["buses"] if bus["status"] == "online"][:3]
    primary_route = commuter_payload["routes"][0] if commuter_payload["routes"] else None
    return render_template(
        "landing/index.html",
        active_bus_count=live_data["active_bus_count"],
        avg_crowd=live_data["avg_crowd"],
        low_count=live_data["low_count"],
        medium_count=live_data["medium_count"],
        high_count=live_data["high_count"],
        preview_buses=preview_buses,
        buses_json=json.dumps(live_data["buses"]),
        primary_route=primary_route,
        commuter_data_json=json.dumps(commuter_payload),
        service_alerts=commuter_payload["serviceAlerts"],
        stop_directory_preview=commuter_payload["stopDirectory"][:6],
        route_cards=commuter_payload["routes"],
    )


@app.route("/track")
def tracker():
    conn = get_db()
    live_data = build_live_bus_data(conn)
    commuter_data = build_public_commuter_data(conn, live_data)
    conn.close()
    commuter_payload = normalize_json_value(commuter_data)
    return render_template(
        "landing/tracker.html",
        buses_json=json.dumps(live_data["buses"]),
        commuter_data_json=json.dumps(commuter_payload),
        service_alerts=commuter_payload["serviceAlerts"],
        active_bus_count=live_data["active_bus_count"],
        avg_crowd=live_data["avg_crowd"],
        low_count=live_data["low_count"],
        medium_count=live_data["medium_count"],
        high_count=live_data["high_count"],
    )


@app.route("/api/public-commuter")
def api_public_commuter():
    conn = get_db()
    live_data = build_live_bus_data(conn)
    payload = build_public_commuter_data(conn, live_data)
    conn.close()
    return jsonify(normalize_json_value(payload))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        submitted = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        conn = get_db()
        user = conn.execute(
            "SELECT * FROM users WHERE username = ? OR email = ?",
            (submitted, submitted),
        ).fetchone()

        if user and user["password"] == password:
            session["user_id"] = user["id"]
            session["role"] = user["role"]
            conn.execute(
                "INSERT INTO sessions (user_id, login_time) VALUES (?, ?)",
                (user["id"], to_db_time(now())),
            )
            log_event(conn, user["id"], user["role"], "Login", f"{user['full_name']} signed in.")
            conn.commit()
            conn.close()

            if user["role"] == "admin":
                return redirect(url_for("admin_dashboard"))
            if user["role"] == "driver":
                return redirect(url_for("driver_dashboard"))
            return redirect(url_for("conductor"))

        conn.close()
        return render_template("login.html", error="Invalid login credentials.")

    return render_template("login.html")


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        user = fetch_one("SELECT full_name FROM users WHERE email = ?", (email,))
        if user:
            return render_template("forgot_password.html", message=f"Password reset request recorded for {user['full_name']}.")
        return render_template("forgot_password.html", error="Email not found.")
    return render_template("forgot_password.html")


@app.route("/admin", methods=["GET", "POST"])
@require_role("admin")
def admin_dashboard():
    conn = get_db()
    active_tab = request.args.get("tab", "analytics")

    if request.method == "POST":
        action = request.form.get("action")
        if action == "update_bus_status":
            bus_id_raw = request.form.get("bus_id", "").strip()
            next_status = request.form.get("status", "").strip().lower()
            redirect_tab = request.form.get("redirect_tab", "analytics").strip() or "analytics"

            if bus_id_raw.isdigit() and next_status in {"online", "offline", "maintenance"}:
                bus_id = int(bus_id_raw)
                bus_row = conn.execute(
                    "SELECT plate_number FROM buses WHERE id = ?",
                    (bus_id,),
                ).fetchone()

                if bus_row:
                    conn.execute(
                        """
                        UPDATE buses
                        SET status = ?
                        WHERE id = ?
                        """,
                        (next_status, bus_id),
                    )
                    log_event(
                        conn,
                        session["user_id"],
                        "admin",
                        "Bus Status Updated",
                        f"Bus {bus_row['plate_number']} marked as {next_status}.",
                    )
                    conn.commit()
                    conn.close()
                    return redirect(url_for("admin_dashboard", tab=redirect_tab))
        elif action == "update_route_settings":
            route_id_raw = request.form.get("route_id", "").strip()
            redirect_tab = request.form.get("redirect_tab", "commuter").strip() or "commuter"
            if route_id_raw.isdigit():
                route_id = int(route_id_raw)
                is_published = 1 if request.form.get("is_published") == "1" else 0
                minimum_fare = max(to_float(request.form.get("minimum_fare"), 15.0), 1.0)
                discounted_fare = max(to_float(request.form.get("discounted_fare"), 12.0), 1.0)
                route_row = conn.execute("SELECT route_name FROM routes WHERE id = ?", (route_id,)).fetchone()
                if route_row:
                    conn.execute(
                        """
                        UPDATE routes
                        SET is_published = ?, minimum_fare = ?, discounted_fare = ?
                        WHERE id = ?
                        """,
                        (is_published, minimum_fare, discounted_fare, route_id),
                    )
                    log_event(
                        conn,
                        session["user_id"],
                        "admin",
                        "Route Settings Updated",
                        f"Route {route_row['route_name']} was {'published' if is_published else 'hidden'} with fares set to regular PHP {minimum_fare:.2f} and discounted PHP {discounted_fare:.2f}.",
                    )
                    conn.commit()
                    conn.close()
                    return redirect(url_for("admin_dashboard", tab=redirect_tab))
        elif action == "create_service_alert":
            redirect_tab = request.form.get("redirect_tab", "commuter").strip() or "commuter"
            title = request.form.get("title", "").strip()
            message = request.form.get("message", "").strip()
            severity = request.form.get("severity", "info").strip().lower()
            route_id_raw = request.form.get("route_id", "").strip()
            stop_name = request.form.get("stop_name", "").strip() or None
            if title and message and severity in {"info", "warning", "critical"}:
                route_id = int(route_id_raw) if route_id_raw.isdigit() else None
                conn.execute(
                    """
                    INSERT INTO service_alerts (trip_id, route_id, stop_name, title, message, severity, is_active, created_by, created_at)
                    VALUES (NULL, ?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (route_id, stop_name, title, message, severity, session["user_id"], to_db_time(now())),
                )
                log_event(conn, session["user_id"], "admin", "Service Alert Created", f"Service alert '{title}' was published.")
                conn.commit()
                conn.close()
                return redirect(url_for("admin_dashboard", tab=redirect_tab))
        elif action == "clear_service_alert":
            alert_id_raw = request.form.get("alert_id", "").strip()
            redirect_tab = request.form.get("redirect_tab", "commuter").strip() or "commuter"
            if alert_id_raw.isdigit():
                alert_id = int(alert_id_raw)
                alert_row = conn.execute("SELECT title FROM service_alerts WHERE id = ?", (alert_id,)).fetchone()
                if alert_row:
                    conn.execute("UPDATE service_alerts SET is_active = 0 WHERE id = ?", (alert_id,))
                    log_event(conn, session["user_id"], "admin", "Service Alert Cleared", f"Service alert '{alert_row['title']}' was cleared.")
                    conn.commit()
                    conn.close()
                    return redirect(url_for("admin_dashboard", tab=redirect_tab))
        elif action == "create_profile":
            redirect_tab = request.form.get("redirect_tab", "profiles").strip() or "profiles"
            username = request.form.get("username", "").strip()
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            role = request.form.get("role", "").strip().lower()
            full_name = request.form.get("full_name", "").strip()

            if username and email and password and full_name and role in {"admin", "driver", "conductor"}:
                existing_user = conn.execute(
                    "SELECT id FROM users WHERE username = ? OR email = ?",
                    (username, email),
                ).fetchone()
                if not existing_user:
                    conn.execute(
                        """
                        INSERT INTO users (username, email, password, role, full_name, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (username, email, password, role, full_name, to_db_time(now())),
                    )
                    log_event(
                        conn,
                        session["user_id"],
                        "admin",
                        "Profile Created",
                        f"Created {role} profile for {full_name} ({username}).",
                    )
                    conn.commit()
                    conn.close()
                    return redirect(url_for("admin_dashboard", tab=redirect_tab))

    overview = build_admin_overview(conn)
    conn.close()
    return render_template("admin/admin_dashboard.html", overview=overview, active_tab=active_tab)


@app.route("/api/live-buses")
def api_live_buses():
    conn = get_db()
    live_data = build_live_bus_data(conn)
    conn.close()
    return jsonify(normalize_json_value(live_data))


@app.route("/api/admin/live")
@require_role("admin")
def api_admin_live():
    conn = get_db()
    payload = build_admin_live_payload(conn)
    conn.close()
    return jsonify(normalize_json_value(payload))


@app.route("/admin/report.pdf")
@require_role("admin")
def admin_report():
    conn = get_db()
    overview = build_admin_overview(conn)
    conn.close()

    output = build_admin_pdf_report(overview)
    return Response(
        output.getvalue(),
        mimetype="application/pdf",
        headers={"Content-Disposition": "attachment; filename=gajoda-crowd-analytics-report.pdf"},
    )


@app.route("/dashboard")
@require_role("driver")
def driver_dashboard():
    conn = get_db()
    overview = build_driver_overview(conn, session["user_id"])
    conn.close()
    return render_template("driver/driver_dashboard.html", overview=overview)


@app.route("/tracker-device")
@require_role("driver")
def tracker_device():
    conn = get_db()
    overview = build_driver_overview(conn, session["user_id"])
    conn.close()
    return render_template("driver/tracker_device.html", overview=overview)


@app.route("/start_trip", methods=["POST"])
@require_role("driver")
def start_trip():
    conn = get_db()
    active_trip = get_active_trip_for_driver(conn, session["user_id"])
    if active_trip:
        conn.close()
        return jsonify({"error": "Driver already has an active trip."}), 400

    bus_id = to_non_negative_int(request.form.get("bus_id", 0), 0)
    route_id = to_non_negative_int(request.form.get("route_id", 0), 0)
    trip_source = request.form.get("source", "").strip().lower()
    source_label = "tracker device" if trip_source == "tracker-device" else "driver dashboard"

    bus_row = conn.execute("SELECT * FROM buses WHERE id = ? AND status = 'online'", (bus_id,)).fetchone()
    route_row = conn.execute("SELECT * FROM routes WHERE id = ?", (route_id,)).fetchone()
    active_bus = conn.execute("SELECT id FROM trips WHERE bus_id = ? AND status = 'active'", (bus_id,)).fetchone()

    if not bus_row or not route_row:
        conn.close()
        return jsonify({"error": "Select a valid bus and route."}), 400

    if active_bus:
        conn.close()
        return jsonify({"error": "This bus is already running an active trip."}), 400

    started_at = now()
    cursor = conn.execute(
        """
        INSERT INTO trips (
            driver_id, bus_id, route_id, status,
            started_at, scheduled_end, occupancy, peak_occupancy, notes
        )
        VALUES (?, ?, ?, 'active', ?, ?, 0, 0, ?)
        """,
        (
            session["user_id"],
            bus_id,
            route_id,
            to_db_time(started_at),
            to_db_time(started_at + timedelta(minutes=route_row["expected_duration_minutes"])),
            f"Started from {source_label}",
        ),
    )
    log_event(conn, session["user_id"], "driver", "Trip Started", f"Driver started {bus_row['plate_number']} on {route_row['route_name']} from the {source_label}.")
    conn.commit()
    trip_id = cursor.lastrowid
    sync_trip_service_alert(conn, trip_id, True)
    conn.commit()
    conn.close()
    return jsonify({"success": True, "trip_id": trip_id})


@app.route("/end_trip", methods=["POST"])
@require_role("driver")
def end_trip():
    conn = get_db()
    trip = get_active_trip_for_driver(conn, session["user_id"])

    if not trip:
        conn.close()
        return jsonify({"error": "No active trip found."}), 400

    started_at = from_db_time(trip["started_at"])
    duration_minutes = int(max((now() - started_at).total_seconds(), 0) // 60) if started_at else trip["duration_minutes"]
    conn.execute(
        """
        UPDATE trips
        SET status = 'completed',
            ended_at = ?,
            duration_minutes = ?
        WHERE id = ?
        """,
        (to_db_time(now()), duration_minutes, trip["id"]),
    )
    sync_trip_service_alert(conn, trip["id"], False)
    log_event(conn, session["user_id"], "driver", "Trip Ended", f"Driver completed trip #{trip['id']} on {trip['plate_number']}.")
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/driver/location", methods=["POST"])
@require_role("driver")
def driver_location():
    conn = get_db()
    trip = get_active_trip_for_driver(conn, session["user_id"])

    if not trip:
        conn.close()
        return jsonify({"error": "No active trip found."}), 400

    payload = request.get_json(silent=True) or {}
    latitude = payload.get("latitude")
    longitude = payload.get("longitude")

    if latitude is None or longitude is None:
        conn.close()
        return jsonify({"error": "Latitude and longitude are required."}), 400

    conn.execute(
        """
        INSERT INTO gps_logs (trip_id, latitude, longitude, recorded_at)
        VALUES (?, ?, ?, ?)
        """,
        (trip["id"], float(latitude), float(longitude), to_db_time(now())),
    )
    current_stop_details = get_trip_current_stop_details(
        trip,
        {"latitude": float(latitude), "longitude": float(longitude)},
        None,
    )
    if current_stop_details:
        auto_offboard_due_passengers(
            conn,
            trip,
            current_stop_details["name"],
            float(latitude),
            float(longitude),
            trip.get("conductor_id"),
        )
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/conductor", methods=["GET", "POST"])
@require_role("conductor")
def conductor():
    conn = get_db()
    conductor_id = session["user_id"]

    if request.method == "POST":
        action = request.form.get("action")
        active_trip = get_active_trip_for_conductor(conn, conductor_id)

        if action == "attach_trip":
            trip_id = int(request.form.get("trip_id", 0))
            monitoring_mode = request.form.get("monitoring_mode", "manual").strip().lower()
            if monitoring_mode not in {"manual", "auto"}:
                monitoring_mode = "manual"
            trip = conn.execute(
                "SELECT t.id, t.notes, b.plate_number, r.route_name FROM trips t JOIN buses b ON b.id = t.bus_id JOIN routes r ON r.id = t.route_id WHERE t.id = ? AND t.status = 'active'",
                (trip_id,),
            ).fetchone()
            if trip:
                conn.execute(
                    "UPDATE trips SET conductor_id = ?, notes = ? WHERE id = ?",
                    (conductor_id, set_monitoring_mode(trip["notes"], monitoring_mode), trip_id),
                )
                log_event(conn, conductor_id, "conductor", "Monitoring Attached", f"Conductor attached to trip #{trip_id} on {trip['plate_number']} ({trip['route_name']}) in {monitoring_mode} mode.")

        elif action == "set_monitoring_mode" and active_trip:
            monitoring_mode = request.form.get("monitoring_mode", "manual").strip().lower()
            if monitoring_mode in {"manual", "auto"}:
                conn.execute(
                    "UPDATE trips SET notes = ? WHERE id = ?",
                    (set_monitoring_mode(active_trip["notes"], monitoring_mode), active_trip["id"]),
                )
                log_event(conn, conductor_id, "conductor", "Monitoring Mode Changed", f"Trip #{active_trip['id']} switched to {monitoring_mode} monitoring.")

        elif action == "record_transaction" and active_trip:
            latest_gps = get_latest_trip_gps(conn, active_trip["id"])
            latitude = latest_gps["latitude"] if latest_gps and latest_gps["latitude"] is not None else None
            longitude = latest_gps["longitude"] if latest_gps and latest_gps["longitude"] is not None else None
            latest_record = get_latest_trip_record(conn, active_trip["id"])
            current_stop_details = get_trip_current_stop_details(
                active_trip,
                latest_gps,
                latest_record["stop_name"] if latest_record else None,
            )
            origin_stop = current_stop_details["name"] if current_stop_details else (active_trip.get("start_point") or "Waiting for driver location")
            destination_options = get_trip_destination_options(active_trip, origin_stop)
            passenger_type = request.form.get("passenger_type", "").strip().lower()
            destination_stop = request.form.get("destination_stop", "").strip()
            valid_destination_names = {option["name"] for option in destination_options}

            if passenger_type in {"student", "pwd", "senior", "regular"} and destination_stop in valid_destination_names:
                fare_amount = calculate_segment_fare_total(active_trip, passenger_type, 1, origin_stop, destination_stop)
            else:
                fare_amount = 0
            recorded_at = to_db_time(now())
            if passenger_type in {"student", "pwd", "senior", "regular"} and destination_stop in valid_destination_names:
                conn.execute(
                    """
                    INSERT INTO trip_transactions (
                        trip_id, conductor_id, event_type, passenger_type, quantity, fare_amount,
                        stop_name, origin_stop, destination_stop, latitude, longitude, occupancy_after, recorded_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        active_trip["id"],
                        conductor_id,
                        "board",
                        passenger_type,
                        1,
                        fare_amount,
                        origin_stop,
                        origin_stop,
                        destination_stop,
                        float(latitude) if latitude is not None else None,
                        float(longitude) if longitude is not None else None,
                        0,
                        recorded_at,
                    ),
                )

                manifest = sync_trip_occupancy_from_destinations(conn, active_trip, origin_stop)
                total = sum(item["count"] for item in manifest)
                crowd_level = classify_capacity(total, active_trip["capacity"])
                passenger_counts = {"student": 0, "pwd": 0, "senior": 0, "regular": 0}
                passenger_counts[passenger_type] = 1
                conn.execute(
                    """
                    INSERT INTO trip_records (
                        trip_id, students, pwd, senior, regular, boarded, dropped, total,
                        occupancy_after, crowd_level, stop_name, latitude, longitude, recorded_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        active_trip["id"],
                        passenger_counts["student"],
                        passenger_counts["pwd"],
                        passenger_counts["senior"],
                        passenger_counts["regular"],
                        1,
                        0,
                        1,
                        total,
                        crowd_level,
                        origin_stop,
                        float(latitude) if latitude is not None else None,
                        float(longitude) if longitude is not None else None,
                        recorded_at,
                    ),
                )
                conn.execute(
                    """
                    UPDATE trip_transactions
                    SET occupancy_after = ?
                    WHERE trip_id = ? AND recorded_at = ? AND conductor_id = ? AND destination_stop = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (total, active_trip["id"], recorded_at, conductor_id, destination_stop),
                )
                log_event(
                    conn,
                    conductor_id,
                    "conductor",
                    "Ticket Mock Saved",
                    f"Trip #{active_trip['id']} boarded 1 {passenger_type} passenger from {origin_stop} to {destination_stop} for PHP {fare_amount:.0f}.",
                )

        elif action == "offboard_due" and active_trip:
            latest_gps = get_latest_trip_gps(conn, active_trip["id"])
            latest_record = get_latest_trip_record(conn, active_trip["id"])
            current_stop_details = get_trip_current_stop_details(
                active_trip,
                latest_gps,
                latest_record["stop_name"] if latest_record else None,
            )
            current_stop = current_stop_details["name"] if current_stop_details else None
            auto_offboard_due_passengers(
                conn,
                active_trip,
                current_stop,
                latest_gps["latitude"] if latest_gps and latest_gps["latitude"] is not None else None,
                latest_gps["longitude"] if latest_gps and latest_gps["longitude"] is not None else None,
                conductor_id,
            )

        elif action == "stop_monitoring" and active_trip:
            if active_trip["driver_id"]:
                conn.execute("UPDATE trips SET conductor_id = NULL WHERE id = ?", (active_trip["id"],))
                log_event(conn, conductor_id, "conductor", "Monitoring Detached", f"Conductor stopped monitoring trip #{active_trip['id']}.")
            else:
                conn.execute(
                    "UPDATE trips SET status = 'completed', ended_at = ?, duration_minutes = ? WHERE id = ?",
                    (
                        to_db_time(now()),
                        int(max((now() - from_db_time(active_trip["started_at"])).total_seconds(), 0) // 60),
                        active_trip["id"],
                    ),
                )
                log_event(conn, conductor_id, "conductor", "Manual Trip Ended", f"Manual monitoring trip #{active_trip['id']} was closed.")

        conn.commit()

    overview = build_conductor_overview(conn, conductor_id)
    conn.commit()
    conn.close()
    return render_template("conductor.html", overview=overview)


@app.route("/api/conductor/live")
@require_role("conductor")
def conductor_live():
    conn = get_db()
    conductor_id = session["user_id"]
    trip = get_active_trip_for_conductor(conn, conductor_id)

    if not trip:
        conn.close()
        return jsonify({"active": False})

    latest_gps = get_latest_trip_gps(conn, trip["id"])
    if not latest_gps:
        conn.close()
        return jsonify({"active": True, "tracking": False, "stop_name": "Waiting for driver location"})

    lat = float(latest_gps["latitude"])
    lng = float(latest_gps["longitude"])
    current_stop_details = get_trip_current_stop_details(trip, latest_gps, None)
    current_stop = current_stop_details["name"] if current_stop_details else derive_trip_location_label(trip, lat, lng)
    auto_offboard_due_passengers(conn, trip, current_stop, lat, lng, conductor_id)
    trip = get_active_trip_for_conductor(conn, conductor_id) or trip
    conn.commit()
    conn.close()
    return jsonify(
        {
            "active": True,
            "tracking": True,
            "stop_name": current_stop,
            "occupancy": int(trip.get("occupancy") or 0),
            "capacity": int(trip.get("capacity") or DEFAULT_BUS_CAPACITY),
            "latitude": lat,
            "longitude": lng,
            "recorded_at": normalize_json_value(latest_gps["recorded_at"]),
        }
    )


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("landing"))


if __name__ == "__main__":
    app.run(debug=True)
