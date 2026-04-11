from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timedelta
from decimal import Decimal
from functools import wraps
from io import BytesIO

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from flask import Flask, Response, jsonify, redirect, render_template, request, session, url_for
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
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
            key=lambda stop: abs(latitude - stop[1]) + abs(longitude - stop[2]),
        )
        if abs(latitude - nearest_stop[1]) + abs(longitude - nearest_stop[2]) <= 0.08:
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
        pagesize=A4,
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
    route_table = Table(route_table_rows, colWidths=[2.8 * inch, 0.8 * inch, 1.0 * inch, 1.0 * inch])
    route_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#D60000")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#dbe2ea")),
                ("PADDING", (0, 0), (-1, -1), 7),
                ("BACKGROUND", (0, 1), (-1, -1), colors.whitesmoke),
            ]
        )
    )
    story.append(route_table)

    if overview["recent_logs"]:
        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph("Recent Logs", styles["Heading3"]))
        log_rows = [["Time", "Role", "Action", "Description"]]
        for log in overview["recent_logs"][:8]:
            log_rows.append([
                str(log["created_at"]),
                str(log["role"] or "system"),
                str(log["action"]),
                str(log["description"]),
            ])
        log_table = Table(log_rows, colWidths=[1.35 * inch, 0.75 * inch, 1.1 * inch, 3.1 * inch])
        log_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#fee2e2")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#7f1d1d")),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dbe2ea")),
                    ("PADDING", (0, 0), (-1, -1), 6),
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
        SELECT recorded_at, event_type, passenger_type, quantity, stop_name, occupancy_after
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
               r.route_name, r.start_point, r.end_point, r.coords_json
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
    lookup = {row[key_name]: row["total"] for row in rows}
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
                "direction": row["route_name"] or "Cabiao - Cabanatuan",
                "start": row["start_point"] or "Cabiao Terminal",
                "end": row["end_point"] or "Cabanatuan Central Terminal",
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


def build_admin_overview(conn):
    live_data = build_live_bus_data(conn)
    today = now().date().isoformat()
    yesterday = (now().date() - timedelta(days=1)).isoformat()

    totals_row = conn.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN DATE(recorded_at) = ? THEN total END), 0) AS today_total,
            COALESCE(SUM(CASE WHEN DATE(recorded_at) = ? THEN total END), 0) AS yesterday_total,
            COUNT(DISTINCT CASE WHEN DATE(recorded_at) = ? THEN trip_id END) AS trips_today,
            COUNT(CASE WHEN DATE(recorded_at) = ? THEN 1 END) AS records_today
        FROM trip_records
        """,
        (today, yesterday, today, today),
    ).fetchone()

    route_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT r.route_name,
                   COUNT(DISTINCT t.id) AS trip_count,
                   COALESCE(SUM(tr.total), 0) AS passengers,
                   COALESCE(ROUND(AVG(t.peak_occupancy * 100.0 / b.capacity), 1), 0) AS avg_load_percent
            FROM routes r
            LEFT JOIN trips t ON t.route_id = r.id
            LEFT JOIN buses b ON b.id = t.bus_id
            LEFT JOIN trip_records tr ON tr.trip_id = t.id
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
            SELECT DATE(recorded_at) AS day, COALESCE(SUM(total), 0) AS total
            FROM trip_records
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
            SELECT CONCAT(LPAD(HOUR(recorded_at), 2, '0'), ':00') AS hour_label, COALESCE(SUM(total), 0) AS total
            FROM trip_records
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
            COALESCE(SUM(students), 0) AS students,
            COALESCE(SUM(pwd), 0) AS pwd,
            COALESCE(SUM(senior), 0) AS senior,
            COALESCE(SUM(regular), 0) AS regular
        FROM trip_records
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

    peak_hour_label = hourly_labels[0]
    peak_hour_value = 0
    if hourly_values:
        peak_hour_value = max(hourly_values)
        peak_hour_label = hourly_labels[hourly_values.index(peak_hour_value)]

    overview = {
        "today_total": int(totals_row["today_total"] or 0),
        "yesterday_total": int(totals_row["yesterday_total"] or 0),
        "trips_today": int(totals_row["trips_today"] or 0),
        "records_today": int(totals_row["records_today"] or 0),
        "active_bus_count": live_data["active_bus_count"],
        "avg_crowd": live_data["avg_crowd"],
        "low_count": live_data["low_count"],
        "medium_count": live_data["medium_count"],
        "high_crowd_count": live_data["high_count"],
        "route_rows": route_rows,
        "live_bus_rows": live_bus_rows,
        "fleet_rows": fleet_rows,
        "recent_logs": recent_logs,
        "peak_hour_label": peak_hour_label,
        "peak_hour_value": peak_hour_value,
        "charts": {
            "daily_labels": daily_labels,
            "daily_values": daily_values,
            "hourly_labels": hourly_labels,
            "hourly_values": hourly_values,
            "route_labels": [row["route_name"] for row in route_rows],
            "route_values": [row["passengers"] for row in route_rows],
            "mix_labels": ["Students", "PWD", "Senior", "Regular"],
            "mix_values": [int(type_row["students"]), int(type_row["pwd"]), int(type_row["senior"]), int(type_row["regular"])],
            "live_buses": live_data["buses"],
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

    routes = [dict(row) for row in conn.execute("SELECT * FROM routes ORDER BY route_name").fetchall()]

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

        trip_metrics = {
            "occupancy": active_trip["occupancy"],
            "capacity": active_trip["capacity"],
            "next_stop": latest_record["stop_name"] if latest_record else active_trip["end_point"],
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
    routes = [dict(row) for row in conn.execute("SELECT * FROM routes ORDER BY route_name").fetchall()]

    transaction_form = {
        "students": 0,
        "pwd": 0,
        "senior": 0,
        "regular": 0,
        "dropped": 0,
        "stop_name": "",
    }
    trip_summary = None
    latest_gps = None
    recent_transactions = []
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
        if latest_record:
            trip_summary = latest_record

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

    return {
        "active_trip": active_trip,
        "available_trips": available_trips,
        "buses": buses,
        "routes": routes,
        "transaction_form": transaction_form,
        "trip_summary": trip_summary,
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
    return [
        (
            "Cabiao - Cabanatuan",
            "Cabiao Terminal",
            "Cabanatuan Central Terminal",
            27.5,
            55,
            json.dumps([
                [15.2484, 120.8542],
                [15.2530, 120.8597],
                [15.2608, 120.8685],
                [15.2682, 120.8761],
                [15.2769, 120.8848],
                [15.2861, 120.8923],
                [15.2977, 120.9007],
                [15.3110, 120.9108],
                [15.3250, 120.9199],
                [15.3385, 120.9272],
                [15.3520, 120.9350],
                [15.3680, 120.9431],
                [15.3897, 120.9504],
                [15.4145, 120.9570],
                [15.4472, 120.9628],
                [15.4865, 120.9667],
            ]),
        ),
        (
            "Gapan - Cabanatuan",
            "Gapan Transport Hub",
            "Cabanatuan Central Terminal",
            22.0,
            46,
            json.dumps([
                [15.3079, 120.9460],
                [15.3154, 120.9467],
                [15.3241, 120.9478],
                [15.3349, 120.9494],
                [15.3487, 120.9511],
                [15.3605, 120.9542],
                [15.3730, 120.9555],
                [15.3878, 120.9571],
                [15.4045, 120.9582],
                [15.4200, 120.9605],
                [15.4389, 120.9620],
                [15.4582, 120.9642],
                [15.4865, 120.9667],
            ]),
        ),
        (
            "San Isidro - Cabanatuan",
            "San Isidro Market",
            "Cabanatuan Central Terminal",
            18.4,
            38,
            json.dumps([
                [15.3295, 120.9392],
                [15.3368, 120.9409],
                [15.3457, 120.9426],
                [15.3563, 120.9451],
                [15.3651, 120.9478],
                [15.3770, 120.9502],
                [15.3924, 120.9527],
                [15.4078, 120.9550],
                [15.4210, 120.9573],
                [15.4380, 120.9602],
                [15.4560, 120.9632],
                [15.4865, 120.9667],
            ]),
        ),
    ]


def sync_default_routes(conn):
    for route_name, start_point, end_point, distance_km, expected_duration_minutes, coords_json in get_default_routes():
        existing = conn.execute(
            "SELECT id FROM routes WHERE route_name = ?",
            (route_name,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE routes
                SET start_point = ?, end_point = ?, distance_km = ?, expected_duration_minutes = ?, coords_json = ?
                WHERE id = ?
                """,
                (start_point, end_point, distance_km, expected_duration_minutes, coords_json, existing["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO routes (route_name, start_point, end_point, distance_km, expected_duration_minutes, coords_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (route_name, start_point, end_point, distance_km, expected_duration_minutes, coords_json),
            )


def seed_demo_data():
    conn = get_db()
    sync_default_routes(conn)
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

    conn.commit()
    conn.close()


bootstrap_db()
seed_demo_data()


@app.route("/")
def landing():
    conn = get_db()
    live_data = build_live_bus_data(conn)
    primary_route = conn.execute(
        """
        SELECT route_name, start_point, end_point, distance_km, expected_duration_minutes
        FROM routes
        ORDER BY id
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    preview_buses = [bus for bus in live_data["buses"] if bus["status"] == "online"][:3]
    return render_template(
        "landing/index.html",
        active_bus_count=live_data["active_bus_count"],
        avg_crowd=live_data["avg_crowd"],
        low_count=live_data["low_count"],
        medium_count=live_data["medium_count"],
        high_count=live_data["high_count"],
        preview_buses=preview_buses,
        primary_route=dict(primary_route) if primary_route else None,
    )


@app.route("/track")
def tracker():
    conn = get_db()
    live_data = build_live_bus_data(conn)
    conn.close()
    return render_template(
        "landing/tracker.html",
        buses_json=json.dumps(live_data["buses"]),
        active_bus_count=live_data["active_bus_count"],
        avg_crowd=live_data["avg_crowd"],
        low_count=live_data["low_count"],
        medium_count=live_data["medium_count"],
        high_count=live_data["high_count"],
    )


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
    active_tab = request.args.get("tab", "overview")

    if request.method == "POST":
        action = request.form.get("action")
        if action == "update_bus_status":
            bus_id_raw = request.form.get("bus_id", "").strip()
            next_status = request.form.get("status", "").strip().lower()
            redirect_tab = request.form.get("redirect_tab", "overview").strip() or "overview"

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


@app.route("/start_trip", methods=["POST"])
@require_role("driver")
def start_trip():
    conn = get_db()
    active_trip = get_active_trip_for_driver(conn, session["user_id"])
    if active_trip:
        conn.close()
        return jsonify({"error": "Driver already has an active trip."}), 400

    bus_id = int(request.form.get("bus_id", 0))
    route_id = int(request.form.get("route_id", 0))

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
            "Started from driver dashboard",
        ),
    )
    log_event(conn, session["user_id"], "driver", "Trip Started", f"Driver started {bus_row['plate_number']} on {route_row['route_name']}.")
    conn.commit()
    trip_id = cursor.lastrowid
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
            current_occupancy = int(active_trip["occupancy"] or 0)
            students = to_non_negative_int(request.form.get("students", 0), 0)
            pwd = to_non_negative_int(request.form.get("pwd", 0), 0)
            senior = to_non_negative_int(request.form.get("senior", 0), 0)
            regular = to_non_negative_int(request.form.get("regular", 0), 0)
            requested_boarded = students + pwd + senior + regular
            dropped = min(to_non_negative_int(request.form.get("dropped", 0), 0), current_occupancy + requested_boarded)
            max_boarded = max((active_trip["capacity"] or DEFAULT_BUS_CAPACITY) - current_occupancy + dropped, 0)
            accepted_boarded = {}
            remaining_boarded = max_boarded
            for passenger_type, quantity in (
                ("student", students),
                ("pwd", pwd),
                ("senior", senior),
                ("regular", regular),
            ):
                accepted_quantity = min(quantity, remaining_boarded)
                accepted_boarded[passenger_type] = accepted_quantity
                remaining_boarded -= accepted_quantity

            students = accepted_boarded["student"]
            pwd = accepted_boarded["pwd"]
            senior = accepted_boarded["senior"]
            regular = accepted_boarded["regular"]
            boarded_total = students + pwd + senior + regular

            latest_gps = get_latest_trip_gps(conn, active_trip["id"])
            latitude = latest_gps["latitude"] if latest_gps and latest_gps["latitude"] is not None else None
            longitude = latest_gps["longitude"] if latest_gps and latest_gps["longitude"] is not None else None
            resolved_stop_name = request.form.get("resolved_stop_name", "").strip()
            stop_name = resolved_stop_name or (
                derive_trip_location_label(active_trip, float(latitude), float(longitude))
                if latitude is not None and longitude is not None
                else "Waiting for driver location"
            )

            total = min(max(current_occupancy + boarded_total - dropped, 0), active_trip["capacity"])
            crowd_level = classify_capacity(total, active_trip["capacity"])
            peak = max(active_trip["peak_occupancy"] or 0, total)

            recorded_at = to_db_time(now())
            transaction_rows = []
            for passenger_type, quantity in (
                ("student", students),
                ("pwd", pwd),
                ("senior", senior),
                ("regular", regular),
            ):
                if quantity > 0:
                    transaction_rows.append(
                        (
                            active_trip["id"],
                            conductor_id,
                            "board",
                            passenger_type,
                            quantity,
                            None,
                            stop_name,
                            float(latitude) if latitude is not None else None,
                            float(longitude) if longitude is not None else None,
                            total,
                            recorded_at,
                        )
                    )
            if dropped > 0:
                transaction_rows.append(
                    (
                        active_trip["id"],
                        conductor_id,
                        "drop",
                        "mixed",
                        dropped,
                        None,
                        stop_name,
                        float(latitude) if latitude is not None else None,
                        float(longitude) if longitude is not None else None,
                        total,
                        recorded_at,
                    )
                )
            if transaction_rows:
                conn.executemany(
                    """
                    INSERT INTO trip_transactions (
                        trip_id, conductor_id, event_type, passenger_type, quantity, fare_amount,
                        stop_name, latitude, longitude, occupancy_after, recorded_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    transaction_rows,
                )

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
                    students,
                    pwd,
                    senior,
                    regular,
                    boarded_total,
                    dropped,
                    boarded_total,
                    total,
                    crowd_level,
                    stop_name,
                    float(latitude) if latitude is not None else None,
                    float(longitude) if longitude is not None else None,
                    recorded_at,
                ),
            )
            conn.execute(
                """
                UPDATE trips
                SET occupancy = ?, peak_occupancy = ?, average_load = ?
                WHERE id = ?
                """,
                (total, peak, round((total / max(active_trip["capacity"], 1)) * 100, 1), active_trip["id"]),
            )
            log_event(
                conn,
                conductor_id,
                "conductor",
                "Transaction Recorded",
                f"Trip #{active_trip['id']} updated at {stop_name}: boarded {boarded_total}, dropped {dropped}, occupancy is now {total}.",
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
    conn.close()
    if not latest_gps:
        return jsonify({"active": True, "tracking": False, "stop_name": "Waiting for driver location"})

    lat = float(latest_gps["latitude"])
    lng = float(latest_gps["longitude"])
    return jsonify(
        {
            "active": True,
            "tracking": True,
            "stop_name": derive_trip_location_label(trip, lat, lng),
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
