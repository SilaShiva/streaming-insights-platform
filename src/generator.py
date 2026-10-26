"""Synthetic order/delivery/review event generator.

Produces realistic food-delivery events with a latent ground-truth delay
reason. The DGP runs the label through observables (prep time, rider supply,
traffic index, weather) so enrichment models have real signal to learn from
-- the lesson learned building the advertiser-prospecting generator.
"""
from __future__ import annotations

import json
import random
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

DELAY_REASONS = (
    "restaurant_prep_delay",
    "rider_shortage",
    "traffic",
    "weather",
    "address_issue",
    "restaurant_backlog",
    "unknown",
)

_REVIEW_TEMPLATES = {
    "restaurant_prep_delay": [
        "Food arrived cold, kitchen clearly took forever to prepare it.",
        "Waited ages at the restaurant itself. Prep was slow today.",
    ],
    "rider_shortage": [
        "No rider was assigned for 20 minutes. Order just sat there.",
        "Rider allocation was delayed, food arrived late.",
    ],
    "traffic": [
        "Rider was stuck in traffic, tracking showed the vehicle crawling.",
        "Heavy traffic on the route delayed the delivery.",
    ],
    "weather": [
        "It was raining heavily, delivery was understandably late.",
        "Bad weather slowed everything down.",
    ],
    "address_issue": [
        "Rider could not find my address, had to call multiple times.",
        "Wrong landmark on the app confused the delivery partner.",
    ],
    "restaurant_backlog": [
        "Restaurant was swamped with orders, mine was queued behind many.",
        "Place had a rush, order backlog delayed the handover.",
    ],
    "unknown": [
        "Delivery was late, no idea what happened.",
        "Order took longer than promised.",
    ],
}

# Weighted ground-truth distribution of delay reasons
_REASON_WEIGHTS = {
    "restaurant_prep_delay": 0.26,
    "rider_shortage": 0.18,
    "traffic": 0.20,
    "weather": 0.08,
    "address_issue": 0.09,
    "restaurant_backlog": 0.11,
    "unknown": 0.08,
}


@dataclass
class Event:
    event_id: str
    event_type: str          # order_placed | order_delivered | review_posted
    ts: str                  # ISO-8601 UTC
    order_id: str
    city: str
    restaurant_id: str
    customer_id: str
    promised_minutes: int
    actual_minutes: int | None = None
    delay_minutes: int | None = None
    delay_reason: str | None = None      # ground truth (generator-only)
    review_text: str | None = None
    rating: int | None = None
    prep_minutes: int = field(default=0)
    riders_online: int = field(default=20)
    traffic_index: float = field(default=1.0)
    weather_bad: int = field(default=0)

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))


class EventGenerator:
    def __init__(self, cfg: dict, seed: int | None = None):
        self.cfg = cfg
        gen_cfg = cfg["generator"]
        self.seed = seed if seed is not None else cfg["project"]["seed"]
        self.rng = random.Random(self.seed)
        self.cities = gen_cfg["churn_cities"]
        self.restaurants_per_city = gen_cfg["restaurants_per_city"]
        self.review_rate = gen_cfg["review_rate"]
        self.burst_probability = gen_cfg["burst_probability"]
        self.burst_multiplier = gen_cfg["burst_multiplier"]

    def _latent_reason(self) -> str:
        r = self.rng.random()
        acc = 0.0
        for reason, w in _REASON_WEIGHTS.items():
            acc += w
            if r < acc:
                return reason
        return "unknown"

    def _city_conditions(self) -> tuple[int, float, int]:
        """Per-event conditions correlated with the latent reason."""
        reason_bias = {
            "rider_shortage": (6, 1.0, 0),      # fewer riders online
            "traffic": (20, 2.2, 0),            # heavy traffic
            "weather": (18, 1.8, 1),            # bad weather
            "restaurant_prep_delay": (24, 1.0, 0),  # slow kitchen
            "restaurant_backlog": (26, 1.1, 0),
            "address_issue": (14, 1.0, 0),
            "unknown": (17, 1.2, 0),
        }
        base_prep, base_traffic, base_weather = reason_bias[self._latent_reason()]
        prep = max(5, int(self.rng.gauss(base_prep, 5)))
        riders = max(2, int(self.rng.gauss(22 - (base_prep == 6) * 14, 5)))
        traffic = round(max(0.6, self.rng.gauss(base_traffic, 0.3)), 2)
        weather = 1 if (base_weather or self.rng.random() < 0.08) else 0
        return prep, riders, traffic, weather

    def make_event(self, now: datetime | None = None) -> Event:
        now = now or datetime.now(timezone.utc)
        order_id = uuid.uuid4().hex[:12]
        city = self.rng.choice(self.cities)
        restaurant_id = f"{city[:3].lower()}-r{self.rng.randint(1, self.restaurants_per_city):03d}"
        customer_id = f"c{self.rng.randint(10000, 99999)}"

        promised = self.rng.choice([25, 30, 35, 40, 45])
        prep, riders, traffic, weather = self._city_conditions()

        # Actual delivery time: promise + slack driven by conditions
        slack = (prep - 15) * 0.4 + (traffic - 1.0) * 8 + weather * 6
        slack += max(0, 18 - riders) * 0.8
        actual = max(promised - 6, int(round(promised + max(0, slack) + self.rng.gauss(0, 4))))
        delay = max(0, actual - promised)

        burst = self.rng.random() < self.burst_probability
        n_reviews = self.rng.randint(1, 3) if burst else (1 if self.rng.random() < self.review_rate else 0)

        events: list[Event] = []

        def base_event(etype: str, ts: datetime, **kw) -> Event:
            return Event(
                event_id=uuid.uuid4().hex,
                event_type=etype,
                ts=ts.isoformat(),
                order_id=order_id,
                city=city,
                restaurant_id=restaurant_id,
                customer_id=customer_id,
                promised_minutes=promised,
                prep_minutes=prep,
                riders_online=riders,
                traffic_index=traffic,
                weather_bad=weather,
                **kw,
            )

        events.append(base_event("order_placed", now))

        delivered_ts = now + timedelta(minutes=actual)
        events.append(base_event(
            "order_delivered", delivered_ts,
            actual_minutes=actual, delay_minutes=delay,
            delay_reason=None,  # ground truth stays generator-side; pipeline must infer it
        ))

        for _ in range(n_reviews):
            if delay < 5:
                text = self.rng.choice([
                    "Order arrived on time, food was hot and well packed.",
                    "Great experience, delivery was quick.",
                    "Good service as always.",
                ])
                rating = self.rng.choice([4, 5, 5])
            else:
                text = self.rng.choice(_REVIEW_TEMPLATES[self._latent_reason() if self.rng.random() < 0.75 else "unknown"])
                rating = self.rng.choice([1, 1, 2, 2, 3])
            events.append(base_event(
                "review_posted", delivered_ts + timedelta(minutes=self.rng.randint(2, 90)),
                review_text=text, rating=rating,
            ))

        return events[0] if len(events) == 1 else events

    def stream_batch(self, n_orders: int) -> list[str]:
        """Generate JSON lines for n_orders (each order may emit 1-4 events)."""
        lines: list[str] = []
        t = datetime.now(timezone.utc)
        for _ in range(n_orders):
            out = self.make_event(now=t)
            evs = out if isinstance(out, list) else [out]
            for ev in evs:
                lines.append(ev.to_json())
        return lines
