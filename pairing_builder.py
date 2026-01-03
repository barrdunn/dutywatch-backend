"""
Pairing Builder - Reconstructs complete pairings from calendar events.

A pairing:
- Has a pairing ID (e.g., C3075F, W1234, D5678)
- Starts and ends at its base airport
- Base is determined by prefix: W=DFW, C=ORD/MDW, D=DEN, etc.

Algorithm:
1. Sort all events chronologically
2. Parse each event to extract legs
3. For each event, determine if it's:
   - START: First leg departs from base
   - MIDDLE: Neither starts nor ends at base
   - END: Last leg arrives at base
4. Group consecutive events with same pairing ID into complete pairings
5. Validate: each complete pairing should start AND end at base
"""

from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from parser import parse_pairing_days, is_valid_pairing_id, extract_pairing_id

logger = logging.getLogger("pairing_builder")

# Pairing prefix -> base airport(s)
PREFIX_TO_BASE: Dict[str, List[str]] = {
    'W': ['DFW'],
    'A': ['ATL'],
    'C': ['ORD', 'MDW'],
    'O': ['CLE'],
    'K': ['CVG'],
    'D': ['DEN'],
    'L': ['LAS'],
    'M': ['MCO'],
    'F': ['MIA'],
    'P': ['PHL'],
    'X': ['PHX'],
    'S': ['SJU'],
    'B': ['TPA'],
}


def get_base_airports(pairing_id: str) -> List[str]:
    """Get valid base airports for a pairing ID based on its prefix."""
    if not pairing_id:
        return []
    prefix = pairing_id[0].upper()
    return PREFIX_TO_BASE.get(prefix, [])


@dataclass
class ParsedEvent:
    """A calendar event with parsed leg information."""
    uid: str
    summary: str  # Raw summary from calendar
    pairing_id: str  # Extracted pairing ID (e.g., "W1234")
    start_utc: str
    end_utc: str
    description: str
    location: Optional[str]
    
    # Whether this is a valid pairing (vs CBT, VAC, etc.)
    is_pairing: bool = False
    
    # Parsed from description
    legs: List[Dict[str, Any]] = field(default_factory=list)
    report_time: Optional[str] = None  # HHMM
    report_date: Optional[str] = None  # e.g., "15NOV"
    release_time: Optional[str] = None  # HHMM
    hotel: Optional[str] = None
    
    # Classification
    first_departure: Optional[str] = None  # Airport code
    last_arrival: Optional[str] = None  # Airport code
    
    def __post_init__(self):
        """Parse the description to extract legs and times."""
        # Check if this is a valid pairing ID
        self.is_pairing = is_valid_pairing_id(self.pairing_id)
        
        if self.description:
            parsed = parse_pairing_days(self.description, self.location)
            days = parsed.get("days", [])
            if days:
                day = days[0]  # Each event should have one day
                self.legs = day.get("legs", [])
                self.report_time = day.get("report")
                self.report_date = day.get("report_date")
                self.release_time = day.get("release")
                self.hotel = day.get("hotel")
        
        # Extract first departure and last arrival
        if self.legs:
            self.first_departure = self.legs[0].get("dep", "").upper()
            self.last_arrival = self.legs[-1].get("arr", "").upper()


@dataclass 
class Pairing:
    """A complete pairing built from one or more calendar events."""
    pairing_id: str
    base_airports: List[str]
    events: List[ParsedEvent] = field(default_factory=list)
    
    # Whether this is a real pairing vs other event type
    is_pairing: bool = True
    
    # Computed fields
    is_complete: bool = False
    starts_at_base: bool = False
    ends_at_base: bool = False
    
    @property
    def first_event(self) -> Optional[ParsedEvent]:
        return self.events[0] if self.events else None
    
    @property
    def last_event(self) -> Optional[ParsedEvent]:
        return self.events[-1] if self.events else None
    
    @property
    def all_legs(self) -> List[Dict[str, Any]]:
        """All legs across all days of this pairing."""
        legs = []
        for event in self.events:
            legs.extend(event.legs)
        return legs
    
    @property
    def num_days(self) -> int:
        return len(self.events)
    
    @property
    def first_departure(self) -> Optional[str]:
        if self.events and self.events[0].legs:
            return self.events[0].first_departure
        return None
    
    @property
    def last_arrival(self) -> Optional[str]:
        if self.events and self.events[-1].legs:
            return self.events[-1].last_arrival
        return None
    
    def validate(self) -> None:
        """Check if pairing starts and ends at base."""
        self.starts_at_base = self.first_departure in self.base_airports if self.first_departure else False
        self.ends_at_base = self.last_arrival in self.base_airports if self.last_arrival else False
        self.is_complete = self.starts_at_base and self.ends_at_base


def parse_events(raw_events: List[Dict[str, Any]]) -> List[ParsedEvent]:
    """Convert raw calendar events to ParsedEvent objects."""
    parsed = []
    for ev in raw_events:
        summary = ev.get("summary", "").strip()
        pairing_id = extract_pairing_id(summary)
        
        parsed_event = ParsedEvent(
            uid=ev.get("uid", ""),
            summary=summary,
            pairing_id=pairing_id,
            start_utc=ev.get("start_utc", ""),
            end_utc=ev.get("end_utc", ""),
            description=ev.get("description", ""),
            location=ev.get("location"),
        )
        parsed.append(parsed_event)
    
    return parsed


def classify_event(event: ParsedEvent, base_airports: List[str]) -> str:
    """
    Classify an event's position within a pairing.
    
    Returns:
        'start' - First leg departs from base
        'end' - Last leg arrives at base
        'single_day' - Both starts and ends at base (complete in one day)
        'middle' - Neither starts nor ends at base
        'no_legs' - Event has no legs (non-flying event)
    """
    if not event.legs:
        return 'no_legs'
    
    starts_at_base = event.first_departure in base_airports
    ends_at_base = event.last_arrival in base_airports
    
    if starts_at_base and ends_at_base:
        return 'single_day'
    elif starts_at_base:
        return 'start'
    elif ends_at_base:
        return 'end'
    else:
        return 'middle'


def build_pairings(raw_events: List[Dict[str, Any]]) -> List[Pairing]:
    """
    Build complete pairings from calendar events.
    
    Algorithm:
    1. Parse all events
    2. Group events by pairing ID first
    3. For each pairing ID group, sort chronologically and build pairing
    4. Combine all pairings and sort by start time
    """
    # Parse all events
    parsed_events = parse_events(raw_events)
    
    logger.info(f"Building pairings from {len(parsed_events)} events")
    
    # Separate pairing events from non-pairing events
    pairing_events = [e for e in parsed_events if e.is_pairing]
    other_events = [e for e in parsed_events if not e.is_pairing]
    
    logger.info(f"  - {len(pairing_events)} pairing events")
    logger.info(f"  - {len(other_events)} non-pairing events (CBT, VAC, etc.)")
    
    # Debug: log C3075F events specifically
    c3075f_events = [e for e in pairing_events if e.pairing_id == "C3075F"]
    if c3075f_events:
        logger.info(f"  DEBUG: Found {len(c3075f_events)} C3075F events:")
        for e in c3075f_events:
            logger.info(f"    - start_utc={e.start_utc} dep={e.first_departure} arr={e.last_arrival}")
    
    # Group events by pairing ID
    events_by_pid: Dict[str, List[ParsedEvent]] = {}
    for event in pairing_events:
        pid = event.pairing_id
        if pid not in events_by_pid:
            events_by_pid[pid] = []
        events_by_pid[pid].append(event)
    
    pairings: List[Pairing] = []
    
    # Process each pairing ID group
    for pid, events in events_by_pid.items():
        # Sort events for this pairing by start time
        events.sort(key=lambda e: e.start_utc)
        
        base_airports = get_base_airports(pid)
        
        # Build pairings from this group
        # Same pairing ID appearing after a return to base = new instance
        current_pairing: Optional[Pairing] = None
        
        for event in events:
            event_type = classify_event(event, base_airports)
            
            logger.debug(
                f"Event {event.pairing_id}: {event.first_departure}->{event.last_arrival} "
                f"type={event_type} base={base_airports}"
            )
            
            if event_type == 'no_legs':
                # No legs - standalone event
                pairing = Pairing(
                    pairing_id=pid,
                    base_airports=base_airports,
                    events=[event],
                    is_pairing=True,
                )
                pairings.append(pairing)
                continue
            
            if current_pairing is None:
                # Start new pairing
                current_pairing = Pairing(
                    pairing_id=pid,
                    base_airports=base_airports,
                    events=[event],
                    is_pairing=True,
                )
            else:
                # Add to current pairing
                current_pairing.events.append(event)
            
            # Check if pairing is complete (returned to base)
            if event_type in ('end', 'single_day'):
                current_pairing.validate()
                pairings.append(current_pairing)
                current_pairing = None
        
        # Handle unclosed pairing
        if current_pairing is not None:
            current_pairing.validate()
            pairings.append(current_pairing)
    
    # Add non-pairing events as separate entries
    for event in other_events:
        pairing = Pairing(
            pairing_id=event.pairing_id,
            base_airports=[],
            events=[event],
            is_pairing=False,
        )
        pairings.append(pairing)
    
    # Sort all pairings by first event's start time
    pairings.sort(key=lambda p: p.events[0].start_utc if p.events else "")
    
    logger.info(f"Built {len(pairings)} total entries ({len([p for p in pairings if p.is_pairing])} pairings, {len([p for p in pairings if not p.is_pairing])} other)")
    
    # Log validation warnings
    for p in pairings:
        if p.is_pairing and not p.is_complete and p.all_legs:
            logger.warning(
                f"Pairing {p.pairing_id} incomplete: "
                f"starts_at_base={p.starts_at_base} ({p.first_departure}), "
                f"ends_at_base={p.ends_at_base} ({p.last_arrival})"
            )
    
    return pairings


def pairings_to_rows_input(pairings: List[Pairing]) -> List[Dict[str, Any]]:
    """
    Convert Pairing objects back to a format that rows.py can use.
    
    This bridges the new pairing builder with the existing row builder.
    Each pairing becomes a single "grouped" event with all its days.
    """
    result = []
    
    for pairing in pairings:
        if not pairing.events:
            continue
        
        # Use first event as the base, but include all days
        first_ev = pairing.first_event
        last_ev = pairing.last_event
        
        result.append({
            "uid": first_ev.uid if first_ev else "",
            "summary": pairing.pairing_id,
            "pairing_id": pairing.pairing_id,
            "start_utc": first_ev.start_utc if first_ev else "",
            "end_utc": last_ev.end_utc if last_ev else "",
            "base_airports": pairing.base_airports,
            "is_complete": pairing.is_complete,
            "is_pairing": pairing.is_pairing,  # True for real pairings, False for CBT/VAC/etc.
            "num_days": pairing.num_days,
            "events": [
                {
                    "uid": ev.uid,
                    "start_utc": ev.start_utc,
                    "end_utc": ev.end_utc,
                    "legs": ev.legs,
                    "report_time": ev.report_time,
                    "report_date": ev.report_date,
                    "release_time": ev.release_time,
                    "hotel": ev.hotel,
                    "first_departure": ev.first_departure,
                    "last_arrival": ev.last_arrival,
                }
                for ev in pairing.events
            ],
            # For compatibility with existing code
            "description": first_ev.description if first_ev else "",
            "location": first_ev.location if first_ev else None,
        })
    
    return result