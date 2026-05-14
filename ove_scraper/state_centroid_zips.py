"""Last-resort ZIP fallback: a single representative ZIP per US state.

Used only when override JSON, alias-driven pgeocode, AND the AI web-search
resolver have all failed. The radius filter on the VPS just needs a usable
location signal — coarse but non-NULL beats NULL.
"""
from __future__ import annotations

STATE_CENTROID_ZIP: dict[str, str] = {
    "AL": "36703", "AK": "99709", "AZ": "85323", "AR": "72211",
    "CA": "93657", "CO": "80132", "CT": "06457", "DE": "19720",
    "DC": "20500", "FL": "32725", "GA": "31021", "HI": "96720",
    "ID": "83278", "IL": "61711", "IN": "46140", "IA": "50112",
    "KS": "67432", "KY": "40359", "LA": "71316", "ME": "04937",
    "MD": "21054", "MA": "01803", "MI": "48642", "MN": "56347",
    "MS": "39096", "MO": "65063", "MT": "59401", "NE": "68959",
    "NV": "89412", "NH": "03287", "NJ": "08560", "NM": "87060",
    "NY": "13088", "NC": "27330", "ND": "58524", "OH": "43018",
    "OK": "73018", "OR": "97045", "PA": "17097", "RI": "02919",
    "SC": "29010", "SD": "57501", "TN": "37098", "TX": "76528",
    "UT": "84602", "VT": "05751", "VA": "24115", "WA": "98926",
    "WV": "26452", "WI": "54409", "WY": "82637",
}


def state_centroid_zip(state_code: str | None) -> str | None:
    if not state_code:
        return None
    return STATE_CENTROID_ZIP.get(state_code.upper())
