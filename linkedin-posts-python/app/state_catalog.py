from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class State:
    code: str
    name: str


ALL_STATES: list[State] = [
    State("AL", "Alabama"),
    State("AK", "Alaska"),
    State("AZ", "Arizona"),
    State("AR", "Arkansas"),
    State("CA", "California"),
    State("CO", "Colorado"),
    State("CT", "Connecticut"),
    State("DE", "Delaware"),
    State("DC", "District of Columbia"),
    State("FL", "Florida"),
    State("GA", "Georgia"),
    State("HI", "Hawaii"),
    State("ID", "Idaho"),
    State("IL", "Illinois"),
    State("IN", "Indiana"),
    State("IA", "Iowa"),
    State("KS", "Kansas"),
    State("KY", "Kentucky"),
    State("LA", "Louisiana"),
    State("ME", "Maine"),
    State("MD", "Maryland"),
    State("MA", "Massachusetts"),
    State("MI", "Michigan"),
    State("MN", "Minnesota"),
    State("MS", "Mississippi"),
    State("MO", "Missouri"),
    State("MT", "Montana"),
    State("NE", "Nebraska"),
    State("NV", "Nevada"),
    State("NH", "New Hampshire"),
    State("NJ", "New Jersey"),
    State("NM", "New Mexico"),
    State("NY", "New York"),
    State("NC", "North Carolina"),
    State("ND", "North Dakota"),
    State("OH", "Ohio"),
    State("OK", "Oklahoma"),
    State("OR", "Oregon"),
    State("PA", "Pennsylvania"),
    State("RI", "Rhode Island"),
    State("SC", "South Carolina"),
    State("SD", "South Dakota"),
    State("TN", "Tennessee"),
    State("TX", "Texas"),
    State("UT", "Utah"),
    State("VT", "Vermont"),
    State("VA", "Virginia"),
    State("WA", "Washington"),
    State("WV", "West Virginia"),
    State("WI", "Wisconsin"),
    State("WY", "Wyoming"),
]


STATE_BY_CODE = {state.code: state for state in ALL_STATES}
STATE_BY_NAME = {state.name.lower(): state for state in ALL_STATES}


def resolve_enabled_states(state_scope: str, enabled_state_codes: list[str] | None) -> list[State]:
    if state_scope == "all" or not enabled_state_codes:
        return list(ALL_STATES)

    resolved: list[State] = []
    seen: set[str] = set()
    for code in enabled_state_codes:
        state = STATE_BY_CODE.get(code.upper())
        if state and state.code not in seen:
            resolved.append(state)
            seen.add(state.code)
    return resolved or list(ALL_STATES)


def build_state_query_variants(keywords: str, state: State) -> list[str]:
    base = " ".join(keywords.split())
    return [
        f'{base} "{state.code}"',
        f"{base} {state.name}",
    ]
