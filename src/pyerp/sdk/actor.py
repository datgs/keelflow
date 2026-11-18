"""Actor model — who is performing an action."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ActorType(str, Enum):
    """Kind of actor performing an action."""

    USER = "user"        # staff member via SSO (id = internal user_id)
    SERVICE = "service"  # integration / API key (id = identity id)
    SYSTEM = "system"    # engine internals: scheduler, reaper, resolver
    JOB = "job"          # action inside a job run; via = trigger actor


@dataclass(frozen=True)
class Actor:
    """Who is performing an action.

    `via` preserves accountability: a job's actor is {type=JOB, id=job_id,
    via=<original user/service>} so audit chains answer "which human
    triggered this system-performed change".
    """

    type: ActorType
    id: str
    via: "Actor | None" = None

    @classmethod
    def user(cls, user_id: str) -> "Actor":
        return cls(ActorType.USER, user_id)

    @classmethod
    def service(cls, identity_id: str) -> "Actor":
        return cls(ActorType.SERVICE, identity_id)

    @classmethod
    def system(cls, name: str = "engine") -> "Actor":
        return cls(ActorType.SYSTEM, name)

    @classmethod
    def job(cls, job_id: str, via: "Actor") -> "Actor":
        return cls(ActorType.JOB, job_id, via)
