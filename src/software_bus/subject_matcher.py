"""Subject matching, per docs/design/subject-matcher.md."""
from __future__ import annotations

from abc import ABC, abstractmethod


class SubjectMatcher(ABC):
    """Tests whether a given subject matches a target subject."""

    @abstractmethod
    def match(self, given_subject: str, target_subject: str) -> bool:
        """Return True if `given_subject` matches `target_subject`."""


class ExactStringMatcher(SubjectMatcher):
    """Matches only when the given subject is exactly the target subject."""

    def match(self, given_subject: str, target_subject: str) -> bool:
        return given_subject == target_subject


class StringPatternMatcher(SubjectMatcher):
    """Matches "."-separated subjects, treating a "*" sub-string in the
    target subject as a wildcard for the corresponding sub-string in the
    given subject."""

    def match(self, given_subject: str, target_subject: str) -> bool:
        given_parts = given_subject.split(".")
        target_parts = target_subject.split(".")
        if len(given_parts) != len(target_parts):
            return False
        return all(
            target_part == "*" or given_part == target_part
            for given_part, target_part in zip(given_parts, target_parts)
        )
