import pytest

from software_bus.subject_matcher import ExactStringMatcher, StringPatternMatcher


@pytest.mark.parametrize(
    "given_subject,target_subject,expected",
    [
        ("a.b", "a.b", True),
        ("a.b", "a.c", False),
        ("a.b", "a.b.c", False),
        ("", "", True),
    ],
)
def test_exact_string_matcher(given_subject, target_subject, expected):
    assert ExactStringMatcher().match(given_subject, target_subject) is expected


@pytest.mark.parametrize(
    "given_subject,target_subject,expected",
    [
        ("a.b", "a.b", True),
        ("a.b", "a.*", True),
        ("a.b", "*.b", True),
        ("a.b", "*.*", True),
        ("a.b.c", "a.*.c", True),
        ("a.b", "a.c", False),
        ("a.b", "a.b.c", False),
        ("a.b.c", "a.*", False),
        ("*", "*", True),
        ("", "*", True),
        ("", "", True),
        # a literal "*" on the given (published) side is not itself a
        # wildcard: only a "*" on the target (subscribed) side is.
        ("a.*", "a.b", False),
        ("a.*", "a.*", True),
        # case is significant.
        ("A.b", "a.b", False),
        ("a.b", "A.*", False),
        # only a whole "*" sub-string is a wildcard: "a*" is literal.
        ("ab", "a*", False),
        ("a*", "a*", True),
        # empty sub-strings count like any other.
        ("a..b", "a.*.b", True),
        ("a..b", "a..b", True),
        ("a.b", "a..b", False),
        ("a.", "a.*", True),
        ("a", "a.*", False),
    ],
)
def test_string_pattern_matcher(given_subject, target_subject, expected):
    assert StringPatternMatcher().match(given_subject, target_subject) is expected
