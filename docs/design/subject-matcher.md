# Subject Matching

## Subject Matcher Interface

- **Match**
    - Test if a given subject matches a target subject.
    - Return true if matches. Return false if not matches.

## Exact String Matcher Class

- Implements the Subject Matcher Interface.
- **Match**
    - When the given subject is exactly the same as the target subject, the result is a match.

## String Pattern Matcher Class

- Implements the Subject Matcher Interface.
- **Match**
    - The given subject matches the target subject if:
        - There are the same number of sub-strings separated by "." in both given subject and the target subject.
        - Each sub-string in the given subject is either:
            - exactly the same as the corresponding sub-string in the target subject, or
            - the corresponding sub-string in the target subject is a wild card "*".
    - A subject is split at every "."; the sub-strings between them, and before the first and after the last, can be empty. So the empty subject has one empty sub-string, and `a.` has two sub-strings, `a` and an empty one.
        - For example, the empty subject matches `*` and the empty subject, and `a.` matches `a.*` but not `a`.
    - Only a whole sub-string "*" in the target subject is a wild card: `a*` matches only `a*` itself.
    - A "*" in the given subject is an ordinary character: the given subject `a.*` matches the target subjects `a.*` and `*.*`, but not `a.b`.
    - Matching is case-sensitive.
